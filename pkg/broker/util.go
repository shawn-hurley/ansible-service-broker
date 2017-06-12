package broker

import (
	"fmt"
	"os"
	"path"
	"regexp"

	schema "github.com/lestrrat/go-jsschema"
	"github.com/openshift/ansible-service-broker/pkg/apb"
	sdk "github.com/openshift/ansible-service-broker/service-broker-generic/servicebroker/broker"
	"github.com/pborman/uuid"
)

func ProjectRoot() string {
	gopath := os.Getenv("GOPATH")
	rootPath := path.Join(gopath, "src", "github.com", "openshift",
		"ansible-service-broker")
	return rootPath
}

// SpecToService converts an apb Spec into a Service usable by the service
// catalog.
func SpecToService(spec *apb.Spec) sdk.Service {
	// default plan, used to be in hack.go
	plans := []sdk.Plan{
		{
			ID:          uuid.Parse("4c10ff42-be89-420a-9bab-27a9bef9aed8"),
			Name:        "default",
			Description: "Default plan",
			Free:        true,
			Schemas:     ParametersToSchema(spec.Parameters, spec.Required),
			// leaving Bindable undefined, defaults to Service value
		},
	}

	retSvc := sdk.Service{
		ID:          uuid.Parse(spec.Id),
		Name:        spec.Name,
		Description: spec.Description,
		Tags:        make([]string, len(spec.Tags)),
		Bindable:    spec.Bindable,
		Plans:       plans,
		// leaving Metadata empty
	}

	copy(retSvc.Tags, spec.Tags)
	return retSvc
}

// getType transforms an apb parameter type to a JSON Schema type
func getType(paramType string) schema.PrimitiveTypes {
	switch paramType {
	case "string", "enum":
		return []schema.PrimitiveType{schema.StringType}
	case "int":
		return []schema.PrimitiveType{schema.IntegerType}
	case "object":
		return []schema.PrimitiveType{schema.ObjectType}
	case "array":
		return []schema.PrimitiveType{schema.ArrayType}
	case "bool", "boolean":
		return []schema.PrimitiveType{schema.BooleanType}
	case "number":
		return []schema.PrimitiveType{schema.NumberType}
	case "nil", "null":
		return []schema.PrimitiveType{schema.NullType}
	}
	return []schema.PrimitiveType{schema.UnspecifiedType}
}

// ParametersToSchema converts the apb parameters into a JSON Schema format.
func ParametersToSchema(params []map[string]*apb.ParameterDescriptor, required []string) sdk.Schema {
	properties := make(map[string]*schema.Schema)

	var patternRegex *regexp.Regexp
	var err error

	for _, paramMap := range params {
		for k, pd := range paramMap {

			properties[k] = &schema.Schema{
				Title:       pd.Title,
				Description: pd.Description,
				Default:     pd.Default,
				Type:        getType(pd.Type),
			}

			// we can NOT set values on the Schema object if we want to be
			// omitempty. Setting maxlength to 0 is NOT the same as omitting it.
			// 0 is a worthless value for Maxlength so we will not set it
			if pd.Maxlength > 0 {
				properties[k].MaxLength = schema.Integer{Val: pd.Maxlength, Initialized: true}
			}

			// do not set the regexp if it does not compile
			if pd.Pattern != "" {
				patternRegex, err = regexp.Compile(pd.Pattern)
				properties[k].Pattern = patternRegex

				if err != nil {
					fmt.Println("Invalid pattern: %s", err.Error())
				}
			}

			// setup enums
			if len(pd.Enum) > 0 {
				properties[k].Enum = make([]interface{}, len(pd.Enum))
				for i, v := range pd.Enum {
					properties[k].Enum[i] = v
				}
			}
		}
	}

	// builds a Schema object for the various methods.
	s := sdk.Schema{
		ServiceInstance: sdk.ServiceInstance{
			Create: map[string]*schema.Schema{
				"parameters": {
					SchemaRef:  schema.SchemaURL,
					Type:       []schema.PrimitiveType{schema.ObjectType},
					Properties: properties,
					Required:   required,
				},
			},
			Update: map[string]*schema.Schema{},
		},
		ServiceBinding: sdk.ServiceBinding{
			Create: map[string]*schema.Schema{
				"parameters": {
					SchemaRef:  schema.SchemaURL,
					Type:       []schema.PrimitiveType{schema.ObjectType},
					Properties: properties,
				},
			},
		},
	}

	return s
}

// StateToLastOperation converts apb State objects into LastOperationStates.
func StateToLastOperation(state apb.State) sdk.LastOperationState {
	switch state {
	case apb.StateInProgress:
		return sdk.LastOperationStateInProgress
	case apb.StateSucceeded:
		return sdk.LastOperationStateSucceeded
	case apb.StateFailed:
		return sdk.LastOperationStateFailed
	default:
		return sdk.LastOperationStateFailed
	}
}
