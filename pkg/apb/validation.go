package apb

import (
	"encoding/base64"
	"fmt"

	yaml "gopkg.in/yaml.v1"
)

// ValidateSpec - Will validate the spec
// If the spec is valid then bool is true and string is empty
// If the spec is invalid then bool is false and string gives the reason
func ValidateSpec(spec *Spec) (bool, string) {
	// Specs must have a defined name.
	if spec.FQName == "" {
		return false, "Specs must have a name"
	}

	// Specs must have a defined description.
	if spec.Description == "" {
		return false, "Specs must have a description"
	}

	// Specs must have at least one plan
	if !(len(spec.Plans) > 0) {
		return false, "Specs must have at least one plan"
	}
	dupes := make(map[string]bool)
	for _, plan := range spec.Plans {
		if _, contains := dupes[plan.Name]; contains {
			reason := fmt.Sprintf("%s: %s",
				"Plans within a spec must not contain duplicate value", plan.Name)

			return false, reason
		}
		dupes[plan.Name] = true
	}
	return true, ""
}

// ValidateSpecYaml - will validate the spec that is 64 encoded
// If the spec is valid then bool is true and string is empty
// If the spec is invalid then bool is false and string gives the reason
func ValidateSpecYaml(b64Spec string) (bool, string) {
	// Decode the string
	specStr, err := base64.StdEncoding.DecodeString(b64Spec)
	if err != nil {
		return false, "Unable to decode 64 encoded string."
	}
	var spec Spec
	if err := yaml.Unmarshal(specStr, &spec); err != nil {
		return false, fmt.Sprintf("Unable to create spec from yaml - %v", err)
	}
	return ValidateSpec(&spec)
}
