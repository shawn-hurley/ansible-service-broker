package broker

import (
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"

	"github.com/coreos/etcd/client"
	docker "github.com/fsouza/go-dockerclient"
	logging "github.com/op/go-logging"
	"github.com/openshift/ansible-service-broker/pkg/apb"
	"github.com/openshift/ansible-service-broker/pkg/dao"
	"github.com/pborman/uuid"
	sdk "github.com/shawn-hurley/service-broker-generic/servicebroker/broker"
)

var (
	// ErrorAlreadyProvisioned - Error for when an service instance has already been provisioned
	ErrorAlreadyProvisioned = errors.New("already provisioned")
	// ErrorDuplicate - Error for when a duplicate service instance already exists
	ErrorDuplicate = errors.New("duplicate instance")
	// ErrorNotFound  - Error for when a service instance is not found. (either etcd or kubernetes)
	ErrorNotFound = errors.New("not found")
)

type DevBroker interface {
	AddSpec(spec apb.Spec) (*sdk.CatalogResponse, error)
}

// AnsibleBroker - Broker using ansible and images to interact with oc/kubernetes/etcd
type AnsibleBroker struct {
	dao           *dao.Dao
	log           *logging.Logger
	clusterConfig apb.ClusterConfig
	registry      apb.Registry
	engine        *WorkEngine
}

// NewAnsibleBroker - creates a new ansible broker
func NewAnsibleBroker(
	dao *dao.Dao,
	log *logging.Logger,
	clusterConfig apb.ClusterConfig,
	registry apb.Registry,
	engine WorkEngine,
) (*AnsibleBroker, error) {

	broker := &AnsibleBroker{
		dao:           dao,
		log:           log,
		clusterConfig: clusterConfig,
		registry:      registry,
		engine:        &engine,
	}

	// If no openshift target is provided, assume we are running in an openshift
	// cluster and try to log in using mounted cert and token
	if clusterConfig.InCluster {
		err := broker.Login()
		if err != nil {
			return broker, err
		}
	}

	return broker, nil
}

func (a AnsibleBroker) Login() error {
	a.log.Debug("Retrieving serviceaccount token")
	token, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		a.log.Debug("Error reading serviceaccount token")
		return err
	}

	return apb.OcLogin(a.log, "https://kubernetes.default",
		"--certificate-authority", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		"--token", string(token),
	)
}

// Bootstrap - Loads all known specs from a registry into local storage for reference
// Potentially a large download; on the order of 10s of thousands
// TODO: Response here? Async?
// TODO: How do we handle a large amount of data on this side as well? Pagination?
func (a AnsibleBroker) Bootstrap() (*sdk.BootstrapResponse, error) {
	a.log.Info("AnsibleBroker::Bootstrap")
	var err error
	var specs []*apb.Spec
	var imageCount int

	if specs, imageCount, err = a.registry.LoadSpecs(); err != nil {
		return nil, err
	}

	if err := a.dao.BatchSetSpecs(apb.NewSpecManifest(specs)); err != nil {
		return nil, err
	}

	return &sdk.BootstrapResponse{SpecCount: len(specs), ImageCount: imageCount}, nil
}

// Catalog - returns the catalog of services defined
func (a AnsibleBroker) Catalog() (*sdk.CatalogResponse, error) {
	a.log.Info("AnsibleBroker::Catalog")

	dir := "/spec"

	specs, err := a.dao.BatchGetSpecs(dir)
	if err != nil {
		a.log.Error("Something went real bad trying to retrieve batch specs...")
		return nil, err
	}

	services := make([]sdk.Service, len(specs))
	for i, spec := range specs {
		services[i] = SpecToService(spec)
	}

	return &sdk.CatalogResponse{services}, nil
}

// Provision  - will provision a service
func (a AnsibleBroker) Provision(instanceUUID uuid.UUID, req *sdk.ProvisionRequest, async bool) (*sdk.ProvisionResponse, error) {
	/////////////////////////////////////////////&///////////////
	//type ProvisionRequest struct {

	//-> OrganizationID    uuid.UUID
	//-> SpaceID           uuid.UUID
	// Used for determining where this service should be provisioned. Analagous to
	// OCP's namespaces and projects. Re: OrganizationID, spec mentions
	// "Most brokers will not use this field, it could be helpful in determining
	// the data placement or applying custom business rules"

	//-> PlanID            uuid.UUID
	// Unclear how this is relevant

	//-> ServiceID         uuid.UUID
	// ServiceID maps directly to a Spec.Id found in etcd. Can pull Spec via
	// Dao::GetSpec(id string)

	//-> Parameters        map[string]string
	// User provided configuration answers for the AnsibleApp

	// -> AcceptsIncomplete bool
	// true indicates both the SC and the requesting client (sc client). If param
	// is not included in the req, and the broker can only provision an instance of
	// the request plan asyncronously, broker should reject with a 422
	// NOTE: Spec.Async should indicate what level of async support is available for
	// a given ansible app

	//}

	// Summary:
	// For our purposes right now, the ServiceID and the Params should be enough to
	// Provision an ansible app.
	////////////////////////////////////////////////////////////
	// Provision Flow
	// -> Retrieve Spec from etcd (if missing, 400, this returns err missing)
	// -> TODO: Check to see if the spec supports or requires async, and reconcile
	//    need a typed error condition so the REST server knows correct response
	//    depending on the scenario
	//    (async requested, unsupported, 422)
	//    (async not requested, required, ?)
	// -> Make entry in /instance, ID'd by instance. Value should be Instance type
	//    Purpose is to make sure everything neeed to deprovision is available
	//    in persistence.
	// -> Provision!
	////////////////////////////////////////////////////////////

	/*
		dao GET returns error strings like CODE: message (entity) [#]
		dao SetServiceInstance returns what error?
		dao.SetState returns what error?
		Provision returns what error?
		SetExtractedCredentials returns what error?

		broker
		* normal synchronous return ProvisionResponse
		* normal async return ProvisionResponse
		* if instance already exists with the same params, return ProvisionResponse, AND InstanceExists
		* if instance already exists DIFFERENT param, return nil AND InstanceExists

		handler returns the following
		* synchronous provision return 201 created
		* instance already exists with IDENTICAL parameters to existing instance, 200 OK
		* async provision 202 Accepted
		* instance already exists with DIFFERENT parameters, 409 Conflict {}
		* if only support async and no accepts_incomplete=true passed in, 422 Unprocessable entity

	*/
	var spec *apb.Spec
	var err error

	// Retrieve requested spec
	specID := req.ServiceID.String()
	if spec, err = a.dao.GetSpec(specID); err != nil {
		// etcd return not found i.e. code 100
		if client.IsKeyNotFound(err) {
			return nil, ErrorNotFound
		}
		// otherwise unknown error bubble it up
		return nil, err
	}

	parameters := apb.Parameters(req.Parameters)

	// Build and persist record of service instance
	serviceInstance := &apb.ServiceInstance{
		Id:         instanceUUID,
		Spec:       spec,
		Parameters: &parameters,
	}

	// Verify we're not reprovisioning the same instance
	// if err is nil, there is an instance. Let's compare it to the instance
	// we're being asked to provision.
	//
	// if err is not nil, we will just bubble that up

	if si, err := a.dao.GetServiceInstance(instanceUUID.String()); err == nil {
		//This will use the package to make sure that if the type is changed away from []byte it can still be evaluated.
		if uuid.Equal(si.Id, serviceInstance.Id) {
			if reflect.DeepEqual(si.Parameters, serviceInstance.Parameters) {
				a.log.Debug("already have this instance returning 200")
				return &sdk.ProvisionResponse{}, ErrorAlreadyProvisioned
			}
			a.log.Info("we have a duplicate instance with parameters that differ, returning 409 conflict")
			return nil, ErrorDuplicate
		}
	}

	//
	// Looks like this is a new provision, let's get started.
	//
	if err = a.dao.SetServiceInstance(instanceUUID.String(), serviceInstance); err != nil {
		return nil, err
	}

	var token string

	if async {
		a.log.Info("ASYNC provisioning in progress")
		// asyncronously provision and return the token for the lastoperation
		pjob := NewProvisionJob(instanceUUID, spec, &parameters, a.clusterConfig, a.log)

		token = a.engine.StartNewJob(pjob)

		// HACK: there might be a delay between the first time the state in etcd
		// is set and the job was already started. But I need the token.
		a.dao.SetState(instanceUUID.String(), apb.JobState{Token: token, State: apb.StateInProgress})
	} else {
		// TODO: do we want to do synchronous provisioning?
		a.log.Info("reverting to synchronous provisioning in progress")
		extCreds, err := apb.Provision(spec, &parameters, a.clusterConfig, a.log)
		if err != nil {
			a.log.Error("broker::Provision error occurred.")
			a.log.Error("%s", err.Error())
			return nil, err
		}

		if extCreds != nil {
			a.log.Debug("broker::Provision, got ExtractedCredentials!")
			err = a.dao.SetExtractedCredentials(instanceUUID.String(), extCreds)
			if err != nil {
				a.log.Error("Could not persist extracted credentials")
				a.log.Error("%s", err.Error())
				return nil, err
			}
		}
	}

	// TODO: What data needs to be sent back on a respone?
	// Not clear what dashboardURL means in an AnsibleApp context
	// operation should be the task id from the work_engine
	return &sdk.ProvisionResponse{Operation: token}, nil
}

// Deprovision - will deprovision a service.
func (a AnsibleBroker) Deprovision(instanceUUID uuid.UUID) (*sdk.DeprovisionResponse, error) {
	////////////////////////////////////////////////////////////
	// Deprovision flow
	// -> Lookup bindings by instance ID; 400 if any are active, related issue:
	//    https://github.com/openservicebrokerapi/servicebroker/issues/127
	// -> Atomic deprovision and removal of service entry in etcd?
	//    * broker::Deprovision
	//    Arguments for this? What data do apbs require to deprovision?
	//    Maybe just hand off a serialized ServiceInstance and let the apb
	//    decide what's important?
	//    * if noerror: delete serviceInstance entry with Dao
	////////////////////////////////////////////////////////////
	instanceID := instanceUUID.String()

	if err := a.validateDeprovision(instanceID); err != nil {
		return nil, err
	}
	instance, err := a.dao.GetServiceInstance(instanceID)
	/// Handle case where service is not found in etcd
	if client.IsKeyNotFound(err) {
		a.log.Debug("unable to find service instance - %#v", err)
		return nil, ErrorNotFound
	}
	// bubble up  error
	if err != nil {
		a.log.Error("Error from etcd - %#v", err)
		return nil, err
	}
	err = apb.Deprovision(instance, a.log)
	if err == docker.ErrNoSuchImage {
		a.log.Debug("unable to find service instance - %#v", err)
		return nil, ErrorNotFound
	}
	// bubble up error.
	if err != nil {
		a.log.Error("error from deprovision - %#v", err)
		return nil, err
	}

	a.dao.DeleteServiceInstance(instanceID)

	return &sdk.DeprovisionResponse{Operation: "successful"}, nil
}

func (a AnsibleBroker) validateDeprovision(id string) error {
	// TODO: Check if there are outstanding bindings; return typed errors indicating
	// *why* things can't be deprovisioned
	a.log.Debug(fmt.Sprintf("AnsibleBroker::validateDeprovision -> [ %s ]", id))
	return nil
}

// Bind - will create a binding between a service.
func (a AnsibleBroker) Bind(instanceUUID uuid.UUID, bindingUUID uuid.UUID, req *sdk.BindRequest) (*sdk.BindResponse, error) {
	// binding_id is the id of the binding.
	// the instanceUUID is the previously provisioned service id.
	//
	// See if the service instance still exists, if not send back a badrequest.

	instance, err := a.dao.GetServiceInstance(instanceUUID.String())
	if err != nil {
		if client.IsKeyNotFound(err) {
			a.log.Errorf("Could not find a service instance in dao - %v", err)
			return nil, ErrorNotFound
		}
		a.log.Error("Couldn't find a service instance: ", err)
		return nil, err
	}

	// GET SERVICE get provision parameters

	// build bind parameters args:
	// {
	//     provision_params: {} same as what was stored in etcd
	//	   bind_params: {}
	// }
	// asbcli passes in user: aone, which bind passes to apb
	params := make(apb.Parameters)
	if instance.Parameters != nil {
		params["provision_params"] = *instance.Parameters
	}
	params["bind_params"] = req.Parameters

	//
	// Create a BindingInstance with a reference to the serviceinstance.
	//

	bindingInstance := &apb.BindInstance{
		Id:         bindingUUID,
		ServiceId:  instanceUUID,
		Parameters: &params,
	}

	// Verify we're not rebinding the same instance. if err is nil, there is an
	// instance. Let's compare it to the instance we're being asked to bind.
	//
	// if err is not nil, we will just bubble that up
	//
	// if binding instance exists, and the parameters are the same return: 200.
	// if binding instance exists, and the parameters are different return: 409.
	//
	// return 201 when we're done.
	if bi, err := a.dao.GetBindInstance(bindingUUID.String()); err == nil {
		if uuid.Equal(bi.Id, bindingInstance.Id) {
			if reflect.DeepEqual(bi.Parameters, bindingInstance.Parameters) {
				a.log.Debug("already have this binding instance, returning 200")
				return &sdk.BindResponse{}, ErrorAlreadyProvisioned
			}

			// parameters are different
			a.log.Info("duplicate binding instance diff params, returning 409 conflict")
			return nil, ErrorDuplicate
		}
	}

	if err := a.dao.SetBindInstance(bindingUUID.String(), bindingInstance); err != nil {
		return nil, err
	}

	/*
		NOTE:

		type BindResponse struct {
		    Credentials     map[string]interface{} `json:"credentials,omitempty"`
		    SyslogDrainURL  string                 `json:"syslog_drain_url,omitempty"`
		    RouteServiceURL string                 `json:"route_service_url,omitempty"`
		    VolumeMounts    []interface{}          `json:"volume_mounts,omitempty"`
		}
	*/

	// NOTE: Design here is very WIP
	// Potentially have data from provision stashed away, and bind may also
	// produce new binding data. Take both sets and merge?
	provExtCreds, err := a.dao.GetExtractedCredentials(instanceUUID.String())
	if err != nil {
		a.log.Debug("provExtCreds a miss!")
		a.log.Debug("%s", err.Error())
	} else {
		a.log.Debug("Got provExtCreds hit!")
		a.log.Debug("%+v", provExtCreds)
	}

	bindExtCreds, err := apb.Bind(instance, &params, a.clusterConfig, a.log)
	if err != nil {
		return nil, err
	}

	// Can't bind to anything if we have nothing to return to the catalog
	if provExtCreds == nil && bindExtCreds == nil {
		a.log.Error("No extracted credentials found from provision or bind")
		a.log.Error("Instance ID: %s", instanceUUID.String())
		return nil, errors.New("No credentials available")
	}

	returnCreds := mergeCredentials(provExtCreds, bindExtCreds)
	// TODO: Insert merged credentials into etcd? Separate into bind/provision
	// so none are overwritten?

	return &sdk.BindResponse{Credentials: returnCreds}, nil
}

func mergeCredentials(
	provExtCreds *apb.ExtractedCredentials, bindExtCreds *apb.ExtractedCredentials,
) map[string]interface{} {
	// TODO: Implement, need to handle case where either are empty
	return provExtCreds.Credentials
}

// Unbind - unbind a services previous binding NOTE: not implemented.
func (a AnsibleBroker) Unbind(instanceUUID uuid.UUID, bindingUUID uuid.UUID) error {
	return notImplemented
}

// Update - update a service NOTE: not implemented
func (a AnsibleBroker) Update(instanceUUID uuid.UUID, req *sdk.UpdateRequest) (*sdk.UpdateResponse, error) {
	return nil, notImplemented
}

// LastOperation - gets the last operation and status
func (a AnsibleBroker) LastOperation(instanceUUID uuid.UUID, req *sdk.LastOperationRequest) (*sdk.LastOperationResponse, error) {
	/*
		look up the resource in etcd the operation should match what was returned by provision
		take the status and return that.

		process:

		if async, provision: it should create a Job that calls apb.Provision. And write the output to etcd.
	*/
	a.log.Debug(fmt.Sprintf("service_id: %s", req.ServiceID.String())) // optional
	a.log.Debug(fmt.Sprintf("plan_id: %s", req.PlanID.String()))       // optional
	a.log.Debug(fmt.Sprintf("operation:  %s", req.Operation))          // this is provided with the provision. task id from the work_engine

	// TODO:validate the format to avoid some sort of injection hack
	jobstate, err := a.dao.GetState(instanceUUID.String(), req.Operation)
	if err != nil {
		// not sure what we do with the error if we can't find the state
		a.log.Error(fmt.Sprintf("problem reading job state: [%s]. error: [%v]", instanceUUID, err.Error()))
	}

	state := StateToLastOperation(jobstate.State)
	return &sdk.LastOperationResponse{State: state, Description: ""}, err
}

//AddSpec - adding the spec to the catalog for local developement
func (a AnsibleBroker) AddSpec(spec apb.Spec) (*sdk.CatalogResponse, error) {
	if err := a.dao.SetSpec(spec.Id, &spec); err != nil {
		return nil, err
	}
	service := SpecToService(&spec)
	return &sdk.CatalogResponse{Services: []sdk.Service{service}}, nil
}
