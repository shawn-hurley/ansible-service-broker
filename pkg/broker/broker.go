package broker

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/coreos/etcd/client"
	"github.com/openshift/ansible-service-broker/pkg/apb"
	"github.com/openshift/ansible-service-broker/pkg/auth"
	"github.com/openshift/ansible-service-broker/pkg/dao"
	"github.com/openshift/ansible-service-broker/pkg/registries"
	"github.com/openshift/ansible-service-broker/pkg/runtime"
	"github.com/openshift/ansible-service-broker/pkg/util"
	"github.com/pborman/uuid"
	k8srestclient "k8s.io/client-go/rest"
)

var (
	// ErrorAlreadyProvisioned - Error for when an service instance has already been provisioned
	ErrorAlreadyProvisioned = errors.New("already provisioned")
	// ErrorDuplicate - Error for when a duplicate service instance already exists
	ErrorDuplicate = errors.New("duplicate instance")
	// ErrorNotFound  - Error for when a service instance is not found. (either etcd or kubernetes)
	ErrorNotFound = errors.New("not found")
	// ErrorBindingExists - Error for when deprovision is called on a service instance with active bindings
	ErrorBindingExists = errors.New("binding exists")
)

const (
	// provisionCredentialsKey - Key used to pass credentials to apb.
	provisionCredentialsKey = "_apb_provision_creds"
	// bindCredentialsKey - Key used to pas bind credentials to apb.
	bindCredentialsKey = "_apb_bind_creds"
)

var log = util.NewLog("broker")

// Broker - A broker is used to to compelete all the tasks that a broker must be able to do.
type Broker interface {
	Bootstrap() (*BootstrapResponse, error)
	Catalog() (*CatalogResponse, error)
	Provision(uuid.UUID, *ProvisionRequest, bool) (*ProvisionResponse, error)
	Update(uuid.UUID, *UpdateRequest) (*UpdateResponse, error)
	Deprovision(uuid.UUID, string, bool) (*DeprovisionResponse, error)
	Bind(uuid.UUID, uuid.UUID, *BindRequest) (*BindResponse, error)
	Unbind(uuid.UUID, uuid.UUID, string) (*UnbindResponse, error)
	LastOperation(uuid.UUID, *LastOperationRequest) (*LastOperationResponse, error)
	// TODO: consider returning a struct + error
	Recover() (string, error)
}

// Config - Configuration for the broker.
type Config struct {
	DevBroker          bool          `yaml:"dev_broker"`
	LaunchApbOnBind    bool          `yaml:"launch_apb_on_bind"`
	BootstrapOnStartup bool          `yaml:"bootstrap_on_startup"`
	Recovery           bool          `yaml:"recovery"`
	OutputRequest      bool          `yaml:"output_request"`
	SSLCertKey         string        `yaml:"ssl_cert_key"`
	SSLCert            string        `yaml:"ssl_cert"`
	RefreshInterval    string        `yaml:"refresh_interval"`
	Auth               []auth.Config `yaml:"auth"`
}

// DevBroker - Interface for the development broker.
type DevBroker interface {
	AddSpec(spec apb.Spec) (*CatalogResponse, error)
	RemoveSpec(specID string) error
	RemoveSpecs() error
}

// AnsibleBroker - Broker using ansible and images to interact with oc/kubernetes/etcd
type AnsibleBroker struct {
	dao           *dao.Dao
	clusterConfig apb.ClusterConfig
	registry      []registries.Registry
	engine        *WorkEngine
	brokerConfig  Config
}

// NewAnsibleBroker - Creates a new ansible broker
func NewAnsibleBroker(dao *dao.Dao, clusterConfig apb.ClusterConfig,
	registry []registries.Registry, engine WorkEngine, brokerConfig Config,
) (*AnsibleBroker, error) {
	broker := &AnsibleBroker{
		dao:           dao,
		clusterConfig: clusterConfig,
		registry:      registry,
		engine:        &engine,
		brokerConfig:  brokerConfig,
	}

	err := broker.Login()
	if err != nil {
		return broker, err
	}

	return broker, nil
}

func (a AnsibleBroker) getServiceInstance(instanceUUID uuid.UUID) (*apb.ServiceInstance, error) {
	instance, err := a.dao.GetServiceInstance(instanceUUID.String())
	if err != nil {
		if client.IsKeyNotFound(err) {
			log.Errorf("Could not find a service instance in dao - %v", err)
			return nil, ErrorNotFound
		}
		log.Error("Couldn't find a service instance: ", err)
		return nil, err
	}
	return instance, nil

}

//Login - Will login the openshift user.
func (a AnsibleBroker) Login() error {
	config, err := a.getLoginDetails()
	if err != nil {
		return err
	}

	if config.CAFile != "" {
		err = ocLogin(config.Host,
			"--token", config.BearerToken,
			"--certificate-authority", config.CAFile,
		)
	} else {
		err = ocLogin(config.Host,
			"--token", config.BearerToken,
			"--insecure-skip-tls-verify=false",
		)
	}

	return err
}

type loginDetails struct {
	Host        string
	CAFile      string
	BearerToken string
}

func (a AnsibleBroker) getLoginDetails() (loginDetails, error) {
	config := loginDetails{}

	// If overrides are passed into the config map, Host and BearerTokenFile
	// values *must* be provided, else we'll default to the k8srestclient details
	if a.clusterConfig.Host != "" && a.clusterConfig.BearerTokenFile != "" {
		log.Info("ClusterConfig Host and BearerToken provided, preferring configurable overrides")
		log.Info("Host: [ %s ]", a.clusterConfig.Host)
		log.Info("BearerTokenFile: [ %s ]", a.clusterConfig.BearerTokenFile)

		token, err := ioutil.ReadFile(a.clusterConfig.BearerTokenFile)
		if err != nil {
			return config, err
		}

		config.Host = a.clusterConfig.Host
		config.BearerToken = string(token)
		config.CAFile = a.clusterConfig.CAFile
	} else {
		log.Info("No cluster credential overrides provided, using k8s InClusterConfig")
		k8sConfig, err := k8srestclient.InClusterConfig()
		if err != nil {
			log.Error("Cluster host & bearer_token_file missing from config, and failed to retrieve InClusterConfig")
			log.Error("Be sure you have configured a cluster host and service account credentials if" +
				" you are running the broker outside of a cluster Pod")
			return config, err
		}

		config.Host = k8sConfig.Host
		config.CAFile = k8sConfig.CAFile
		config.BearerToken = k8sConfig.BearerToken
	}

	return config, nil
}

// Bootstrap - Loads all known specs from a registry into local storage for reference
// Potentially a large download; on the order of 10s of thousands
// TODO: Response here? Async?
// TODO: How do we handle a large amount of data on this side as well? Pagination?
func (a AnsibleBroker) Bootstrap() (*BootstrapResponse, error) {
	log.Info("AnsibleBroker::Bootstrap")
	var err error
	var specs []*apb.Spec
	var imageCount int

	//Remove all specs that have been saved.
	dir := "/spec"
	specs, err = a.dao.BatchGetSpecs(dir)
	if err != nil {
		log.Error("Something went real bad trying to retrieve batch specs for deletion... - %v", err)
		return nil, err
	}
	err = a.dao.BatchDeleteSpecs(specs)
	if err != nil {
		log.Error("Something went real bad trying to delete batch specs... - %v", err)
		return nil, err
	}
	specs = []*apb.Spec{}

	//Load Specs for each registry
	registryErrors := []error{}
	for _, r := range a.registry {
		s, count, err := r.LoadSpecs()
		if err != nil && r.Fail(err) {
			log.Errorf("registry caused bootstrap failure - %v", err)
			return nil, err
		}
		if err != nil {
			log.Warningf("registry: %v was unable to complete bootstrap - %v",
				r.RegistryName, err)
			registryErrors = append(registryErrors, err)
		}
		imageCount += count
		addNameAndIDForSpec(s, r.RegistryName())
		specs = append(specs, s...)
	}
	if len(registryErrors) == len(a.registry) {
		return nil, errors.New("all registries failed on bootstrap")
	}
	specManifest := map[string]*apb.Spec{}
	for _, s := range specs {
		specManifest[s.ID] = s
	}
	if err := a.dao.BatchSetSpecs(specManifest); err != nil {
		return nil, err
	}

	return &BootstrapResponse{SpecCount: len(specs), ImageCount: imageCount}, nil
}

// addNameAndIDForSpec - will create the unique spec name and id
// and set it for each spec
func addNameAndIDForSpec(specs []*apb.Spec, registryName string) {
	for _, spec := range specs {
		//need to make / a hyphen to allow for global uniqueness but still match spec.

		imageName := strings.Replace(spec.Image, ":", "-", -1)
		spec.FQName = strings.Replace(fmt.Sprintf("%v-%v", registryName, imageName),
			"/", "-", -1)
		spec.FQName = fmt.Sprintf("%.51v", spec.FQName)

		// ID Will be a md5 hash of the fully qualified spec name.
		hasher := md5.New()
		hasher.Write([]byte(spec.FQName))
		spec.ID = hex.EncodeToString(hasher.Sum(nil))
	}
}

// Recover - Will recover the broker.
func (a AnsibleBroker) Recover() (string, error) {
	// At startup we should write a key to etcd.
	// Then in recovery see if that key exists, which means we are restarting
	// and need to try to recover.

	// do we have any jobs that wre still running?
	// get all /state/*/jobs/* == in progress
	// For each job, check the status of each of their containers to update
	// their status in case any of them finished.

	recoverStatuses, err := a.dao.FindJobStateByState(apb.StateInProgress)
	if err != nil {
		// no jobs or states to recover, this is OK.
		if client.IsKeyNotFound(err) {
			log.Info("No jobs to recover")
			return "", nil
		}
		return "", err
	}

	/*
		if job was in progress we know instanceuuid & token. do we have a podname?
		if no, job never started
			restart
		if yes,
			did it finish?
				yes
					* update status
					* extractCreds if available
				no
					* create a monitoring job to update status
	*/

	// let's see if we need to recover any of these
	for _, rs := range recoverStatuses {

		// We have an in progress job
		instanceID := rs.InstanceID.String()
		instance, err := a.dao.GetServiceInstance(instanceID)
		if err != nil {
			return "", err
		}

		// Do we have a podname?
		if rs.State.Podname == "" {
			// NO, we do not have a podname

			log.Info(fmt.Sprintf("No podname. Attempting to restart job: %s", instanceID))

			log.Debug(fmt.Sprintf("%v", instance))

			// Handle bad write of service instance
			if instance.Spec == nil || instance.Parameters == nil {
				a.dao.SetState(instanceID, apb.JobState{Token: rs.State.Token, State: apb.StateFailed})
				a.dao.DeleteServiceInstance(instance.ID.String())
				log.Warning(fmt.Sprintf("incomplete ServiceInstance [%s] record, marking job as failed", instance.ID))
				// skip to the next item
				continue
			}

			pjob := NewProvisionJob(instance, a.clusterConfig)

			// Need to use the same token as before, since that's what the
			// catalog will try to ping.
			_, err := a.engine.StartNewJob(rs.State.Token, pjob, ProvisionTopic)
			if err != nil {
				return "", err
			}

			// HACK: there might be a delay between the first time the state in etcd
			// is set and the job was already started. But I need the token.
			a.dao.SetState(instanceID, apb.JobState{Token: rs.State.Token, State: apb.StateInProgress})
		} else {
			// YES, we have a podname
			log.Info(fmt.Sprintf("We have a pod to recover: %s", rs.State.Podname))

			// TODO: ExtractCredentials is doing more than it should
			// be and it needs to be broken up.

			// did the pod finish?
			extCreds, extErr := apb.ExtractCredentials(rs.State.Podname, instance.Context.Namespace)

			// NO, pod failed.
			// TODO: do we restart the job or mark it as failed?
			if extErr != nil {
				log.Error("broker::Recover error occurred.")
				log.Error("%s", extErr.Error())
				return "", extErr
			}

			// YES, pod finished we have creds
			if extCreds != nil {
				log.Debug("broker::Recover, got ExtractedCredentials!")
				a.dao.SetState(instanceID, apb.JobState{Token: rs.State.Token,
					State: apb.StateSucceeded, Podname: rs.State.Podname})
				err = a.dao.SetExtractedCredentials(instanceID, extCreds)
				if err != nil {
					log.Error("Could not persist extracted credentials")
					log.Error("%s", err.Error())
					return "", err
				}
			}
		}
	}

	// if no pods, do we restart? or just return failed?

	//binding

	log.Info("Recovery complete")
	return "recover called", nil
}

// Catalog - returns the catalog of services defined
func (a AnsibleBroker) Catalog() (*CatalogResponse, error) {
	log.Info("AnsibleBroker::Catalog")

	var specs []*apb.Spec
	var err error
	var services []Service
	dir := "/spec"

	if specs, err = a.dao.BatchGetSpecs(dir); err != nil {
		log.Error("Something went real bad trying to retrieve batch specs...")
		return nil, err
	}

	services = make([]Service, len(specs))
	for i, spec := range specs {
		services[i] = SpecToService(spec)
	}

	return &CatalogResponse{services}, nil
}

// Provision  - will provision a service
func (a AnsibleBroker) Provision(instanceUUID uuid.UUID, req *ProvisionRequest, async bool,
) (*ProvisionResponse, error) {
	////////////////////////////////////////////////////////////
	//type ProvisionRequest struct {

	//-> OrganizationID    uuid.UUID
	//-> SpaceID           uuid.UUID
	// Used for determining where this service should be provisioned. Analogous to
	// OCP's namespaces and projects. Re: OrganizationID, spec mentions
	// "Most brokers will not use this field, it could be helpful in determining
	// the data placement or applying custom business rules"

	//-> PlanID            uuid.UUID
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
	specID := req.ServiceID
	if spec, err = a.dao.GetSpec(specID); err != nil {
		// etcd return not found i.e. code 100
		if client.IsKeyNotFound(err) {
			return nil, ErrorNotFound
		}
		// otherwise unknown error bubble it up
		return nil, err
	}

	context := &req.Context
	parameters := req.Parameters
	if parameters == nil {
		parameters = make(apb.Parameters)
	}

	if req.PlanID == "" {
		errMsg :=
			"PlanID from provision request is blank. " +
				"Provision requests must specify PlanIDs"
		return nil, errors.New(errMsg)
	}

	log.Debugf(
		"Injecting PlanID as parameter: { %s: %s }",
		planParameterKey, req.PlanID)
	parameters[planParameterKey] = req.PlanID

	// Build and persist record of service instance
	serviceInstance := &apb.ServiceInstance{
		ID:         instanceUUID,
		Spec:       spec,
		Context:    context,
		Parameters: &parameters,
	}

	// Verify we're not reprovisioning the same instance
	// if err is nil, there is an instance. Let's compare it to the instance
	// we're being asked to provision.
	//
	// if err is not nil, we will just bubble that up

	if si, err := a.dao.GetServiceInstance(instanceUUID.String()); err == nil {
		//This will use the package to make sure that if the type is changed away from []byte it can still be evaluated.
		if uuid.Equal(si.ID, serviceInstance.ID) {
			if reflect.DeepEqual(si.Parameters, serviceInstance.Parameters) {
				log.Debug("already have this instance returning 200")
				return &ProvisionResponse{}, ErrorAlreadyProvisioned
			}
			log.Info("we have a duplicate instance with parameters that differ, returning 409 conflict")
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
		log.Info("ASYNC provisioning in progress")
		// asyncronously provision and return the token for the lastoperation
		pjob := NewProvisionJob(serviceInstance, a.clusterConfig)

		token, err = a.engine.StartNewJob("", pjob, ProvisionTopic)
		if err != nil {
			log.Error("Failed to start new job for async provision\n%s", err.Error())
			return nil, err
		}

		// HACK: there might be a delay between the first time the state in etcd
		// is set and the job was already started. But I need the token.
		a.dao.SetState(instanceUUID.String(), apb.JobState{Token: token, State: apb.StateInProgress})
	} else {
		// TODO: do we want to do synchronous provisioning?
		log.Info("reverting to synchronous provisioning in progress")
		podName, extCreds, err := apb.Provision(serviceInstance, a.clusterConfig)

		sm := apb.NewServiceAccountManager()
		log.Info("Destroying APB sandbox...")
		sm.DestroyApbSandbox(podName, context.Namespace)
		if err != nil {
			log.Error("broker::Provision error occurred.")
			log.Error("%s", err.Error())
			return nil, err
		}

		if extCreds != nil {
			log.Debug("broker::Provision, got ExtractedCredentials!")
			err = a.dao.SetExtractedCredentials(instanceUUID.String(), extCreds)
			if err != nil {
				log.Error("Could not persist extracted credentials")
				log.Error("%s", err.Error())
				return nil, err
			}
		}
	}

	// TODO: What data needs to be sent back on a response?
	// Not clear what dashboardURL means in an AnsibleApp context
	// operation should be the task id from the work_engine
	return &ProvisionResponse{Operation: token}, nil
}

// Deprovision - will deprovision a service.
func (a AnsibleBroker) Deprovision(
	instanceUUID uuid.UUID, planID string, async bool,
) (*DeprovisionResponse, error) {
	////////////////////////////////////////////////////////////
	// Deprovision flow
	// -> Lookup bindings by instance ID; 400 if any are active, related issue:
	//    https://github.com/openservicebrokerapi/servicebroker/issues/127
	// -> Atomic deprovision and removal of service entry in etcd?
	//    * broker::Deprovision
	//    Arguments for this? What data do apbs require to deprovision?
	//    * namespace
	//    Maybe just hand off a serialized ServiceInstance and let the apb
	//    decide what's important?
	//    * delete credentials from etcd
	//    * if noerror: delete serviceInstance entry with Dao
	instance, err := a.getServiceInstance(instanceUUID)
	if err != nil {
		return nil, err
	}

	if planID == "" {
		errMsg := "Deprovision request contains an empty plan_id"
		return nil, errors.New(errMsg)
	}

	if err := a.validateDeprovision(instance); err != nil {
		return nil, err
	}

	var token string

	if async {
		log.Info("ASYNC deprovision in progress")
		// asynchronously provision and return the token for the lastoperation
		dpjob := NewDeprovisionJob(instance, a.clusterConfig, a.dao)

		token, err = a.engine.StartNewJob("", dpjob, DeprovisionTopic)
		if err != nil {
			log.Error("Failed to start new job for async deprovision\n%s", err.Error())
			return nil, err
		}

		// HACK: there might be a delay between the first time the state in etcd
		// is set and the job was already started. But I need the token.
		a.dao.SetState(instanceUUID.String(), apb.JobState{Token: token, State: apb.StateInProgress})
		return &DeprovisionResponse{Operation: token}, nil
	}

	// TODO: do we want to do synchronous deprovisioning?
	log.Info("Synchronous deprovision in progress")
	podName, err := apb.Deprovision(instance, a.clusterConfig)
	if err != nil {
		return nil, err
	}

	err = cleanupDeprovision(podName, instance, a.dao)
	if err != nil {
		return nil, err
	}
	return &DeprovisionResponse{}, nil
}

func (a AnsibleBroker) validateDeprovision(instance *apb.ServiceInstance) error {
	// -> Lookup bindings by instance ID; 400 if any are active, related issue:
	//    https://github.com/openservicebrokerapi/servicebroker/issues/127
	if len(instance.BindingIDs) > 0 {
		log.Debugf("Found bindings with ids: %v", instance.BindingIDs)
		return ErrorBindingExists
	}
	// TODO WHAT TO DO IF ASYNC BIND/PROVISION IN PROGRESS
	return nil
}

// Bind - will create a binding between a service.
func (a AnsibleBroker) Bind(instanceUUID uuid.UUID, bindingUUID uuid.UUID, req *BindRequest,
) (*BindResponse, error) {
	// binding_id is the id of the binding.
	// the instanceUUID is the previously provisioned service id.
	//
	// See if the service instance still exists, if not send back a badrequest.

	instance, err := a.getServiceInstance(instanceUUID)
	if err != nil {
		return nil, err
	}

	// GET SERVICE get provision parameters
	params := make(apb.Parameters)
	if instance.Parameters != nil {
		params["provision_params"] = *instance.Parameters
	}
	params["bind_params"] = req.Parameters
	// Inject PlanID into parameters passed to APBs
	if req.PlanID == "" {
		errMsg :=
			"PlanID from bind request is blank. " +
				"Bind requests must specify PlanIDs"
		return nil, errors.New(errMsg)
	}

	log.Debugf(
		"Injecting PlanID as parameter: { %s: %s }",
		planParameterKey, req.PlanID)
	params[planParameterKey] = req.PlanID

	// Create a BindingInstance with a reference to the serviceinstance.
	bindingInstance := &apb.BindInstance{
		ID:         bindingUUID,
		ServiceID:  instanceUUID,
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
		if uuid.Equal(bi.ID, bindingInstance.ID) {
			if reflect.DeepEqual(bi.Parameters, bindingInstance.Parameters) {
				log.Debug("already have this binding instance, returning 200")
				return &BindResponse{}, ErrorAlreadyProvisioned
			}

			// parameters are different
			log.Info("duplicate binding instance diff params, returning 409 conflict")
			return nil, ErrorDuplicate
		}
	}

	if err := a.dao.SetBindInstance(bindingUUID.String(), bindingInstance); err != nil {
		return nil, err
	}

	provExtCreds, err := a.dao.GetExtractedCredentials(instanceUUID.String())
	if err != nil && !client.IsKeyNotFound(err) {
		log.Warningf("unable to retrieve provision time credentials - %v", err)
	}

	// Add the DB Credentials this will allow the apb to use these credentials if it so chooses.
	if provExtCreds != nil {
		params[provisionCredentialsKey] = provExtCreds.Credentials
	}

	// NOTE: We are currently disabling running an APB on bind via 'LaunchApbOnBind'
	// of the broker config, due to lack of async support of bind in Open Service Broker API
	// Currently, the 'launchapbonbind' is set to false in the 'config' ConfigMap
	var podName string
	var bindExtCreds *apb.ExtractedCredentials
	if a.brokerConfig.LaunchApbOnBind {
		log.Info("Broker configured to run APB bind")
		podName, bindExtCreds, err = apb.Bind(instance, &params, a.clusterConfig)

		sm := apb.NewServiceAccountManager()
		log.Info("Destroying APB sandbox...")
		sm.DestroyApbSandbox(podName, instance.Context.Namespace)

		if err != nil {
			return nil, err
		}
	} else {
		log.Warning("Broker configured to *NOT* launch and run APB bind")
	}
	instance.AddBinding(bindingUUID)
	if err := a.dao.SetServiceInstance(instanceUUID.String(), instance); err != nil {
		return nil, err
	}
	// Can't bind to anything if we have nothing to return to the catalog
	if provExtCreds == nil && bindExtCreds == nil {
		log.Errorf("No extracted credentials found from provision or bind instance ID: %s",
			instanceUUID.String())
		return nil, errors.New("No credentials available")
	}

	if bindExtCreds != nil {
		err = a.dao.SetExtractedCredentials(bindingUUID.String(), bindExtCreds)
		if err != nil {
			log.Errorf("Could not persist extracted credentials - %v", err)
			return nil, err
		}
		return &BindResponse{Credentials: bindExtCreds.Credentials}, nil
	}
	return &BindResponse{Credentials: provExtCreds.Credentials}, nil
}

// Unbind - unbind a services previous binding
func (a AnsibleBroker) Unbind(
	instanceUUID uuid.UUID, bindingUUID uuid.UUID, planID string,
) (*UnbindResponse, error) {
	if planID == "" {
		errMsg :=
			"PlanID from unbind request is blank. " +
				"Unbind requests must specify PlanIDs"
		return nil, errors.New(errMsg)
	}

	params := make(apb.Parameters)
	provExtCreds, err := a.dao.GetExtractedCredentials(instanceUUID.String())
	if err != nil && !client.IsKeyNotFound(err) {
		return nil, err
	}
	bindExtCreds, err := a.dao.GetExtractedCredentials(bindingUUID.String())
	if err != nil && !client.IsKeyNotFound(err) {
		return nil, err
	}
	// Add the credentials to the parameters so that an APB can choose what
	// it would like to do.
	if provExtCreds == nil && bindExtCreds == nil {
		log.Warningf("Unable to find credentials for instance id: %v and binding id: %v"+
			" something may have gone wrong. Proceeding with unbind.",
			instanceUUID, bindingUUID)
	}
	if provExtCreds != nil {
		params[provisionCredentialsKey] = provExtCreds.Credentials
	}
	if bindExtCreds != nil {
		params[bindCredentialsKey] = bindExtCreds.Credentials
	}
	serviceInstance, err := a.getServiceInstance(instanceUUID)
	if err != nil {
		log.Debugf("Service instance with id %s does not exist", instanceUUID.String())
		return nil, err
	}
	if serviceInstance.Parameters != nil {
		params["provision_params"] = *serviceInstance.Parameters
	}
	// only launch apb if we are always launching the APB.
	if a.brokerConfig.LaunchApbOnBind {
		err = apb.Unbind(serviceInstance, &params, a.clusterConfig)
		if err != nil {
			return nil, err
		}
	} else {
		log.Warning("Broker configured to *NOT* launch and run APB unbind")
	}

	if bindExtCreds != nil {
		err = a.dao.DeleteExtractedCredentials(bindingUUID.String())
		if err != nil {
			return nil, err
		}
	}

	err = a.dao.DeleteBindInstance(bindingUUID.String())
	if err != nil {
		return nil, err
	}

	serviceInstance.RemoveBinding(bindingUUID)
	err = a.dao.SetServiceInstance(instanceUUID.String(), serviceInstance)
	if err != nil {
		return nil, err
	}

	return &UnbindResponse{}, nil
}

// Update - update a service NOTE: not implemented
func (a AnsibleBroker) Update(instanceUUID uuid.UUID, req *UpdateRequest,
) (*UpdateResponse, error) {
	return nil, notImplemented
}

// LastOperation - gets the last operation and status
func (a AnsibleBroker) LastOperation(instanceUUID uuid.UUID, req *LastOperationRequest,
) (*LastOperationResponse, error) {
	/*
		look up the resource in etcd the operation should match what was returned by provision
		take the status and return that.

		process:

		if async, provision: it should create a Job that calls apb.Provision. And write the output to etcd.
	*/
	log.Debug(fmt.Sprintf("service_id: %s", req.ServiceID)) // optional
	log.Debug(fmt.Sprintf("plan_id: %s", req.PlanID))       // optional
	log.Debug(fmt.Sprintf("operation:  %s", req.Operation)) // this is provided with the provision. task id from the work_engine

	// TODO:validate the format to avoid some sort of injection hack
	jobstate, err := a.dao.GetState(instanceUUID.String(), req.Operation)
	if err != nil {
		// not sure what we do with the error if we can't find the state
		log.Error(fmt.Sprintf("problem reading job state: [%s]. error: [%v]", instanceUUID, err.Error()))
	}

	state := StateToLastOperation(jobstate.State)
	return &LastOperationResponse{State: state, Description: ""}, err
}

//AddSpec - adding the spec to the catalog for local development
func (a AnsibleBroker) AddSpec(spec apb.Spec) (*CatalogResponse, error) {
	log.Debug("broker::AddSpec")
	addNameAndIDForSpec([]*apb.Spec{&spec}, apbPushRegName)
	log.Debugf("Generated name for pushed APB: [%s], ID: [%s]", spec.FQName, spec.ID)

	if err := a.dao.SetSpec(spec.ID, &spec); err != nil {
		return nil, err
	}
	service := SpecToService(&spec)
	return &CatalogResponse{Services: []Service{service}}, nil
}

// RemoveSpec - remove the spec specified from the catalog/etcd
func (a AnsibleBroker) RemoveSpec(specID string) error {
	spec, err := a.dao.GetSpec(specID)
	if client.IsKeyNotFound(err) {
		return ErrorNotFound
	}
	if err != nil {
		log.Error("Something went real bad trying to retrieve spec for deletion... - %v", err)
		return err
	}
	err = a.dao.DeleteSpec(spec.ID)
	if err != nil {
		log.Error("Something went real bad trying to delete spec... - %v", err)
		return err
	}
	return nil
}

// RemoveSpecs - remove all the specs from the catalog/etcd
func (a AnsibleBroker) RemoveSpecs() error {
	dir := "/spec"
	specs, err := a.dao.BatchGetSpecs(dir)
	if err != nil {
		log.Error("Something went real bad trying to retrieve batch specs for deletion... - %v", err)
		return err
	}
	err = a.dao.BatchDeleteSpecs(specs)
	if err != nil {
		log.Error("Something went real bad trying to delete batch specs... - %v", err)
		return err
	}
	return nil
}

func ocLogin(args ...string) error {
	log.Debug("Logging into openshift...")

	fullArgs := append([]string{"login"}, args...)

	output, err := runtime.RunCommand("oc", fullArgs...)
	log.Debug("Login output:")
	log.Debug(string(output))

	if err != nil {
		log.Debug(string(output))
		return err
	}
	return nil
}
