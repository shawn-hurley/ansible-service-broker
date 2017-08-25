package broker

import (
	"encoding/json"

	"github.com/openshift/ansible-service-broker/pkg/apb"
)

// ProvisionJob - Job to provision
type ProvisionJob struct {
	serviceInstance *apb.ServiceInstance
	clusterConfig   apb.ClusterConfig
}

// ProvisionMsg - Message to be returned from the provision job
type ProvisionMsg struct {
	InstanceUUID string `json:"instance_uuid"`
	JobToken     string `json:"job_token"`
	SpecID       string `json:"spec_id"`
	PodName      string `json:"podname"`
	Msg          string `json:"msg"`
	Error        string `json:"error"`
}

// Render - Display the provision message.
func (m ProvisionMsg) Render() string {
	render, _ := json.Marshal(m)
	return string(render)
}

// NewProvisionJob - Create a new provision job.
func NewProvisionJob(serviceInstance *apb.ServiceInstance, clusterConfig apb.ClusterConfig,
) *ProvisionJob {
	return &ProvisionJob{
		serviceInstance: serviceInstance,
		clusterConfig:   clusterConfig}
}

// Run - run the provision job.
func (p *ProvisionJob) Run(token string, msgBuffer chan<- WorkMsg) {
	podName, extCreds, err := apb.Provision(p.serviceInstance, p.clusterConfig)
	sm := apb.NewServiceAccountManager()

	if err != nil {
		log.Error("broker::Provision error occurred.")
		log.Errorf("%s", err.Error())

		log.Error("Attempting to destroy APB sandbox if it has been created")
		sm.DestroyApbSandbox(podName, p.serviceInstance.Context.Namespace)
		// send error message
		// can't have an error type in a struct you want marshalled
		// https://github.com/golang/go/issues/5161
		msgBuffer <- ProvisionMsg{InstanceUUID: p.serviceInstance.ID.String(),
			JobToken: token, SpecID: p.serviceInstance.Spec.ID, PodName: "", Msg: "", Error: err.Error()}
		return
	}

	log.Info("Destroying APB sandbox...")
	sm.DestroyApbSandbox(podName, p.serviceInstance.Context.Namespace)

	// send creds
	jsonmsg, err := json.Marshal(extCreds)
	if err != nil {
		msgBuffer <- ProvisionMsg{InstanceUUID: p.serviceInstance.ID.String(),
			JobToken: token, SpecID: p.serviceInstance.Spec.ID, PodName: "", Msg: "", Error: err.Error()}
		return
	}

	msgBuffer <- ProvisionMsg{InstanceUUID: p.serviceInstance.ID.String(),
		JobToken: token, SpecID: p.serviceInstance.Spec.ID, PodName: podName, Msg: string(jsonmsg), Error: ""}
}
