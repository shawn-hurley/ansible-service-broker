package broker

import (
	"encoding/json"

	"github.com/openshift/ansible-service-broker/pkg/apb"
	"github.com/openshift/ansible-service-broker/pkg/dao"
)

// DeprovisionJob - Job to deprovision.
type DeprovisionJob struct {
	serviceInstance *apb.ServiceInstance
	clusterConfig   apb.ClusterConfig
	dao             *dao.Dao
}

// DeprovisionMsg - Message returned for a deprovison job.
type DeprovisionMsg struct {
	InstanceUUID string `json:"instance_uuid"`
	PodName      string `json:"podname"`
	JobToken     string `json:"job_token"`
	SpecID       string `json:"spec_id"`
	Error        string `json:"error"`
}

// Render - render the message
func (m DeprovisionMsg) Render() string {
	render, _ := json.Marshal(m)
	return string(render)
}

// NewDeprovisionJob - Create a deprovision job.
func NewDeprovisionJob(serviceInstance *apb.ServiceInstance, clusterConfig apb.ClusterConfig,
	dao *dao.Dao,
) *DeprovisionJob {
	return &DeprovisionJob{
		serviceInstance: serviceInstance,
		clusterConfig:   clusterConfig,
		dao:             dao}
}

// Run - will run the deprovision job.
func (p *DeprovisionJob) Run(token string, msgBuffer chan<- WorkMsg) {
	podName, err := apb.Deprovision(p.serviceInstance, p.clusterConfig)
	if err != nil {
		log.Error("broker::Deprovision error occurred.")
		log.Errorf("%s", err.Error())
		msgBuffer <- DeprovisionMsg{InstanceUUID: p.serviceInstance.ID.String(), PodName: podName,
			JobToken: token, SpecID: p.serviceInstance.Spec.ID, Error: err.Error()}
		return
	}

	log.Debug("sending deprovision complete msg to channel")
	msgBuffer <- DeprovisionMsg{InstanceUUID: p.serviceInstance.ID.String(), PodName: podName,
		JobToken: token, SpecID: p.serviceInstance.Spec.ID, Error: ""}
}
