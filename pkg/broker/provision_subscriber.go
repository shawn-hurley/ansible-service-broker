package broker

import (
	"encoding/json"

	"github.com/openshift/ansible-service-broker/pkg/apb"
	"github.com/openshift/ansible-service-broker/pkg/dao"
)

// ProvisionWorkSubscriber - Lissten for provision messages
type ProvisionWorkSubscriber struct {
	dao       *dao.Dao
	msgBuffer <-chan WorkMsg
}

// NewProvisionWorkSubscriber - Create a new work subscriber.
func NewProvisionWorkSubscriber(dao *dao.Dao) *ProvisionWorkSubscriber {
	return &ProvisionWorkSubscriber{dao: dao}
}

// Subscribe - will start the work subscriber listenning on the message buffer for provision messages.
func (p *ProvisionWorkSubscriber) Subscribe(msgBuffer <-chan WorkMsg) {
	p.msgBuffer = msgBuffer

	var pmsg *ProvisionMsg
	var extCreds *apb.ExtractedCredentials
	go func() {
		log.Info("Listening for provision messages")
		for {
			msg := <-msgBuffer

			log.Debug("Processed provision message from buffer")
			// HACK: this seems like a hack, there's probably a better way to
			// get the data sent through instead of a string
			json.Unmarshal([]byte(msg.Render()), &pmsg)

			if pmsg.Error != "" {
				log.Errorf("Provision job reporting error: %s", pmsg.Error)
				p.dao.SetState(pmsg.InstanceUUID, apb.JobState{Token: pmsg.JobToken,
					State: apb.StateFailed, Podname: pmsg.PodName})
			} else if pmsg.Msg == "" {
				// HACK: OMG this is horrible. We should probably pass in a
				// state. Since we'll also be using this to get more granular
				// updates one day.
				p.dao.SetState(pmsg.InstanceUUID, apb.JobState{Token: pmsg.JobToken,
					State: apb.StateInProgress, Podname: pmsg.PodName})
			} else {
				json.Unmarshal([]byte(pmsg.Msg), &extCreds)
				p.dao.SetState(pmsg.InstanceUUID, apb.JobState{Token: pmsg.JobToken,
					State: apb.StateSucceeded, Podname: pmsg.PodName})
				p.dao.SetExtractedCredentials(pmsg.InstanceUUID, extCreds)
			}
		}
	}()
}
