//
// Copyright (c) 2018 Red Hat, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package dao

import (
	"context"
	"fmt"
	"strings"

	"github.com/automationbroker/bundle-lib/bundle"
	"github.com/automationbroker/bundle-lib/clients"
	"github.com/coreos/etcd/client"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
)

// Dao - object to interface with the data store.
type Dao struct {
	client client.Client
	kapi   client.KeysAPI // Used to interact with kvp API over HTTP
}

// NewDao - Create a new Dao object
func NewDao() (*Dao, error) {
	dao := Dao{}

	etcdClient, err := clients.Etcd()
	if err != nil {
		return nil, err
	}
	dao.client = etcdClient
	dao.kapi = client.NewKeysAPI(dao.client)
	return &dao, nil
}

// SetRaw - Allows the setting of the value json string to the key in the kvp API.
func (d *Dao) SetRaw(key string, val string) error {
	_, err := d.kapi.Set(context.Background(), key, val /*opts*/, nil)
	return err
}

// GetRaw - gets a specific json string for a key from the kvp API.
func (d *Dao) GetRaw(key string) (string, error) {
	res, err := d.kapi.Get(context.Background(), key /*opts*/, nil)
	if err != nil {
		return "", err
	}

	val := res.Node.Value
	return val, nil
}

// BatchGetRaw - Get multiple  types as individual json strings
// TODO: Streaming interface? Going to need to optimize all this for
// a full-load catalog response of 10k
// This is more likely to be paged given current proposal
// In which case, we need paged Batch gets
// 2 steps?
// GET /spec/manifest [/*ordered ids*/]
// BatchGet(offset, count)?
func (d *Dao) BatchGetRaw(dir string) (*[]string, error) {

	var res *client.Response
	var err error

	opts := &client.GetOptions{Recursive: true}
	if res, err = d.kapi.Get(context.Background(), dir, opts); err != nil {
		return nil, err
	}

	specNodes := res.Node.Nodes
	specCount := len(specNodes)

	log.Debugf("Successfully loaded [ %d ] objects from etcd dir [ %s ]", specCount, dir)

	payloads := make([]string, specCount)
	for i, node := range specNodes {
		payloads[i] = node.Value
	}

	return &payloads, nil
}

// GetSpec - Retrieve the spec for the kvp API.
func (d *Dao) GetSpec(id string) (*bundle.Spec, error) {
	spec := &bundle.Spec{}
	if err := d.getObject(specKey(id), spec); err != nil {
		return nil, err
	}
	return spec, nil
}

// SetSpec - set spec for an id in the kvp API.
func (d *Dao) SetSpec(id string, spec *bundle.Spec) error {
	return d.setObject(specKey(id), spec)
}

// DeleteSpec - Delete the spec for a given spec id.
func (d *Dao) DeleteSpec(specID string) error {
	log.Debug(fmt.Sprintf("Dao::DeleteSpec-> [ %s ]", specID))
	_, err := d.kapi.Delete(context.Background(), specKey(specID), nil)
	return err
}

// BatchSetSpecs - set specs based on SpecManifest in the kvp API.
func (d *Dao) BatchSetSpecs(specs bundle.SpecManifest) error {
	for id, spec := range specs {
		err := d.SetSpec(id, spec)
		if err != nil {
			return err
		}
	}

	return nil
}

// BatchGetSpecs - Retrieve all the specs for dir.
func (d *Dao) BatchGetSpecs(dir string) ([]*bundle.Spec, error) {
	payloads, err := d.BatchGetRaw(dir)

	if client.IsKeyNotFound(err) {
		return []*bundle.Spec{}, nil
	} else if err != nil {
		return []*bundle.Spec{}, err
	}

	specs := make([]*bundle.Spec, len(*payloads))
	for i, payload := range *payloads {
		spec := &bundle.Spec{}
		bundle.LoadJSON(payload, spec)
		specs[i] = spec
		log.Debugf("Batch idx [ %d ] -> [ %s ]", i, spec.ID)
	}

	return specs, nil
}

// BatchDeleteSpecs - set specs based on SpecManifest in the kvp API.
func (d *Dao) BatchDeleteSpecs(specs []*bundle.Spec) error {
	for _, spec := range specs {
		err := d.DeleteSpec(spec.ID)
		if err != nil {
			return err
		}
	}
	return nil
}

// FindJobStateByState - Retrieve all the jobs that match the specified state
func (d *Dao) FindJobStateByState(state bundle.State) ([]bundle.RecoverStatus, error) {
	log.Debug("Dao::FindByState")

	var res *client.Response
	var err error

	opts := &client.GetOptions{Recursive: true}
	if res, err = d.kapi.Get(context.Background(), "/state/", opts); err != nil {
		return nil, err
	}

	stateNodes := res.Node.Nodes
	stateCount := len(stateNodes)

	log.Debugf("Successfully loaded [ %d ] jobstate objects from etcd dir [ /state/ ]", stateCount)

	var recoverstatus []bundle.RecoverStatus
	for _, node := range stateNodes {
		k := fmt.Sprintf("%s/job", node.Key)

		status := bundle.RecoverStatus{InstanceID: uuid.Parse(stateKeyID(node.Key))}
		jobstate := bundle.JobState{}
		nodes, e := d.kapi.Get(context.Background(), k, opts)
		if e != nil {
			// if key is invalid do we keep it?
			log.Warning(
				fmt.Sprintf("Error processing jobstate record, moving on to next. %v", e.Error()))
			continue
		}

		for _, n := range nodes.Node.Nodes {
			bundle.LoadJSON(n.Value, &jobstate)
			if jobstate.State == state {
				log.Debug(fmt.Sprintf(
					"Found! jobstate [%v] matched given state: [%v].", jobstate, state))
				status.State = jobstate
				recoverstatus = append(recoverstatus, status)
			} else {
				// we could probably remove this once we're happy with how this
				// works.
				log.Debug(fmt.Sprintf(
					"Skipping, jobstate [%v] did not match given state: [%v].", jobstate, state))
			}
		}
	}

	return recoverstatus, nil
}

// GetSvcInstJobsByState - Lookup all jobs of a given state for a specific instance
func (d *Dao) GetSvcInstJobsByState(
	instanceID string, reqState bundle.State,
) ([]bundle.JobState, error) {
	log.Debug("Dao::GetSvcInstJobsByState")
	allStates, err := d.getJobsForSvcInst(instanceID)

	if err != nil {
		return nil, err
	} else if len(allStates) == 0 {
		return allStates, nil
	}

	filtStates := []bundle.JobState{}
	for _, state := range allStates {
		if state.State == reqState {
			filtStates = append(filtStates, state)
		}
	}

	log.Debugf("Filtered on state: [ %v ], returning %d jobs", reqState, len(filtStates))

	return filtStates, nil
}

func (d *Dao) getJobsForSvcInst(instanceID string) ([]bundle.JobState, error) {
	log.Debug("Dao::getJobsForSvcInst")

	var res *client.Response
	var err error

	lookupKey := fmt.Sprintf("/state/%s/job", instanceID)
	opts := &client.GetOptions{Recursive: true}
	if res, err = d.kapi.Get(context.Background(), lookupKey, opts); err != nil {
		return nil, err
	}

	jobNodes := res.Node.Nodes
	jobsCount := len(jobNodes)
	if jobsCount == 0 {
		return []bundle.JobState{}, nil
	}

	log.Debugf("Successfully loaded [ %d ] jobs objects from [ %s ]",
		jobsCount, lookupKey)

	retJobs := []bundle.JobState{}
	for _, node := range jobNodes {
		js := bundle.JobState{}
		err := bundle.LoadJSON(node.Value, &js)
		if err != nil {
			return nil, fmt.Errorf("An error occurred trying to parse job state of [ %s ]\n%s", node.Key, err.Error())
		}
		retJobs = append(retJobs, js)
	}
	return retJobs, nil
}

// GetServiceInstance - Retrieve specific service instance from the kvp API.
func (d *Dao) GetServiceInstance(id string) (*bundle.ServiceInstance, error) {
	spec := &bundle.ServiceInstance{}
	if err := d.getObject(serviceInstanceKey(id), spec); err != nil {
		return nil, err
	}
	return spec, nil
}

// SetServiceInstance - Set service instance for an id in the kvp API.
func (d *Dao) SetServiceInstance(id string, serviceInstance *bundle.ServiceInstance) error {
	return d.setObject(serviceInstanceKey(id), serviceInstance)
}

func removeFalseBindings(bindings map[string]bool) map[string]bool {
	newBindings := map[string]bool{}
	for k, v := range bindings {
		if v {
			newBindings[k] = v
		}
	}
	return newBindings
}

// DeleteServiceInstance - Delete the service instance for an service instance id.
func (d *Dao) DeleteServiceInstance(id string) error {
	log.Debug(fmt.Sprintf("Dao::DeleteServiceInstance -> [ %s ]", id))
	_, err := d.kapi.Delete(context.Background(), serviceInstanceKey(id), nil)
	return err
}

// GetBindInstance - Retrieve a specific bind instance from the kvp API
func (d *Dao) GetBindInstance(id string) (*bundle.BindInstance, error) {
	spec := &bundle.BindInstance{}
	if err := d.getObject(bindInstanceKey(id), spec); err != nil {
		return nil, err
	}
	return spec, nil
}

// SetBindInstance - Set the bind instance for id in the kvp API.
func (d *Dao) SetBindInstance(id string, bindInstance *bundle.BindInstance) error {
	return d.setObject(bindInstanceKey(id), bindInstance)
}

// DeleteBindInstance - Delete the binding instance for an id in the kvp API.
func (d *Dao) DeleteBindInstance(id string) error {
	log.Debug(fmt.Sprintf("Dao::DeleteBindInstance -> [ %s ]", id))
	_, err := d.kapi.Delete(context.Background(), bindInstanceKey(id), nil)
	return err
}

// DeleteBinding - Delete the binding instance and remove the association with the service instance.
func (d *Dao) DeleteBinding(bindingInstance bundle.BindInstance, serviceInstance bundle.ServiceInstance) error {
	if err := d.DeleteBindInstance(bindingInstance.ID.String()); err != nil {
		return err
	}
	serviceInstance.RemoveBinding(bindingInstance.ID)
	if err := d.SetServiceInstance(serviceInstance.ID.String(), &serviceInstance); err != nil {
		return err
	}
	return nil
}

// SetState - Set the Job State in the kvp API for id.
func (d *Dao) SetState(id string, state bundle.JobState) (string, error) {
	key := stateKey(id, state.Token)
	return key, d.setObject(key, state)
}

// GetState - Retrieve a job state from the kvp API for an ID and Token.
func (d *Dao) GetState(id string, token string) (bundle.JobState, error) {
	return d.GetStateByKey(stateKey(id, token))
}

// GetStateByKey - Retrieve a job state from the kvp API for a job key
func (d *Dao) GetStateByKey(key string) (bundle.JobState, error) {
	state := bundle.JobState{}
	if err := d.getObject(key, &state); err != nil {
		return bundle.JobState{State: bundle.StateFailed}, err
	}
	return state, nil
}

// IsNotFoundError - Will determine if an error is a key is not found error.
func (d *Dao) IsNotFoundError(err error) bool {
	return client.IsKeyNotFound(err)
}

func (d *Dao) getObject(key string, data interface{}) error {
	raw, err := d.GetRaw(key)
	if err != nil {
		return err
	}
	bundle.LoadJSON(raw, data)
	return nil
}

func (d *Dao) setObject(key string, data interface{}) error {
	payload, err := bundle.DumpJSON(data)
	if err != nil {
		return err
	}
	return d.SetRaw(key, payload)
}

////////////////////////////////////////////////////////////
// Key generators
////////////////////////////////////////////////////////////

func stateKey(id string, jobid string) string {
	return fmt.Sprintf("/state/%s/job/%s", id, jobid)
}

func stateKeyID(key string) string {
	s := strings.TrimPrefix(key, "/state/")
	s = strings.TrimSuffix(s, "/job")
	return s
}

func extractedCredentialsKey(id string) string {
	return fmt.Sprintf("/extracted_credentials/%s", id)
}

func specKey(id string) string {
	return fmt.Sprintf("/spec/%s", id)
}

func serviceInstanceKey(id string) string {
	return fmt.Sprintf("/service_instance/%s", id)
}

func bindInstanceKey(id string) string {
	return fmt.Sprintf("/bind_instance/%s", id)
}

func planNameKey(id string) string {
	return fmt.Sprintf("/plan_name/%s", id)
}
