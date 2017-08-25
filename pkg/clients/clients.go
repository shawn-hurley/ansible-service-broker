package clients

import (
	"sync"

	etcd "github.com/coreos/etcd/client"
	"github.com/openshift/ansible-service-broker/pkg/util"
	k8s "k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
)

var instances struct {
	Etcd       etcd.Client
	Kubernetes *k8s.Clientset
}

var once struct {
	Etcd       sync.Once
	Kubernetes sync.Once
}

var log = util.NewLog("clients")
