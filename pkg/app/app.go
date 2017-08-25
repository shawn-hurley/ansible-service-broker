package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	kubeversiontypes "k8s.io/apimachinery/pkg/version"

	logging "github.com/op/go-logging"
	"github.com/openshift/ansible-service-broker/pkg/broker"
	"github.com/openshift/ansible-service-broker/pkg/clients"
	"github.com/openshift/ansible-service-broker/pkg/dao"
	"github.com/openshift/ansible-service-broker/pkg/handler"
	"github.com/openshift/ansible-service-broker/pkg/registries"
	"github.com/openshift/ansible-service-broker/pkg/util"
)

// MsgBufferSize - The buffer for the message channel.
const MsgBufferSize = 20

var log *logging.Logger

// App - All the application pieces that are installed.
type App struct {
	broker   *broker.AnsibleBroker
	args     Args
	config   Config
	dao      *dao.Dao
	registry []registries.Registry
	engine   *broker.WorkEngine
}

//CreateApp - Creates the application
func CreateApp() App {
	var err error
	app := App{}

	// Writing directly to stderr because log has not been bootstrapped
	if app.args, err = CreateArgs(); err != nil {
		os.Exit(1)
	}

	if app.args.Version {
		fmt.Println(Version)
		os.Exit(0)
	}

	fmt.Println("============================================================")
	fmt.Println("==           Starting Ansible Service Broker...           ==")
	fmt.Println("============================================================")

	//TODO: Let's take all these validations and delegate them to the client
	// pkg.
	if app.config, err = CreateConfig(app.args.ConfigFile); err != nil {
		os.Stderr.WriteString("ERROR: Failed to read config file\n")
		os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}

	if err := util.SetLogConfig(app.config.Log); err != nil {
		os.Stderr.WriteString("ERROR: Failed to initialize logger\n")
		os.Stderr.WriteString(err.Error())
		os.Exit(1)
	}
	log = util.NewLog("app")

	// Initializing clients as soon as we have deps ready.
	err = initClients(app.config.Dao.GetEtcdConfig())
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	log.Debug("Connecting Dao")
	app.dao, err = dao.NewDao(app.config.Dao)

	k8scli, err := clients.Kubernetes()
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	restcli := k8scli.CoreV1().RESTClient()
	body, err := restcli.Get().AbsPath("/version").Do().Raw()
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
	switch {
	case err == nil:
		var kubeServerInfo kubeversiontypes.Info
		err = json.Unmarshal(body, &kubeServerInfo)
		if err != nil && len(body) > 0 {
			log.Error(err.Error())
			os.Exit(1)
		}
		log.Info("Kubernetes version: %v", kubeServerInfo)
	case kapierrors.IsNotFound(err) || kapierrors.IsUnauthorized(err) || kapierrors.IsForbidden(err):
	default:
		log.Error(err.Error())
		os.Exit(1)
	}

	log.Debug("Connecting Registry")
	for _, r := range app.config.Registry {
		reg, err := registries.NewRegistry(r)
		if err != nil {
			log.Errorf(
				"Failed to initialize %v Registry err - %v \n", r.Name, err)
			os.Exit(1)
		}
		app.registry = append(app.registry, reg)
	}

	log.Debug("Initializing WorkEngine")
	app.engine = broker.NewWorkEngine(MsgBufferSize)
	err = app.engine.AttachSubscriber(
		broker.NewProvisionWorkSubscriber(app.dao),
		broker.ProvisionTopic)
	if err != nil {
		log.Errorf("Failed to attach subscriber to WorkEngine: %s", err.Error())
		os.Exit(1)
	}
	err = app.engine.AttachSubscriber(
		broker.NewDeprovisionWorkSubscriber(app.dao),
		broker.DeprovisionTopic)
	if err != nil {
		log.Errorf("Failed to attach subscriber to WorkEngine: %s", err.Error())
		os.Exit(1)
	}
	log.Debugf("Active work engine topics: %+v", app.engine.GetActiveTopics())

	log.Debug("Creating AnsibleBroker")
	if app.broker, err = broker.NewAnsibleBroker(
		app.dao, app.config.Openshift, app.registry, *app.engine, app.config.Broker,
	); err != nil {
		log.Error("Failed to create AnsibleBroker\n")
		log.Error(err.Error())
		os.Exit(1)
	}

	return app
}

// Recover - Recover the application
// TODO: Make this a go routine once we have a strong and well tested
// recovery sequence.
func (a *App) Recover() {
	msg, err := a.broker.Recover()

	if err != nil {
		log.Error(err.Error())
	}

	log.Notice(msg)
}

// Start - Will start the application to listen on the specified port.
func (a *App) Start() {
	// TODO: probably return an error or some sort of message such that we can
	// see if we need to go any further.

	if a.config.Broker.Recovery {
		log.Info("Initiating Recovery Process")
		a.Recover()
	}

	if a.config.Broker.BootstrapOnStartup {
		log.Info("Broker configured to bootstrap on startup")
		log.Info("Attempting bootstrap...")
		if _, err := a.broker.Bootstrap(); err != nil {
			log.Error("Failed to bootstrap on startup!")
			log.Error(err.Error())
			os.Exit(1)
		}
		log.Notice("Broker successfully bootstrapped on startup")
	}

	interval, err := time.ParseDuration(a.config.Broker.RefreshInterval)
	log.Debug("RefreshInterval: %v", interval.String())
	if err != nil {
		log.Error(err.Error())
		log.Error("Not using a refresh interval")
	} else {
		ticker := time.NewTicker(interval)
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		go func() {
			for {
				select {
				case v := <-ticker.C:
					log.Info("Broker configured to refresh specs every %v seconds", interval)
					log.Info("Attempting bootstrap at %v", v.UTC())
					if _, err := a.broker.Bootstrap(); err != nil {
						log.Error("Failed to bootstrap")
						log.Error(err.Error())
					}
					log.Notice("Broker successfully bootstrapped")
				case <-ctx.Done():
					ticker.Stop()
					return
				}
			}
		}()
	}

	log.Notice("Ansible Service Broker Started")
	listeningAddress := "0.0.0.0:1338"
	if a.args.Insecure {
		log.Notice("Listening on http://%s", listeningAddress)
		err = http.ListenAndServe(":1338",
			handler.NewHandler(a.broker, a.config.Broker))
	} else {
		log.Notice("Listening on https://%s", listeningAddress)
		err = http.ListenAndServeTLS(":1338",
			a.config.Broker.SSLCert,
			a.config.Broker.SSLCertKey,
			handler.NewHandler(a.broker, a.config.Broker))
	}
	if err != nil {
		log.Error("Failed to start HTTP server")
		log.Error(err.Error())
		os.Exit(1)
	}
}

func initClients(ec clients.EtcdConfig) error {
	// Designed to panic early if we cannot construct required clients.
	// this likely means we're in an unrecoverable configuration or environment.
	// Best we can do is alert the operator as early as possible.
	//
	// Deliberately forcing the injection of deps here instead of running as a
	// method on the app. Forces developers at authorship time to think about
	// dependencies / make sure things are ready.
	log.Notice("Initializing clients...")
	log.Debug("Trying to connect to etcd")

	etcdClient, err := clients.Etcd(ec)
	if err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	version, err := etcdClient.GetVersion(ctx)
	if err != nil {
		return err
	}

	log.Info("Etcd Version [Server: %s, Cluster: %s]", version.Server, version.Cluster)

	log.Debug("Connecting to Cluster")
	_, err = clients.Kubernetes()
	if err != nil {
		return err
	}

	return nil
}
