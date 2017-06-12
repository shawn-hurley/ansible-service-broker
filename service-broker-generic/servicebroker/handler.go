package servicebroker

import (
	"net/http"
	"strconv"

	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/gorilla/mux"
	"github.com/openshift/ansible-service-broker/service-broker-generic/servicebroker/broker"
	"github.com/pborman/uuid"
)

// TODO: implement asynchronous operations

// GorillaRouteHandler - gorilla route handler
// making the handler methods more testable by moving the reliance of mux.Vars()
// outside of the handlers themselves
type GorillaRouteHandler func(http.ResponseWriter, *http.Request)

func (g GorillaRouteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g(w, r)
}

// VarHandler - Variable route handler.
type VarHandler func(http.ResponseWriter, *http.Request, map[string]string)

func createVarHandler(r VarHandler) GorillaRouteHandler {
	return func(writer http.ResponseWriter, request *http.Request) {
		r(writer, request, mux.Vars(request))
	}
}

func authMiddleWare(h http.Handler, authFunc func(username, password string) bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if authFunc(username, password) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// NewHandler - Create a new handler by attaching the routes and setting logger and broker.
func newHandler(b broker.Broker, authFunc func(username, password string) bool) Handler {
	h := Handler{
		Router: *mux.NewRouter(),
		Broker: b,
	}

	// TODO: Reintroduce router restriction based on API version when settled upstream
	//root := h.router.Headers("X-Broker-API-Version", "2.9").Subrouter()

	h.Router.Handle("/v2/bootstrap", authMiddleWare(createVarHandler(h.bootstrap), authFunc)).Methods("POST")
	h.Router.HandleFunc("/v2/catalog", createVarHandler(h.catalog)).Methods("GET")
	h.Router.HandleFunc("/v2/service_instances/{instance_uuid}", createVarHandler(h.provision)).Methods("PUT")
	h.Router.HandleFunc("/v2/service_instances/{instance_uuid}", createVarHandler(h.update)).Methods("PATCH")
	h.Router.HandleFunc("/v2/service_instances/{instance_uuid}", createVarHandler(h.deprovision)).Methods("DELETE")
	h.Router.HandleFunc("/v2/service_instances/{instance_uuid}/service_bindings/{binding_uuid}",
		createVarHandler(h.bind)).Methods("PUT")
	h.Router.HandleFunc("/v2/service_instances/{instance_uuid}/service_bindings/{binding_uuid}",
		createVarHandler(h.unbind)).Methods("DELETE")
	h.Router.HandleFunc("/v2/service_instances/{instance_uuid}/last_operation",
		createVarHandler(h.lastoperation)).Methods("GET")
	return h
}

// NewServiceBrokerHandler - Create a new Service Borker.
func NewServiceBrokerHandler(broker broker.Broker, authFunc func(username, password string) bool) Handler {
	return newHandler(broker, authFunc)

}

// Handler -
type Handler struct {
	Router mux.Router
	Broker broker.Broker
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.Router.ServeHTTP(w, r)
}

func (h Handler) bootstrap(w http.ResponseWriter, r *http.Request, params map[string]string) {
	defer r.Body.Close()
	resp, err := h.Broker.Bootstrap()
	writeDefaultResponse(w, http.StatusOK, resp, err)
}

func (h Handler) catalog(w http.ResponseWriter, r *http.Request, params map[string]string) {
	defer r.Body.Close()

	resp, err := h.Broker.Catalog()

	writeDefaultResponse(w, http.StatusOK, resp, err)
}

func (h Handler) provision(w http.ResponseWriter, r *http.Request, params map[string]string) {
	defer r.Body.Close()

	instanceUUID := uuid.Parse(params["instance_uuid"])
	if instanceUUID == nil {
		writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: "invalid instance_uuid"})
		return
	}

	var async bool
	queryparams := r.URL.Query()

	if val, ok := queryparams["accepts_incomplete"]; ok {
		// ignore the error, if async can't be parsed it will be false
		async, _ = strconv.ParseBool(val[0])
	}

	var req *broker.ProvisionRequest
	err := readRequest(r, &req)

	if err != nil {
		writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: "could not read request: " + err.Error()})
		return
	}

	// Ok let's provision this bad boy

	resp, err := h.Broker.Provision(instanceUUID, req, async)

	if err != nil {
		switch err {
		case broker.ErrorDuplicate:
			writeResponse(w, http.StatusConflict, broker.ProvisionResponse{})
		case broker.ErrorAlreadyProvisioned:
			writeResponse(w, http.StatusOK, resp)
		case broker.ErrorNotFound:
			writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: err.Error()})
		default:
			writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: err.Error()})
		}
	} else if async {
		writeDefaultResponse(w, http.StatusAccepted, resp, err)
	} else {
		writeDefaultResponse(w, http.StatusCreated, resp, err)
	}
}

func (h Handler) update(w http.ResponseWriter, r *http.Request, params map[string]string) {
	defer r.Body.Close()

	instanceUUID := uuid.Parse(params["instance_uuid"])
	if instanceUUID == nil {
		writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: "invalid instance_uuid"})
		return
	}

	var req *broker.UpdateRequest
	if err := readRequest(r, &req); err != nil {
		writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: err.Error()})
		return
	}

	resp, err := h.Broker.Update(instanceUUID, req)

	writeDefaultResponse(w, http.StatusOK, resp, err)
}

func (h Handler) deprovision(w http.ResponseWriter, r *http.Request, params map[string]string) {
	defer r.Body.Close()

	instanceUUID := uuid.Parse(params["instance_uuid"])
	if instanceUUID == nil {
		writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: "invalid instance_uuid"})
		return
	}

	resp, err := h.Broker.Deprovision(instanceUUID)

	if err != nil {
		//log.Debug("err for deprovision - %#v", err)
	}
	if err == broker.ErrorNotFound {
		writeResponse(w, http.StatusGone, broker.DeprovisionResponse{})
		return
	}

	writeDefaultResponse(w, http.StatusOK, resp, err)
}

func (h Handler) bind(w http.ResponseWriter, r *http.Request, params map[string]string) {
	defer r.Body.Close()

	// validate input uuids
	instanceUUID := uuid.Parse(params["instance_uuid"])
	if instanceUUID == nil {
		writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: "invalid instance_uuid"})
		return
	}

	bindingUUID := uuid.Parse(params["binding_uuid"])
	if bindingUUID == nil {
		writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: "invalid binding_uuid"})
		return
	}

	var req *broker.BindRequest
	if err := readRequest(r, &req); err != nil {
		writeResponse(w, http.StatusInternalServerError, broker.ErrorResponse{Description: err.Error()})
		return
	}

	// process binding request
	resp, err := h.Broker.Bind(instanceUUID, bindingUUID, req)

	if err != nil {
		switch err {
		case broker.ErrorDuplicate:
			writeResponse(w, http.StatusConflict, broker.BindResponse{})
		case broker.ErrorAlreadyProvisioned:
			writeResponse(w, http.StatusOK, resp)
		case broker.ErrorNotFound:
			writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: err.Error()})
		default:
			writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: err.Error()})
		}
	} else {
		writeDefaultResponse(w, http.StatusCreated, resp, err)
	}
}

func (h Handler) unbind(w http.ResponseWriter, r *http.Request, params map[string]string) {
	defer r.Body.Close()

	instanceUUID := uuid.Parse(params["instance_uuid"])
	if instanceUUID == nil {
		writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: "invalid instance_uuid"})
		return
	}

	bindingUUID := uuid.Parse(params["binding_uuid"])
	if bindingUUID == nil {
		writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: "invalid binding_uuid"})
		return
	}

	err := h.Broker.Unbind(instanceUUID, bindingUUID)

	if errors.IsNotFound(err) {
		writeResponse(w, http.StatusGone, struct{}{})
	} else {
		writeDefaultResponse(w, http.StatusOK, struct{}{}, err)
	}
	return
}

func (h Handler) lastoperation(w http.ResponseWriter, r *http.Request, params map[string]string) {
	defer r.Body.Close()

	instanceUUID := uuid.Parse(params["instance_uuid"])
	if instanceUUID == nil {
		writeResponse(w, http.StatusBadRequest, broker.ErrorResponse{Description: "invalid instance_uuid"})
		return
	}

	req := broker.LastOperationRequest{}

	queryparams := r.URL.Query()

	// operation is rqeuired
	if val, ok := queryparams["operation"]; ok {
		req.Operation = val[0]
	} else {
		//log.Warning(fmt.Sprintf("operation not supplied, relying solely on the instance_uuid [%s]", instanceUUID))
	}

	// service_id is optional
	if val, ok := queryparams["service_id"]; ok {
		req.ServiceID = uuid.Parse(val[0])
	}

	// plan_id is optional
	if val, ok := queryparams["plan_id"]; ok {
		req.PlanID = uuid.Parse(val[0])
	}

	resp, err := h.Broker.LastOperation(instanceUUID, &req)

	writeDefaultResponse(w, http.StatusOK, resp, err)
}
