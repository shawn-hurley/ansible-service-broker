package runtime

// PreSandboxCreate - The pre sand box creation function will be called
// before the sandbox is created for the APB. This function should not expect
// to panic and should fail gracefully by bubbling up the error and cleaning up
// after itself. You can assume that the Namespace has been created.
// Parameters in order of adding to the function.
// string - pod name is also the svc accounts name.
// string - namespace of the transient namespace.
// list of strings - target namespaces.
// string - abp role.
// return error.
type PreSandboxCreate func(string, string, []string, string) error

// AddPreCreateSandbox - Adds a pre create sandbox function to the runtime.
// Before the sandbox is created all of the functions that have been added here
// will be executed. in the order that they were added.
func (p *provider) AddPreCreateSandbox(f PreSandboxCreate) {
	p.preSandboxCreate = append(p.preSandboxCreate, f)
}

// PostSandboxCreate - The post sand box creation function will be called
// after the sandbox is created for the APB. This function should not expect
// to panic and should fail gracefully by bubbling up the error and cleaning up
// after itself.
// Parameters in order of adding to the function.
// string - pod name is also the svc accounts name.
// string - namespace of the transient namespace.
// list of strings - target namespaces.
// string - abp role.
// return error.
type PostSandboxCreate func(string, string, []string, string) error

// AddPostCreateSandbox - Adds a post create sandbox function to the runtime.
// Once the sandbox is created all of the functions that have been added here
// will be executed in the order they were added.
func (p *provider) AddPostCreateSandbox(f PostSandboxCreate) {
	p.postSandboxCreate = append(p.postSandboxCreate, f)
}

// PreSandboxDestroy - The pre sand box destroy function will be called
// before the sandbox is destoryed. This could mean the namespace is kept around
// if the apb failed and configuration conditions are met. This function should not
// expect to panic and should fail gracefully by bubbling up the error. This
// function should also not delete the namespace or the pod directly. This
// will most likely be used to clean up resources in pre/post create sandbox hooks.
// Parameters:
// string - pod / svc-account name
// string - pod transient namespace
// []string - target namespaces
type PreSandboxDestroy func(string, string, []string) error

// AddPreDestroySandbox - Adds a pre destroy sandbox function to the runtime.
// before the sandbox is destroyed all of the functions that have been added here
// will be executed in the order they were added.
func (p *provider) AddPreDestroySandbox(f PreSandboxDestroy) {
	p.preSandboxDestroy = append(p.preSandboxDestroy, f)
}

// PostSandboxDestroy - The post sand box destroy function will be called
// after the sandbox is destoryed. This could mean the namespace is kept around
// if the apb failed and configuration conditions are met. This function should not
// expect to panic and should fail gracefully by bubbling up the error. This
// function should also not delete the namespace or the pod directly. This
// will most likely be used to clean up resources in pre/post create sandbox hooks.
// Parameters:
// string - pod / svc-account name
// string - pod transient namespace
// []string - target namespaces
type PostSandboxDestroy func(string, string, []string) error

// AddPostDestroySandbox - Adds a post destroy sandbox function to the runtime.
// after the sandbox is destroyed all of the functions that have been added here
// will be executed in the order they were added.
func (p *provider) AddPostDestroySandbox(f PostSandboxDestroy) {
	p.postSandboxDestroy = append(p.postSandboxDestroy, f)
}
