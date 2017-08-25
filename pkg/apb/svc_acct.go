package apb

import (
	"os"
	"path/filepath"

	"github.com/openshift/ansible-service-broker/pkg/runtime"

	yaml "gopkg.in/yaml.v2"
)

// ServiceAccountManager - managers the service account methods
type ServiceAccountManager struct {
}

// NewServiceAccountManager - Creates a new service account manager
func NewServiceAccountManager() ServiceAccountManager {
	return ServiceAccountManager{}
}

// CreateApbSandbox - Sets up ServiceAccount based apb sandbox
// Returns service account name to be used as a handle for destroying
// the sandbox at the conclusion of running the apb
func (s *ServiceAccountManager) CreateApbSandbox(namespace string, apbID string,
) (string, error) {
	svcAccountName := apbID
	roleBindingName := apbID

	svcAcctM := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ServiceAccount",
		"metadata": map[string]string{
			"name":      apbID,
			"namespace": namespace,
		},
	}

	roleBindingM := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "RoleBinding",
		"metadata": map[string]string{
			"name":      roleBindingName,
			"namespace": namespace,
		},
		"subjects": []map[string]string{
			map[string]string{
				"kind":      "ServiceAccount",
				"name":      svcAccountName,
				"namespace": namespace,
			},
		},
		"roleRef": map[string]string{
			"name": ApbRole,
		},
	}

	s.createResourceDir()
	rFilePath, err := s.writeResourceFile(apbID, &svcAcctM, &roleBindingM)
	if err != nil {
		return "", err
	}

	// Create resources in cluster
	s.createResources(rFilePath, namespace)

	log.Info("Successfully created apb sandbox: [ %s ]", apbID)

	return apbID, nil
}

func (s *ServiceAccountManager) createResources(rFilePath string, namespace string) error {
	log.Debug("Creating resources from file at path: %s", rFilePath)
	output, err := runtime.RunCommand("oc", "create", "-f", rFilePath, "--namespace="+namespace)
	// TODO: Parse output somehow to validate things got created?
	if err != nil {
		log.Error("Something went wrong trying to create resources in cluster")
		log.Error("Returned error:")
		log.Error(err.Error())
		log.Error("oc create -f output:")
		log.Error(string(output))
		return err
	}
	log.Debug("Successfully created resources, oc create -f output:")
	log.Debug("\n" + string(output))
	return nil
}

func (s *ServiceAccountManager) writeResourceFile(handle string,
	svcAcctM *map[string]interface{}, roleBindingM *map[string]interface{},
) (string, error) {
	// Create file if doesn't already exist
	filePath, err := s.createFile(handle)
	if err != nil {
		return "", err // Bubble, error logged in createFile
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	defer file.Close()

	if err != nil {
		log.Error("Something went wrong writing resources to file!")
		log.Error(err.Error())
		return "", err
	}

	file.WriteString("---\n")
	svcAcctY, err := yaml.Marshal(svcAcctM)
	if err != nil {
		log.Error("Something went wrong marshalling svc acct to yaml")
		log.Error(err.Error())
		return "", err
	}
	file.WriteString(string(svcAcctY))

	file.WriteString("---\n")
	roleBindingY, err := yaml.Marshal(roleBindingM)
	if err != nil {
		log.Error("Something went wrong marshalling role binding to yaml")
		log.Error(err.Error())
		return "", err
	}
	file.WriteString(string(roleBindingY))

	log.Info("Successfully wrote resources to %s", filePath)
	return filePath, nil
}

func (s *ServiceAccountManager) createResourceDir() {
	dir := resourceDir()
	log.Debug("Creating resource file dir: %s", dir)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, os.ModePerm)
	}
}

func (s *ServiceAccountManager) createFile(handle string) (string, error) {
	rFilePath := filePathFromHandle(handle)
	log.Debug("Creating resource file %s", rFilePath)

	if _, err := os.Stat(rFilePath); os.IsNotExist(err) {
		// Valid behavior if the file does not exist, create
		file, err := os.Create(rFilePath)
		// Handle file creation error
		if err != nil {
			log.Error("Something went wrong touching new resource file!")
			log.Error(err.Error())
			return "", err
		}
		defer file.Close()
	} else if err != nil {
		// Bubble any non-expected errors
		return "", err
	}

	return rFilePath, nil
}

// DestroyApbSandbox - Destroys the apb sandbox
func (s *ServiceAccountManager) DestroyApbSandbox(handle string, namespace string) error {
	if handle == "" {
		log.Info("Requested destruction of APB sandbox with empty handle, skipping.")
		return nil
	}

	log.Debug("Deleting serviceaccount %s, namespace %s", handle, namespace)
	output, err := runtime.RunCommand(
		"oc", "delete", "serviceaccount", handle, "--namespace="+namespace,
	)
	if err != nil {
		log.Error("Something went wrong trying to destroy the serviceaccount!")
		log.Error(err.Error())
		log.Error("oc delete output:")
		log.Error(string(output))
		return err
	}
	log.Debug("Successfully deleted serviceaccount %s, namespace %s", handle, namespace)
	log.Debug("oc delete output:")
	log.Debug(string(output))

	log.Debug("Deleting rolebinding %s, namespace %s", handle, namespace)
	output, err = runtime.RunCommand(
		"oc", "delete", "rolebinding", handle, "--namespace="+namespace,
	)
	if err != nil {
		log.Error("Something went wrong trying to destroy the rolebinding!")
		log.Error(err.Error())
		log.Error("oc delete output:")
		log.Error(string(output))
		return err
	}
	log.Debug("Successfully deleted rolebinding %s, namespace %s", handle, namespace)
	log.Debug("oc delete output:")
	log.Debug(string(output))

	// If file doesn't exist, ignore
	// "If there is an error, it will be of type *PathError"
	// We don't care, because it's gone
	os.Remove(filePathFromHandle(handle))

	return nil
}

func resourceDir() string {
	return filepath.FromSlash("/tmp/asb-resource-files")
}

func filePathFromHandle(handle string) string {
	return filepath.Join(resourceDir(), handle+".yaml")
}
