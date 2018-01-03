//
// Copyright (c) 2017 Red Hat, Inc.
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

package main

import (
	"fmt"
	"os"

	flags "github.com/jessevdk/go-flags"
	"github.com/openshift/ansible-service-broker/pkg/app"
	"github.com/openshift/ansible-service-broker/pkg/config"
	"github.com/openshift/ansible-service-broker/pkg/registries"
	logutil "github.com/openshift/ansible-service-broker/pkg/util/logging"
	"honnef.co/go/tools/version"
)

// Command Line arguments that we can handle.
type args struct {
	ConfigFile string `short:"c" long:"config" description:"Config File" default:"/etc/ansible-service-broker/config.yaml"`
	Version    bool   `short:"v" long:"version" description:"Print version information"`
}

func main() {
	arg := args{}
	_, err := flags.Parse(&arg)
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}

	// Handle printing the version from the arguement.
	if arg.Version {
		fmt.Println(version.Version)
		os.Exit(0)
	}

	config, err := config.CreateConfig(arg.ConfigFile)
	if err != nil {
		os.Stderr.WriteString("ERROR: Failed to read config file\n")
		os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
	c := logutil.LogConfig{
		LogFile: config.GetString("log.logfile"),
		Stdout:  config.GetBool("log.stdout"),
		Level:   config.GetString("log.level"),
		Color:   config.GetBool("log.color"),
	}
	if err = logutil.InitializeLog(c); err != nil {
		os.Stderr.WriteString("ERROR: Failed to initialize logger\n")
		os.Stderr.WriteString(err.Error())
		os.Exit(1)
	}

	regs := []registries.Registry{}
	for name := range config.GetSubConfig("registry").ToMap() {
		reg, err := registries.NewRegistry(config.GetSubConfig(fmt.Sprintf("%v.%v", "registry", name)), nil, config.GetString("openshift.namespace"))
		if err != nil {
			os.Stderr.WriteString(
				fmt.Sprintf("Failed to initialize %v Registry err - %v \n", name, err))
			os.Exit(1)
		}
		regs = append(regs, reg)
	}

	asb := app.CreateASB(regs, config)
	asb.Start()
	////////////////////////////////////////////////////////////
	// TODO:
	// try/finally to make sure we clean things up cleanly?
	//if stopsignal {
	//app.stop() // Stuff like close open files
	//}
	////////////////////////////////////////////////////////////
}
