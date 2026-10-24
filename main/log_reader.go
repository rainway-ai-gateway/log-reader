// Copyright (c) 2026 The BFE Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"github.com/bfenetworks/go-lib/log"
	"github.com/bfenetworks/go-lib/log/log4go"
	"github.com/rainway-ai-gateway/log-reader/bfe_log_reader"
	"github.com/rainway-ai-gateway/log-reader/reader_conf"
	"github.com/rainway-ai-gateway/log-reader/reader_module"
	"github.com/rainway-ai-gateway/log-reader/reader_modules"
)

var (
	// version is injected at build time via
	// -ldflags "-X main.version=$(LOG_READER_VERSION)" (see Makefile).
	version   string
	help      *bool   = flag.Bool("h", false, "to show help")
	showVer   *bool   = flag.Bool("v", false, "to show version")
	confRoot  *string = flag.String("c", "../conf", "root path of config file")
	logPath   *string = flag.String("l", "../log", "dir path of log")
	stdOut    *bool   = flag.Bool("s", false, "to show log in stdout")
	debugLog  *bool   = flag.Bool("d", false, "to show debug log (otherwise >= info)")
	autoBns   *bool   = flag.Bool("a", false, "automatically get name of bfe cluster")
	fromBegin *bool   = flag.Bool("b", false, "is read from begin")
)

func init() {
	// version is empty when built without the ldflags above
	if version == "" {
		version = "debug"
	}
}

func Exit(code int) {
	// Close all modules first
	for name, module := range reader_module.GetWorkModules() {
		log.Logger.Info("Closing module: %s", name)
		if err := module.Close(); err != nil {
			log.Logger.Error("Error closing module %s: %v", name, err)
		}
	}

	log.Logger.Close()
	/* to overcome bug in log, sleep for a while    */
	time.Sleep(1 * time.Second)
	os.Exit(code)
}

/* the main function. */
func main() {
	var err error
	var logSwitch string

	flag.Parse()
	if *help {
		flag.PrintDefaults()
		return
	}
	if *showVer {
		fmt.Printf("version %s\n", version)
		return
	}

	// debug switch
	if *debugLog {
		logSwitch = "DEBUG"
	} else {
		logSwitch = "INFO"
	}

	if *fromBegin {
		bfe_log_reader.SetReadFromBegin(true)
	}

	// initialize log
	// set log buffer size
	log4go.SetLogBufferLength(10000)
	// if blocking, log will be dropped
	log4go.SetLogWithBlocking(false)

	err = log.Init("log-reader", logSwitch, *logPath, *stdOut, "midnight", 5)
	if err != nil {
		fmt.Printf("log-reader: err in log.Init():%s\n", err.Error())
		Exit(1)
	}

	log.Logger.Info("log-reader start, version:%s", version)

	// load config
	confPath := path.Join(*confRoot, "config.conf")
	config, err := reader_conf.ReaderConfigLoad(confPath)
	if err != nil {
		log.Logger.Error("main():err in LoadConfig():%s", err.Error())
		Exit(1)
	}

	// set program name, used in web_monitor
	config.Main.ProgramName = "log-reader"

	// register all modules
	reader_modules.SetModules()

	// 1. create bfeLogReader
	bfeLogReader, err := bfe_log_reader.NewBfeLogReader(&config, version)
	if err != nil {
		log.Logger.Error("main():err in NewBfeLogReader():%s", err.Error())
		Exit(1)
	}

	// setup signal table
	bfeLogReader.InitSignalTable()
	log.Logger.Info("main():logReader.InitSignalTable() OK")

	// 2. register modules for logReader
	err = bfeLogReader.RegisterModules(&config)
	if err != nil {
		log.Logger.Error("main():err in RegisterModules():%s", err.Error())
		Exit(1)
	}
	log.Logger.Info("main():logReader.RegisterModules() OK")

	// 3. start logReader
	if err = bfeLogReader.Start(*confRoot); err != nil {
		log.Logger.Error("main():error in logReader.Start():%v", err)
		Exit(1)
	}

	// start embeded web server
	go bfeLogReader.WebServer.Start()

	// set "SERVER_READY" to YES
	bfeLogReader.SetReady()

	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	// ensure that all logs are export and normal exit
	Exit(0)
}
