package main

import (
	"github.com/go-dmux/metrics"
	"log"
	"os"

	"github.com/go-dmux/logging"
)

//

// **************** Bootstrap ***********

func main() {
	args := os.Args[1:]
	sz := len(args)

	var path string

	if sz == 1 {
		path = args[0]
	}

	dconf := DMuxConfigSetting{
		FilePath: path,
	}
	conf := dconf.GetDmuxConf()

	//update metric port
	metrics.MetricPort = conf.MetricPort
	dmuxLogging := new(logging.DMuxLogging)
	dmuxLogging.Start(conf.Logging)

	c := Controller{config: conf}
	go c.start()

	log.Printf("config: %v", conf)

	//Initialize metrics before reading conf to avoid registering collectors multiple times in case of multiple dmux connections
	metrics.Registry = metrics.Init()

	//Track metrics
	go metrics.Registry.TrackMetrics()

	for _, item := range conf.DMuxItems {
		go func(connType ConnectionType, connConf interface{}, logDebug bool) {
			connType.Start(connConf, logDebug)
		}(item.ConnType, item.Connection, dmuxLogging.EnableDebug)
	}

	//main thread halts. TODO make changes to listen to kill and reboot
	select {}
}
