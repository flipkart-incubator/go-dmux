package main

import (
	"github.com/flipkart-incubator/go-dmux/config"
	"github.com/flipkart-incubator/go-dmux/metrics"
	"log"
	"os"

	"github.com/flipkart-incubator/go-dmux/logging"
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

	dconf := config.DMuxConfigSetting{
		FilePath: path,
	}
	conf := dconf.GetDmuxConf()

	dmuxLogging := new(logging.DMuxLogging)
	dmuxLogging.Start(conf.Logging)

	c := config.Controller{config: conf}
	go c.start()

	log.Printf("config: %v", conf)

	//start showing metrics at the endpoint
	metrics.Start(conf.MetricPort)

	for _, item := range conf.DMuxItems {
		go func(connType config.ConnectionType, connConf interface{}, logDebug bool) {
			connType.Start(connConf, logDebug)
		}(item.ConnType, item.Connection, dmuxLogging.EnableDebug)
	}

	//main thread halts. TODO make changes to listen to kill and reboot
	select {}
}
