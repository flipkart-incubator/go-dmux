package main

import (
	"github.com/tesrohit-developer/go-dmux/configs"
	"log"
	"os"

	"github.com/tesrohit-developer/go-dmux/logging"
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

	dmuxLogging := new(logging.DMuxLogging)
	dmuxLogging.Start(conf.Logging)

	c := Controller{config: conf}
	go c.start()

	log.Printf("config: %v", conf)

	for _, item := range conf.DMuxItems {
		go func(connType configs.ConnectionType, connConf interface{}, logDebug bool) {
			connType.Start(connConf, logDebug)
		}(item.ConnType, item.Connection, dmuxLogging.EnableDebug)
	}

	//main thread halts. TODO make changes to listen to kill and reboot
	select {}
}
