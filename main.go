package main

import (
	"github.com/afex/hystrix-go/hystrix"
	"log"
	"net"
	"net/http"
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

	dmuxLogging := new(logging.DMuxLogging)
	dmuxLogging.Start(conf.Logging)

	c := Controller{config: conf}
	go c.start()

	log.Printf("config: %v", conf)

	for _, item := range conf.DMuxItems {
		go func(connType ConnectionType, connConf interface{}, logDebug bool, name string) {
			connType.Start(connConf, logDebug, name)
		}(item.ConnType, item.Connection, dmuxLogging.EnableDebug, item.Name)
	}
	hystrixStreamHandler := hystrix.NewStreamHandler()
	hystrixStreamHandler.Start()
	go http.ListenAndServe(net.JoinHostPort("", "9999"), hystrixStreamHandler)

	//main thread halts. TODO make changes to listen to kill and reboot
	select {}
}
