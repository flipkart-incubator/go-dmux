package main

import (
	"encoding/json"
	"github.com/tesrohit-developer/go-dmux/plugins"
	"io/ioutil"
	"log"
	"os"

	"github.com/flipkart-incubator/go-dmux/connection"
	"github.com/flipkart-incubator/go-dmux/logging"
)

//ConnectionType based on this type of Connection and related forks happen
type ConnectionType string

const (
	//KafkaHTTP key to define kafka to generic http sink
	KafkaHTTP ConnectionType = "kafka_http"
	//KafkaHTTPSideline key to define kafka to generic http sink
	KafkaHTTPSideline ConnectionType = "kafka_http_sideline"
	//KafkaFoxtrot key to define kafka to foxtrot http sink
	KafkaFoxtrot ConnectionType = "kafka_foxtrot"
)

type SidelinePluginConfig struct {
	SidelinePluginConfigDetails SidelinePluginConfigDetails `json:"sidelinePluginConfig"`
}

type SidelinePluginConfigDetails struct {
	Prefix string `json:"prefix"`
	Id     string `json:"id"`
	Path   string `json:"path"`
}

func getSidelinePlugin(conf interface{}) interface{} {
	data, _ := json.Marshal(conf)
	var sidelinePluginConfig *SidelinePluginConfig
	sidelinePluginConfigErr := json.Unmarshal(data, &sidelinePluginConfig)
	if sidelinePluginConfigErr != nil {
		log.Fatal("Error in initialising sidelinePluginConfig")
	}
	sidelineImpls := plugins.NewManager("sideline_plugin",
		sidelinePluginConfig.SidelinePluginConfigDetails.Prefix,
		sidelinePluginConfig.SidelinePluginConfigDetails.Path,
		&plugins.CheckMessageSidelineImplPlugin{})
	// defer sidelineImpls.Dispose()
	// Initialize sidelineImpls manager
	err := sidelineImpls.Init()
	if err != nil {
		log.Fatal(err.Error())
	}

	// Launch all sidelineImpls binaries
	sidelineImpls.Launch()

	p, err := sidelineImpls.GetInterface(sidelinePluginConfig.SidelinePluginConfigDetails.Id)
	if err != nil {
		log.Fatal(err.Error())
	}
	return p
}

//Start invokes Run of the respective connection in a go routine
func (c ConnectionType) Start(conf interface{}, enableDebug bool, sidelineEnabled bool) {
	switch c {
	case KafkaHTTP:
		connObj := &connection.KafkaHTTPConn{
			EnableDebugLog: enableDebug,
			Conf:           conf,
		}
		log.Println("Starting Without Sideline ", KafkaHTTP)
		connObj.Run()
	case KafkaHTTPSideline:
		plugin := getSidelinePlugin(conf)
		confBytes, err := json.Marshal(conf)
		if err != nil {
			log.Fatal("Error in InitialisePlugin " + err.Error())
		}
		initErr := plugin.(plugins.CheckMessageSidelineImpl).InitialisePlugin(confBytes)
		if initErr != nil {
			log.Fatal(initErr.Error())
		}
		connObj := &connection.KafkaHTTPConnWithSideline{
			EnableDebugLog: enableDebug,
			Conf:           conf,
			SidelinePlugin: plugin,
		}
		log.Println("Starting With Sideline ", KafkaHTTP)
		connObj.Run()
	case KafkaFoxtrot:
		log.Printf("Sidelining not supported for Foxtrot")
		connObj := &connection.KafkaFoxtrotConn{
			EnableDebugLog: enableDebug,
			Conf:           conf,
		}
		log.Println("Starting ", KafkaFoxtrot)
		connObj.Run()
	default:
		panic("Invalid Connection Type")

	}

}

//DMuxConfigSetting dumx obj
type DMuxConfigSetting struct {
	FilePath string
}

//DmuxConf hold config data
type DmuxConf struct {
	Name      string     `json:"name"`
	DMuxItems []DmuxItem `json:"dmuxItems"`
	// DMuxMap    map[string]KafkaHTTPConnConfig `json:"dmuxMap"`
	MetricPort int             `json:"metric_port"`
	Logging    logging.LogConf `json:"logging"`
}

//DmuxItem struct defines name and type of connection
type DmuxItem struct {
	Name            string         `json:"name"`
	Disabled        bool           `json:"disabled`
	ConnType        ConnectionType `json:"connectionType"`
	Connection      interface{}    `json:connection`
	SidelineEnabled bool           `json:"sidelineEnabled"`
}

//GetDmuxConf parses config file and return DmuxConf
func (s DMuxConfigSetting) GetDmuxConf() DmuxConf {
	raw, err := ioutil.ReadFile(s.FilePath)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	var conf DmuxConf
	if err := json.Unmarshal(raw, &conf); err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	return conf
}
