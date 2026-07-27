package config

import (
	"encoding/json"
	sideline_models "github.com/flipkart-incubator/go-dmux/sideline"
	"log"
	"os"

	"github.com/flipkart-incubator/go-dmux/connection"
	"github.com/flipkart-incubator/go-dmux/logging"
)

// ConnectionType based on this type of Connection and related forks happen
type ConnectionType string

const (
	//KafkaHTTP key to define kafka to generic http sink
	KafkaHTTP ConnectionType = "kafka_http"
	//KafkaFoxtrot key to define kafka to foxtrot http sink
	KafkaFoxtrot ConnectionType = "kafka_foxtrot"

	//PulsarHTTP key to define pulsar to generic http sink
	PulsarHTTP ConnectionType = "pulsar_http"
)

func (c ConnectionType) getConfig(data []byte) interface{} {
	switch c {
	case KafkaHTTP:
		var connConf []*connection.KafkaHTTPConnConfig
		json.Unmarshal(data, &connConf)
		return connConf[0]
	case KafkaFoxtrot:
		var connConf []*connection.KafkaFoxtrotConnConfig
		json.Unmarshal(data, &connConf)
		return connConf[0]
	case PulsarHTTP:
		var connConf []*connection.PulsarConnConfig
		json.Unmarshal(data, &connConf)
		return connConf[0]
	default:
		panic("Invalid Connection Type")

	}
}

// Start invokes Run of the respective connection in a go routine
func (c ConnectionType) Start(conf interface{}, enableDebug bool, sidelineImpl interface{}) {
	switch c {
	case KafkaHTTP:
		if sidelineImpl != nil {
			confBytes, err := json.Marshal(conf)
			if err != nil {
				log.Fatal("Error in InitialisePlugin " + err.Error())
			}
			initErr := sidelineImpl.(sideline_models.CheckMessageSideline).InitialisePlugin(confBytes)
			if initErr != nil {
				log.Fatal(initErr.Error())
			}
		}
		connObj := &connection.KafkaHTTPConn{
			EnableDebugLog: enableDebug,
			Conf:           conf,
			SidelineImpl:   sidelineImpl,
		}
		log.Println("Starting ", KafkaHTTP)
		connObj.Run()
	case KafkaFoxtrot:
		connObj := &connection.KafkaFoxtrotConn{
			EnableDebugLog: enableDebug,
			Conf:           conf,
		}
		log.Println("Starting ", KafkaFoxtrot)
		connObj.Run()
	case PulsarHTTP:
		connObj := &connection.PulsarConn{
			EnableDebugLog: enableDebug,
			Conf:           conf,
		}
		log.Println("Starting ", PulsarHTTP)
		connObj.Run()
	default:
		panic("Invalid Connection Type")
	}

}

// DMuxConfigSetting dumx obj
type DMuxConfigSetting struct {
	FilePath string
}

// DmuxConf hold Config data
type DmuxConf struct {
	Name      string     `json:"name"`
	DMuxItems []DmuxItem `json:"dmuxItems"`
	// DMuxMap    map[string]KafkaHTTPConnConfig `json:"dmuxMap"`
	MetricPort int             `json:"metric_port"`
	Logging    logging.LogConf `json:"logging"`
}

// DmuxItem struct defines name and type of connection
type DmuxItem struct {
	Name           string         `json:"name"`
	Disabled       bool           `json:"disabled"`
	ConnType       ConnectionType `json:"connectionType"`
	Connection     interface{}    `json:"connection"`
	SidelineEnable bool           `json:"sidelineEnable"`
}

// GetDmuxConf parses Config file and return DmuxConf
func (s DMuxConfigSetting) GetDmuxConf() DmuxConf {
	raw, err := os.ReadFile(s.FilePath)
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
