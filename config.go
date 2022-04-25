package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"github.com/go-dmux/connection"
	"github.com/go-dmux/logging"
)

//ConnectionType based on this type of Connection and related forks happen
type ConnectionType string

const (
	//KafkaHTTP key to define kafka to generic http sink
	KafkaHTTP ConnectionType = "kafka_http"
	//KafkaFoxtrot key to define kafka to foxtrot http sink
	KafkaFoxtrot ConnectionType = "kafka_foxtrot"
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
	default:
		panic("Invalid Connection Type")

	}
}

//Start invokes Run of the respective connection in a go routine
func (c ConnectionType) Start(conf interface{}, enableDebug bool) {
	switch c {
	case KafkaHTTP:
		connObj := &connection.KafkaHTTPConn{
			EnableDebugLog: enableDebug,
			Conf:           conf,
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
	MetricPort int	     `json:"metric_port"`
	Logging logging.LogConf `json:"logging"`
}

//DmuxItem struct defines name and type of connection
type DmuxItem struct {
	Name       string         `json:"name"`
	Disabled   bool           `json:"disabled`
	ConnType   ConnectionType `json:"connectionType"`
	Connection interface{}    `json:connection`
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
