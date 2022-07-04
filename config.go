package main

import (
	"encoding/json"
	"github.com/tesrohit-developer/go-dmux/configs"
	"io/ioutil"
	"log"
	"os"

	"github.com/tesrohit-developer/go-dmux/logging"
)

//ConnectionType based on this type of Connection and related forks happen
/*type ConnectionType string

const (
	//KafkaHTTP key to define kafka to generic http sink
	KafkaHTTP ConnectionType = "kafka_http"
	//KafkaFoxtrot key to define kafka to foxtrot http sink
	KafkaFoxtrot ConnectionType = "kafka_foxtrot"
)*/

/*func (c configs.ConnectionType) getConfig(data []byte) interface{} {
	switch c {
	case configs.KafkaHTTP:
		var connConf []*connection.KafkaHTTPConnConfig
		json.Unmarshal(data, &connConf)
		return connConf[0]
	case configs.KafkaFoxtrot:
		var connConf []*connection.KafkaFoxtrotConnConfig
		json.Unmarshal(data, &connConf)
		return connConf[0]
	default:
		panic("Invalid Connection Type")

	}
}
*/
/*func getSidelinePlugin() interface{} {
	sidelineImpls := plugins.NewManager("sideline_plugin",
		"sideline-*", "", &plugins.CheckMessageSidelineImplPlugin{})
	// defer sidelineImpls.Dispose()
	// Initialize sidelineImpls manager
	err := sidelineImpls.Init()
	if err != nil {
		log.Fatal(err.Error())
	}

	// Launch all greeters binaries
	sidelineImpls.Launch()
	p, err := sidelineImpls.GetInterface("em")
	if err != nil {
		log.Fatal(err.Error())
	}
	return p
}*/

/*//Start invokes Run of the respective connection in a go routine
func (c configs.ConnectionType) Start(conf interface{}, enableDebug bool) {
	switch c {
	case configs.KafkaHTTP:
		connObj := &connection.KafkaHTTPConn{
			EnableDebugLog: enableDebug,
			Conf:           conf,
			SidelinePlugin: getSidelinePlugin(),
		}
		log.Println("Starting ", configs.KafkaHTTP)
		connObj.Run()
	case configs.KafkaFoxtrot:
		connObj := &connection.KafkaFoxtrotConn{
			EnableDebugLog: enableDebug,
			Conf:           conf,
		}
		log.Println("Starting ", configs.KafkaFoxtrot)
		connObj.Run()
	default:
		panic("Invalid Connection Type")

	}

}*/

//DMuxConfigSetting dumx obj
type DMuxConfigSetting struct {
	FilePath string
}

//DmuxConf hold config data
type DmuxConf struct {
	Name      string     `json:"name"`
	DMuxItems []DmuxItem `json:"dmuxItems"`
	// DMuxMap    map[string]KafkaHTTPConnConfig `json:"dmuxMap"`
	Logging logging.LogConf `json:"logging"`
}

//DmuxItem struct defines name and type of connection
type DmuxItem struct {
	Name       string                 `json:"name"`
	Disabled   bool                   `json:"disabled`
	ConnType   configs.ConnectionType `json:"connectionType"`
	Connection interface{}            `json:connection`
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
