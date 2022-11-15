package connection

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/flipkart-incubator/go-dmux/core"
	sink "github.com/flipkart-incubator/go-dmux/http"
	source "github.com/flipkart-incubator/go-dmux/kafka"
	"log"
	"os"
)

// **************** CONFIG ***********

//KafkaHTTPConnWithSideline struct to abstract this connections Run
type KafkaHTTPConnWithSideline struct {
	EnableDebugLog bool
	Conf           interface{}
	SidelinePlugin interface{}
}

func (c *KafkaHTTPConnWithSideline) getConfiguration() *KafkaHTTPConnConfig {
	data, _ := json.Marshal(c.Conf)
	var config *KafkaHTTPConnConfig
	json.Unmarshal(data, &config)
	return config
}

//Run method to start this Connection from source to sink
func (c *KafkaHTTPConnWithSideline) Run() {
	conf := c.getConfiguration()
	fmt.Println("starting go-dmux with conf", conf)
	if c.EnableDebugLog {
		// enable sarama logs if booted with debug logs
		log.Println("enabling sarama logs")
		sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	}
	kafkaMsgFactory := getKafkaHTTPFactory()
	src := source.GetKafkaSource(conf.Source, kafkaMsgFactory)
	offsetTracker := source.GetKafkaOffsetTracker(conf.PendingAcks, src)
	hook := GetKafkaHook(offsetTracker, c.EnableDebugLog)
	sk := sink.GetHTTPSink(conf.Dmux.Size, conf.Sink)
	sk.RegisterHook(hook)
	src.RegisterHook(hook)

	//hash distribution
	h := GetKafkaMsgHasher()

	d := core.GetDistribution(conf.Dmux.DistributorType, h)

	dmux := core.GetDmux(conf.Dmux, d)
	dmux.ConnectWithSideline(src, sk, c.SidelinePlugin)
	dmux.Join()
}

/*
func parseConf(path string) KafkaHTTPConnConfig {

	raw, err := ioutil.ReadFile(path)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	var conf KafkaHTTPConnConfig
	json.Unmarshal(raw, &conf)

	return conf
}
*/
//******************KafkaSource Interface implementation ******
