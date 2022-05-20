package connection

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/go-dmux/core"
	sink "github.com/go-dmux/http"
	source "github.com/go-dmux/kafka"
)

// **************** CONFIG ***********

//KafkaFoxtrotConnConfig holds config to connect KafkaSource to http_sink
type KafkaFoxtrotConnConfig struct {
	KafkaHTTPConnConfig
}

//KafkaFoxtrotConn struct to abstract this connections Run
type KafkaFoxtrotConn struct {
	EnableDebugLog bool
	Conf           interface{}
}

//CustomURLKey  place holder name, which will be replaced by kafka key
const CustomURLKey = "__KEY_NAME__"

func (c *KafkaFoxtrotConn) getConfiguration() *KafkaFoxtrotConnConfig {
	data, _ := json.Marshal(c.Conf)
	var config *KafkaFoxtrotConnConfig
	json.Unmarshal(data, &config)
	return config
}

//Run method to start this Connection from source to sink
func (c *KafkaFoxtrotConn) Run() {
	conf := c.getConfiguration()
	log.Println("starting kafka_foxtrot with conf", conf)
	if c.EnableDebugLog {
		// enable sarama logs if booted with debug logs
		log.Println("enabling sarama logs")
		sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	}
	kafkaMsgFactory := getKafkaFoxtrotFactory()
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
	dmux.Connect(src, sk)
	dmux.Join()
}

//******************KafkaSource Interface implementation ******

//KafkaFoxtrotMessage is data attribute that will be passed from Source to Sink
type KafkaFoxtrotMessage struct {
	KafkaMessage
}

func getKafkaFoxtrotFactory() source.KafkaMsgFactory {
	return new(kafkaFoxtrotFactoryImpl)
}

type kafkaFoxtrotFactoryImpl struct {
}

//Create KafkaMessage which implments KafkaMsg and HTTPMsg and wraps sarama.ConsumerMessage
func (*kafkaFoxtrotFactoryImpl) Create(msg *sarama.ConsumerMessage) source.KafkaMsg {
	kafkaMsg := &KafkaFoxtrotMessage{}
	kafkaMsg.KafkaMessage.Msg = msg
	kafkaMsg.KafkaMessage.Processed = false
	return kafkaMsg
}

// **************** HTTPSink Interface implementation ***********

//GetPayload implements HTTPMsg for HttpSink processing
func (k *KafkaFoxtrotMessage) GetPayload() []byte {
	return k.Msg.Value
}

//GetHeaders implements HTTPMsg for HttpSink processing
func (k *KafkaFoxtrotMessage) GetHeaders(conf sink.HTTPSinkConf) map[string]string {
	header := make(map[string]string)
	for _, val := range conf.Headers {
		header[val["name"]] = val["value"]
	}
	header["Content-Type"] = "application/json" // force json for foxtrot

	return header
}

//GetURL implements HTTPMsg for HttpSink processing
//This implementation passes in query parameter partition and offset for debuggin
func (k *KafkaFoxtrotMessage) GetURL(endpoint string) string {
	// log.Println("In GetURL ")
	url := strings.Replace(endpoint, CustomURLKey, string(k.Msg.Key), 1)
	//debug query string to help in debuggin
	url = url + "?debug=" + k.Msg.Topic + "," + strconv.FormatInt(int64(k.Msg.Partition), 10) +
		// "," + string(k.Msg.Key)
		"," + strconv.FormatInt(k.Msg.Offset, 10)
	// log.Println("Got URl ", url)
	return url
}

func (k *KafkaFoxtrotMessage) GetDebugPath() string {
	if k.URL == "" {
		k.URL = "/" + k.Msg.Topic + "/" + strconv.FormatInt(int64(k.Msg.Partition), 10) +
			"/" + string(k.Msg.Key) + "/" + strconv.FormatInt(k.Msg.Offset, 10)
	}
	return k.URL
}

//BatchURL implements HTTPMsg for HttpSink processing
//This implementation passes in query parameter partition and offset for debuggin
func (k *KafkaFoxtrotMessage) BatchURL(msgs []interface{}, endpoint string, version int) string {
	url := strings.Replace(endpoint, CustomURLKey, string(k.Msg.Key), 1)
	url = url + "/bulk"

	var builder strings.Builder
	topic := ""
	for i, msg := range msgs {
		kmsg := msg.(*KafkaFoxtrotMessage)
		if i == 0 {
			topic = kmsg.Msg.Topic
		} else {
			builder.WriteString("~")
		}
		builder.WriteString(strconv.FormatInt(int64(kmsg.Msg.Partition), 10))
		// builder.WriteString(",")
		// builder.WriteString(string(kmsg.Msg.Key))
		builder.WriteString(",")
		builder.WriteString(strconv.FormatInt(kmsg.Msg.Offset, 10))
	}
	return url + "?topic=" + topic + "&batch=" + builder.String()
}

//BatchPayload implements HTTPMsg for HttpSink processing
func (k *KafkaFoxtrotMessage) BatchPayload(msgs []interface{}, version int) []byte {
	payload := make([]interface{}, len(msgs))
	for i := 0; i < len(msgs); i++ {
		msg := msgs[i].(*KafkaFoxtrotMessage)
		data := msg.GetPayload()
		var obj interface{}
		err := json.Unmarshal(data, &obj)
		if err != nil {
			panic("failed to unmarshal data in batch payload construction")
		}
		payload[i] = obj
	}

	output, err := json.Marshal(payload)
	if err != nil {
		panic("failed to marshal batch data into payload construction")
	}
	return output
}
