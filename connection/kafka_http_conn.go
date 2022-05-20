package connection

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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

//KafkaHTTPConnConfig holds config to connect KafkaSource to http_sink
type KafkaHTTPConnConfig struct {
	Dmux        core.DmuxConf     `json:"dmux"`
	Source      source.KafkaConf  `json:"source"`
	Sink        sink.HTTPSinkConf `json:"sink"`
	PendingAcks int               `json:"pending_acks"`
}

//KafkaHTTPConn struct to abstract this connections Run
type KafkaHTTPConn struct {
	EnableDebugLog bool
	Conf           interface{}
}

func (c *KafkaHTTPConn) getConfiguration() *KafkaHTTPConnConfig {
	data, _ := json.Marshal(c.Conf)
	var config *KafkaHTTPConnConfig
	json.Unmarshal(data, &config)
	return config
}

//Run method to start this Connection from source to sink
func (c *KafkaHTTPConn) Run() {
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
	dmux.Connect(src, sk)
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

//KafkaMessage is data attribute that will be passed from Source to Sink
type KafkaMessage struct {
	Msg       *sarama.ConsumerMessage
	Processed bool   //marker to know once this message has been processed by Sink
	Sidelined bool   // marker to know if the message gets sideliend
	URL       string // added to avoid GetURLPath to repeate concat during logging
}

func getKafkaHTTPFactory() source.KafkaMsgFactory {
	return new(kafkaHTTPFactoryImpl)
}

type kafkaHTTPFactoryImpl struct {
}

//Create KafkaMessage which implments KafkaMsg and HTTPMsg and wraps sarama.ConsumerMessage
func (*kafkaHTTPFactoryImpl) Create(msg *sarama.ConsumerMessage) source.KafkaMsg {
	return &KafkaMessage{
		Msg:       msg,
		Processed: false,
	}
}

//MarkDone the  KafkaMessage as processed
func (k *KafkaMessage) MarkDone() {
	k.Processed = true
}

//GetRawMsg returns saram.ConsumerMessage under KafkaMessage
func (k *KafkaMessage) GetRawMsg() *sarama.ConsumerMessage {
	return k.Msg
}

//IsProcessed returns true if KafkaMessage was MarkDone
func (k *KafkaMessage) IsProcessed() bool {
	return k.Processed
}

// *****************************************

// **************** Hooks ***********

//KafkaOffsetHook implments HTTPSinkHook amd KafkaSourceHook interface to track kafka offsets
type KafkaOffsetHook struct {
	offsetTracker  source.OffsetTracker
	enableDebugLog bool
}

//Pre is invoked - before KafaSource pushes message to DMux. This implementation
//invokes OffsetTracker TrackMe method here, to ensure the Message to track is
//queued before its execution
func (h *KafkaOffsetHook) Pre(data source.KafkaMsg) {
	// fmt.Println(h.enableDebugLog)
	// msg := data.(*KafkaMessage)
	h.offsetTracker.TrackMe(data)
	if h.enableDebugLog {
		msg := data.(sink.HTTPMsg)
		log.Printf("%s after kafka source", msg.GetDebugPath())
	}
}

//PreHTTPCall is invoked - before HttpSink exection.
func (h *KafkaOffsetHook) PreHTTPCall(msg interface{}) {
	if h.enableDebugLog {
		data := msg.(sink.HTTPMsg)
		log.Printf("%s before http sink", data.GetDebugPath())
	}
}

//PostHTTPCall is invoked - after HttpSink execution. This implementation calls
//KafkaMessage MarkDone method on the data argument of Post, to mark this
//message and sucessfuly processed.
func (h *KafkaOffsetHook) PostHTTPCall(msg interface{}, success bool) {
	data := msg.(source.KafkaMsg)
	if success {
		data.MarkDone()
	}
	if h.enableDebugLog {
		val := msg.(sink.HTTPMsg)
		log.Printf("%s after http sink, status = %t", val.GetDebugPath(), success)
	}
}

//GetKafkaHook is a global function that returns instance of KafkaOffsetHook
func GetKafkaHook(offsetTracker source.OffsetTracker, enableDebugLog bool) *KafkaOffsetHook {
	return &KafkaOffsetHook{offsetTracker, enableDebugLog}
}

// **************** HashLogic ***********

//KafkaMsgHasher implements hasher
//a hash logic implementation of KafkaMessage.
//This runs consistenHashin on Key of KafkaKeyedMessage
type KafkaMsgHasher struct{}

//ComputeHash method for KafkaMessage
func (o *KafkaMsgHasher) ComputeHash(data interface{}) int {
	val := data.(source.KafkaMsg)
	h := fnv.New32a()
	h.Write(val.GetRawMsg().Key)
	return int(h.Sum32())
}

//GetKafkaMsgHasher is Gloab function to get instance of KafkaMsgHasher
func GetKafkaMsgHasher() core.Hasher {
	return new(KafkaMsgHasher)
}

// **************** HTTPSink Interface implementation ***********

//GetPayload implements HTTPMsg for HttpSink processing
func (k *KafkaMessage) GetPayload() []byte {
	return k.Msg.Value
}

//GetHeaders implements HTTPMsg for HttpSink processing
func (k *KafkaMessage) GetHeaders(conf sink.HTTPSinkConf) map[string]string {
	header := make(map[string]string)
	for _, val := range conf.Headers {
		header[val["name"]] = val["value"]
	}
	header["Content-Type"] = "application/octet-stream" //force bytestream

	return header
}

//GetURL implements HTTPMsg for HttpSink processing
func (k *KafkaMessage) GetURL(endpoint string) string {
	return endpoint + k.GetDebugPath()
}

func (k *KafkaMessage) GetDebugPath() string {
	if k.URL == "" {
		k.URL = "/" + k.Msg.Topic + "/" + strconv.FormatInt(int64(k.Msg.Partition), 10) +
			"/" + string(k.Msg.Key) + "/" + strconv.FormatInt(k.Msg.Offset, 10)
	}
	return k.URL
}

//BatchURL implements HTTPMsg for HttpSink processing
func (k *KafkaMessage) BatchURL(msgs []interface{}, endpoint string, version int) string {
	var builder strings.Builder
	topic := ""
	if version == 1 {
		for i, msg := range msgs {
			kmsg := msg.(*KafkaMessage)
			if i == 0 {
				topic = kmsg.Msg.Topic
			} else {
				builder.WriteString("~")
			}
			builder.WriteString(strconv.FormatInt(int64(kmsg.Msg.Partition), 10))
			builder.WriteString(",")
			builder.WriteString(string(kmsg.Msg.Key))
			builder.WriteString(",")
			builder.WriteString(strconv.FormatInt(kmsg.Msg.Offset, 10))
		}
		return endpoint + "/" + topic + "?batch=" + builder.String()
	} else {
		if len(msgs) > 0 {
			topic = msgs[0].(*KafkaMessage).Msg.Topic
		}
		return endpoint + "/" + topic
	}
}

//BatchPayload implements HTTPMsg for HttpSink processing
func (k *KafkaMessage) BatchPayload(msgs []interface{}, version int) []byte {
	payload := make([][]byte, len(msgs))
	partition := int(0)
	for i := 0; i < len(msgs); i++ {
		msg := msgs[i].(*KafkaMessage)
		if version == 1 {
			payload[i] = msg.GetPayload()
		} else {
			partition = int(msg.Msg.Partition)
			offset := msg.Msg.Offset
			payload[i] = core.EncodePayload(msg.Msg.Key, offset, msg.GetPayload())
		}
	}
	if version == 1 {
		return core.Encode(payload)
	} else {
		return core.EncodeV2(partition, payload)
	}
}

// func (k *KafkaMessage) GetURL(urlPath string, customURL bool) string {
// 	if customURL {
// 		return strings.Replace(endpoint, CustomURLKey, string(k.Msg.Key), 1)
// 	}
// 	return endpoint + urlPath
// }
