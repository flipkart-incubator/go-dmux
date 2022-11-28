package kafka

import (
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/flipkart-incubator/go-dmux/core"
	"github.com/stretchr/testify/assert"
	// "hash/fnv"
	// "time"
)

type ConsoleSink struct {
}

func (c *ConsoleSink) Consume(msg interface{}) {
	data := msg.(KafkaMsg)
	fmt.Println(string(data.GetRawMsg().Key))
}

func (c *ConsoleSink) BatchConsume(msgs []interface{}, version int) {
	for _, msg := range msgs {
		c.Consume(msg)
	}
}

func (c *ConsoleSink) Clone() core.Sink {
	return c
}

// **************** HashLogic ***********

//KafkaMsgHasher implements hasher
//a hash logic implementation of KafkaMessage.
//This runs consistenHashin on Key of KafkaKeyedMessage
type KafkaMsgHasher struct{}

//ComputeHash method for KafkaMessage
func (o *KafkaMsgHasher) ComputeHash(data interface{}) int {
	val := data.(KafkaMsg)
	h := fnv.New32a()
	h.Write(val.GetRawMsg().Key)
	return int(h.Sum32())
}

//GetKafkaMsgHasher is Gloab function to get instance of KafkaMsgHasher
func GetKafkaMsgHasher() core.Hasher {
	return new(KafkaMsgHasher)
}

// Factory

type KafkaMsgFactoryImpl struct {
}

func (*KafkaMsgFactoryImpl) Create(msg *sarama.ConsumerMessage) KafkaMsg {
	return &KafkaMessage{
		Msg:       msg,
		Processed: false,
	}
}

type KafkaMessage struct {
	Msg       *sarama.ConsumerMessage
	Processed bool
}

func (k *KafkaMessage) MarkDone() {
	k.Processed = true
}

func (k *KafkaMessage) GetRawMsg() *sarama.ConsumerMessage {
	return k.Msg
}
func (k *KafkaMessage) IsProcessed() bool {
	return k.Processed
}

func TestKafkaSource(t *testing.T) {
	fmt.Println("running test TestKafkaSource")
	hasher := new(KafkaMsgHasher)
	d := core.GetHashDistribution(hasher)

	kconf := KafkaConf{
		ConsumerGroupName: "go-dmux-test",
		ZkPath:            "zookeeper-1:2181/kafka",
		Topic:             "my-topic",
		ForceRestart:      false,
	}

	kfactory := &KafkaMsgFactoryImpl{}
	source := GetKafkaSource(kconf, kfactory)
	sink := new(ConsoleSink)
	dconf := core.DmuxConf{
		Size:        4,
		SourceQSize: 1,
		SinkQSize:   100,
	}
	dmux := core.GetDmux(dconf, d)
	// dmux.Connect(source, sink)
	fmt.Printf("source: %v", source)
	assert.NotNil(t, source, "Source should not be Nil")
	assert.NotNil(t, sink, "Sink should not be Nil")
	assert.NotNil(t, dmux, "Dmux should not be Nil")
	// dmux.Await(30 * time.Second)
}
