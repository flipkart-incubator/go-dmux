package kafka

import (
	"context"
	"github.com/flipkart-incubator/go-dmux/offset_monitor"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/flipkart-incubator/go-dmux/kafka/consumer-group"
	"github.com/flipkart-incubator/go-dmux/kafka/kazoo-go"
)

//KafkaSourceHook to track messages coming out of the source in order
type KafkaSourceHook interface {
	//Pre called before passing the message to DMux
	Pre(k KafkaMsg)
}
type KafkaMsgFactory interface {
	//Create call to wrap consumer message inside KafkaMsg
	Create(msg *sarama.ConsumerMessage) KafkaMsg
}
type KafkaMsg interface {
	MarkDone()
	GetRawMsg() *sarama.ConsumerMessage
	IsProcessed() bool
}

//KafkaSource is Source implementation which reads from Kafka. This implementation
//uses sarama lib and wvanbergen implementation of HA Kafka Consumer using
//zookeeper
type KafkaSource struct {
	conf       KafkaConf
	consumer   *consumergroup.ConsumerGroup
	hook       KafkaSourceHook
	factory    KafkaMsgFactory
	offMonitor offset_monitor.OffMonitor
}

//KafkaConf holds configuration options for KafkaSource
type KafkaConf struct {
	ConsumerGroupName string `json:"name"`
	ZkPath            string `json:"zk_path"`
	Topic             string `json:"topic"`
	ForceRestart      bool   `json:"force_restart"`
	ReadNewest        bool   `json:"read_newest"`
	KafkaVersion      int    `json:"kafka_version_major"`
	SASLEnabled       bool   `json:"sasl_enabled"`
	SASLUsername      string `json:"username"`
	SASLPasswordKey   string `json:"passwordKey"`
}

//GetKafkaSource method is used to get instance of KafkaSource.
func GetKafkaSource(conf KafkaConf, factory KafkaMsgFactory, offMonitor offset_monitor.OffMonitor) *KafkaSource {
	return &KafkaSource{
		conf:       conf,
		factory:    factory,
		offMonitor: offMonitor,
	}
}

//RegisterHook used to registerHook with KafkSource
func (k *KafkaSource) RegisterHook(hook KafkaSourceHook) {
	k.hook = hook
}

// //MarkDone is a behaviour added to KafkaMessage to update when it has been
// //processed by the Sink
// func (k *KafkaMessage) MarkDone() {
// 	k.Processed = true
// }

//Generate is Source method implementation, which connect to Kafka and pushes
//KafkaMessage into the channel
func (k *KafkaSource) Generate(out chan<- interface{}) {

	kconf := k.conf
	//config
	config := consumergroup.NewConfig()

	if kconf.KafkaVersion > 1 {
		config.Version = sarama.V2_0_1_0
	}
	config.Offsets.ResetOffsets = kconf.ForceRestart
	if kconf.ForceRestart && kconf.ReadNewest {
		config.Offsets.Initial = sarama.OffsetNewest
	}

	if kconf.SASLEnabled {
		//sarama config plain by default
		config.Net.SASL.User = kconf.SASLUsername
		config.Net.SASL.Password = os.Getenv(kconf.SASLPasswordKey)
		config.Net.SASL.Enable = true
	}

	config.Offsets.ProcessingTimeout = 10 * time.Second

	//parse zookeeper
	zookeeperNodes, chroot := kazoo.ParseConnectionString(kconf.ZkPath)
	config.Zookeeper.Chroot = chroot

	//get topics
	kafkaTopics := []string{kconf.Topic}

	// create consumer
	consumer, err := consumergroup.JoinConsumerGroup(kconf.ConsumerGroupName, kafkaTopics, zookeeperNodes, config)
	if err != nil {
		panic(err)
	}

	k.consumer = consumer

	//context for gracefully shutting down the producerConsumer offset reader goroutine
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	k.offMonitor.StartProducerConsumerMonitor(consumer.GetBrokerList(), kconf.Topic, k.conf.ConsumerGroupName, consumer, ctx)

	for message := range k.consumer.Messages() {
		//TODO handle Create failure
		kafkaMsg := k.factory.Create(message)

		if k.hook != nil {
			//TODO handle PreHook failure
			k.hook.Pre(kafkaMsg)
		}

		out <- kafkaMsg
	}

}

//Stop method implements Source interface stop method, to Stop the KafkaConsumer
func (k *KafkaSource) Stop() {
	err := k.consumer.Close()
	if err != nil {
		panic(err)
	}
}

//CommitOffsets enables cliento explicity commit the Offset that is processed.
func (k *KafkaSource) CommitOffsets(data KafkaMsg) (bool, error) {
	return k.consumer.CommitUpto(data.GetRawMsg())
}
