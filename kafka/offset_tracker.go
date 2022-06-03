package kafka

import (
	"github.com/go-dmux/metrics"
	"log"
	"time"
)

//OffsetTracker is interface which defines methods to track Messages which
//have been queued for processing
type OffsetTracker interface {
	TrackMe(kmsg KafkaMsg)
}

//KafkaOffsetTracker is implementation of OffsetTracker to track offsets for
//KafkaSource, KafkaMessage
type KafkaOffsetTracker struct {
	ch     chan KafkaMsg
	source *KafkaSource
	size   int
}

//TrackMe method ensures messages to track are enqued for tracking
func (k *KafkaOffsetTracker) TrackMe(kmsg KafkaMsg) {
	if len(k.ch) == k.size {
		log.Printf("warning: pending_acks threshold %d reached, please increase pending_acks size", k.size)
	}
	rawMsg := kmsg.GetRawMsg()
	//Create Source offset and send it for ingestion through source channel
	metrics.Registry.SourceCh <- metrics.SourceOffset{Topic: rawMsg.Topic, Partition: rawMsg.Partition, Offset: rawMsg.Offset}
	k.ch <- kmsg
}

//GetKafkaOffsetTracker is Global function to get instance of KafkaOffsetTracker
func GetKafkaOffsetTracker(size int, source *KafkaSource) OffsetTracker {
	k := &KafkaOffsetTracker{
		ch:     make(chan KafkaMsg, size),
		source: source,
		size:   size,
	}
	go k.run()
	return k
}

func (k *KafkaOffsetTracker) run() {
	for kmsg := range k.ch {
		for !kmsg.IsProcessed() {
			//log.Printf("waiting for url %s to process, queue_len %d", kmsg.GetURLPath(), len(k.ch))
			time.Sleep(100 * time.Microsecond)
		}
		k.source.CommitOffsets(kmsg)
	}
}
