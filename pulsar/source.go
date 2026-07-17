package pulsar

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	pulsar "github.com/apache/pulsar-client-go/pulsar"
)

type PulsarSource struct {
	conf     PulsarConf
	client   pulsar.Client
	hook     SourceHook
	consumer pulsar.Consumer
}

func (p *PulsarSource) GetKey(msg interface{}) []byte {
	//TODO implement me
	panic("implement me")
}

func (p *PulsarSource) GetPartition(msg interface{}) int32 {
	//TODO implement me
	panic("implement me")
}

func (p *PulsarSource) GetValue(msg interface{}) []byte {
	//TODO implement me
	panic("implement me")
}

func (p *PulsarSource) GetOffset(msg interface{}) int64 {
	//TODO implement me
	panic("implement me")
}

func (p *PulsarSource) RegisterHook(hook SourceHook) {
	p.hook = hook
}

func GetPulsarSource(conf PulsarConf) *PulsarSource {
	return &PulsarSource{conf: conf}
}

// buildAuthentication constructs the Pulsar authentication provider based on
// the configured auth mode. machine_identity uses a secret-less AuthN
// token-exchange supplier; any other value (including empty) falls back to the
// legacy OAuth2 client-credentials flow for backward compatibility.
func (p *PulsarSource) buildAuthentication() (pulsar.Authentication, error) {
	if p.conf.AuthMode == AuthModeMachineIdentity {
		provider, err := newAuthTokenProvider(p.conf)
		if err != nil {
			return nil, err
		}
		log.Println("prepared pulsar auth via machine identity (token supplier)")
		return pulsar.NewAuthenticationTokenFromSupplier(provider.tokenSupplier()), nil
	}

	// Legacy client-credentials flow (requires a client secret).
	props := map[string]string{
		"type":          "client_credentials",
		"client_id":     p.conf.AuthClientId,
		"client_secret": p.conf.AuthClientSecret,
		"issuer_url":    p.conf.AuthIssuerURL,
	}
	privateKey, _ := json.Marshal(props)

	auth := pulsar.NewAuthenticationOAuth2(map[string]string{
		"type":       "client_credentials",
		"issuerUrl":  p.conf.AuthIssuerURL,
		"audience":   p.conf.AuthAudience,
		"privateKey": fmt.Sprintf("data://%s", string(privateKey)),
		"clientId":   p.conf.AuthClientId,
	})
	log.Println("prepared pulsar oauth2")
	return auth, nil
}

// Generate is Source method implementation, which connects to Pulsar and pushes
// PulsarMessage into the channel
func (p *PulsarSource) Generate(out chan<- interface{}) {
	auth, err := p.buildAuthentication()
	if err != nil {
		panic(err)
	}

	// Authenticate client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            p.conf.Url,
		Authentication: auth,
	})
	log.Println("prepared client with authentication")
	if err != nil {
		panic(err)
	}
	var subsriptionType pulsar.SubscriptionType
	if strings.EqualFold(p.conf.SubscriptionType, "KeyShared") {
		subsriptionType = pulsar.KeyShared
		log.Printf("starting with subscription type keyshared \n")
	} else {
		log.Printf("starting with subscription type failover \n")
		subsriptionType = pulsar.Failover
	}

	// Open channel for consumer
	channel := make(chan pulsar.ConsumerMessage, 100)
	options := pulsar.ConsumerOptions{
		Topic:            p.conf.Topic,
		SubscriptionName: p.conf.SubscriptionName,
		Type:             subsriptionType,
	}
	options.MessageChannel = channel
	consumer, err := client.Subscribe(options)
	if err != nil {
		client.Close()
		panic(err)
	}

	if p.conf.ForceRestart && p.conf.ReadNewest {

		log.Println("Setting force restart as true and readnewest as true, the consumers will start listenning from time  = %v", time.Now().UTC())
		//er := consumer.Seek(pulsar.EarliestMessageID())
		er := consumer.SeekByTime(time.Now().UTC())
		if er != nil {
			client.Close()
			panic(er)
		}
		log.Println("Done setting force restart as true and readnewest as true")
	} else if p.conf.ForceRestart && p.conf.SeekByTime != 0 {
		seekByTime := time.Duration(p.conf.SeekByTime) * time.Second
		updatedTime := time.Now().UTC().Add(-seekByTime)
		er := consumer.SeekByTime(updatedTime)
		if er != nil {
			client.Close()
			panic(er)
		}
		log.Printf("Done Setting SeekByTime as %v %v\n", seekByTime.String(), updatedTime)
	} else if p.conf.ForceRestart {
		log.Println("Setting force restart as true")

		er := consumer.SeekByTime(time.Unix(0, 0))
		if er != nil {
			client.Close()
			panic(er)
		}
		log.Println("Done Setting force restart as true")

	} else {
		log.Println("no specific configs were given during the start of this topology, will use the sane defaults")
	}

	p.client = client
	p.consumer = consumer
	pulsarMessageFactoryImpl := getPulsarMessageFactory()

	// Receive messages from channel. The channel returns a struct which contains message and the consumer from where
	// the message was received. It's not necessary here since we have 1 single consumer, but the channel could be
	// shared across multiple consumers as well
	for cm := range channel {
		processor := pulsarMessageFactoryImpl.Create(cm)
		if p.hook != nil {
			p.hook.Pre(processor)
		}
		out <- processor
	}
}

// Stop method implements Source interface stop method, to Stop the KafkaConsumer
func (p *PulsarSource) Stop() {
	p.consumer.Close()
	p.client.Close()
}

func (p *PulsarSource) commitCursor(data MessageProcessor) {
	log.Printf("going to ack message " + data.GetRawMsg().Key() + " " + data.GetRawMsg().ID().String() + "\n")
	p.consumer.Ack(data.GetRawMsg())
}
