package main

import (
	"strings"

	"github.com/Shopify/sarama"
	logger "github.com/ricardo-ch/go-logger"
)

// Producer ...
type Producer struct {
	Sync sarama.SyncProducer
}

var newSyncProducer = sarama.NewSyncProducer

// NewProducer : creates the kafka producer
func NewProducer(brokers []string) (Producer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	sync, err := newSyncProducer(brokers, config)
	if err != nil {
		return Producer{
			Sync: sync,
		}, err
	}

	return Producer{
		Sync: sync,
	}, nil
}

// ProduceMessage : produces the kafka message
func (p Producer) ProduceMessage(topic string, key, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
		Key:   sarama.ByteEncoder(key),
	}

	_, _, err := p.Sync.SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}

func main() {

	brokerPeers := "localhost:9092"

	// init Producer
	producer, err := NewProducer(strings.Split(brokerPeers, ","))
	if err != nil {
		logger.Error(err.Error())
		return
	}

	producer.ProduceMessage("test-topic", []byte("key"), []byte(`{"test_id":"test-1", "description":"my first elasticsearch sink connector with kafka connect"}`))
}
