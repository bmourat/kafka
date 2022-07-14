package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

type Record struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Timestamp time.Time
}

func NewProducer(brokers []string, defaultTopic string) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(defaultTopic),
		kgo.AllowAutoTopicCreation(),
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Producer{cl}, nil
}

func NewConsumer(brokers []string, topics []string, group string) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup(group),
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Consumer{cl}, nil
}
