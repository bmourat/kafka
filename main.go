package main

import (
	"context"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"pspclienttest/kafka"
	"strconv"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	broker := "localhost:9092"
	rnd := strconv.Itoa(rand.Intn(1000000))
	startTopic := "start-topic-" + rnd
	firstTopic := "first-topic-" + rnd
	secondTopic := "second-topic-" + rnd
	lastTopic := "end-topic-" + rnd
	count := 100
	ctx, cancel := context.WithCancel(context.Background())

	go start(ctx, broker, startTopic, count, "START")

	go service(ctx, broker, startTopic, firstTopic, "FIRST")
	go service(ctx, broker, firstTopic, secondTopic, "SECOND")
	go service(ctx, broker, secondTopic, lastTopic, "THIRD")

	go end(ctx, broker, lastTopic, cancel, count, "END")

	<-ctx.Done()
}

func start(ctx context.Context, broker string, topic string, count int, name string) {
	producer, _ := kafka.NewProducer([]string{broker}, topic)
	for i := 0; i < count; i++ {
		producer.Produce(ctx, &kafka.Record{Value: []byte(name + "-" + strconv.Itoa(i))}, func(record *kafka.Record, err error) {
			if err != nil {
				log.WithError(err).Panic("Error producing value from " + name)
			}
		})
		time.Sleep(time.Millisecond * 100)
	}
}

func end(ctx context.Context, broker string, topic string, cancel context.CancelFunc, count int, name string) {
	consumer, _ := kafka.NewConsumer([]string{broker}, []string{topic}, name)
	consumedMessageCount := 0
	consumer.Consume(ctx, func(record *kafka.Record, err error) {
		if err != nil {
			log.WithError(err).Panic("Error consuming value from " + topic + " in " + name)
		} else {
			consumedMessageCount++
			log.Info("Record arrived: " + string(record.Value))
			if consumedMessageCount >= count {
				cancel()
			}
		}
	})
}

func service(ctx context.Context, broker string, topicToConsume string, topicToProduce string, name string) {
	producer, _ := kafka.NewProducer([]string{broker}, topicToProduce)
	consumer, _ := kafka.NewConsumer([]string{broker}, []string{topicToConsume}, name)
	consumer.Consume(ctx, func(record *kafka.Record, err error) {
		if err != nil {
			log.WithError(err).Panic("Error consuming value from " + topicToConsume + " in " + name)
		} else {
			newRecord := kafka.Record{
				Value: []byte(name + "-" + string(record.Value)),
			}
			producer.Produce(ctx, &newRecord, func(record *kafka.Record, err error) {
				if err != nil {
					log.WithError(err).Panic("Error producing value from " + name)
				}
			})
		}
	})
}
