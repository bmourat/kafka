package kafka

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	consumer *kgo.Client
}

func (c *Consumer) Consume(ctx context.Context, result func(*Record, error)) {
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				break
			default:
				fetches := c.consumer.PollFetches(ctx)
				if err := fetches.Err(); err != nil {
					log.WithError(err).Error("Error consuming values")
					result(nil, err)
				}
				for iter := fetches.RecordIter(); !iter.Done(); {
					result(mapFromClientRecord(iter.Next()), nil)
				}
			}
		}
	}(ctx)
}

func (c *Consumer) Close() {
	c.consumer.Close()
}

func mapFromClientRecord(record *kgo.Record) *Record {
	return &Record{
		Key:       record.Key,
		Value:     record.Value,
		Topic:     record.Topic,
		Partition: record.Partition,
		Timestamp: record.Timestamp,
	}
}
