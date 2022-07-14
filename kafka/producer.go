package kafka

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	producer *kgo.Client
}

func (p *Producer) Produce(ctx context.Context, record *Record, result func(*Record, error)) {
	p.producer.Produce(ctx, record.mapToClientRecord(), func(franzRecord *kgo.Record, err error) {
		updateFromClientRecord(franzRecord, record)
		result(record, err)
	})
}

func (p *Producer) Close() {
	p.producer.Close()
}

func (record *Record) mapToClientRecord() *kgo.Record {
	return &kgo.Record{
		Key:       record.Key,
		Value:     record.Value,
		Topic:     record.Topic,
		Partition: record.Partition,
		Timestamp: record.Timestamp,
	}
}

func updateFromClientRecord(record *kgo.Record, originalRecord *Record) {
	originalRecord.Partition = record.Partition
	originalRecord.Timestamp = record.Timestamp
}
