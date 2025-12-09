package kafkalib

import (
	"context"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var (
	ErrNonePartionFound = errors.New("none partition found")
)

// LookupPartitions 轮询所有broker,查找对应topic的所有partions
func LookupPartitions(log *zap.Logger, brokers []string, topic string) ([]kafka.Partition, error) {
	for _, addr := range brokers {
		if partitions, err := lookupPartitions(addr, topic); err == nil {
			return partitions, nil
		} else {
			log.Error("lookupPartions failed", zap.Error(err),
				zap.String("addr", addr),
				zap.String("topic", topic))
		}
	}

	return nil, ErrNonePartionFound
}

func lookupPartitions(addr, topic string) ([]kafka.Partition, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return kafka.LookupPartitions(ctx, "tcp", addr, topic)
}
