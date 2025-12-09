package kafkareader

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type PartitionReader struct {
	parent    *Reader
	log       *zap.Logger
	reader    *kafka.Reader
	partition kafka.Partition
	stopCh    chan struct{}
}

func (pr *PartitionReader) Start() {
	ctx := context.Background()

	maxOffset := int64(0)

	for {
		if msg, err := pr.reader.FetchMessage(ctx); err == nil {
			if msg.Offset <= maxOffset {
				continue
			}
			maxOffset = msg.Offset
			pr.parent.handleEvent(pr.log, msg)
		} else {
			time.Sleep(time.Millisecond * 200)
			pr.log.Error("reader broken, start to recover...", zap.Error(err))
			pr.recover()
		}
	}
}

func (pr *PartitionReader) Stop() {
	// TODO 支持正确关闭
}

func (pr *PartitionReader) recover() {
	if pr.reader != nil {
		pr.reader.Close()
	}

	for {
		if err := pr.createReader(); err != nil {
			pr.log.Error("recover failed", zap.Error(err))
			time.Sleep(time.Second * 3)
			continue
		}
		break
	}
}

func (pr *PartitionReader) createReader() error {
	pr.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        pr.parent.brokers,
		Topic:          pr.parent.topic,
		Partition:      pr.partition.ID,
		MinBytes:       pr.parent.minBytes,
		MaxBytes:       pr.parent.maxBytes,
		ReadBackoffMin: pr.parent.readBackoffMin,
	})
	err := pr.reader.SetOffset(kafka.LastOffset)
	return err
}

func NewPartitionReader(reader *Reader, partition kafka.Partition) (*PartitionReader, error) {
	pr := &PartitionReader{
		parent:    reader,
		partition: partition,
		stopCh:    make(chan struct{}, 1),
		log:       reader.log.With(zap.Int("partition", partition.ID)),
	}

	err := pr.createReader()

	return pr, err
}
