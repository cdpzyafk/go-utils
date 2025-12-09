package kafkareader

import (
	"time"

	"github.com/cdpzyafk/go-utils/jsonize"
	"github.com/cdpzyafk/go-utils/kafkalib"
	"github.com/cdpzyafk/go-utils/logutil"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var (
	log = logutil.GetLogger().With(zap.String("pkg", "kfreader"))
)

type Reader struct {
	handleEvent        func(*zap.Logger, kafka.Message)
	log                *zap.Logger
	topic              string
	brokers            []string
	minBytes, maxBytes int
	readers            []*PartitionReader
	partitions         []kafka.Partition
	status             bool
	closing            bool
	readBackoffMin     time.Duration
}

func CreateReader(cfg *Config) (*Reader, error) {
	if len(cfg.Brokers) == 0 {
		return nil, ErrNoBrokers
	}
	if cfg.Topic == "" {
		return nil, ErrNoTopic
	}
	if cfg.Handler == nil {
		return nil, ErrNoHandler
	}
	if cfg.MinBytes <= 0 {
		cfg.MinBytes = MINBYTES
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = MAXBYTES
	}
	if cfg.MaxBytes < cfg.MinBytes {
		cfg.MaxBytes = cfg.MinBytes + 64
	}
	if cfg.ReadBackoffMin <= READBACKOFFMIN {
		cfg.ReadBackoffMin = READBACKOFFMIN
	}

	log := log
	if cfg.Name != "" {
		log = log.With(zap.String("name", cfg.Name))
	}

	partitions, err := kafkalib.LookupPartitions(log, cfg.Brokers, cfg.Topic)
	if err != nil {
		log.Error("failed look partions",
			zap.Error(err),
			zap.String("brokers", jsonize.V(cfg.Brokers, false)),
			zap.String("topic", cfg.Topic))
		return nil, err
	}

	r := &Reader{
		log:            log,
		topic:          cfg.Topic,
		brokers:        cfg.Brokers,
		handleEvent:    cfg.Handler,
		partitions:     partitions,
		minBytes:       cfg.MinBytes,
		maxBytes:       cfg.MaxBytes,
		readBackoffMin: cfg.ReadBackoffMin,
		readers:        make([]*PartitionReader, 0, len(partitions)),
	}

	for i := 0; i < len(partitions); i++ {
		partition := partitions[i]
		reader, err := NewPartitionReader(r, partition)
		if err != nil {
			r.log.Error("createReader failed",
				zap.Error(err),
				zap.Int("id", partition.ID),
				zap.String("topic", cfg.Topic))
			return nil, err
		}
		r.readers = append(r.readers, reader)
	}

	// r.Start()

	return r, nil
}

func (p *Reader) Start() {
	for _, reader := range p.readers {
		go reader.Start()
	}
	p.status = true
}

func (p *Reader) Close() {
	p.closing = true
	for _, reader := range p.readers {
		reader.Stop()
	}
}
