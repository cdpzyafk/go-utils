package kafkareader

import (
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	MINBYTES       = 512
	MAXBYTES       = 1024 * 1024 * 4
	READBACKOFFMIN = time.Millisecond * 100
)

type Config struct {
	Name           string
	Brokers        []string
	Topic          string
	MinBytes       int           // default MINBYTES
	MaxBytes       int           // default MAXBYTES
	ReadBackoffMin time.Duration // default READBACKOFFMIN
	Handler        func(*zap.Logger, kafka.Message)
}
