package kafkareader

import (
	"errors"
)

var (
	ErrNoBrokers = errors.New("no brokers")
	ErrNoTopic   = errors.New("no topic")
	ErrNoHandler = errors.New("no handler")
)
