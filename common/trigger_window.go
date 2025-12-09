package common

import (
	"sync"
	"time"
)

type TriggerWindow[T comparable] struct {
	mu       *sync.Mutex
	records  map[T][]time.Time
	interval time.Duration
	limit    int
}

func (tc *TriggerWindow[T]) Trigger(symbol T) (reached bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	currentTime := time.Now()
	if _, exists := tc.records[symbol]; !exists {
		tc.records[symbol] = make([]time.Time, 0, 32)
	}

	var validTimes []time.Time
	for _, t := range tc.records[symbol] {
		if currentTime.Sub(t) <= tc.interval {
			validTimes = append(validTimes, t)
		}
	}
	tc.records[symbol] = validTimes
	tc.records[symbol] = append(tc.records[symbol], currentTime)

	reached = len(tc.records[symbol]) >= tc.limit
	if reached { // 达到次数后清空
		tc.records[symbol] = make([]time.Time, 0, 32)
	}
	return
}

func NewTriggerWindow[T comparable](limit int, interval time.Duration) *TriggerWindow[T] {
	return &TriggerWindow[T]{
		mu:       &sync.Mutex{},
		limit:    limit,
		interval: interval,
		records:  make(map[T][]time.Time, 128),
	}
}
