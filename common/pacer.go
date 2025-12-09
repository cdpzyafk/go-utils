package common

import (
	"math/rand"
	"time"

	"go.uber.org/atomic"
)

type Pacer struct {
	last *atomic.Time
	pace time.Duration
}

func (p *Pacer) Go(f func()) {
	if now := time.Now(); now.Sub(p.last.Load()) > p.pace {
		p.last.Store(now)
		go f()
	}
}

func (p *Pacer) Run(f func()) {
	if now := time.Now(); now.Sub(p.last.Load()) > p.pace {
		p.last.Store(now)
		f()
	}
}

func NewPacer(pace time.Duration) *Pacer {
	return &Pacer{
		pace: pace,
		last: atomic.NewTime(time.Now().Add(-pace * 2)),
	}
}

func NewPacerWithRand(pace time.Duration, extraSec int) *Pacer {
	randpace := time.Duration(rand.Intn(extraSec)) * time.Second
	return &Pacer{
		pace: pace + randpace,
	}
}

type TickPacer struct {
	pace uint64
	tick uint64
	last uint64
}

func (p *TickPacer) Go(f func()) {
	p.tick++
	if (p.tick - p.last) > p.pace {
		p.last = p.tick

		go f()
	}
}

func NewTickPacer(pace uint64) *TickPacer {
	return &TickPacer{
		pace: pace,
	}
}
