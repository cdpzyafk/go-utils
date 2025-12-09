package common

import (
	"context"
	"sync"

	"go.uber.org/multierr"
	"golang.org/x/sync/semaphore"
)

type TaskGroup struct {
	err   error
	wg    sync.WaitGroup
	mutex sync.Mutex
}

func (ms *TaskGroup) Go(f func() error) *TaskGroup {
	ms.wg.Add(1)
	go func() {
		ms.done(f())
	}()
	return ms
}

func (ms *TaskGroup) Wait() error {
	ms.wg.Wait()
	return ms.err
}

func (ms *TaskGroup) done(err error) {
	defer ms.wg.Done()
	if err == nil {
		return
	}
	ms.mutex.Lock()
	defer ms.mutex.Unlock()
	ms.err = multierr.Append(ms.err, err)
}

type WeightedTaskGroup struct {
	syncer *TaskGroup
	weight *semaphore.Weighted
}

func NewWeightedTaskGroup(weight int) *WeightedTaskGroup {
	return &WeightedTaskGroup{
		syncer: &TaskGroup{},
		weight: semaphore.NewWeighted(int64(weight)),
	}
}

func (ms *WeightedTaskGroup) Go(f func() error) {
	ms.syncer.Go(func() error {
		_ = ms.weight.Acquire(context.Background(), 1)
		defer ms.weight.Release(1)

		return f()
	})
}

func (ms *WeightedTaskGroup) Wait() error {
	return ms.syncer.Wait()
}
