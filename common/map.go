package common

import (
	"sync"

	"github.com/samber/mo"
)

type SyncMap[K comparable, T any] struct {
	mu *sync.RWMutex
	d  map[K]T
}

func (lm *SyncMap[K, T]) Get(k K) (T, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	v, ok := lm.d[k]
	return v, ok
}

func (lm *SyncMap[K, T]) Update(key K, n T) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.d[key] = n
}

func (lm *SyncMap[K, T]) UpdateIf(key K, n T, f func(T, T) bool) (update bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	old, ok := lm.d[key]
	if update = !ok || f(old, n); update {
		lm.d[key] = n
	}
	return
}

func NewSyncMap[K comparable, T any](capacity int) *SyncMap[K, T] {
	return &SyncMap[K, T]{
		mu: &sync.RWMutex{},
		d:  make(map[K]T, capacity),
	}
}

func NewSyncMapGroup[K comparable, T any](g, c int) []*SyncMap[K, T] {
	if !IsPowerOfTwo(g) {
		panic("not power of two")
	}

	r := make([]*SyncMap[K, T], g)
	for i := 0; i < g; i++ {
		r[i] = NewSyncMap[K, T](c)
	}

	return r
}

func ClearMap[M ~map[K]V, K comparable, V any](data M) {
	if len(data) == 0 {
		return
	}
	for k := range data {
		delete(data, k)
	}
}

func CloneMap[M ~map[K]V, K comparable, V any](m M) M {
	if m == nil {
		return nil
	}
	r := make(M, len(m))
	for k, v := range m {
		r[k] = v
	}
	return r
}

func MapGet[M ~map[K]V, K comparable, V any](m M, k K) mo.Option[V] {
	if m == nil {
		return mo.None[V]()
	}

	if r, ok := m[k]; ok {
		return mo.Some(r)
	} else {
		return mo.None[V]()
	}
}
