package common

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// 定义可配置的选项（通过函数选项模式增强扩展性）
type SyncedDataOption[T any] func(*SyncedData[T])

// WithDefaultValue 设置默认值（f() 失败时 Get() 返回该值）
func WithDefaultValue[T any](val T) SyncedDataOption[T] {
	return func(sd *SyncedData[T]) {
		sd.defaultVal = val
		sd.d.Store(val) // 初始化时存储默认值，避免 Get()  panic
	}
}

// WithLogger 注入自定义日志器（默认使用 log.Default()）
func WithLogger[T any](logger *log.Logger) SyncedDataOption[T] {
	return func(sd *SyncedData[T]) {
		if logger != nil {
			sd.logger = logger
		}
	}
}

// WithRetryPolicy 设置失败重试策略（默认不重试）
func WithRetryPolicy[T any](maxRetries int, retryInterval time.Duration) SyncedDataOption[T] {
	return func(sd *SyncedData[T]) {
		if maxRetries > 0 {
			sd.retryMax = maxRetries
			sd.retryInterval = retryInterval
		}
	}
}

// WithImmediateRefresh 初始化时是否立即执行一次刷新（默认 true，与原逻辑一致）
func WithImmediateRefresh[T any](immediate bool) SyncedDataOption[T] {
	return func(sd *SyncedData[T]) {
		sd.immediateRefresh = immediate
	}
}

type SyncedData[T any] struct {
	d                *atomic.Value     // 存储核心数据
	f                func() (T, error) // 数据刷新函数
	t                time.Duration     // 刷新间隔
	defaultVal       T                 // 兜底默认值
	logger           *log.Logger       // 日志器
	retryMax         int               // 最大重试次数
	retryInterval    time.Duration     // 重试间隔
	immediateRefresh bool              // 初始化时是否立即刷新

	initDone        atomic.Bool        // 初始化完成标志（确保 Init 仅执行一次）
	ctx             context.Context    // 管理 Goroutine 生命周期
	cancel          context.CancelFunc // 取消函数
	wg              sync.WaitGroup     // 等待 Goroutine 退出
	runningMu       sync.Mutex         // 防止 f() 并发执行
	lastRefreshTime atomic.Value       // 最后一次刷新时间（time.Time）
	lastRefreshOk   atomic.Bool        // 最后一次刷新是否成功
}

// NewSyncedData 创建 SyncedData 实例（新增参数校验和选项配置）
func NewSyncedData[T any](t time.Duration, f func() (T, error), opts ...SyncedDataOption[T]) (*SyncedData[T], error) {
	// 1. 校验核心参数合法性
	if t <= 0 {
		return nil, fmt.Errorf("refresh interval must be positive: %v", t)
	}
	if f == nil {
		return nil, errors.New("refresh function f cannot be nil")
	}

	// 2. 初始化默认值
	ctx, cancel := context.WithCancel(context.Background())
	sd := &SyncedData[T]{
		d:                &atomic.Value{},
		f:                f,
		t:                t,
		logger:           log.Default(),
		retryMax:         0,
		retryInterval:    1 * time.Second,
		immediateRefresh: true,
		ctx:              ctx,
		cancel:           cancel,
	}

	// 3. 应用用户配置选项
	for _, opt := range opts {
		opt(sd)
	}

	// 4. 初始化状态字段
	sd.lastRefreshTime.Store(time.Time{})
	sd.lastRefreshOk.Store(false)

	return sd, nil
}

// Get 获取数据（返回 (T, error) 避免 Panic，支持默认值兜底）
func (c *SyncedData[T]) Get() (T, error) {
	// 1. 检查是否初始化
	if !c.initDone.Load() {
		return c.defaultVal, errors.New("synced data not initialized (call Init() first)")
	}

	// 2. 安全加载数据（避免类型断言失败）
	val := c.d.Load()
	data, ok := val.(T)
	if !ok {
		c.logger.Printf("warning: stored data type mismatch, use default value")
		return c.defaultVal, errors.New("data type mismatch")
	}

	return data, nil
}

// Set 手动设置数据（新增并发安全检查）
func (c *SyncedData[T]) Set(v T) error {
	if !c.initDone.Load() {
		return errors.New("cannot set data before initialization")
	}
	c.d.Store(v)
	c.lastRefreshTime.Store(time.Now())
	c.lastRefreshOk.Store(true)
	return nil
}

// Init 初始化（确保仅执行一次，启动刷新 Goroutine）
func (c *SyncedData[T]) Init() error {
	// 1. 原子检查：确保 Init 仅执行一次
	if !c.initDone.CompareAndSwap(false, true) {
		return errors.New("init() has already been called")
	}

	// 2. 立即刷新（可选，与原逻辑兼容）
	if c.immediateRefresh {
		if err := c.refreshWithRetry(); err != nil {
			c.logger.Printf("initial refresh failed: %v (use default value)", err)
		}
	}

	// 3. 启动定时刷新 Goroutine
	c.wg.Add(1)
	go c.refreshLoop()

	return nil
}

// Stop 停止刷新 Goroutine（优雅退出，避免资源泄漏）
func (c *SyncedData[T]) Stop() {
	c.cancel()  // 触发上下文取消
	c.wg.Wait() // 等待 Goroutine 退出
	c.logger.Println("synced data refresh loop stopped")
}

// GetStatus 获取刷新状态（新增可观测性）
func (c *SyncedData[T]) GetStatus() (lastRefreshTime time.Time, lastRefreshOk bool) {
	return c.lastRefreshTime.Load().(time.Time), c.lastRefreshOk.Load()
}

// refreshLoop 定时刷新循环（优化定时逻辑，支持优雅退出）
func (c *SyncedData[T]) refreshLoop() {
	defer c.wg.Done()

	// 初始化定时器（首次刷新后开始计时）
	ticker := time.NewTicker(c.t)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Println("refresh loop exiting...")
			return
		case <-ticker.C:
			// 避免 f() 并发执行（加锁）
			c.runningMu.Lock()
			if err := c.refreshWithRetry(); err != nil {
				c.logger.Printf("scheduled refresh failed: %v", err)
			}
			c.runningMu.Unlock()
		}
	}
}

// refreshWithRetry 带重试的刷新逻辑（新增重试机制）
func (c *SyncedData[T]) refreshWithRetry() error {
	var (
		data T
		err  error
	)

	// 执行刷新（带重试）
	for attempt := 0; attempt <= c.retryMax; attempt++ {
		data, err = c.f()
		if err == nil {
			break
		}

		// 重试逻辑（最后一次失败则返回错误）
		if attempt == c.retryMax {
			c.lastRefreshOk.Store(false)
			return fmt.Errorf("refresh failed after %d attempts: %v", c.retryMax+1, err)
		}

		c.logger.Printf("refresh attempt %d failed: %v, retry in %v", attempt+1, err, c.retryInterval)
		time.Sleep(c.retryInterval)
	}

	// 刷新成功：更新数据和状态
	c.d.Store(data)
	c.lastRefreshTime.Store(time.Now())
	c.lastRefreshOk.Store(true)
	c.logger.Printf("refresh success, updated data at %v", c.lastRefreshTime.Load().(time.Time))
	return nil
}
