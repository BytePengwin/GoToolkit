// Package singleflight provides a concurrent-safe single-flight downloader
// that ensures only one download happens per key, with other requests waiting
// for completion using channels and atomic operations.
package singleflight

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/BytePengwin/GoToolkit/profiling"
)

// Promise handles single-flight downloads for a specific key
type Promise[T any] struct {
	key    string
	mu     sync.Mutex
	cond   *sync.Cond
	done   bool
	result unsafe.Pointer // *T
	err    unsafe.Pointer // *error

	createdAt time.Time
}

// newPromise creates a new promise
func newPromise[T any](key string) *Promise[T] {
	p := &Promise[T]{
		key:       key,
		createdAt: time.Now(),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// WaitForCompletion blocks until download completes
func (p *Promise[T]) WaitForCompletion(ctx context.Context) (T, error) {
	timer := profiling.Start(WaitForCompletion)
	defer timer.End()

	var zero T

	// Fast path: lock-free check if done
	p.mu.Lock()
	if p.done {
		result := (*T)(p.result)
		err := (*error)(p.err)
		p.mu.Unlock()

		if err != nil {
			return zero, *err
		}
		return *result, nil
	}

	// Wait for completion
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !p.done {
			p.cond.Wait()
		}
		p.mu.Unlock()
	}()

	select {
	case <-done:
		result := (*T)(p.result)
		err := (*error)(p.err)
		if err != nil {
			return zero, *err
		}
		return *result, nil
	case <-ctx.Done():
		p.mu.Unlock()
		return zero, ctx.Err()
	}
}

// setResult sets the download result and notifies waiters
func (p *Promise[T]) setResult(result T, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.done {
		return
	}

	if err != nil {
		p.err = unsafe.Pointer(&err)
	} else {
		p.result = unsafe.Pointer(&result)
	}

	p.done = true
	p.cond.Broadcast()
}

// isDone returns whether the promise is completed
func (p *Promise[T]) isDone() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done
}

// DownloadFunc is the function signature for download implementations
type DownloadFunc[T any] func(ctx context.Context, key string) (T, error)

// Downloader manages single-flight downloads
type Downloader[T any] struct {
	downloadFunc DownloadFunc[T]
	promises     sync.Map // map[string]*Promise[T]

	cleanupInterval time.Duration
	promiseTimeout  time.Duration

	stopCleanup chan struct{}
	wg          sync.WaitGroup
}

// Config holds configuration for the downloader
type Config struct {
	CleanupInterval time.Duration
	PromiseTimeout  time.Duration
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		CleanupInterval: 5 * time.Minute,
		PromiseTimeout:  10 * time.Minute,
	}
}

// New creates a new single-flight downloader
func New[T any](downloadFunc DownloadFunc[T], config Config) *Downloader[T] {
	if config.CleanupInterval == 0 {
		config.CleanupInterval = DefaultConfig().CleanupInterval
	}
	if config.PromiseTimeout == 0 {
		config.PromiseTimeout = DefaultConfig().PromiseTimeout
	}

	sf := &Downloader[T]{
		downloadFunc:    downloadFunc,
		cleanupInterval: config.CleanupInterval,
		promiseTimeout:  config.PromiseTimeout,
		stopCleanup:     make(chan struct{}),
	}

	sf.wg.Add(1)
	go sf.cleanupRoutine()

	return sf
}

// Download ensures a key is downloaded using single-flight pattern
func (sf *Downloader[T]) Download(ctx context.Context, key string) (T, error) {
	timer := profiling.Start(Download)
	defer timer.End()

	// Try to load existing promise
	if value, ok := sf.promises.Load(key); ok {
		promise := value.(*Promise[T])

		// Fast path: check if already done without locking
		if promise.isDone() {
			return promise.WaitForCompletion(ctx)
		}

		return promise.WaitForCompletion(ctx)
	}

	// Create new promise
	newPromise := newPromise[T](key)

	// Try to store our promise (race with other goroutines)
	if value, loaded := sf.promises.LoadOrStore(key, newPromise); loaded {
		existingPromise := value.(*Promise[T])
		return existingPromise.WaitForCompletion(ctx)
	}

	// We won the race, start the download
	go sf.performDownload(ctx, newPromise)

	return newPromise.WaitForCompletion(ctx)
}

// GetCached returns cached result without waiting
func (sf *Downloader[T]) GetCached(key string) (T, bool) {
	var zero T
	if value, ok := sf.promises.Load(key); ok {
		promise := value.(*Promise[T])
		if promise.isDone() {
			// Quick check - if done and no error, return result
			promise.mu.Lock()
			defer promise.mu.Unlock()
			if promise.err == nil && promise.result != nil {
				return *(*T)(promise.result), true
			}
		}
	}
	return zero, false
}

// performDownload executes the actual download
func (sf *Downloader[T]) performDownload(ctx context.Context, promise *Promise[T]) {
	timer := profiling.Start(PerformDownload)
	defer timer.End()

	result, err := sf.downloadFunc(ctx, promise.key)
	promise.setResult(result, err)
}

// cleanupRoutine removes expired promises
func (sf *Downloader[T]) cleanupRoutine() {
	defer sf.wg.Done()

	ticker := time.NewTicker(sf.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sf.cleanupExpiredPromises()
		case <-sf.stopCleanup:
			return
		}
	}
}

// cleanupExpiredPromises removes old completed promises
func (sf *Downloader[T]) cleanupExpiredPromises() {
	timer := profiling.Start(CleanupExpiredPromises)
	defer timer.End()

	cutoff := time.Now().Add(-sf.promiseTimeout)

	sf.promises.Range(func(key, value interface{}) bool {
		promise := value.(*Promise[T])

		// Only cleanup completed promises that are old
		if promise.isDone() && promise.createdAt.Before(cutoff) {
			sf.promises.Delete(key)
		}

		return true
	})
}

// Shutdown stops the cleanup routine
func (sf *Downloader[T]) Shutdown() {
	close(sf.stopCleanup)
	sf.wg.Wait()
}
