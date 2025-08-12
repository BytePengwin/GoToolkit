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

// newPromise creates a new Promise object to represent an in-progress download operation.
// It initializes the promise with a key identifier and sets up the synchronization primitives.
//
// Parameters:
//   - key: A unique identifier for the download operation
//
// Returns a new Promise object ready to track the download operation.
//
// This function is used internally by the Downloader to create promises for
// tracking in-progress downloads and enabling the single-flight pattern.
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

// setResult sets the download result or error and notifies all waiting goroutines.
// It stores either the result or error using unsafe.Pointer for type-safe generic storage.
//
// Parameters:
//   - result: The downloaded data (only used if err is nil)
//   - err: Any error that occurred during download (nil if successful)
//
// This method is called when a download completes (successfully or with an error).
// It uses sync.Cond.Broadcast to wake up all goroutines waiting on this promise.
// If the promise is already marked as done, this method is a no-op to prevent overwriting results.
//
// Thread safety: This method acquires the promise's mutex to ensure thread-safe updates.
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

// isDone checks if the promise has completed (either successfully or with an error).
//
// Returns true if the download operation has completed, false otherwise.
//
// This method is used primarily by the cleanup routine to determine if a promise
// can be safely removed from the promises map when it has timed out but hasn't completed.
// It's also used internally to check promise state.
//
// Thread safety: This method acquires the promise's mutex to ensure a consistent view of the done flag.
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

// performDownload executes the actual download operation for a promise.
// It calls the user-provided downloadFunc and sets the result on the promise.
//
// Parameters:
//   - ctx: Context for the download operation (can be used for cancellation)
//   - promise: The Promise object representing this download operation
//
// This method is called as a goroutine when a new download is initiated.
// It's responsible for the actual data retrieval and for notifying waiters
// when the download completes by calling setResult on the promise.
//
// The method is profiled to track performance metrics of download operations.
func (sf *Downloader[T]) performDownload(ctx context.Context, promise *Promise[T]) {
	timer := profiling.Start(PerformDownload)
	defer timer.End()

	result, err := sf.downloadFunc(ctx, promise.key)
	promise.setResult(result, err)
}

// cleanupRoutine is a background goroutine that periodically removes expired promises.
// It runs on the interval specified by cleanupInterval and continues until the downloader is shut down.
//
// This method is started automatically when the Downloader is created and is responsible for
// preventing memory leaks by ensuring old promises don't accumulate indefinitely.
// It calls cleanupExpiredPromises at regular intervals to do the actual cleanup work.
//
// The goroutine exits gracefully when the Downloader's Shutdown method is called,
// signaling via the stopCleanup channel and notifying the wait group when complete.
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

// cleanupExpiredPromises identifies and removes old completed promises from the promises map.
// It only removes promises that are both completed (done) and older than the configured timeout.
//
// This method is called periodically by the cleanupRoutine to maintain memory efficiency.
// It uses sync.Map.Range to safely iterate over the promises map without locking the entire map.
// Only promises that meet both criteria (completed and expired) are removed:
//  1. The promise must be done (completed successfully or with an error)
//  2. The promise must be older than promiseTimeout
//
// The method is profiled to track performance metrics of cleanup operations.
// In-progress promises are never removed, even if they exceed the timeout,
// to prevent disrupting ongoing downloads.
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
