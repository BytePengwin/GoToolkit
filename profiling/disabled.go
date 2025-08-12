//go:build !profile
// +build !profile

// Package profiling provides utilities for performance measurement and profiling.
// This is the no-op implementation used when the "profile" build tag is not set.
//
// All functions in this version have empty implementations that add no overhead
// to your application. This allows you to include profiling code in your application
// without affecting performance when profiling is disabled.
//
// To enable profiling, build your application with the "profile" tag:
//
//	go build -tags=profile
//
// See the documentation for the profiling package with the "profile" build tag
// for details on the available functionality.
package profiling

// Operation is an interface that represents a profilable operation.
// Implementations must provide a String method that returns a unique identifier for the operation.
type Operation interface {
	String() string
}

// Timer represents an ongoing timing operation.
// In this no-op implementation, it's an empty struct.
type Timer struct{}

// Start begins timing an operation and returns a Timer.
// In this no-op implementation, it returns an empty Timer.
func Start(op Operation) *Timer { return &Timer{} }

// End completes the timing operation.
// In this no-op implementation, it does nothing.
func (t *Timer) End() {}

// Time executes the provided function.
// In this no-op implementation, it simply calls the function without timing.
func Time(op Operation, fn func()) { fn() }

// Print outputs all recorded timing entries.
// In this no-op implementation, it does nothing.
func Print() {}

// ExportCSV writes timing data to a CSV file.
// In this no-op implementation, it returns nil (no error).
func ExportCSV(filename string) error { return nil }

// Clear removes all recorded timing entries.
// In this no-op implementation, it does nothing.
func Clear() {}

// Stats calculates a statistic for a specific operation.
// In this no-op implementation, it returns 0.
func Stats(op Operation, fn func([]int) int) int { return 0 }

// AllStats calculates statistics for all operations.
// In this no-op implementation, it returns nil.
func AllStats(fn func([]int) int) map[string]int { return nil }
