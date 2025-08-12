//go:build profile
// +build profile

// Package profiling provides utilities for performance measurement and profiling.
// It allows tracking execution time of operations, collecting statistics, and exporting
// performance data for analysis. This package is only included in builds with the "profile" tag.
//
// Usage:
//
//	// Define an operation type that implements the Operation interface
//	type MyOp string
//	func (o MyOp) String() string { return string(o) }
//
//	// Start timing an operation
//	timer := profiling.Start(MyOp("my_operation"))
//	// ... do work ...
//	timer.End() // Record the duration
//
//	// Or use the Time helper function
//	profiling.Time(MyOp("another_operation"), func() {
//	    // ... do work ...
//	})
//
//	// Print all recorded timings
//	profiling.Print()
//
//	// Export timings to CSV
//	profiling.ExportCSV("profiling_results.csv")
package profiling

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"
)

// Operation is an interface that represents a profilable operation.
// Implementations must provide a String method that returns a unique identifier for the operation.
type Operation interface {
	String() string
}

// Timer represents an ongoing timing operation.
// It tracks the start time and the operation being timed.
type Timer struct {
	start     time.Time
	operation Operation
}

// Entry represents a completed timing operation with its duration and metadata.
type Entry struct {
	Operation Operation     // The operation that was timed
	Duration  time.Duration // How long the operation took
	Start     time.Time     // When the operation started
}

// StatFunc is a function type that computes a statistic from a slice of duration values (in microseconds).
// It's used with Stats and AllStats functions to calculate metrics like average, median, etc.
type StatFunc func([]int) int

var (
	entries []Entry    // Stores all completed timing operations
	mu      sync.Mutex // Protects access to the entries slice
)

// Start begins timing an operation and returns a Timer that must be ended with End().
// This function is used when you need to time operations that span multiple function calls
// or when you need more control over when timing ends.
//
// Example:
//
//	timer := profiling.Start(MyOp("database_query"))
//	// ... perform database query ...
//	timer.End()
func Start(op Operation) *Timer {
	return &Timer{
		start:     time.Now(),
		operation: op,
	}
}

// End completes the timing operation and records the duration.
// This method should be called exactly once per Timer instance.
func (t *Timer) End() {
	duration := time.Since(t.start)
	mu.Lock()
	entries = append(entries, Entry{
		Operation: t.operation,
		Duration:  duration,
		Start:     t.start,
	})
	mu.Unlock()
}

// Time is a convenience function that times the execution of a function.
// It starts a timer, executes the provided function, and then ends the timer.
//
// Example:
//
//	profiling.Time(MyOp("calculate_hash"), func() {
//	    // ... calculation code ...
//	})
func Time(op Operation, fn func()) {
	timer := Start(op)
	fn()
	timer.End()
}

// Print outputs all recorded timing entries to stdout.
// Each entry is printed as "operation: duration".
// This function is useful for quick debugging and analysis.
func Print() {
	mu.Lock()
	defer mu.Unlock()

	for _, e := range entries {
		fmt.Printf("%s: %v\n", e.Operation.String(), e.Duration)
	}
}

// ExportCSV writes all timing data to a CSV file for further analysis.
// The CSV includes columns for operation name, duration in nanoseconds,
// duration in milliseconds, and the start time.
//
// Returns an error if the file cannot be created or written to.
func ExportCSV(filename string) error {
	mu.Lock()
	defer mu.Unlock()

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Header
	writer.Write([]string{"Operation", "Duration (ns)", "Duration (ms)", "Start Time"})

	// Data
	for _, e := range entries {
		writer.Write([]string{
			e.Operation.String(),
			fmt.Sprintf("%d", e.Duration.Nanoseconds()),
			fmt.Sprintf("%.3f", float64(e.Duration.Nanoseconds())/1e6),
			e.Start.Format(time.RFC3339Nano),
		})
	}

	return nil
}

// Clear removes all recorded timing entries.
// This is useful when you want to start a new profiling session
// without the results from previous operations.
func Clear() {
	mu.Lock()
	entries = entries[:0]
	mu.Unlock()
}

// Stats calculates a statistic for a specific operation using the provided StatFunc.
// It filters the timing entries to include only those matching the specified operation,
// then applies the StatFunc to the durations (in microseconds).
//
// If no entries match the operation, it returns 0.
//
// Example:
//
//	// Calculate average duration for database queries
//	avg := profiling.Stats(MyOp("database_query"), func(durations []int) int {
//	    sum := 0
//	    for _, d := range durations {
//	        sum += d
//	    }
//	    return sum / len(durations)
//	})
func Stats(op Operation, fn StatFunc) int {
	mu.Lock()
	defer mu.Unlock()

	var durations []int
	for _, e := range entries {
		if e.Operation.String() == op.String() {
			durations = append(durations, int(e.Duration.Microseconds()))
		}
	}

	if len(durations) == 0 {
		return 0
	}

	return fn(durations)
}

// AllStats calculates a statistic for all operations using the provided StatFunc.
// It groups timing entries by operation, then applies the StatFunc to each group's
// durations (in microseconds).
//
// Returns a map where keys are operation names and values are the calculated statistics.
//
// Example:
//
//	// Calculate median duration for all operations
//	medians := profiling.AllStats(func(durations []int) int {
//	    sort.Ints(durations)
//	    return durations[len(durations)/2]
//	})
func AllStats(fn StatFunc) map[string]int {
	mu.Lock()
	defer mu.Unlock()

	grouped := make(map[string][]int)
	for _, e := range entries {
		key := e.Operation.String()
		grouped[key] = append(grouped[key], int(e.Duration.Microseconds()))
	}

	result := make(map[string]int)
	for op, durations := range grouped {
		result[op] = fn(durations)
	}

	return result
}
