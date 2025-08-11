//go:build profile
// +build profile

package profiling

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/BytePengwin/GoToolkit/operations"
)

type Operation interface {
	String() string
}

type Timer struct {
	start     time.Time
	operation Operation
}

type Entry struct {
	Operation Operation
	Duration  time.Duration
	Start     time.Time
}

type StatFunc func([]int) int

var (
	entries []Entry
	mu      sync.Mutex
)

func Start(op Operation) *Timer {
	return &Timer{
		start:     time.Now(),
		operation: op,
	}
}

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

func Time(op Operation, fn func()) {
	timer := Start(op)
	fn()
	timer.End()
}

func Print() {
	mu.Lock()
	defer mu.Unlock()

	for _, e := range entries {
		fmt.Printf("%s: %v\n", e.Operation.String(), e.Duration)
	}
}

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

func Clear() {
	mu.Lock()
	entries = entries[:0]
	mu.Unlock()
}

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
