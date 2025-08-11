//go:build !profile
// +build !profile

package profiling

type Operation interface {
	String() string
}

type Timer struct{}

func Start(op Operation) *Timer                  { return &Timer{} }
func (t *Timer) End()                            {}
func Time(op Operation, fn func())               { fn() }
func Print()                                     {}
func ExportCSV(filename string) error            { return nil }
func Clear()                                     {}
func Stats(op Operation, fn func([]int) int) int { return 0 }
func AllStats(fn func([]int) int) map[string]int { return nil }
