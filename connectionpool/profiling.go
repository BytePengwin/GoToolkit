package connectionpool

// OpType represents the type of operation being profiled
type OpType int

const (
	// Pool operations
	PoolAcquire OpType = iota
	PoolQuery
	PoolRelease
)

// String returns the string representation of the operation type
func (o OpType) String() string {
	switch o {
	case PoolAcquire:
		return "pool_acquire"
	case PoolQuery:
		return "pool_query"
	case PoolRelease:
		return "pool_release"
	default:
		return "unknown"
	}
}
