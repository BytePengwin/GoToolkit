package duckdbcache

// OpType represents the type of operation being profiled
type OpType int

const (
	// Cache operations
	CacheGet OpType = iota
	CacheDownload
	CacheUpload
	CacheCleanup
)

// String returns the string representation of the operation type
func (o OpType) String() string {
	switch o {
	case CacheGet:
		return "cache_get"
	case CacheDownload:
		return "cache_download"
	case CacheUpload:
		return "cache_upload"
	case CacheCleanup:
		return "cache_cleanup"
	default:
		return "unknown"
	}
}
