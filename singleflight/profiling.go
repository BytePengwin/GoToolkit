package singleflight

// OpType represents the type of operation being profiled
type OpType int

const (
	// Singleflight operations
	Download OpType = iota
	WaitForCompletion
	PerformDownload
	CleanupExpiredPromises
)

// String returns the string representation of the operation type
func (o OpType) String() string {
	switch o {
	case Download:
		return "singleflight_download"
	case WaitForCompletion:
		return "singleflight_wait_for_completion"
	case PerformDownload:
		return "singleflight_perform_download"
	case CleanupExpiredPromises:
		return "singleflight_cleanup_expired_promises"
	default:
		return "unknown"
	}
}
