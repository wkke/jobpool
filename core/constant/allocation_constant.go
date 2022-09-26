package constant

const (
	AllocDesiredStatusRun   = "run"   // Allocation should run
	AllocDesiredStatusStop  = "stop"  // Allocation should stop
	AllocDesiredStatusEvict = "evict" // Allocation should stop, and was evicted
)

const (
	AllocClientStatusPending   = "pending"
	AllocClientStatusRunning   = "running"
	AllocClientStatusComplete  = "complete"
	AllocClientStatusSkipped   = "skipped"
	AllocClientStatusFailed    = "failed"
	AllocClientStatusCancelled = "cancelled"
	AllocClientStatusExpired   = "expired"
	AllocClientStatusUnknown   = "unknown"
)

var AllocStatusSet = map[string]bool {
	AllocClientStatusPending: true,
	AllocClientStatusRunning: true,
	AllocClientStatusComplete: true,
	AllocClientStatusSkipped: true,
	AllocClientStatusFailed: true,
	AllocClientStatusCancelled: true,
	AllocClientStatusExpired: true,
	AllocClientStatusUnknown: true,
}