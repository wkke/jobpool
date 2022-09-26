package structs

import "time"

// RPCInfo is used to describe common information about query
type RPCInfo interface {
	RequestRegion() string
	IsRead() bool
	AllowStaleRead() bool
	IsForwarded() bool
	SetForwarded()
	TimeToBlock() time.Duration
	// SetTimeToBlock sets how long this request can block. The requested time may not be possible,
	// so Callers should readback TimeToBlock. E.g. you cannot set time to block at all on WriteRequests
	// and it cannot exceed MaxBlockingRPCQueryTime
	SetTimeToBlock(t time.Duration)
}

