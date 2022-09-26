package dto

import (
	"time"
	"yunli.com/jobpool/core/structs"
)

// EvalDequeueRequest is used when we want to dequeue an evaluation
type EvalDequeueRequest struct {
	Schedulers       []string
	Timeout          time.Duration
	SchedulerVersion uint16
	WriteRequest
}

// EvalDequeueResponse is used to return from a dequeue
type EvalDequeueResponse struct {
	Eval  *structs.Evaluation
	Token string

	// WaitIndex is the Raft index the worker should wait until invoking the
	// scheduler.
	WaitIndex uint64

	QueryMeta
}

// GetWaitIndex is used to retrieve the Raft index in which state should be at
// or beyond before invoking the scheduler.
func (e *EvalDequeueResponse) GetWaitIndex() uint64 {
	// Prefer the wait index sent. This will be populated on all responses from
	// 0.7.0 and above
	if e.WaitIndex != 0 {
		return e.WaitIndex
	} else if e.Eval != nil {
		return e.Eval.ModifyIndex
	}

	// This should never happen
	return 1
}

type EvalAckRequest struct {
	EvalID string
	JobID  string
	Token  string
	WriteRequest
}

type EvalStatRequest struct {
	QueryOptions
}

type EvalStatResponse struct {
	AllReady          int `json:"ready"`
	AllPending        int `json:"pending"`
	TotalReady        int
	TotalUnacked      int
	TotalBlocked      int
	TotalCaptured     int
	TotalWaiting      int
	DelayedEvals      int
	ByScheduler       int
	TotalFailedQueue  int
	TotalEscaped      int
	TotalBlockedQueue int
	TotalQuotaLimit   int

	QueryMeta
}
