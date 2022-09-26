package structs

import (
	"fmt"
	"time"
	"yunli.com/jobpool/core/constant"
	xtime "yunli.com/jobpool/helper/time"
	"yunli.com/jobpool/helper/uuid"
)

type Evaluation struct {
	// msgpack omit empty fields during serialization
	_struct bool `codec:",omitempty"` // nolint: structcheck

	// ID is a randomly generated UUID used for this evaluation. This
	// is assigned upon the creation of the evaluation.
	ID string

	// Namespace is the namespace the evaluation is created in
	Namespace string

	// Priority is used to control scheduling importance and if this plan
	// can preempt other jobs.
	Priority int

	// Type is used to control which schedulers are available to handle
	// this evaluation.
	Type string

	// TriggeredBy is used to give some insight into why this Eval
	// was created. (Plan change, node failure, alloc failure, etc).
	TriggeredBy string

	// PlanID is the plan this evaluation is scoped to. Evaluations cannot
	// be run in parallel for a given PlanID, so we serialize on this.
	PlanID string

	// plan -> job -> eval
	JobID string

	// PlanModifyIndex is the modify index of the plan at the time
	// the evaluation was created
	PlanModifyIndex uint64

	// NodeID is the node that was affected triggering the evaluation.
	NodeID string

	// NodeModifyIndex is the modify index of the node at the time
	// the evaluation was created
	NodeModifyIndex uint64

	// Status of the evaluation
	Status string

	// StatusDescription is meant to provide more human useful information
	StatusDescription string

	// Wait is a minimum wait time for running the eval. This is used to
	// support a rolling upgrade in versions prior to 0.7.0
	// Deprecated
	Wait time.Duration

	// WaitUntil is the time when this eval should be run. This is used to
	// supported delayed rescheduling of failed allocations
	WaitUntil time.Time

	// NextEval is the evaluation ID for the eval created to do a followup.
	// This is used to support rolling upgrades and failed-follow-up evals, where
	// we need a chain of evaluations.
	NextEval string

	// PreviousEval is the evaluation ID for the eval creating this one to do a followup.
	// This is used to support rolling upgrades and failed-follow-up evals, where
	// we need a chain of evaluations.
	PreviousEval string

	// BlockedEval is the evaluation ID for a created blocked eval. A
	// blocked eval will be created if all allocations could not be placed due
	// to constraints or lacking resources.
	BlockedEval string

	// RelatedEvals is a list of all the evaluations that are related (next,
	// previous, or blocked) to this one. It may be nil if not requested.
	RelatedEvals []*EvaluationStub

	// ClassEligibility tracks computed node classes that have been explicitly
	// marked as eligible or ineligible.
	ClassEligibility map[string]bool

	// QuotaLimitReached marks whether a quota limit was reached for the
	// evaluation.
	QuotaLimitReached string

	// EscapedComputedClass marks whether the plan has constraints that are not
	// captured by computed node classes.
	EscapedComputedClass bool

	// AnnotatePlan triggers the scheduler to provide additional annotations
	// during the evaluation. This should not be set during normal operations.
	AnnotatePlan bool

	// QueuedAllocations is the number of unplaced allocations at the time the
	// evaluation was processed. The map is keyed by Task Group names.
	QueuedAllocations map[string]int

	// LeaderACL provides the ACL token to when issuing RPCs back to the
	// leader. This will be a valid management token as long as the leader is
	// active. This should not ever be exposed via the API.
	LeaderACL string

	// SnapshotIndex is the Raft index of the snapshot used to process the
	// evaluation. The index will either be set when it has gone through the
	// scheduler or if a blocked evaluation is being created. The index is set
	// in this case so we can determine if an early unblocking is required since
	// capacity has changed since the evaluation was created. This can result in
	// the SnapshotIndex being less than the CreateIndex.
	SnapshotIndex uint64

	// Raft Indexes
	CreateIndex uint64
	ModifyIndex uint64

	CreateTime xtime.FormatTime `json:"createTime"`
	UpdateTime xtime.FormatTime `json:"updateTime"`
}

type EvaluationStub struct {
	ID                string
	Namespace         string
	Priority          int
	Type              string
	TriggeredBy       string
	JobID             string
	PlanID            string
	NodeID            string
	DeploymentID      string
	Status            string
	StatusDescription string
	WaitUntil         time.Time
	NextEval          string
	PreviousEval      string
	BlockedEval       string
	CreateIndex       uint64
	ModifyIndex       uint64
	CreateTime        xtime.FormatTime
	UpdateTime        xtime.FormatTime
}

// GetID implements the IDGetter interface, required for pagination.
func (e *Evaluation) GetID() string {
	if e == nil {
		return ""
	}
	return e.ID
}

// GetCreateIndex implements the CreateIndexGetter interface, required for
// pagination.
func (e *Evaluation) GetCreateIndex() uint64 {
	if e == nil {
		return 0
	}
	return e.CreateIndex
}

func (e *Evaluation) GoString() string {
	return fmt.Sprintf("<Eval %q PlanID: %q Namespace: %q>", e.ID, e.PlanID, e.Namespace)
}

func (e *Evaluation) RelatedIDs() []string {
	if e == nil {
		return nil
	}

	ids := []string{e.NextEval, e.PreviousEval, e.BlockedEval}
	related := make([]string, 0, len(ids))

	for _, id := range ids {
		if id != "" {
			related = append(related, id)
		}
	}

	return related
}

func (e *Evaluation) Stub() *EvaluationStub {
	if e == nil {
		return nil
	}

	return &EvaluationStub{
		ID:                e.ID,
		Namespace:         e.Namespace,
		Priority:          e.Priority,
		Type:              e.Type,
		TriggeredBy:       e.TriggeredBy,
		PlanID:            e.PlanID,
		JobID:             e.JobID,
		NodeID:            e.NodeID,
		Status:            e.Status,
		StatusDescription: e.StatusDescription,
		WaitUntil:         e.WaitUntil,
		NextEval:          e.NextEval,
		PreviousEval:      e.PreviousEval,
		BlockedEval:       e.BlockedEval,
		CreateIndex:       e.CreateIndex,
		ModifyIndex:       e.ModifyIndex,
		CreateTime:        e.CreateTime,
		UpdateTime:        e.UpdateTime,
	}
}

func (e *Evaluation) Copy() *Evaluation {
	if e == nil {
		return nil
	}
	ne := new(Evaluation)
	*ne = *e

	// Copy ClassEligibility
	if e.ClassEligibility != nil {
		classes := make(map[string]bool, len(e.ClassEligibility))
		for class, elig := range e.ClassEligibility {
			classes[class] = elig
		}
		ne.ClassEligibility = classes
	}

	// Copy queued allocations
	if e.QueuedAllocations != nil {
		queuedAllocations := make(map[string]int, len(e.QueuedAllocations))
		for tg, num := range e.QueuedAllocations {
			queuedAllocations[tg] = num
		}
		ne.QueuedAllocations = queuedAllocations
	}

	return ne
}

func (e *Evaluation) ShouldEnqueue() bool {
	switch e.Status {
	case constant.EvalStatusPending:
		return true
	case constant.EvalStatusComplete, constant.EvalStatusFailed, constant.EvalStatusBlocked, constant.EvalStatusCancelled:
		return false
	default:
		panic(fmt.Sprintf("unhandled evaluation (%s) status %s", e.ID, e.Status))
	}
}

// ShouldBlock checks if a given evaluation should be entered into the blocked
// eval tracker.
func (e *Evaluation) ShouldBlock() bool {
	switch e.Status {
	case constant.EvalStatusBlocked:
		return true
	case constant.EvalStatusComplete, constant.EvalStatusFailed, constant.EvalStatusPending, constant.EvalStatusCancelled:
		return false
	default:
		panic(fmt.Sprintf("unhandled evaluation (%s) status %s", e.ID, e.Status))
	}
}

// TerminalStatus returns if the current status is terminal and
// will no longer transition.
func (e *Evaluation) TerminalStatus() bool {
	switch e.Status {
	case constant.EvalStatusComplete, constant.EvalStatusFailed, constant.EvalStatusCancelled:
		return true
	default:
		return false
	}
}

func (e *Evaluation) CreateFailedFollowUpEval(wait time.Duration) *Evaluation {
	return &Evaluation{
		ID:              uuid.Generate(),
		Namespace:       e.Namespace,
		Priority:        e.Priority,
		Type:            e.Type,
		TriggeredBy:     constant.EvalTriggerFailedFollowUp,
		PlanID:          e.PlanID,
		JobID:           e.JobID,
		PlanModifyIndex: e.PlanModifyIndex,
		Status:          constant.EvalStatusPending,
		Wait:            wait,
		PreviousEval:    e.ID,
		CreateTime:      xtime.NewFormatTime(time.Now()),
		UpdateTime:      xtime.NewFormatTime(time.Now()),
	}
}
