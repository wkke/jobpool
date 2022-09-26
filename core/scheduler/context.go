package scheduler

import (
	log "github.com/hashicorp/go-hclog"
	"yunli.com/jobpool/core/structs"
)

// EvalContext is a Context used during an Evaluation
type EvalContext struct {
	eventsCh    chan<- interface{}
	state       State
	plan        *structs.PlanAlloc
	job         *structs.Job
	logger      log.Logger
	eligibility *EvalEligibility
}

// EvalEligibility tracks eligibility of nodes by computed node class over the
// course of an evaluation.
type EvalEligibility struct {
	// plan tracks the eligibility at the job level per computed node class.
	plan map[string]ComputedClassFeasibility

	// planEscaped marks whether constraints have escaped at the job level.
	planEscaped bool

	// taskGroups tracks the eligibility at the task group level per computed
	// node class.
	taskGroups map[string]map[string]ComputedClassFeasibility

	// tgEscapedConstraints is a map of task groups to whether constraints have
	// escaped.
	tgEscapedConstraints map[string]bool

	// quotaReached marks that the quota limit has been reached for the given
	// quota
	quotaReached string
}

func NewEvalEligibility() *EvalEligibility {
	return &EvalEligibility{
		plan:                 make(map[string]ComputedClassFeasibility),
		taskGroups:           make(map[string]map[string]ComputedClassFeasibility),
		tgEscapedConstraints: make(map[string]bool),
	}
}

type ComputedClassFeasibility byte

const (
	// EvalComputedClassUnknown is the initial state until the eligibility has
	// been explicitly marked to eligible/ineligible or escaped.
	EvalComputedClassUnknown ComputedClassFeasibility = iota

	// EvalComputedClassIneligible is used to mark the computed class as
	// ineligible for the evaluation.
	EvalComputedClassIneligible

	// EvalComputedClassIneligible is used to mark the computed class as
	// eligible for the evaluation.
	EvalComputedClassEligible

	// EvalComputedClassEscaped signals that computed class can not determine
	// eligibility because a constraint exists that is not captured by computed
	// node classes.
	EvalComputedClassEscaped
)

func (e *EvalContext) Eligibility() *EvalEligibility {
	if e.eligibility == nil {
		e.eligibility = NewEvalEligibility()
	}

	return e.eligibility
}

// NewEvalContext constructs a new EvalContext
func NewEvalContext(eventsCh chan<- interface{}, s State, p *structs.PlanAlloc, j *structs.Job, log log.Logger) *EvalContext {
	ctx := &EvalContext{
		eventsCh: eventsCh,
		state:    s,
		plan:     p,
		job:      j,
		logger:   log,
	}
	return ctx
}

func (e *EvalEligibility) HasEscaped() bool {
	if e.planEscaped {
		return true
	}

	for _, escaped := range e.tgEscapedConstraints {
		if escaped {
			return true
		}
	}

	return false
}

// GetClasses returns the tracked classes to their eligibility, across the job
// and task groups.
func (e *EvalEligibility) GetClasses() map[string]bool {
	elig := make(map[string]bool)

	// Go through the task groups.
	for _, classes := range e.taskGroups {
		for class, feas := range classes {
			switch feas {
			case EvalComputedClassEligible:
				elig[class] = true
			case EvalComputedClassIneligible:
				// Only mark as ineligible if it hasn't been marked before. This
				// prevents one task group marking a class as ineligible when it
				// is eligible on another task group.
				if _, ok := elig[class]; !ok {
					elig[class] = false
				}
			}
		}
	}

	// Go through the job.
	for class, feas := range e.plan {
		switch feas {
		case EvalComputedClassEligible:
			// Only mark as eligible if it hasn't been marked before. This
			// prevents the job marking a class as eligible when it is ineligible
			// to all the task groups.
			if _, ok := elig[class]; !ok {
				elig[class] = true
			}
		case EvalComputedClassIneligible:
			elig[class] = false
		}
	}

	return elig
}
