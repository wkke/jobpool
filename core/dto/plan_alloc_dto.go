package dto

import "yunli.com/jobpool/core/structs"

// ApplyPlanResultsRequest is used by the planner to apply a Raft transaction
// committing the result of a plan.
type ApplyPlanResultsRequest struct {
	// AllocUpdateRequest holds the allocation updates to be made by the
	// scheduler.
	AllocUpdateRequest

	// EvalID is the eval ID of the plan being applied. The modify index of the
	// evaluation is updated as part of applying the plan to ensure that subsequent
	// scheduling events for the same job will wait for the index that last produced
	// state changes. This is necessary for blocked evaluations since they can be
	// processed many times, potentially making state updates, without the state of
	// the evaluation itself being updated.
	EvalID string

	// COMPAT 0.11
	// NodePreemptions is a slice of allocations from other lower priority jobs
	// that are preempted. Preempted allocations are marked as evicted.
	// Deprecated: Replaced with AllocsPreempted which contains only the diff
	NodePreemptions []*structs.Allocation

	// AllocsPreempted is a slice of allocation diffs from other lower priority jobs
	// that are preempted. Preempted allocations are marked as evicted.
	AllocsPreempted []*structs.AllocationDiff

	// PreemptionEvals is a slice of follow up evals for jobs whose allocations
	// have been preempted to place allocs in this plan
	PreemptionEvals []*structs.Evaluation
}


type PlanAllocRequest struct {
	PlanAlloc *structs.PlanAlloc
	WriteRequest
}

// PlanResponse is used to return from a PlanRequest
type PlanAllocResponse struct {
	Result *structs.PlanResult
	WriteMeta
}