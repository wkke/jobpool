package dto

import "yunli.com/jobpool/core/structs"

// AllocUpdateRequest is used to submit changes to allocations, either
// to cause evictions or to assign new allocations. Both can be done
// within a single transaction
type AllocUpdateRequest struct {
	Alloc []*structs.Allocation
	// Allocations to stop. Contains only the diff, not the entire allocation
	AllocsStopped []*structs.AllocationDiff

	// New or updated allocations
	AllocsUpdated []*structs.Allocation

	// Evals is the list of new evaluations to create
	// Evals are valid only when used in the Raft RPC
	Evals []*structs.Evaluation

	// Plan is the shared parent plan of the allocations.
	// It is pulled out since it is common to reduce payload size.
	Plan *structs.Plan

	WriteRequest
}

// NodeClientAllocsResponse is used to return allocs meta data for a single node
type NodeClientAllocsResponse struct {
	Allocs map[string]uint64

	QueryMeta
}

// AllocsGetRequest is used to query a set of allocations
type AllocsGetRequest struct {
	AllocIDs []string
	QueryOptions
}

// AllocsGetResponse is used to return a set of allocations
type AllocsGetResponse struct {
	Allocs []*structs.Allocation `json:"data"`
	QueryMeta
	ExcludePageResponse
}

type AllocsGetByJobIdRequest struct {
	JobId          string
	AnyCreateIndex bool
	WriteRequest
}

type AllocsGetByJobIdResponse struct {
	Allocs []*structs.Allocation
}

type AllocUpdateStatusByJobRequest struct {
	JobId       string `json:"jobId"`
	Status      string `json:"status"`
	Description string `json:"description"`
	WriteRequest
}

type AllocUpdateDesiredTransitionRequest struct {
	// Allocs is the mapping of allocation ids to their desired state
	// transition
	Allocs map[string]*structs.DesiredTransition

	// Evals is the set of evaluations to create
	Evals []*structs.Evaluation

	WriteRequest
}