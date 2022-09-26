package dto

import "yunli.com/jobpool/core/structs"

type EvalUpdateRequest struct {
	Evals     []*structs.Evaluation
	EvalToken string
	WriteRequest
}

// EvalListRequest is used to list the evaluations
type EvalListRequest struct {
	FilterJobID      string
	FilterEvalStatus string
	QueryOptions
}

// EvalListResponse is used for a list request
type EvalListResponse struct {
	Evaluations []*structs.Evaluation `json:"data"`
	QueryMeta
	ExcludePageResponse
}

// ShouldBeFiltered indicates that the eval should be filtered (that
// is, removed) from the results
func (req *EvalListRequest) ShouldBeFiltered(e *structs.Evaluation) bool {
	if req.FilterJobID != "" && req.FilterJobID != e.JobID {
		return true
	}
	if req.FilterEvalStatus != "" && req.FilterEvalStatus != e.Status {
		return true
	}
	return false
}
