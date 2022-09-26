package dto

import "yunli.com/jobpool/core/structs"

type JobListRequest struct {
	QueryOptions
}

type JobListResponse struct {
	Jobs []*structs.Job `json:"data"`
	QueryMeta
	ExcludePageResponse
}

type JobDeleteRequest struct {
	JobID string
	WriteRequest
}

// PlanDeregisterResponse is used to respond to a plan deregistration
type JobDeleteResponse struct {
	JobID string
	QueryMeta
}

type JobRegisterRequest struct {
	Job  *structs.Job
	Plan *structs.Plan
	WriteRequest
}

// PlanAddResponse is used to respond to a plan registration
type JobRegisterResponse struct {
	JobID           string
	EvalCreateIndex uint64
	PlanModifyIndex uint64
	QueryMeta
}

type JobDetailRequest struct {
	JobID string	`json:"jobId"`
	QueryOptions
}

type JobDetailResponse struct {
	Job *structs.Job `json:"data"`
	QueryMeta
	ExcludePageResponse
}

type JobStatusUpdateRequest struct {
	JobID  string
	Status string
	Info   string
	WriteRequest
}

type JobStatusUpdateResponse struct {
	JobID  string
	Status string
	QueryMeta
}

type JobMapStatRequest struct {
	QueryOptions
}

type JobMapDto struct {
	Pending     int      `json:"pending"`
	Running     int      `json:"running"`
	Retry       int      `json:"retry"`
	UnUsed      int      `json:"unUsed"`
	RunningJobs []string `json:"runningJobs"`
	PendingJobs []string `json:"pendingJobs"`
}

type JobMapStatResponse struct {
	TaskRoadmapDto *JobMapDto `json:"data"`
	QueryMeta
	ExcludeDataResponse
}
