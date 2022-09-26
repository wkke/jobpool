package structs

import (
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/helper/time"
)

type Job struct {
	ID            string          `json:"id"`
	PlanID        string          `json:"planId"`
	DerivedPlanId string          `json:"derivedPlanId"`
	Name          string          `json:"name"`
	Type          string          `json:"type"`
	Namespace     string          `json:"namespace"`
	OperatorId    string          `json:"operatorId"`
	Timeout       uint64          `json:"timeout"`
	Status        string          `json:"status"`
	Parameters    []byte          `json:"-"`
	ParameDetail  string          `json:"parameters"`
	Info          string          `json:"info"`
	CreateTime    time.FormatTime `json:"createTime"`
	UpdateTime    time.FormatTime `json:"updateTime"`
	// Raft Indexes
	CreateIndex uint64 `json:"-"`
	ModifyIndex uint64 `json:"-"`
}

func (j *Job) Copy() *Job {
	if j == nil {
		return nil
	}
	nj := new(Job)
	*nj = *j
	return nj
}

func (e *Job) TerminalStatus() bool {
	switch e.Status {
	case constant.JobStatusComplete, constant.JobStatusFailed, constant.JobStatusCancelled, constant.JobStatusSkipped, constant.JobStatusExpired:
		return true
	default:
		return false
	}
}

func (job *Job) ConvertJobSlot() *JobSlot {
	return &JobSlot{
		ID:          job.ID,
		Name:        job.Name,
		CreateIndex: job.CreateIndex,
	}
}

func (j *Job) GetNamespace() string {
	if j == nil {
		return ""
	}
	return j.Namespace
}
