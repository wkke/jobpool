package dto

import (
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper/time"
)

type PlanDto struct {
	*structs.Plan
	LaunchTime time.FormatTime `json:"launchTime"`
}

type PlanAddDto struct {
	ID          string                  `json:"id"`
	Name        string                  `json:"name"`
	ParentID    string                  `json:"parentId"`
	Type        string                  `json:"type"`
	NamespaceId string                  `json:"namespaceId"`
	CreatorId   string                  `json:"creatorId"`
	Priority    int                     `json:"priority,optional"`
	Stop        bool                    `json:"-"`
	Periodic    *structs.PeriodicConfig `json:"periodic"`
	AllAtOnce   bool                    `json:"allAtOnce"` // 一次性任务
	Status      string                  `json:"status"`
	Description string                  `json:"description"`
	// config and detail
	BusinessType string `json:"businessType"`
	Parameters   string `json:"parameters"`
	// other attr
	SubmitTime time.FormatTime `json:"submitTime"`
	CreateTime time.FormatTime `json:"createTime"`
	UpdateTime time.FormatTime `json:"updateTime"`
	Version    uint64
}

// request and response
type PlanAddRequest struct {
	*PlanAddDto
	WriteRequest
}

// PlanAddResponse is used to respond to a plan registration
type PlanAddResponse struct {
	ID              string `json:"id"`
	EvalCreateIndex uint64 `json:"-"`
	PlanModifyIndex uint64 `json:"-"`
	QueryMeta
}

func (plan *PlanAddDto) ConvertPlan() *structs.Plan {
	planInfo := structs.Plan{
		ID:          plan.ID,
		Name:        plan.Name,
		ParentID:    plan.ParentID,
		Type:        plan.Type,
		Namespace:   plan.NamespaceId,
		CreatorId:   plan.CreatorId,
		Priority:    plan.Priority,
		Stop:        plan.Stop,
		Periodic:    plan.Periodic,
		AllAtOnce:   plan.AllAtOnce,
		Status:      plan.Status,
		Description: plan.Description,
		// config and detail
		BusinessType: plan.BusinessType,
		// other attr
		SubmitTime: plan.SubmitTime,
		CreateTime: plan.CreateTime,
		UpdateTime: plan.UpdateTime,
		Version:    plan.Version,
	}
	if plan.Parameters != "" {
		planInfo.Parameters = []byte(plan.Parameters)
	}
	return &planInfo
}

// PlanListRequest is used to parameterize a list request
type PlanListRequest struct {
	QueryOptions
}

// PlanListResponse is used for a list request
type PlanListResponse struct {
	Plans []*PlanDto `json:"data"`
	QueryMeta
	ExcludePageResponse
}

// PlanDeregisterRequest is used for Plan.Deregister endpoint
// to deregister a plan as being a schedulable entity.
type PlanDeregisterRequest struct {
	PlanID string
	WriteRequest
}

// PlanDeregisterResponse is used to respond to a plan deregistration
type PlanDeregisterResponse struct {
	PlanID          string
	EvalID          string `json:"-"`
	EvalCreateIndex uint64 `json:"-"`
	PlanModifyIndex uint64 `json:"-"`
	QueryMeta
	ExcludeDataResponse
}

func (j *PlanAddDto) IsPeriodicActive() bool {
	plan := j.ConvertPlan()
	return plan.IsPeriodic() && plan.Periodic.Enabled && !plan.Stopped() && !plan.IsParameterized()
}

func (j *PlanAddDto) IsPeriodic() bool {
	return j.Periodic != nil
}

type PlanOnlineOfflineRequest struct {
	PlanID string
	Status string
	Reason string
	WriteRequest
}

type PlanOnlineOfflineResponse struct {
	PlanID string
	Status string
	QueryMeta
	ExcludeDataResponse
}

type PlanDetailRequest struct {
	PlanID string
	WriteRequest
}

type PlanDetailResponse struct {
	*structs.Plan
	QueryMeta
	ExcludeDataResponse
}
