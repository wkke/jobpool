package structs

import (
	"yunli.com/jobpool/helper"
	"yunli.com/jobpool/helper/time"
)

type Plan struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	ParentID string `json:"parentId"`
	Type     string `json:"type"`

	Namespace   string          `json:"namespace"`
	CreatorId   string          `json:"creatorId"`
	Priority    int             `json:"priority,optional"`
	Stop        bool            `json:"-"`
	Periodic    *PeriodicConfig `json:"periodic"`
	AllAtOnce   bool            `json:"allAtOnce"` // 一次性任务
	Status      string          `json:"status"`
	Description string          `json:"description"`
	// config and detail
	BusinessType string `json:"businessType"`
	Parameters   []byte `json:"parameters"`
	// other attr
	SubmitTime time.FormatTime `json:"submitTime"`
	CreateTime time.FormatTime `json:"createTime"`
	UpdateTime time.FormatTime `json:"updateTime"`
	Version    uint64          `json:"version"`
	// Raft Indexes
	CreateIndex     uint64 `json:"-"`
	ModifyIndex     uint64 `json:"-"`
	PlanModifyIndex uint64 `json:"-"`
}

// NamespacedID returns the namespaced id useful for logging
func (p *Plan) NamespacedID() NamespacedID {
	return NamespacedID{
		ID:        p.ID,
		Namespace: p.Namespace,
	}
}

// Copy returns a deep copy of the Plan. It is expected that callers use recover.
// This plan can panic if the deep copy failed as it uses reflection.
func (p *Plan) Copy() *Plan {
	if p == nil {
		return nil
	}
	nj := new(Plan)
	*nj = *p
	nj.Parameters = helper.CopySliceByte(p.Parameters)
	return nj
}

// IsPeriodicActive returns whether the plan is an active periodic plan that will
// create child plans
func (p *Plan) IsPeriodicActive() bool {
	return p.IsPeriodic() && p.Periodic.Enabled && !p.Stopped() && !p.IsParameterized()
}

// IsPeriodic returns whether a plan is periodic.
func (p *Plan) IsPeriodic() bool {
	return p.Periodic != nil
}

// Stopped returns if a plan is stopped.
func (p *Plan) Stopped() bool {
	return p == nil || p.Stop
}

// IsParameterized returns whether a plan is parameterized plan.
func (p *Plan) IsParameterized() bool {
	// TODO
	return false
	// return j.Parameterizedplan != nil && !j.Dispatched
}

// Canonicalize is used to canonicalize fields in the Job. This should be
// called when registering a Job.
func (p *Plan) Canonicalize() {
	if p == nil {
		return
	}

	// Ensure the job is in a namespace.
	if p.Namespace == "" {
		p.Namespace = "default"
	}

	if p.Periodic != nil {
		p.Periodic.Canonicalize()
	}
}

func (p *Plan) Merge(b *Plan) *Plan {
	var result *Plan
	if p == nil {
		result = &Plan{}
	} else {
		result = p.Copy()
	}
	if b == nil {
		return result
	}

	if b.ID != "" {
		result.ID = b.ID
	}
	if b.Name != "" {
		result.Name = b.Name
	}
	if b.ParentID != "" {
		result.ParentID = b.ParentID
	}
	if b.Type != "" {
		result.Type = b.Type
	}
	if b.Namespace != "" {
		result.Namespace = b.Namespace
	}
	if b.CreatorId != "" {
		result.CreatorId = b.CreatorId
	}
	if &b.Priority != nil {
		result.Priority = b.Priority
	}
	if &b.Stop != nil {
		result.Stop = b.Stop
	}
	if b.Periodic != nil {
		if result.Periodic != nil {
			result.Periodic = result.Periodic.Merge(b.Periodic)
		} else {
			result.Periodic = b.Periodic
		}
	}
	if &b.AllAtOnce != nil {
		result.AllAtOnce = b.AllAtOnce
	}
	if b.Status != "" {
		result.Status = b.Status
	}
	if b.Description != "" {
		result.Description = b.Description
	}
	if b.BusinessType != "" {
		result.BusinessType = b.BusinessType
	}
	if b.Parameters != nil && len(b.Parameters) > 0 {
		result.Parameters = helper.CopySliceByte(b.Parameters)
	}
	if &b.SubmitTime != nil {
		result.SubmitTime = b.SubmitTime
	}
	if &b.CreateTime != nil {
		result.CreateTime = b.CreateTime
	}
	if &b.UpdateTime != nil {
		result.UpdateTime = b.UpdateTime
	}
	if &b.Version != nil {
		result.Version = b.Version
	}
	if &b.CreateIndex != nil {
		result.CreateIndex = b.CreateIndex
	}
	if &b.ModifyIndex != nil {
		result.ModifyIndex = b.ModifyIndex
	}
	if &b.PlanModifyIndex != nil {
		result.PlanModifyIndex = b.PlanModifyIndex
	}
	return result
}

func (p *Plan) GetNamespace() string {
	if p == nil {
		return ""
	}
	return p.Namespace
}
