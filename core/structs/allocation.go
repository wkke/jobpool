package structs

import (
	"yunli.com/jobpool/core/constant"
	xtime "yunli.com/jobpool/helper/time"
)

type Allocation struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	EvalID    string `json:"evalId"`
	NodeID    string `json:"nodeId"`

	ClientDescription string `json:"clientDescription"`

	PlanID string `json:"planId"`
	Plan   *Plan  `json:"-"`
	JobId  string `json:"jobId"`

	ClientStatus string `json:"clientStatus"`

	NextAllocation string

	// Desired Status of the allocation on the client
	DesiredStatus      string
	DesiredDescription string
	DesiredTransition  DesiredTransition

	FollowupEvalID string

	// PreviousAllocation is the allocation that this allocation is replacing
	PreviousAllocation string

	AllocModifyIndex uint64

	// PreemptedByAllocation tracks the alloc ID of the allocation that caused this allocation
	// to stop running because it got preempted
	PreemptedByAllocation string

	// Raft Indexes
	CreateIndex uint64
	ModifyIndex uint64

	// CreateTime is the time the allocation has finished scheduling and been
	// verified by the plan applier.
	CreateTime xtime.FormatTime `json:"createTime"`

	// UpdateTime is the time the allocation was last updated.
	ModifyTime xtime.FormatTime `json:"updateTime"`
}

// Terminated returns if the allocation is in a terminal state on a client.
func (a *Allocation) Terminated() bool {
	if a.ClientStatus == constant.AllocClientStatusFailed ||
		a.ClientStatus == constant.AllocClientStatusComplete ||
		a.ClientStatus == constant.AllocClientStatusSkipped ||
		a.ClientStatus == constant.AllocClientStatusExpired ||
		a.ClientStatus == constant.AllocClientStatusCancelled {
		return true
	}
	return false
}

// TerminalStatus returns if the desired or actual status is terminal and
// will no longer transition.
func (a *Allocation) TerminalStatus() bool {
	// First check the desired state and if that isn't terminal, check client
	// state.
	return a.ServerTerminalStatus() || a.ClientTerminalStatus()
}

func (a *Allocation) MigrateStatus() bool {
	// 检查是否需要迁移这个alloc
	switch a.ClientStatus {
	case constant.AllocClientStatusPending:
		return true
	case constant.AllocClientStatusRunning:
		if a.Plan != nil && a.Plan.BusinessType == constant.PlanBusinessTypeCurl {
			return true
		} else {
			return false
		}
	default:
		return false
	}
}

// ServerTerminalStatus returns true if the desired state of the allocation is terminal
func (a *Allocation) ServerTerminalStatus() bool {
	switch a.DesiredStatus {
	case constant.AllocDesiredStatusStop, constant.AllocDesiredStatusEvict:
		return true
	default:
		return false
	}
}

// ClientTerminalStatus returns if the client status is terminal and will no longer transition
func (a *Allocation) ClientTerminalStatus() bool {
	switch a.ClientStatus {
	case constant.AllocClientStatusComplete, constant.AllocClientStatusFailed, constant.AllocClientStatusCancelled:
		return true
	default:
		return false
	}
}

// 将alloc的状态转换为job状态，二者状态应该是同步的
//func (a *Allocation) ConvertJobStatus() string {
//	switch a.ClientStatus {
//	case constant.AllocClientStatusComplete:
//		return constant.JobStatusComplete
//	case constant.AllocClientStatusFailed, constant.AllocClientStatusLost:
//		return constant.JobStatusFailed
//	case constant.AllocClientStatusRunning:
//		return constant.JobStatusRunning
//	case constant.AllocClientStatusUnknown:
//		return constant.JobStatusRunning
//	}
//	switch a.DesiredStatus {
//	case constant.AllocDesiredStatusStop:
//		return constant.JobStatusComplete
//	}
//	return constant.JobStatusRunning
//}

// Copy provides a copy of the allocation and deep copies the plan
func (a *Allocation) Copy() *Allocation {
	return a.copyImpl(true)
}

func (a *Allocation) copyImpl(plan bool) *Allocation {
	if a == nil {
		return nil
	}
	na := new(Allocation)
	*na = *a

	if plan {
		na.Plan = na.Plan.Copy()
	}
	return na
}

// AllocationDiff is another named type for Allocation (to use the same fields),
// which is used to represent the delta for an Allocation. If you need a method
// defined on the al
type AllocationDiff Allocation

// DenormalizeAllocationPlans is used to attach a plan to all allocations that are
// non-terminal and do not have a plan already. This is useful in cases where the
// plan is normalized.
func DenormalizeAllocationPlans(plan *Plan, allocs []*Allocation) {
	if plan != nil {
		for _, alloc := range allocs {
			if alloc.Plan == nil && !alloc.TerminalStatus() {
				alloc.Plan = plan
			}
		}
	}
}

func (a *Allocation) AllocationDiff() *AllocationDiff {
	return (*AllocationDiff)(a)
}

// Canonicalize Allocation to ensure fields are initialized to the expectations
// of this version of Jobpool. Should be called when restoring persisted
// Allocations or receiving Allocations from Jobpool agents potentially on an
// older version of Jobpool.
func (a *Allocation) Canonicalize() {
	a.Plan.Canonicalize()
}

func AllocSubset(allocs []*Allocation, subset []*Allocation) bool {
	if len(subset) == 0 {
		return true
	}
	// Convert allocs into a map
	allocMap := make(map[string]struct{})
	for _, alloc := range allocs {
		allocMap[alloc.ID] = struct{}{}
	}

	for _, alloc := range subset {
		if _, ok := allocMap[alloc.ID]; !ok {
			return false
		}
	}
	return true
}
func RemoveAllocs(allocs []*Allocation, remove []*Allocation) []*Allocation {
	if len(remove) == 0 {
		return allocs
	}
	// Convert remove into a set
	removeSet := make(map[string]struct{})
	for _, remove := range remove {
		removeSet[remove.ID] = struct{}{}
	}

	r := make([]*Allocation, 0, len(allocs))
	for _, alloc := range allocs {
		if _, ok := removeSet[alloc.ID]; !ok {
			r = append(r, alloc)
		}
	}
	return r
}

// TODO 根据资源情况找到合适的client进行分配
func AllocsFit(node *Node, allocs []*Allocation, checkDevices bool) (bool, string, error) {

	// Allocations fit!
	return true, "", nil
}

func (a *Allocation) GetNamespace() string {
	if a == nil {
		return ""
	}
	return a.Namespace
}

func (a *Allocation) PlanNamespacedID() NamespacedID {
	return NewNamespacedID(a.PlanID, a.Namespace)
}

// 迁移标记
type DesiredTransition struct {
	// Migrate is used to indicate that this allocation should be stopped and
	// migrated to another node.
	Migrate *bool

	// Reschedule is used to indicate that this allocation is eligible to be
	// rescheduled.
	Reschedule *bool

	ForceReschedule *bool
}

// ShouldMigrate returns whether the transition object dictates a migration.
func (d DesiredTransition) ShouldMigrate() bool {
	return d.Migrate != nil && *d.Migrate
}

func (d *DesiredTransition) Merge(o *DesiredTransition) {
	if o.Migrate != nil {
		d.Migrate = o.Migrate
	}

	if o.Reschedule != nil {
		d.Reschedule = o.Reschedule
	}

	if o.ForceReschedule != nil {
		d.ForceReschedule = o.ForceReschedule
	}
}
