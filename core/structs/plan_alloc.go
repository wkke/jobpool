package structs

import (
	"yunli.com/jobpool/core/constant"
)

type PlanAlloc struct {
	// msgpack omit empty fields during serialization
	_struct bool `codec:",omitempty"` // nolint: structcheck

	// EvalID is the evaluation ID this plan is associated with
	EvalID string

	// EvalToken is used to prevent a split-brain processing of
	// an evaluation. There should only be a single scheduler running
	// an Eval at a time, but this could be violated after a leadership
	// transition. This unique token is used to reject plans that are
	// being submitted from a different leader.
	EvalToken string

	// Priority is the priority of the upstream job
	Priority int

	// AllAtOnce is used to control if incremental scheduling of task groups
	// is allowed or if we must do a gang scheduling of the entire job.
	// If this is false, a plan may be partially applied. Otherwise, the
	// entire plan must be able to make progress.
	AllAtOnce bool

	Plan *Plan

	JobId string

	// NodeUpdate contains all the allocations for each node. For each node,
	// this is a list of the allocations to update to either stop or evict.
	NodeUpdate map[string][]*Allocation

	// NodeAllocation contains all the allocations for each node.
	// The evicts must be considered prior to the allocations.
	NodeAllocation map[string][]*Allocation

	// Annotations contains annotations by the scheduler to be used by operators
	// to understand the decisions made by the scheduler.
	Annotations *PlanAnnotations

	// NodePreemptions is a map from node id to a set of allocations from other
	// lower priority jobs that are preempted. Preempted allocations are marked
	// as evicted.
	NodePreemptions map[string][]*Allocation

	// SnapshotIndex is the Raft index of the snapshot used to create the
	// Plan. The leader will wait to evaluate the plan until its StateStore
	// has reached at least this index.
	SnapshotIndex uint64
}

// PlanAnnotations holds annotations made by the scheduler to give further debug
// information to operators.
type PlanAnnotations struct {

	// PreemptedAllocs is the set of allocations to be preempted to make the placement successful.
	PreemptedAllocs []*AllocListStub
}

// AllocListStub is used to return a subset of alloc information
type AllocListStub struct {
	ID                    string
	EvalID                string
	Name                  string
	Namespace             string
	NodeID                string
	NodeName              string
	JobID                 string
	JobType               string
	JobVersion            uint64
	TaskGroup             string
	DesiredStatus         string
	DesiredDescription    string
	ClientStatus          string
	ClientDescription     string
	FollowupEvalID        string
	PreemptedAllocations  []string
	PreemptedByAllocation string
	CreateIndex           uint64
	ModifyIndex           uint64
	CreateTime            int64
	ModifyTime            int64
}

// PlanResult is the result of a plan submitted to the leader.
type PlanResult struct {
	// NodeUpdate contains all the updates that were committed.
	NodeUpdate map[string][]*Allocation

	// NodeAllocation contains all the allocations that were committed.
	NodeAllocation map[string][]*Allocation

	// NodePreemptions is a map from node id to a set of allocations from other
	// lower priority jobs that are preempted. Preempted allocations are marked
	// as stopped.
	NodePreemptions map[string][]*Allocation

	// RefreshIndex is the index the worker should refresh state up to.
	// This allows all evictions and allocations to be materialized.
	// If any allocations were rejected due to stale data (node state,
	// over committed) this can be used to force a worker refresh.
	RefreshIndex uint64

	// AllocIndex is the Raft index in which the evictions and
	// allocations took place. This is used for the write index.
	AllocIndex uint64
}

// IsNoOp checks if this plan result would do nothing
func (p *PlanResult) IsNoOp() bool {
	return len(p.NodeUpdate) == 0 && len(p.NodeAllocation) == 0
}

func (p *PlanResult) FullCommit(planAlloc *PlanAlloc) (bool, int, int) {
	expected := 0
	actual := 0
	for name, allocList := range planAlloc.NodeAllocation {
		didAlloc := p.NodeAllocation[name]
		expected += len(allocList)
		actual += len(didAlloc)
	}
	return actual == expected, expected, actual
}

// NormalizeAllocations normalizes allocations to remove fields that can
// be fetched from the MemDB instead of sending over the wire
func (p *PlanAlloc) NormalizeAllocations() {
	for _, allocs := range p.NodeUpdate {
		for i, alloc := range allocs {
			allocs[i] = &Allocation{
				ID:                 alloc.ID,
				DesiredDescription: alloc.DesiredDescription,
				ClientStatus:       alloc.ClientStatus,
				FollowupEvalID:     alloc.FollowupEvalID,
			}
		}
	}

	for _, allocs := range p.NodePreemptions {
		for i, alloc := range allocs {
			allocs[i] = &Allocation{
				ID:                    alloc.ID,
				PreemptedByAllocation: alloc.PreemptedByAllocation,
			}
		}
	}
}

// AppendStoppedAlloc marks an allocation to be stopped. The clientStatus of the
// allocation may be optionally set by passing in a non-empty value.
func (p *PlanAlloc) AppendStoppedAlloc(alloc *Allocation, desiredDesc, clientStatus, followupEvalID string) {
	newAlloc := new(Allocation)
	*newAlloc = *alloc

	// If the job is not set in the plan we are deregistering a job so we
	// extract the job from the allocation.
	if p.Plan == nil && newAlloc.Plan != nil {
		p.Plan = newAlloc.Plan
	}

	// Normalize the job
	newAlloc.Plan = nil

	newAlloc.DesiredStatus = constant.AllocDesiredStatusStop
	newAlloc.DesiredDescription = desiredDesc

	if clientStatus != "" {
		newAlloc.ClientStatus = clientStatus
	}

	// newAlloc.AppendState(AllocStateFieldClientStatus, clientStatus)

	if followupEvalID != "" {
		newAlloc.FollowupEvalID = followupEvalID
	}

	node := alloc.NodeID
	existing := p.NodeUpdate[node]
	p.NodeUpdate[node] = append(existing, newAlloc)
}

func (p *PlanAlloc) AppendAlloc(alloc *Allocation, plan *Plan) {
	node := alloc.NodeID
	existing := p.NodeAllocation[node]
	alloc.Plan = plan
	p.NodeAllocation[node] = append(existing, alloc)
}

// IsNoOp checks if this plan would do nothing
func (p *PlanAlloc) IsNoOp() bool {
	return len(p.NodeUpdate) == 0 &&
		len(p.NodeAllocation) == 0
}
