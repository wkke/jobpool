package core

import (
	"context"
	"fmt"
	"github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/raft"
	"runtime"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/scheduler"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
	"yunli.com/jobpool/helper/uuid"
)

type planner struct {
	*Server
	log       log.Logger
	planQueue *scheduler.PlanQueue
}

func newPlanner(s *Server) (*planner, error) {
	planQueue, err := scheduler.NewPlanQueue()
	if err != nil {
		return nil, err
	}
	return &planner{
		Server:    s,
		log:       s.logger,
		planQueue: planQueue,
	}, nil
}

// 任务分配逻辑
func (p *planner) planApply() {
	p.logger.Info("------start apply plan in leader node-----")
	// planIndexCh is used to track an outstanding application and receive
	// its committed index while snap holds an optimistic state which
	// includes that plan application.
	var planIndexCh chan uint64
	var snap *state.StateSnapshot

	// prevPlanResultIndex is the index when the last PlanResult was
	// committed. Since only the last plan is optimistically applied to the
	// snapshot, it's possible the current snapshot's and plan's indexes
	// are less than the index the previous plan result was committed at.
	// prevPlanResultIndex also guards against the previous plan committing
	// during Dequeue, thus causing the snapshot containing the optimistic
	// commit to be discarded and potentially evaluating the current plan
	// against an index older than the previous plan was committed at.
	var prevPlanResultIndex uint64

	// Setup a worker pool with half the cores, with at least 1
	poolSize := runtime.NumCPU() / 2
	if poolSize == 0 {
		poolSize = 1
	}
	pool := scheduler.NewEvaluatePool(poolSize, 64, p)
	defer pool.Shutdown()

	for {
		// Pull the next pending plan, exit if we are no longer leader
		pending, err := p.planQueue.Dequeue(0)
		if err != nil {
			return
		}
		p.logger.Debug("get pending plan from plan Queue", "pending plan", pending)

		// If last plan has completed get a new snapshot
		select {
		case idx := <-planIndexCh:
			// Previous plan committed. Discard snapshot and ensure
			// future snapshots include this plan. idx may be 0 if
			// plan failed to apply, so use max(prev, idx)
			prevPlanResultIndex = max(prevPlanResultIndex, idx)
			planIndexCh = nil
			snap = nil
		default:
		}

		if snap != nil {
			// If snapshot doesn't contain the previous plan
			// result's index and the current plan's snapshot it,
			// discard it and get a new one below.
			minIndex := max(prevPlanResultIndex, pending.Plan.SnapshotIndex)
			if idx, err := snap.LatestIndex(); err != nil || idx < minIndex {
				snap = nil
			}
		}

		// Snapshot the state so that we have a consistent view of the world
		// if no snapshot is available.
		//  - planIndexCh will be nil if the previous plan result applied
		//    during Dequeue
		//  - snap will be nil if its index < max(prevIndex, curIndex)
		if planIndexCh == nil || snap == nil {
			snap, err = p.snapshotMinIndex(prevPlanResultIndex, pending.Plan.SnapshotIndex)
			if err != nil {
				p.logger.Error("failed to snapshot state", "error", err)
				pending.Respond(nil, err)
				continue
			}
		}

		// Evaluate the plan
		result, err := evaluatePlan(pool, snap, pending.Plan, p.logger)
		if err != nil {
			p.logger.Error("failed to evaluate plan", "error", err)
			pending.Respond(nil, err)
			continue
		}
		p.logger.Debug("---- get IsNoOp of the plan", "result", result, "IsNoOp", result.IsNoOp())

		// Fast-path the response if there is nothing to do
		if result.IsNoOp() {
			pending.Respond(result, nil)
			continue
		}

		// Ensure any parallel apply is complete before starting the next one.
		// This also limits how out of date our snapshot can be.
		if planIndexCh != nil {
			idx := <-planIndexCh
			prevPlanResultIndex = max(prevPlanResultIndex, idx)
			snap, err = p.snapshotMinIndex(prevPlanResultIndex, pending.Plan.SnapshotIndex)
			if err != nil {
				p.logger.Error("failed to update snapshot state", "error", err)
				pending.Respond(nil, err)
				continue
			}
		}

		// Dispatch the Raft transaction for the plan
		future, err := p.applyPlan(pending.Plan, result, snap)
		if err != nil {
			p.logger.Error("failed to submit plan", "error", err)
			pending.Respond(nil, err)
			continue
		}

		// Respond to the plan in async; receive plan's committed index via chan
		planIndexCh = make(chan uint64, 1)
		go p.asyncPlanWait(planIndexCh, future, result, pending)
	}
}

// asyncPlanWait is used to apply and respond to a plan async. On successful
// commit the plan's index will be sent on the chan. On error the chan will be
// closed.
func (p *planner) asyncPlanWait(indexCh chan<- uint64, future raft.ApplyFuture,
	result *structs.PlanResult, pending *scheduler.PendingPlan) {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "plan", "apply"}, time.Now())

	// Wait for the plan to apply
	if err := future.Error(); err != nil {
		p.logger.Error("failed to apply plan", "error", err)
		pending.Respond(nil, err)
		// Close indexCh on error
		close(indexCh)
		return
	}

	// Respond to the plan
	index := future.Index()
	result.AllocIndex = index

	// If this is a partial plan application, we need to ensure the scheduler
	// at least has visibility into any placements it made to avoid double placement.
	// The RefreshIndex computed by evaluatePlan may be stale due to evaluation
	// against an optimistic copy of the state.
	if result.RefreshIndex != 0 {
		result.RefreshIndex = maxUint64(result.RefreshIndex, result.AllocIndex)
	}
	pending.Respond(result, nil)
	indexCh <- index
}

// evaluatePlan is used to determine what portions of a plan
// can be applied if any. Returns if there should be a plan application
// which may be partial or if there was an error
func evaluatePlan(pool *scheduler.EvaluatePool, snap *state.StateSnapshot, plan *structs.PlanAlloc, logger log.Logger) (*structs.PlanResult, error) {
	defer metrics.MeasureSince([]string{"jobpool", "plan", "evaluate"}, time.Now())

	logger.Debug("evaluating plan", "plan", log.Fmt("%#v", plan))
	// TODO compute the quota
	/*
		// Check if the plan exceeds quota
		overQuota, err := evaluatePlanQuota(snap, plan)
		if err != nil {
			return nil, err
		}

		// Reject the plan and force the scheduler to refresh
		if overQuota {
			index, err := refreshIndex(snap)
			if err != nil {
				return nil, err
			}

			logger.Debug("plan for evaluation exceeds quota limit. Forcing state refresh", "eval_id", plan.EvalID, "refresh_index", index)
			return &structs.PlanResult{RefreshIndex: index}, nil
		}
	*/
	return evaluatePlanPlacements(pool, snap, plan, logger)
}

// evaluatePlanPlacements is used to determine what portions of a plan can be
// applied if any, looking for node over commitment. Returns if there should be
// a plan application which may be partial or if there was an error
func evaluatePlanPlacements(pool *scheduler.EvaluatePool, snap *state.StateSnapshot, plan *structs.PlanAlloc, logger log.Logger) (*structs.PlanResult, error) {
	// Create a result holder for the plan
	result := &structs.PlanResult{
		NodeUpdate:      make(map[string][]*structs.Allocation),
		NodeAllocation:  make(map[string][]*structs.Allocation),
		NodePreemptions: make(map[string][]*structs.Allocation),
	}

	// Collect all the nodeIDs
	nodeIDs := make(map[string]struct{})
	nodeIDList := make([]string, 0, len(plan.NodeUpdate)+len(plan.NodeAllocation))
	for nodeID := range plan.NodeUpdate {
		if _, ok := nodeIDs[nodeID]; !ok {
			nodeIDs[nodeID] = struct{}{}
			nodeIDList = append(nodeIDList, nodeID)
		}
	}
	for nodeID := range plan.NodeAllocation {
		if _, ok := nodeIDs[nodeID]; !ok {
			nodeIDs[nodeID] = struct{}{}
			nodeIDList = append(nodeIDList, nodeID)
		}
	}

	// Setup a multierror to handle potentially getting many
	// errors since we are processing in parallel.
	var mErr multierror.Error
	partialCommit := false

	// handleResult is used to process the result of evaluateNodePlan
	handleResult := func(nodeID string, fit bool, reason string, err error) (cancel bool) {
		// Evaluate the plan for this node
		if err != nil {
			mErr.Errors = append(mErr.Errors, err)
			return true
		}
		if !fit {
			metrics.IncrCounterWithLabels([]string{constant.JobPoolName, "plan", "node_rejected"}, 1, []metrics.Label{{Name: "node_id", Value: nodeID}})

			// Log the reason why the node's allocations could not be made
			if reason != "" {
				logger.Info("plan for node rejected",
					"node_id", nodeID, "reason", reason, "eval_id", plan.EvalID,
					"namespace", plan.Plan.Namespace)
			}
			// Set that this is a partial commit
			partialCommit = true

			// If we require all-at-once scheduling, there is no point
			// to continue the evaluation, as we've already failed.
			if plan.AllAtOnce {
				result.NodeUpdate = nil
				result.NodeAllocation = nil
				result.NodePreemptions = nil
				return true
			}
			// Skip this node, since it cannot be used.
			return
		}

		// Add this to the plan result
		if nodeUpdate := plan.NodeUpdate[nodeID]; len(nodeUpdate) > 0 {
			result.NodeUpdate[nodeID] = nodeUpdate
		}
		if nodeAlloc := plan.NodeAllocation[nodeID]; len(nodeAlloc) > 0 {
			result.NodeAllocation[nodeID] = nodeAlloc
		}

		if nodePreemptions := plan.NodePreemptions[nodeID]; nodePreemptions != nil {
			// Do a pass over preempted allocs in the plan to check
			// whether the alloc is already in a terminal state
			var filteredNodePreemptions []*structs.Allocation
			for _, preemptedAlloc := range nodePreemptions {
				alloc, err := snap.AllocByID(nil, preemptedAlloc.ID)
				if err != nil {
					mErr.Errors = append(mErr.Errors, err)
					continue
				}
				if alloc != nil && !alloc.TerminalStatus() {
					filteredNodePreemptions = append(filteredNodePreemptions, preemptedAlloc)
				}
			}

			result.NodePreemptions[nodeID] = filteredNodePreemptions
		}

		return
	}

	// Get the pool channels
	req := pool.RequestCh()
	resp := pool.ResultCh()
	outstanding := 0
	didCancel := false

	// Evaluate each node in the plan, handling results as they are ready to
	// avoid blocking.
OUTER:
	for len(nodeIDList) > 0 {
		nodeID := nodeIDList[0]
		select {
		case req <- scheduler.EvaluateRequest{snap, plan, nodeID}:
			outstanding++
			nodeIDList = nodeIDList[1:]
		case r := <-resp:
			outstanding--

			// Handle a result that allows us to cancel evaluation,
			// which may save time processing additional entries.
			if cancel := handleResult(r.NodeID, r.Fit, r.Reason, r.Err); cancel {
				didCancel = true
				break OUTER
			}
		}
	}
	// Drain the remaining results
	for outstanding > 0 {
		r := <-resp
		if !didCancel {
			if cancel := handleResult(r.NodeID, r.Fit, r.Reason, r.Err); cancel {
				didCancel = true
			}
		}
		outstanding--
	}

	// If the plan resulted in a partial commit, we need to determine
	// a minimum refresh index to force the scheduler to work on a more
	// up-to-date state to avoid the failures.
	if partialCommit {
		index, err := refreshIndex(snap)
		if err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
		result.RefreshIndex = index

		if result.RefreshIndex == 0 {
			err := fmt.Errorf("partialCommit with RefreshIndex of 0")
			mErr.Errors = append(mErr.Errors, err)
		}
	}
	return result, mErr.ErrorOrNil()
}

// refreshIndex returns the index the scheduler should refresh to as the maximum
// of both the allocation and node tables.
func refreshIndex(snap *state.StateSnapshot) (uint64, error) {
	allocIndex, err := snap.Index("allocs")
	if err != nil {
		return 0, err
	}
	nodeIndex, err := snap.Index("nodes")
	if err != nil {
		return 0, err
	}
	return maxUint64(nodeIndex, allocIndex), nil
}

func (p *planner) snapshotMinIndex(prevPlanResultIndex, planSnapshotIndex uint64) (*state.StateSnapshot, error) {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "plan", "wait_for_index"}, time.Now())

	// Minimum index the snapshot must include is the max of the previous
	// plan result's and current plan's snapshot index.
	minIndex := max(prevPlanResultIndex, planSnapshotIndex)

	const timeout = 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	snap, err := p.fsm.State().SnapshotMinIndex(ctx, minIndex)
	cancel()
	if err == context.DeadlineExceeded {
		return nil, fmt.Errorf("timed out after %s waiting for index=%d (previous plan result index=%d; plan snapshot index=%d)",
			timeout, minIndex, prevPlanResultIndex, planSnapshotIndex)
	}

	return snap, err
}

// applyPlan is used to apply the plan result and to return the alloc index
func (p *planner) applyPlan(planAlloc *structs.PlanAlloc, result *structs.PlanResult, snap *state.StateSnapshot) (raft.ApplyFuture, error) {
	// Setup the update request
	req := dto.ApplyPlanResultsRequest{
		AllocUpdateRequest: dto.AllocUpdateRequest{
			Plan: planAlloc.Plan,
		},
		EvalID: planAlloc.EvalID,
	}

	preemptedJobIDs := make(map[structs.NamespacedID]struct{})
	// now := time.Now().UTC().UnixNano()
	now := xtime.NewFormatTime(time.Now())

	// Initialize the allocs request using the new optimized log entry format.
	// Determine the minimum number of updates, could be more if there
	// are multiple updates per node
	req.AllocsStopped = make([]*structs.AllocationDiff, 0, len(result.NodeUpdate))
	req.AllocsUpdated = make([]*structs.Allocation, 0, len(result.NodeAllocation))
	req.AllocsPreempted = make([]*structs.AllocationDiff, 0, len(result.NodePreemptions))

	for _, updateList := range result.NodeUpdate {
		for _, stoppedAlloc := range updateList {
			req.AllocsStopped = append(req.AllocsStopped, normalizeStoppedAlloc(stoppedAlloc, now))
		}
	}

	for _, allocList := range result.NodeAllocation {
		req.AllocsUpdated = append(req.AllocsUpdated, allocList...)
	}

	// Set the time the alloc was applied for the first time. This can be used
	// to approximate the scheduling time.
	updateAllocTimestamps(req.AllocsUpdated, now)

	for _, preemptions := range result.NodePreemptions {
		for _, preemptedAlloc := range preemptions {
			req.AllocsPreempted = append(req.AllocsPreempted, normalizePreemptedAlloc(preemptedAlloc, now))

			// Gather jobids to create follow up evals
			appendNamespacedJobID(preemptedJobIDs, preemptedAlloc)
		}
	}

	var evals []*structs.Evaluation
	for preemptedJobID := range preemptedJobIDs {
		plan, _ := p.State().PlanByID(nil, preemptedJobID.Namespace, preemptedJobID.ID)
		if plan != nil {
			eval := &structs.Evaluation{
				ID:          uuid.Generate(),
				Namespace:   plan.Namespace,
				TriggeredBy: constant.EvalTriggerPreemption,
				PlanID:      plan.ID,
				Type:        plan.Type,
				Priority:    plan.Priority,
				Status:      constant.EvalStatusPending,
				CreateTime:  xtime.NewFormatTime(time.Now()),
				UpdateTime:  xtime.NewFormatTime(time.Now()),
			}
			evals = append(evals, eval)
		}
	}
	req.PreemptionEvals = evals

	// Dispatch the Raft transaction
	future, err := p.raftApplyFuture(constant.ApplyPlanResultsRequestType, &req)
	if err != nil {
		return nil, err
	}

	// Optimistically apply to our state view
	if snap != nil {
		nextIdx := p.raft.AppliedIndex() + 1
		if err := snap.UpsertPlanResults(constant.ApplyPlanResultsRequestType, nextIdx, &req); err != nil {
			return future, err
		}
	}
	return future, nil
}

// normalizeStoppedAlloc removes redundant fields from a stopped allocation and
// returns AllocationDiff. Since a stopped allocation is always an existing allocation,
// the struct returned by this method contains only the differential, which can be
// applied to an existing allocation, to yield the updated struct
func normalizeStoppedAlloc(stoppedAlloc *structs.Allocation, now xtime.FormatTime) *structs.AllocationDiff {
	return &structs.AllocationDiff{
		ID:                 stoppedAlloc.ID,
		DesiredDescription: stoppedAlloc.DesiredDescription,
		ClientStatus:       stoppedAlloc.ClientStatus,
		ModifyTime:         now,
		FollowupEvalID:     stoppedAlloc.FollowupEvalID,
	}
}

func updateAllocTimestamps(allocations []*structs.Allocation, timestamp xtime.FormatTime) {
	for _, alloc := range allocations {
		if alloc.CreateTime == "" {
			alloc.CreateTime = timestamp
		}
		alloc.ModifyTime = timestamp
	}
}

func normalizePreemptedAlloc(preemptedAlloc *structs.Allocation, now xtime.FormatTime) *structs.AllocationDiff {
	return &structs.AllocationDiff{
		ID:                    preemptedAlloc.ID,
		PreemptedByAllocation: preemptedAlloc.PreemptedByAllocation,
		ModifyTime:            now,
	}
}
func appendNamespacedJobID(jobIDs map[structs.NamespacedID]struct{}, alloc *structs.Allocation) {
	id := structs.NamespacedID{Namespace: alloc.Namespace, ID: alloc.PlanID}
	if _, ok := jobIDs[id]; !ok {
		jobIDs[id] = struct{}{}
	}
}

func (p *planner) EvaluateNodePlan(snap *state.StateSnapshot, plan *structs.PlanAlloc, nodeID string) (bool, string, error) {
	// If this is an evict-only plan, it always 'fits' since we are removing things.
	if len(plan.NodeAllocation[nodeID]) == 0 {
		return true, "", nil
	}

	// Get the node itself
	ws := memdb.NewWatchSet()
	node, err := snap.NodeByID(ws, nodeID)
	if err != nil {
		return false, "", fmt.Errorf("failed to get node '%s': %v", nodeID, err)
	}

	// If the node does not exist or is not ready for scheduling it is not fit
	// XXX: There is a potential race between when we do this check and when
	// the Raft commit happens.
	if node == nil {
		return false, "node does not exist", nil
	} else if node.Status == constant.NodeStatusDisconnected {
		if isValidForDisconnectedNode(plan, node.ID) {
			return true, "", nil
		}
		return false, "node is disconnected and contains invalid updates", nil
	} else if node.Status != constant.NodeStatusReady {
		return false, "node is not ready for placements", nil
	}

	// Get the existing allocations that are non-terminal
	existingAlloc, err := snap.AllocsByNodeTerminal(ws, nodeID, false)
	if err != nil {
		return false, "", fmt.Errorf("failed to get existing allocations for '%s': %v", nodeID, err)
	}

	// If nodeAllocations is a subset of the existing allocations we can continue,
	// even if the node is not eligible, as only in-place updates or stop/evict are performed
	if structs.AllocSubset(existingAlloc, plan.NodeAllocation[nodeID]) {
		return true, "", nil
	}
	if node.SchedulingEligibility == constant.NodeSchedulingIneligible {
		return false, "node is not eligible", nil
	}

	// Determine the proposed allocation by first removing allocations
	// that are planned evictions and adding the new allocations.
	var remove []*structs.Allocation
	if update := plan.NodeUpdate[nodeID]; len(update) > 0 {
		remove = append(remove, update...)
	}

	// Remove any preempted allocs
	if preempted := plan.NodePreemptions[nodeID]; len(preempted) > 0 {
		remove = append(remove, preempted...)
	}

	if updated := plan.NodeAllocation[nodeID]; len(updated) > 0 {
		remove = append(remove, updated...)
	}
	proposed := structs.RemoveAllocs(existingAlloc, remove)
	proposed = append(proposed, plan.NodeAllocation[nodeID]...)

	// Check if these allocations fit
	fit, reason, err := structs.AllocsFit(node, proposed, true)
	return fit, reason, err
}

// The plan is only valid for disconnected nodes if it only contains
// updates to mark allocations as unknown.
func isValidForDisconnectedNode(plan *structs.PlanAlloc, nodeID string) bool {
	for _, alloc := range plan.NodeAllocation[nodeID] {
		if alloc.ClientStatus != constant.AllocClientStatusUnknown {
			return false
		}
	}

	return true
}
func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
