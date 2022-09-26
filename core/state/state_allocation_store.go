package state

import (
	"fmt"
	"github.com/hashicorp/go-memdb"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
)

func (s *StateStore) Allocs(ws memdb.WatchSet) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	// Walk the entire allocs table
	iter, err := txn.GetReverse("allocs", "create")
	if err != nil {
		return nil, err
	}
	ws.Add(iter.WatchCh())
	return iter, nil
}

// 因为槽位满导致的状态更新
func (s *StateStore) UpdateAllocsStatusFailed(msgType constant.MessageType, index uint64, eval *structs.Evaluation) error {
	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()
	existing, err := txn.First("allocs", "eval", eval.ID)
	if err != nil {
		return fmt.Errorf("alloc lookup failed: %v", err)
	}
	reason := "the slot of jobRoadMap is full, reject the new job"
	if existing != nil {

		exist := existing.(*structs.Allocation)
		alloc := exist.Copy()
		alloc.ClientStatus = constant.AllocClientStatusFailed
		alloc.ModifyIndex = index
		alloc.AllocModifyIndex = index
		alloc.ClientDescription = reason
		if err := txn.Insert("allocs", alloc); err != nil {
			return fmt.Errorf("alloc insert failed: %v", err)
		}
	}
	// update eval
	existing, err = txn.First("evals", "id", eval.ID)
	if err != nil {
		return fmt.Errorf("eval lookup failed: %v", err)
	}
	if existing != nil {
		existEval := existing.(*structs.Evaluation)
		evalUpdate := existEval.Copy()
		evalUpdate.Status = constant.EvalStatusFailed
		evalUpdate.StatusDescription = reason
		if err := txn.Insert("evals", evalUpdate); err != nil {
			return fmt.Errorf("eval insert failed: %v", err)
		}
	}

	// update job
	existing, err = txn.First(Jobs, "id", eval.Namespace, eval.JobID)
	if err != nil {
		return fmt.Errorf("job lookup failed: %v", err)
	}
	if existing != nil {
		existJob := existing.(*structs.Job)
		jobUpdate := existJob.Copy()
		jobUpdate.Status = constant.JobStatusFailed
		if err := txn.Insert(Jobs, jobUpdate); err != nil {
			return fmt.Errorf("job insert failed: %v", err)
		}
	}

	return txn.Commit()
}

// UpsertAllocs is used to evict a set of allocations and allocate new ones at
// the same time.
func (s *StateStore) UpsertAllocs(msgType constant.MessageType, index uint64, allocs []*structs.Allocation) error {
	txn := s.db.WriteTxn(index)
	defer txn.Abort()
	if err := s.upsertAllocsImpl(index, allocs, txn); err != nil {
		return err
	}
	return txn.Commit()
}

// upsertAllocs is the actual implementation of UpsertAllocs so that it may be
// used with an existing transaction.
func (s *StateStore) upsertAllocsImpl(index uint64, allocs []*structs.Allocation, txn *txn) error {
	// Handle the allocations
	plans := make(map[structs.NamespacedID]string, 1)
	for _, alloc := range allocs {
		existing, err := txn.First("allocs", "id", alloc.ID)
		if err != nil {
			return fmt.Errorf("alloc lookup failed: %v", err)
		}
		exist, _ := existing.(*structs.Allocation)

		if exist == nil {
			alloc.CreateIndex = index
			alloc.ModifyIndex = index
			alloc.AllocModifyIndex = index
			if alloc.Plan == nil {
				return fmt.Errorf("attempting to upsert allocation %q without a plan", alloc.ID)
			}
		} else {
			alloc.CreateIndex = exist.CreateIndex
			alloc.ModifyIndex = index
			alloc.AllocModifyIndex = index

			// If the scheduler is marking this allocation as lost or unknown we do not
			// want to reuse the status of the existing allocation.
			//if alloc.ClientStatus != constant.AllocClientStatusLost &&
			//	alloc.ClientStatus != constant.AllocClientStatusUnknown {
			//	alloc.ClientStatus = exist.ClientStatus
			//	alloc.ClientDescription = exist.ClientDescription
			//}

			// The plan has been denormalized so re-attach the original plan
			if alloc.Plan == nil {
				alloc.Plan = exist.Plan
			}
		}
		// info, err := json.Marshal(alloc)
		// s.logger.Info(fmt.Sprintf("the plan alloc json is : %s", info))

		if err := txn.Insert("allocs", alloc); err != nil {
			return fmt.Errorf("alloc insert failed: %v", err)
		}
		if alloc.PreviousAllocation != "" {
			prevAlloc, err := txn.First("allocs", "id", alloc.PreviousAllocation)
			if err != nil {
				return fmt.Errorf("alloc lookup failed: %v", err)
			}
			existingPrevAlloc, _ := prevAlloc.(*structs.Allocation)
			if existingPrevAlloc != nil {
				prevAllocCopy := existingPrevAlloc.Copy()
				prevAllocCopy.NextAllocation = alloc.ID
				prevAllocCopy.ModifyIndex = index
				if err := txn.Insert("allocs", prevAllocCopy); err != nil {
					return fmt.Errorf("alloc insert failed: %v", err)
				}
			}
		}

		// If the allocation is running, force the plan to running status.
		forceStatus := ""
		if !alloc.TerminalStatus() {
			forceStatus = constant.PlanStatusRunning
		}

		tuple := structs.NamespacedID{
			ID:        alloc.PlanID,
			Namespace: alloc.Namespace,
		}
		plans[tuple] = forceStatus
	}

	// Update the indexes
	if err := txn.Insert("index", &IndexEntry{"allocs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	// Set the plan's status
	if err := s.setPlanStatuses(index, txn, plans, false); err != nil {
		return fmt.Errorf("setting plan status failed: %v", err)
	}

	return nil
}

// setPlanStatuses is a helper for calling setPlanStatus on multiple plans by ID.
// It takes a map of plan IDs to an optional forceStatus string. It returns an
// error if the plan doesn't exist or setPlanStatus fails.
func (s *StateStore) setPlanStatuses(index uint64, txn *txn,
	plans map[structs.NamespacedID]string, evalDelete bool) error {
	for tuple, forceStatus := range plans {

		existing, err := txn.First(Plans, "id", tuple.Namespace, tuple.ID)
		if err != nil {
			return fmt.Errorf("plan lookup failed: %v", err)
		}

		if existing == nil {
			continue
		}

		if err := s.setPlanStatus(index, txn, existing.(*structs.Plan), evalDelete, forceStatus); err != nil {
			return err
		}

	}

	return nil
}

// AllocByID is used to lookup an allocation by its ID
func (s *StateStore) AllocByID(ws memdb.WatchSet, id string) (*structs.Allocation, error) {
	txn := s.db.ReadTxn()
	return s.allocByIDImpl(txn, ws, id)
}

// allocByIDImpl retrives an allocation and is called under and existing
// transaction. An optional watch set can be passed to add allocations to the
// watch set
func (s *StateStore) allocByIDImpl(txn Txn, ws memdb.WatchSet, id string) (*structs.Allocation, error) {
	watchCh, raw, err := txn.FirstWatch("allocs", "id", id)
	if err != nil {
		return nil, fmt.Errorf("alloc lookup failed: %v", err)
	}

	ws.Add(watchCh)

	if raw == nil {
		return nil, nil
	}
	alloc := raw.(*structs.Allocation)
	return alloc, nil
}

// AllocsByNode returns all the allocations by node
func (s *StateStore) AllocsByNode(ws memdb.WatchSet, node string) ([]*structs.Allocation, error) {
	txn := s.db.ReadTxn()
	return allocsByNodeTxn(txn, ws, node)
}

// 根据节点和状态查询alloc信息
func (s *StateStore) AllocsByStatusBefore(ws memdb.WatchSet, minIndex uint64, minuteBefore time.Time, state string) ([]*structs.Allocation, uint64, error) {
	var err error
	var iter memdb.ResultIterator
	iter, err = s.Allocs(ws)
	if err != nil {
		return nil, minIndex, err
	}
	var allocs []*structs.Allocation
	var maxIndex uint64 = 0
	failedJob := make(map[string]*struct{})
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		alloc := raw.(*structs.Allocation)
		// 如果alloc对应的job为failed，但是alloc为pending，也需要拿到
		if state == constant.AllocClientStatusPending && alloc.ClientStatus == state {
			job, _ := s.JobByID(ws, alloc.Namespace, alloc.JobId)
			if job != nil && job.Status == constant.JobStatusFailed {
				failedJob[job.ID] = &struct{}{}
			}
		}

		if failedJob[alloc.JobId] != nil {
			allocs = append(allocs, alloc)
			continue
		}

		if maxIndex < alloc.ModifyIndex {
			maxIndex = alloc.ModifyIndex
		}
		if alloc.ModifyIndex <= minIndex {
			continue
		}
		if alloc.CreateTime == "" || minuteBefore.Before(alloc.CreateTime.TimeValue()) {
			continue
		}
		if alloc.ClientStatus == state {
			allocs = append(allocs, alloc)
		}
	}
	return allocs, maxIndex, nil
}

func allocsByNodeTxn(txn ReadTxn, ws memdb.WatchSet, node string) ([]*structs.Allocation, error) {
	// Get an iterator over the node allocations, using only the
	// node prefix which ignores the terminal status
	iter, err := txn.Get("allocs", "node_prefix", node)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*structs.Allocation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		out = append(out, raw.(*structs.Allocation))
	}
	return out, nil
}

func (s *StateSnapshot) DenormalizeAllocationSlice(allocs []*structs.Allocation) ([]*structs.Allocation, error) {
	allocDiffs := make([]*structs.AllocationDiff, len(allocs))
	for i, alloc := range allocs {
		allocDiffs[i] = alloc.AllocationDiff()
	}

	return s.DenormalizeAllocationDiffSlice(allocDiffs)
}

// DenormalizeAllocationDiffSlice queries the Allocation for each AllocationDiff and merges
// the updated attributes with the existing Allocation, and attaches the Job provided.
//
// This should only be called on terminal alloc, particularly stopped or preempted allocs
func (s *StateSnapshot) DenormalizeAllocationDiffSlice(allocDiffs []*structs.AllocationDiff) ([]*structs.Allocation, error) {
	// Output index for denormalized Allocations
	j := 0

	denormalizedAllocs := make([]*structs.Allocation, len(allocDiffs))
	for _, allocDiff := range allocDiffs {
		alloc, err := s.AllocByID(nil, allocDiff.ID)
		if err != nil {
			return nil, fmt.Errorf("alloc lookup failed: %v", err)
		}
		if alloc == nil {
			return nil, fmt.Errorf("alloc %v doesn't exist", allocDiff.ID)
		}

		// Merge the updates to the Allocation.  Don't update alloc.Job for terminal allocs
		// so alloc refers to the latest Job view before destruction and to ease handler implementations
		allocCopy := alloc.Copy()

		if allocDiff.PreemptedByAllocation != "" {
			allocCopy.PreemptedByAllocation = allocDiff.PreemptedByAllocation
			allocCopy.DesiredDescription = getPreemptedAllocDesiredDescription(allocDiff.PreemptedByAllocation)
			allocCopy.DesiredStatus = constant.AllocDesiredStatusEvict
		} else {
			// If alloc is a stopped alloc
			allocCopy.DesiredDescription = allocDiff.DesiredDescription
			allocCopy.DesiredStatus = constant.AllocDesiredStatusStop
			if allocDiff.ClientStatus != "" {
				allocCopy.ClientStatus = allocDiff.ClientStatus
			}
			if allocDiff.FollowupEvalID != "" {
				allocCopy.FollowupEvalID = allocDiff.FollowupEvalID
			}
		}
		if allocDiff.ModifyTime != "" {
			allocCopy.ModifyTime = allocDiff.ModifyTime
		}

		// Update the allocDiff in the slice to equal the denormalized alloc
		denormalizedAllocs[j] = allocCopy
		j++
	}
	// Retain only the denormalized Allocations in the slice
	denormalizedAllocs = denormalizedAllocs[:j]
	return denormalizedAllocs, nil
}

func (s *StateStore) AllocsByNodeTerminal(ws memdb.WatchSet, node string, terminal bool) ([]*structs.Allocation, error) {
	txn := s.db.ReadTxn()

	// Get an iterator over the node allocations
	iter, err := txn.Get("allocs", "node", node, terminal)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*structs.Allocation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		out = append(out, raw.(*structs.Allocation))
	}
	return out, nil
}

// AllocsByPlan returns allocations by job id
func (s *StateStore) AllocsByPlan(ws memdb.WatchSet, namespace, planId string, anyCreateIndex bool) ([]*structs.Allocation, error) {
	txn := s.db.ReadTxn()

	// Get the plan
	var plan *structs.Plan
	rawJob, err := txn.First("plans", "id", namespace, planId)
	if err != nil {
		return nil, err
	}
	if rawJob != nil {
		plan = rawJob.(*structs.Plan)
	}

	// Get an iterator over the node allocations
	iter, err := txn.Get("allocs", "plan", namespace, planId)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*structs.Allocation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}

		alloc := raw.(*structs.Allocation)
		// If the allocation belongs to a plan with the same ID but a different
		// create index and we are not getting all the allocations whose Jobs
		// matches the same Job ID then we skip it
		if !anyCreateIndex && plan != nil && alloc.Plan.CreateIndex != plan.CreateIndex {
			continue
		}
		out = append(out, raw.(*structs.Allocation))
	}
	return out, nil
}

// AllocsByJob returns allocations by job id
func (s *StateStore) AllocsByJob(ws memdb.WatchSet, namespace, jobId string) ([]*structs.Allocation, error) {
	txn := s.db.ReadTxn()
	// Get the job
	rawJob, err := txn.First(Jobs, "id", namespace, jobId)
	if err != nil {
		return nil, err
	}
	if rawJob == nil {
		return nil, structs.NewErr1101None("job", jobId)
	}

	// Get an iterator over the node allocations
	iter, err := txn.Get("allocs", "job", jobId)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*structs.Allocation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		out = append(out, raw.(*structs.Allocation))
	}
	return out, nil
}

// AllocsByEval returns all the allocations by eval id
func (s *StateStore) AllocsByEval(ws memdb.WatchSet, evalID string) ([]*structs.Allocation, error) {
	txn := s.db.ReadTxn()

	// Get an iterator over the eval allocations
	iter, err := txn.Get("allocs", "eval", evalID)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*structs.Allocation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		out = append(out, raw.(*structs.Allocation))
	}
	return out, nil
}

func (s *StateStore) UpdateAllocsFromClient(msgType constant.MessageType, index uint64, allocs []*structs.Allocation) error {
	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()

	// Handle each of the updated allocations
	for _, alloc := range allocs {
		if err := s.nestedUpdateAllocFromClient(txn, index, alloc); err != nil {
			return err
		}
	}

	// Update the indexes
	if err := txn.Insert("index", &IndexEntry{"allocs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return txn.Commit()
}

// nestedUpdateAllocFromClient is used to nest an update of an allocation with client status
func (s *StateStore) nestedUpdateAllocFromClient(txn *txn, index uint64, alloc *structs.Allocation) error {
	// Look for existing alloc
	existing, err := txn.First("allocs", "id", alloc.ID)
	if err != nil {
		return fmt.Errorf("alloc lookup failed: %v", err)
	}

	// Nothing to do if this does not exist
	if existing == nil {
		return nil
	}
	exist := existing.(*structs.Allocation)

	// Copy everything from the existing allocation
	copyAlloc := exist.Copy()

	if copyAlloc.ClientStatus != "" && alloc.ClientStatus == "" {
		return fmt.Errorf("update alloc error: the programe trying update status to empty")
	}

	// Pull in anything the client is the authority on
	copyAlloc.ClientStatus = alloc.ClientStatus
	copyAlloc.ClientDescription = alloc.ClientDescription

	// Update the modify index
	copyAlloc.ModifyIndex = index

	// Update the modify time
	copyAlloc.ModifyTime = alloc.ModifyTime

	// Update the allocation
	if err := txn.Insert("allocs", copyAlloc); err != nil {
		return fmt.Errorf("alloc insert failed: %v", err)
	}

	// update the job's status
	jobStatus := copyAlloc.ClientStatus
	if err := s.UpdateJobStatus(index, exist.Namespace, exist.JobId, jobStatus, copyAlloc.ClientDescription, txn); err != nil {
		return fmt.Errorf("update job status failed: %v", err)
	}
	// 一个job可能有多个alloc，其他的alloc如果pending或running也需要更新掉

	return nil
}

// UpdateAllocsDesiredTransitions is used to update a set of allocations
// desired transitions.
func (s *StateStore) UpdateAllocsDesiredTransitions(msgType constant.MessageType, index uint64, allocs map[string]*structs.DesiredTransition,
	evals []*structs.Evaluation) error {

	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()

	// Handle each of the updated allocations
	for id, transition := range allocs {
		if err := s.UpdateAllocDesiredTransitionTxn(txn, index, id, transition); err != nil {
			return err
		}
	}

	for _, eval := range evals {
		if err := s.nestedUpsertEval(txn, index, eval); err != nil {
			return err
		}
	}

	// Update the indexes
	if err := txn.Insert("index", &IndexEntry{"allocs", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return txn.Commit()
}

// UpdateAllocDesiredTransitionTxn is used to nest an update of an
// allocations desired transition
func (s *StateStore) UpdateAllocDesiredTransitionTxn(
	txn *txn, index uint64, allocID string,
	transition *structs.DesiredTransition) error {

	// Look for existing alloc
	existing, err := txn.First("allocs", "id", allocID)
	if err != nil {
		return fmt.Errorf("alloc lookup failed: %v", err)
	}

	// Nothing to do if this does not exist
	if existing == nil {
		return nil
	}
	exist := existing.(*structs.Allocation)

	// Copy everything from the existing allocation
	copyAlloc := exist.Copy()

	// Merge the desired transitions
	copyAlloc.DesiredTransition.Merge(transition)

	// Update the modify indexes
	copyAlloc.ModifyIndex = index
	copyAlloc.AllocModifyIndex = index

	// Update the allocation
	if err := txn.Insert("allocs", copyAlloc); err != nil {
		return fmt.Errorf("alloc insert failed: %v", err)
	}

	return nil
}
