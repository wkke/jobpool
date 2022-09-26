package state

import (
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
)

// UpsertPlanResults is used to upsert the results of a plan.
func (s *StateStore) UpsertPlanResults(msgType constant.MessageType, index uint64, results *dto.ApplyPlanResultsRequest) error {
	// s.logger.Info("---------UpsertPlanResults--------", "results", results)

	snapshot, err := s.Snapshot()
	if err != nil {
		return err
	}

	allocsStopped, err := snapshot.DenormalizeAllocationDiffSlice(results.AllocsStopped)
	if err != nil {
		return err
	}

	allocsPreempted, err := snapshot.DenormalizeAllocationDiffSlice(results.AllocsPreempted)
	if err != nil {
		return err
	}

	// COMPAT 0.11: Remove this denormalization when NodePreemptions is removed
	results.NodePreemptions, err = snapshot.DenormalizeAllocationSlice(results.NodePreemptions)
	if err != nil {
		return err
	}

	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()

	if results.EvalID != "" {
		// Update the modify index of the eval id
		if err := s.updateEvalModifyIndex(txn, index, results.EvalID); err != nil {
			return err
		}
	}

	numAllocs := 0
	if len(results.Alloc) > 0 || len(results.NodePreemptions) > 0 {
		numAllocs = len(results.Alloc) + len(results.NodePreemptions)
	} else {
		numAllocs = len(allocsStopped) + len(results.AllocsUpdated) + len(allocsPreempted)
	}

	allocsToUpsert := make([]*structs.Allocation, 0, numAllocs)
	allocsToUpsert = append(allocsToUpsert, results.Alloc...)
	allocsToUpsert = append(allocsToUpsert, results.NodePreemptions...)
	allocsToUpsert = append(allocsToUpsert, allocsStopped...)
	allocsToUpsert = append(allocsToUpsert, results.AllocsUpdated...)
	allocsToUpsert = append(allocsToUpsert, allocsPreempted...)

	// handle upgrade path
	for _, alloc := range allocsToUpsert {
		alloc.Canonicalize()
	}
	// s.logger.Info("---------upsertAllocsImpl--------", "allocs", allocsToUpsert)
	if err := s.upsertAllocsImpl(index, allocsToUpsert, txn); err != nil {
		return err
	}

	// Upsert followup evals for allocs that were preempted
	for _, eval := range results.PreemptionEvals {
		if err := s.nestedUpsertEval(txn, index, eval); err != nil {
			return err
		}
	}
	return txn.Commit()
}
