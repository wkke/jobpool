package state

import (
	"fmt"
	"github.com/hashicorp/go-memdb"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
)


// Evals returns an iterator over all the evaluations in ascending or descending
// order of CreationIndex as determined by the reverse parameter.
func (s *StateStore) Evals(ws memdb.WatchSet, sort SortOption) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	var it memdb.ResultIterator
	var err error

	switch sort {
	case SortReverse:
		it, err = txn.GetReverse("evals", "create")
	default:
		it, err = txn.Get("evals", "create")
	}

	if err != nil {
		return nil, err
	}

	ws.Add(it.WatchCh())

	return it, nil
}


// EvalsByIDPrefix is used to lookup evaluations by prefix in a particular
// namespace
func (s *StateStore) EvalsByIDPrefix(ws memdb.WatchSet, namespace, id string, sort SortOption) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	var iter memdb.ResultIterator
	var err error

	// Get an iterator over all evals by the id prefix
	switch sort {
	case SortReverse:
		iter, err = txn.GetReverse("evals", "id_prefix", id)
	default:
		iter, err = txn.Get("evals", "id_prefix", id)
	}
	if err != nil {
		return nil, fmt.Errorf("eval lookup failed: %v", err)
	}

	ws.Add(iter.WatchCh())

	// Wrap the iterator in a filter
	wrap := memdb.NewFilterIterator(iter, evalNamespaceFilter(namespace))
	return wrap, nil
}

// UpsertEvals is used to upsert a set of evaluations
func (s *StateStore) UpsertEvals(msgType constant.MessageType, index uint64, evals []*structs.Evaluation) error {
	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()

	err := s.UpsertEvalsTxn(index, evals, txn)
	if err == nil {
		return txn.Commit()
	}
	return err
}

// UpsertEvalsTxn is used to upsert a set of evaluations, like UpsertEvals but
// in a transaction.  Useful for when making multiple modifications atomically.
func (s *StateStore) UpsertEvalsTxn(index uint64, evals []*structs.Evaluation, txn Txn) error {
	// Do a nested upsert
	plans := make(map[structs.NamespacedID]string, len(evals))
	for _, eval := range evals {
		if err := s.nestedUpsertEval(txn, index, eval); err != nil {
			return err
		}

		tuple := structs.NamespacedID{
			ID:        eval.PlanID,
			Namespace: eval.Namespace,
		}
		plans[tuple] = ""
	}

	// Set the plan's status
	if err := s.setPlanStatuses(index, txn, plans, false); err != nil {
		return fmt.Errorf("setting plan status failed: %v", err)
	}

	return nil
}

// nestedUpsertEvaluation is used to nest an evaluation upsert within a transaction
func (s *StateStore) nestedUpsertEval(txn *txn, index uint64, eval *structs.Evaluation) error {
	// Lookup the evaluation
	existing, err := txn.First("evals", "id", eval.ID)
	if err != nil {
		return fmt.Errorf("eval lookup failed: %v", err)
	}

	// Update the indexes
	if existing != nil {
		eval.CreateIndex = existing.(*structs.Evaluation).CreateIndex
		eval.ModifyIndex = index
		eval.UpdateTime = xtime.NewFormatTime(time.Now())
	} else {
		eval.CreateIndex = index
		eval.ModifyIndex = index
	}

	// Check if the plan has any blocked evaluations and cancel them
	if eval.Status == constant.EvalStatusComplete {
		// Get the blocked evaluation for a plan if it exists
		iter, err := txn.Get("evals", "plan", eval.Namespace, eval.PlanID, constant.EvalStatusBlocked)
		if err != nil {
			return fmt.Errorf("failed to get blocked evals for plan %q in namespace %q: %v", eval.PlanID, eval.Namespace, err)
		}

		var blocked []*structs.Evaluation
		for {
			raw := iter.Next()
			if raw == nil {
				break
			}
			blocked = append(blocked, raw.(*structs.Evaluation))
		}

		// Go through and update the evals
		for _, eval := range blocked {
			newEval := eval.Copy()
			newEval.Status = constant.EvalStatusCancelled
			newEval.StatusDescription = fmt.Sprintf("evaluation %q successful", newEval.ID)
			newEval.ModifyIndex = index

			if err := txn.Insert("evals", newEval); err != nil {
				return fmt.Errorf("eval insert failed: %v", err)
			}
		}
	}

	// Insert the eval
	if err := txn.Insert("evals", eval); err != nil {
		return fmt.Errorf("eval insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"evals", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}
	return nil
}

// updateEvalModifyIndex is used to update the modify index of an evaluation that has been
// through a scheduler pass. This is done as part of plan apply. It ensures that when a subsequent
// scheduler workers process a re-queued evaluation it sees any partial updates from the plan apply.
func (s *StateStore) updateEvalModifyIndex(txn *txn, index uint64, evalID string) error {
	// Lookup the evaluation
	existing, err := txn.First("evals", "id", evalID)
	if err != nil {
		return fmt.Errorf("eval lookup failed: %v", err)
	}
	if existing == nil {
		s.logger.Error("unable to find eval", "eval_id", evalID)
		return fmt.Errorf("unable to find eval id %q", evalID)
	}
	eval := existing.(*structs.Evaluation).Copy()
	// Update the indexes
	eval.ModifyIndex = index

	// Insert the eval
	if err := txn.Insert("evals", eval); err != nil {
		return fmt.Errorf("eval insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"evals", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}
	return nil
}


// EvalsByJob returns all the evaluations by job id
func (s *StateStore) EvalsByPlan(ws memdb.WatchSet, namespace, planId string) ([]*structs.Evaluation, error) {
	txn := s.db.ReadTxn()

	// Get an iterator over the node allocations
	iter, err := txn.Get("evals", "plan_prefix", namespace, planId)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*structs.Evaluation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}

		e := raw.(*structs.Evaluation)

		// Filter non-exact matches
		if e.PlanID != planId {
			continue
		}

		out = append(out, e)
	}
	return out, nil
}

func (s *StateStore) EvalsFailedByPlanAndJob(ws memdb.WatchSet, namespace, planId string, jobId string) ([]*structs.Evaluation, error) {
	txn := s.db.ReadTxn()

	// Get an iterator over the node allocations
	iter, err := txn.Get("evals", "plan_prefix", namespace, planId)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	var out []*structs.Evaluation
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		e := raw.(*structs.Evaluation)
		// Filter non-exact matches
		if e.PlanID != planId {
			continue
		}
		if e.JobID != jobId {
			continue
		}
		if e.Status != constant.EvalStatusFailed {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}


// EvalByID is used to lookup an eval by its ID
func (s *StateStore) EvalByID(ws memdb.WatchSet, id string) (*structs.Evaluation, error) {
	txn := s.db.ReadTxn()

	watchCh, existing, err := txn.FirstWatch("evals", "id", id)
	if err != nil {
		return nil, fmt.Errorf("eval lookup failed: %v", err)
	}

	ws.Add(watchCh)

	if existing != nil {
		return existing.(*structs.Evaluation), nil
	}
	return nil, nil
}


// evalNamespaceFilter returns a filter function that filters all evaluations
// not in the given namespace.
func evalNamespaceFilter(namespace string) func(interface{}) bool {
	return func(raw interface{}) bool {
		eval, ok := raw.(*structs.Evaluation)
		if !ok {
			return true
		}

		return eval.Namespace != namespace
	}
}