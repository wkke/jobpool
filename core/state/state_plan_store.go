package state

import (
	"fmt"
	"github.com/hashicorp/go-memdb"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
)

// UpsertPlan is used to register a plan or update a plan definition
func (s *StateStore) UpsertPlan(msgType constant.MessageType, index uint64, plan *structs.Plan) error {
	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()
	if err := s.upsertPlanImpl(index, plan, false, txn); err != nil {
		return err
	}
	return txn.Commit()
}

// UpsertPlanTxn is used to register a plan or update a plan definition, like UpsertPlan,
// but in a transaction.  Useful for when making multiple modifications atomically
func (s *StateStore) UpsertPlanTxn(index uint64, plan *structs.Plan, txn Txn) error {
	return s.upsertPlanImpl(index, plan, false, txn)
}

// upsertPlanImpl is the implementation for registering a plan or updating a plan definition
func (s *StateStore) upsertPlanImpl(index uint64, plan *structs.Plan, keepVersion bool, txn *txn) error {
	// Assert the namespace exists
	if exists, err := s.namespaceExists(txn, plan.Namespace); err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("plan %q is in nonexistent namespace %q", plan.ID, plan.Namespace)
	}

	// Check if the plan already exists
	existing, err := txn.First(Plans, "id", plan.Namespace, plan.ID)
	if err != nil {
		return fmt.Errorf("plan lookup failed: %v", err)
	}

	var saveOrUpdatePlan *structs.Plan
	// Setup the indexes correctly
	if existing != nil {
		plan.CreateIndex = existing.(*structs.Plan).CreateIndex
		plan.ModifyIndex = index
		plan.UpdateTime = xtime.NewFormatTime(time.Now())

		existingPlan := existing.(*structs.Plan)

		// Bump the version unless asked to keep it. This should only be done
		// when changing an internal field such as Stable. A spec change should
		// always come with a version bump
		if !keepVersion {
			plan.PlanModifyIndex = index
			if plan.Version <= existingPlan.Version {
				plan.Version = existingPlan.Version + 1
			}
		}

		// Compute the plan status
		plan.Status, err = s.getPlanStatus(txn, plan, false)
		if err != nil {
			return fmt.Errorf("setting plan status for %q failed: %v", plan.ID, err)
		}

		// merge exist plan and new plan
		saveOrUpdatePlan = existingPlan.Merge(plan)

	} else {
		plan.CreateIndex = index
		plan.ModifyIndex = index
		plan.PlanModifyIndex = index

		if err := s.setPlanStatus(index, txn, plan, false, ""); err != nil {
			return fmt.Errorf("setting plan status for %q failed: %v", plan.ID, err)
		}

		// Have to get the plan again since it could have been updated
		updated, err := txn.First(Plans, "id", plan.Namespace, plan.ID)
		if err != nil {
			return fmt.Errorf("plan lookup failed: %v", err)
		}
		if updated != nil {
			updatedPlan := updated.(*structs.Plan)
			saveOrUpdatePlan = updatedPlan.Merge(plan)
		}
	}
	//
	//if err := s.upsertPlanVersion(index, plan, txn); err != nil {
	//	return fmt.Errorf("unable to upsert plan into plan_version table: %v", err)
	//}

	// Insert the plan
	if err := txn.Insert(Plans, saveOrUpdatePlan); err != nil {
		return fmt.Errorf("plan insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{Plans, index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return nil
}

func (s *StateStore) getPlanStatus(txn *txn, plan *structs.Plan, evalDelete bool) (string, error) {
	// System, Periodic and Parameterized plans are running until explicitly
	// stopped.
	if plan.IsParameterized() ||
		plan.IsPeriodic() {
		if plan.Stop {
			return constant.PlanStatusDead, nil
		}
		return constant.PlanStatusRunning, nil
	}

	// If there is a non-terminal allocation, the plan is running.
	hasAlloc := false
	//for alloc := allocs.Next(); alloc != nil; alloc = allocs.Next() {
	//	hasAlloc = true
	//	if !alloc.(*structs.Allocation).TerminalStatus() {
	//		return structs.PlanStatusRunning, nil
	//	}
	//}

	evals, err := txn.Get("evals", "plan_prefix", plan.Namespace, plan.ID)
	if err != nil {
		return "", err
	}

	hasEval := false
	for raw := evals.Next(); raw != nil; raw = evals.Next() {
		e := raw.(*structs.Evaluation)

		// Filter non-exact matches
		if e.PlanID != plan.ID {
			continue
		}

		hasEval = true
		if !e.TerminalStatus() {
			return constant.PlanStatusPending, nil
		}
	}

	// The plan is dead if all the allocations and evals are terminal or if there
	// are no evals because of garbage collection.
	if evalDelete || hasEval || hasAlloc {
		return constant.PlanStatusDead, nil
	}

	return constant.PlanStatusPending, nil
}

// setPlanStatus sets the status of the plan by looking up associated evaluations
// and allocations. evalDelete should be set to true if setPlanStatus is being
// called because an evaluation is being deleted (potentially because of garbage
// collection). If forceStatus is non-empty, the plan's status will be set to the
// passed status.
func (s *StateStore) setPlanStatus(index uint64, txn *txn,
	plan *structs.Plan, evalDelete bool, forceStatus string) error {

	// Capture the current status so we can check if there is a change
	oldStatus := plan.Status
	newStatus := forceStatus

	// If forceStatus is not set, compute the plans status.
	if forceStatus == "" {
		var err error
		newStatus, err = s.getPlanStatus(txn, plan, evalDelete)
		if err != nil {
			return err
		}
	}

	// Fast-path if the plan has not changed.
	if oldStatus == newStatus {
		return nil
	}

	// Copy and update the existing plan
	updated := plan.Copy()
	updated.Status = newStatus
	updated.ModifyIndex = index

	// Insert the plan
	if err := txn.Insert(Plans, updated); err != nil {
		return fmt.Errorf("plan insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{Plans, index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}
	return nil
}

// PlansByPeriodic returns an iterator over all the periodic or non-periodic plans.
func (s *StateStore) PlansByPeriodic(ws memdb.WatchSet, periodic bool) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	iter, err := txn.Get(Plans, "periodic", periodic)
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

// PlanByID is used to lookup a plan by its ID. PlanByID returns the current/latest plan
// version.
func (s *StateStore) PlanByID(ws memdb.WatchSet, namespace, id string) (*structs.Plan, error) {
	txn := s.db.ReadTxn()
	return s.PlanByIDTxn(ws, namespace, id, txn)
}

// PlanByIDTxn is used to lookup a plan by its ID, like  PlanByID. PlanByID returns the plan version
// accessible through in the transaction
func (s *StateStore) PlanByIDTxn(ws memdb.WatchSet, namespace, id string, txn Txn) (*structs.Plan, error) {
	watchCh, existing, err := txn.FirstWatch(Plans, "id", namespace, id)
	if err != nil {
		return nil, fmt.Errorf("plan lookup failed: %v", err)
	}
	ws.Add(watchCh)

	if existing != nil {
		return existing.(*structs.Plan), nil
	}
	return nil, nil
}

// Plans returns an iterator over all the plans
func (s *StateStore) Plans(ws memdb.WatchSet) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	// Walk the entire plans table
	iter, err := txn.Get(Plans, "id")
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

// PlansByIDPrefix is used to lookup a plan by prefix. If querying all namespaces
// the prefix will not be filtered by an index.
func (s *StateStore) PlansByIDPrefix(ws memdb.WatchSet, namespace, id string) (memdb.ResultIterator, error) {

	txn := s.db.ReadTxn()

	iter, err := txn.Get(Plans, "id_prefix", namespace, id)
	if err != nil {
		return nil, fmt.Errorf("plan lookup failed: %v", err)
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

// DeletePlanTxn is used to deregister a plan, like DeletePlan,
// but in a transaction.  Useful for when making multiple modifications atomically
func (s *StateStore) DeletePlanTxn(index uint64, namespace, planID string, txn Txn) error {
	// Lookup the node
	existing, err := txn.First(Plans, "id", namespace, planID)
	if err != nil {
		return fmt.Errorf("plan lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("plan not found")
	}

	// Delete the plan
	if err := txn.Delete(Plans, existing); err != nil {
		return fmt.Errorf("plan delete failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{Plans, index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return nil
}
