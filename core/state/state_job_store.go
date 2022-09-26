package state

import (
	"fmt"
	"github.com/hashicorp/go-memdb"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
)

func (s *StateStore) UpsertJob(msgType constant.MessageType, index uint64, job *structs.Job) error {
	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()
	if err := s.upsertJobImpl(index, job, false, txn); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *StateStore) UpsertJobDefaultTxn(index uint64, job *structs.Job) error {
	txn := s.db.WriteTxn(index)
	defer txn.Abort()
	if err := s.upsertJobImpl(index, job, false, txn); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *StateStore) UpsertJobTxn(index uint64, job *structs.Job, txn Txn) error {
	return s.upsertJobImpl(index, job, false, txn)
}

// upsertJobImpl is the implementation for registering a job or updating a job definition
func (s *StateStore) upsertJobImpl(index uint64, job *structs.Job, keepVersion bool, txn *txn) error {
	// Assert the namespace exists
	if exists, err := s.namespaceExists(txn, job.Namespace); err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("job %q is in nonexistent namespace %q", job.ID, job.Namespace)
	}

	// Check if the job already exists
	existing, err := txn.First(Jobs, "id", job.Namespace, job.ID)
	if err != nil {
		return fmt.Errorf("job lookup failed: %v", err)
	}

	// Setup the indexes correctly
	if existing != nil {
		job.CreateIndex = existing.(*structs.Job).CreateIndex
		job.ModifyIndex = index
		job.UpdateTime = xtime.NewFormatTime(time.Now())
	} else {
		job.CreateIndex = index
		job.ModifyIndex = index
		// Have to get the job again since it could have been updated
		updated, err := txn.First(Jobs, "id", job.Namespace, job.ID)
		if err != nil {
			return fmt.Errorf("job lookup failed: %v", err)
		}
		if updated != nil {
			job = updated.(*structs.Job)
		}
	}
	// Insert the job
	if err := txn.Insert(Jobs, job); err != nil {
		return fmt.Errorf("job insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{Jobs, index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return nil
}

func (s *StateStore) setJobStatus(index uint64, txn *txn,
	job *structs.Job, forceStatus string, info string) error {

	// Capture the current status so we can check if there is a change
	oldStatus := job.Status
	newStatus := forceStatus

	// Fast-path if the job has not changed.
	if oldStatus == newStatus {
		return nil
	}

	// Copy and update the existing job
	updated := job.Copy()
	updated.Status = newStatus
	if constant.JobStatusComplete == newStatus {
		// reset the info for complete
		updated.Info = ""
	}else {
		updated.Info = info
	}

	updated.ModifyIndex = index

	// Insert the job
	if err := txn.Insert(Jobs, updated); err != nil {
		return fmt.Errorf("job insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{Jobs, index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}
	return nil
}

func (s *StateStore) JobByID(ws memdb.WatchSet, namespace, id string) (*structs.Job, error) {
	txn := s.db.ReadTxn()
	return s.JobByIDTxn(ws, namespace, id, txn)
}

func (s *StateStore) JobByIDTxn(ws memdb.WatchSet, namespace, id string, txn Txn) (*structs.Job, error) {
	watchCh, existing, err := txn.FirstWatch(Jobs, "id", namespace, id)
	if err != nil {
		return nil, fmt.Errorf("job lookup failed: %v", err)
	}
	ws.Add(watchCh)

	if existing != nil {
		return existing.(*structs.Job), nil
	}
	return nil, nil
}

// Jobs returns an iterator over all the Jobs
func (s *StateStore) Jobs(ws memdb.WatchSet, sort SortOption) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()
	var iter memdb.ResultIterator
	var err error
	// Walk the entire Jobs table
	switch sort {
	case SortReverse:
		iter, err = txn.GetReverse(Jobs, "create")
	default:
		iter, err = txn.Get(Jobs, "create")
	}
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

func (s *StateStore) JobsByIDPrefix(ws memdb.WatchSet, namespace, id string) (memdb.ResultIterator, error) {

	txn := s.db.ReadTxn()

	iter, err := txn.Get(Jobs, "id_prefix", namespace, id)
	if err != nil {
		return nil, fmt.Errorf("job lookup failed: %v", err)
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

func (s *StateStore) JobsBySync(ws memdb.WatchSet, sync uint8) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	iter, err := txn.Get(Jobs, "sync", sync)
	if err != nil {
		return nil, fmt.Errorf("job lookup failed: %v", err)
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}

func (s *StateStore) JobsByPlanID(ws memdb.WatchSet, namespace, planId string) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()
	iter, err := txn.Get(Jobs, "plan", namespace, planId)
	if err != nil {
		return nil, fmt.Errorf("job lookup failed: %v", err)
	}
	ws.Add(iter.WatchCh())
	return iter, nil
}

// DeleteJobTxn is used to deregister a job, like DeleteJob,
// but in a transaction.  Useful for when making multiple modifications atomically
func (s *StateStore) DeleteJobTxn(index uint64, namespace, jobId string, txn Txn) error {
	// Lookup the node
	existing, err := txn.First(Jobs, "id", namespace, jobId)
	if err != nil {
		return fmt.Errorf("job lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("job not found")
	}

	// Delete the job
	if err := txn.Delete(Jobs, existing); err != nil {
		return fmt.Errorf("job delete failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{Jobs, index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return nil
}

func (s *StateStore) UpdateJobStatus(index uint64, namespace string, jobId string, status string, info string, txn Txn) error {
	// Check if the job already exists
	existing, err := txn.First(Jobs, "id", namespace, jobId)
	if err != nil {
		return fmt.Errorf("job lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf(fmt.Sprintf("job not found with namespace:%s id :%s", namespace, jobId))
	}
	exist := existing.(*structs.Job)
	updated := exist.Copy()
	return s.setJobStatus(index, txn, updated, status, info)
}
