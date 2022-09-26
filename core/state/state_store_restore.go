package state

import (
	"fmt"

	"yunli.com/jobpool/core/structs"
)

// StateRestore is used to optimize the performance when restoring state by
// only using a single large transaction instead of thousands of sub
// transactions.
type StateRestore struct {
	txn *txn
}

// Abort is used to abort the restore operation
func (r *StateRestore) Abort() {
	r.txn.Abort()
}

// Commit is used to commit the restore operation
func (r *StateRestore) Commit() error {
	return r.txn.Commit()
}

// NodeRestore is used to restore a node
func (r *StateRestore) NodeRestore(node *structs.Node) error {
	if err := r.txn.Insert("nodes", node); err != nil {
		return fmt.Errorf("node insert failed: %v", err)
	}
	return nil
}

// IndexRestore is used to restore an index
func (r *StateRestore) IndexRestore(idx *IndexEntry) error {
	if err := r.txn.Insert("index", idx); err != nil {
		return fmt.Errorf("index insert failed: %v", err)
	}
	return nil
}


func (r *StateRestore) ClusterMetadataRestore(meta *structs.ClusterMetadata) error {
	if err := r.txn.Insert("cluster_meta", meta); err != nil {
		return fmt.Errorf("inserting cluster meta failed: %v", err)
	}
	return nil
}

// NamespaceRestore is used to restore a namespace
func (r *StateRestore) NamespaceRestore(ns *structs.Namespace) error {
	if err := r.txn.Insert(TableNamespaces, ns); err != nil {
		return fmt.Errorf("namespace insert failed: %v", err)
	}
	return nil
}
func (r *StateRestore) KvRestore(kv *structs.Kv) error {
	if err := r.txn.Insert(Kvs, kv); err != nil {
		return fmt.Errorf("kv insert failed: %v", err)
	}
	return nil
}

// PeriodicLaunchRestore is used to restore a periodic launch.
func (r *StateRestore) PeriodicLaunchRestore(launch *structs.PeriodicLaunch) error {
	if err := r.txn.Insert("periodic_launch", launch); err != nil {
		return fmt.Errorf("periodic launch insert failed: %v", err)
	}
	return nil
}

// JobRestore is used to restore a plan
func (r *StateRestore) PlanRestore(plan *structs.Plan) error {
	if err := r.txn.Insert("plans", plan); err != nil {
		return fmt.Errorf("plan insert failed: %v", err)
	}
	return nil
}

// EvalRestore is used to restore an evaluation
func (r *StateRestore) EvalRestore(eval *structs.Evaluation) error {
	if err := r.txn.Insert("evals", eval); err != nil {
		return fmt.Errorf("eval insert failed: %v", err)
	}
	return nil
}

// AllocRestore is used to restore an allocation
func (r *StateRestore) AllocRestore(alloc *structs.Allocation) error {
	if err := r.txn.Insert("allocs", alloc); err != nil {
		return fmt.Errorf("alloc insert failed: %v", err)
	}
	return nil
}


func (r *StateRestore) JobRestore(job *structs.Job) error {
	if err := r.txn.Insert("jobs", job); err != nil {
		return fmt.Errorf("job insert failed: %v", err)
	}
	return nil
}

