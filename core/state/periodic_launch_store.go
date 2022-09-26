package state

import (
	"fmt"
	"github.com/hashicorp/go-memdb"
	"yunli.com/jobpool/core/structs"
)

// PeriodicLaunchByID is used to lookup a periodic launch by the periodic plan
// ID.
func (s *StateStore) PeriodicLaunchByID(ws memdb.WatchSet, namespace, id string) (*structs.PeriodicLaunch, error) {
	txn := s.db.ReadTxn()

	watchCh, existing, err := txn.FirstWatch("periodic_launch", "id", namespace, id)
	if err != nil {
		return nil, fmt.Errorf("periodic launch lookup failed: %v", err)
	}

	ws.Add(watchCh)

	if existing != nil {
		return existing.(*structs.PeriodicLaunch), nil
	}
	return nil, nil
}

// UpsertPeriodicLaunch is used to register a launch or update it.
func (s *StateStore) UpsertPeriodicLaunch(index uint64, launch *structs.PeriodicLaunch) error {
	txn := s.db.WriteTxn(index)
	defer txn.Abort()

	// Check if the plan already exists
	existing, err := txn.First("periodic_launch", "id", launch.Namespace, launch.ID)
	if err != nil {
		return fmt.Errorf("periodic launch lookup failed: %v", err)
	}

	// Setup the indexes correctly
	if existing != nil {
		launch.CreateIndex = existing.(*structs.PeriodicLaunch).CreateIndex
		launch.ModifyIndex = index
	} else {
		launch.CreateIndex = index
		launch.ModifyIndex = index
	}

	// Insert the plan
	if err := txn.Insert("periodic_launch", launch); err != nil {
		return fmt.Errorf("launch insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"periodic_launch", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return txn.Commit()
}


// PeriodicLaunches returns an iterator over all the periodic launches
func (s *StateStore) PeriodicLaunches(ws memdb.WatchSet) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	// Walk the entire table
	iter, err := txn.Get("periodic_launch", "id")
	if err != nil {
		return nil, err
	}

	ws.Add(iter.WatchCh())

	return iter, nil
}


// DeletePeriodicLaunchTxn is used to delete the periodic launch, like DeletePeriodicLaunch
// but in a transaction.  Useful for when making multiple modifications atomically
func (s *StateStore) DeletePeriodicLaunchTxn(index uint64, namespace, jobID string, txn Txn) error {
	// Lookup the launch
	existing, err := txn.First("periodic_launch", "id", namespace, jobID)
	if err != nil {
		return fmt.Errorf("launch lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("launch not found")
	}

	// Delete the launch
	if err := txn.Delete("periodic_launch", existing); err != nil {
		return fmt.Errorf("launch delete failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"periodic_launch", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return nil
}