package state

import (
	"fmt"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"yunli.com/jobpool/core/structs"
)

func (s *StateStore) ClusterMetadata(ws memdb.WatchSet) (*structs.ClusterMetadata, error) {
	txn := s.db.ReadTxn()
	defer txn.Abort()

	// Get the cluster metadata
	watchCh, m, err := txn.FirstWatch("cluster_meta", "id")
	if err != nil {
		return nil, errors.Wrap(err, "failed cluster metadata lookup")
	}
	ws.Add(watchCh)

	if m != nil {
		return m.(*structs.ClusterMetadata), nil
	}

	return nil, nil
}

func (s *StateStore) ClusterSetMetadata(index uint64, meta *structs.ClusterMetadata) error {
	txn := s.db.WriteTxn(index)
	defer txn.Abort()

	if err := s.setClusterMetadata(txn, meta); err != nil {
		return errors.Wrap(err, "set cluster metadata failed")
	}

	return txn.Commit()
}

func (s *StateStore) setClusterMetadata(txn *txn, meta *structs.ClusterMetadata) error {
	// Check for an existing cfg, if it exists, verify that the cluster ID matches
	existing, err := txn.First("cluster_meta", "id")
	if err != nil {
		return fmt.Errorf("failed cluster meta lookup: %v", err)
	}

	if existing != nil {
		existingClusterID := existing.(*structs.ClusterMetadata).ClusterID
		if meta.ClusterID != existingClusterID && existingClusterID != "" {
			// there is a bug in cluster ID detection
			return fmt.Errorf("refusing to set new cluster id, previous: %s, new: %s", existingClusterID, meta.ClusterID)
		}
	}

	// update is technically a noop, unless someday we add more / mutable fields
	if err := txn.Insert("cluster_meta", meta); err != nil {
		return fmt.Errorf("set cluster metadata failed: %v", err)
	}

	return nil
}