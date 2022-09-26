package state

import (
	"fmt"
	"github.com/hashicorp/go-memdb"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
)

// Nodes returns an iterator over all the nodes
func (s *StateStore) Kvs(ws memdb.WatchSet, sort SortOption) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()
	var iter memdb.ResultIterator
	var err error
	// Walk the entire nodes table
	switch sort {
	case SortReverse:
		iter, err = txn.GetReverse(Kvs, "id")
	default:
		iter, err = txn.Get(Kvs, "id")
	}
	if err != nil {
		return nil, err
	}
	ws.Add(iter.WatchCh())
	return iter, nil
}

func (s *StateStore) UpsertKv(msgType constant.MessageType, index uint64, node *structs.Kv) error {
	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()
	err := upsertKvTxn(txn, index, node)
	if err != nil {
		return nil
	}
	return txn.Commit()
}

func upsertKvTxn(txn *txn, index uint64, model *structs.Kv) error {
	// Check if the model already exists
	existing, err := txn.First(Kvs, "id", model.Key)
	if err != nil {
		return fmt.Errorf("kv lookup failed: %v", err)
	}

	// Setup the indexes correctly
	if existing != nil {
		exist := existing.(*structs.Kv)
		model.UpdateTime = xtime.NewFormatTime(time.Now())
		model.CreateIndex = exist.CreateIndex
		model.ModifyIndex = index
	} else {
		model.CreateIndex = index
		model.ModifyIndex = index
	}

	// Insert the model
	if err := txn.Insert(Kvs, model); err != nil {
		return fmt.Errorf("kv insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{Kvs, index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}
	return nil
}

func (s *StateStore) KvByKey(ws memdb.WatchSet, key string) (*structs.Kv, error) {
	txn := s.db.ReadTxn()
	return s.kvByNameImpl(ws, txn, key)
}

func (s *StateStore) kvByNameImpl(ws memdb.WatchSet, txn *txn, key string) (*structs.Kv, error) {
	watchCh, existing, err := txn.FirstWatch(Kvs, "id", key)
	if err != nil {
		return nil, fmt.Errorf("namespace lookup failed: %v", err)
	}
	ws.Add(watchCh)

	if existing != nil {
		return existing.(*structs.Kv), nil
	}
	return nil, nil
}

func (s *StateStore) DeleteKv(index uint64, key string) error {
	txn := s.db.WriteTxn(index)
	defer txn.Abort()

	// Lookup the namespace
	existing, err := txn.First(Kvs, "id", key)
	if err != nil {
		return fmt.Errorf("kv lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("namespace not found")
	}

	// Delete the kv
	if err := txn.Delete(Kvs, existing); err != nil {
		return fmt.Errorf("kv deletion failed: %v", err)
	}

	if err := txn.Insert("index", &IndexEntry{Kvs, index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return txn.Commit()
}
