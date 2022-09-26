package state

import (
	"fmt"
	"github.com/hashicorp/go-memdb"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
)


// NamespaceByName is used to lookup a namespace by name
func (s *StateStore) NamespaceByName(ws memdb.WatchSet, name string) (*structs.Namespace, error) {
	txn := s.db.ReadTxn()
	return s.namespaceByNameImpl(ws, txn, name)
}

// namespaceByNameImpl is used to lookup a namespace by name
func (s *StateStore) namespaceByNameImpl(ws memdb.WatchSet, txn *txn, name string) (*structs.Namespace, error) {
	watchCh, existing, err := txn.FirstWatch(TableNamespaces, "id", name)
	if err != nil {
		return nil, fmt.Errorf("namespace lookup failed: %v", err)
	}
	ws.Add(watchCh)

	if existing != nil {
		return existing.(*structs.Namespace), nil
	}
	return nil, nil
}

// namespaceExists returns whether a namespace exists
func (s *StateStore) namespaceExists(txn *txn, namespace string) (bool, error) {
	if namespace == constant.DefaultNamespace {
		return true, nil
	}

	existing, err := txn.First(TableNamespaces, "id", namespace)
	if err != nil {
		return false, fmt.Errorf("namespace lookup failed: %v", err)
	}

	return existing != nil, nil
}

// NamespacesByNamePrefix is used to lookup namespaces by prefix
func (s *StateStore) NamespacesByNamePrefix(ws memdb.WatchSet, namePrefix string) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	iter, err := txn.Get(TableNamespaces, "id_prefix", namePrefix)
	if err != nil {
		return nil, fmt.Errorf("namespaces lookup failed: %v", err)
	}
	ws.Add(iter.WatchCh())

	return iter, nil
}

// Namespaces returns an iterator over all the namespaces
func (s *StateStore) Namespaces(ws memdb.WatchSet) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	// Walk the entire namespace table
	iter, err := txn.Get(TableNamespaces, "id")
	if err != nil {
		return nil, err
	}
	ws.Add(iter.WatchCh())
	return iter, nil
}

func (s *StateStore) NamespaceNames() ([]string, error) {
	it, err := s.Namespaces(nil)
	if err != nil {
		return nil, err
	}

	nses := []string{}
	for {
		next := it.Next()
		if next == nil {
			break
		}
		ns := next.(*structs.Namespace)
		nses = append(nses, ns.Name)
	}

	return nses, nil
}

// UpsertNamespaces is used to register or update a set of namespaces.
func (s *StateStore) UpsertNamespaces(index uint64, namespaces []*structs.Namespace) error {
	txn := s.db.WriteTxn(index)
	defer txn.Abort()

	for _, ns := range namespaces {
		if err := s.upsertNamespaceImpl(index, txn, ns); err != nil {
			return err
		}
	}

	if err := txn.Insert("index", &IndexEntry{TableNamespaces, index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return txn.Commit()
}

// upsertNamespaceImpl is used to upsert a namespace
func (s *StateStore) upsertNamespaceImpl(index uint64, txn *txn, namespace *structs.Namespace) error {
	// Ensure the namespace hash is non-nil. This should be done outside the state store
	// for performance reasons, but we check here for defense in depth.
	ns := namespace
	if len(ns.Hash) == 0 {
		ns.SetHash()
	}

	// Check if the namespace already exists
	existing, err := txn.First(TableNamespaces, "id", ns.Name)
	if err != nil {
		return fmt.Errorf("namespace lookup failed: %v", err)
	}

	// Setup the indexes correctly and determine which quotas need to be
	// reconciled
	if existing != nil {
		exist := existing.(*structs.Namespace)
		ns.CreateIndex = exist.CreateIndex
		ns.ModifyIndex = index
	} else {
		ns.CreateIndex = index
		ns.ModifyIndex = index
	}

	// Insert the namespace
	if err := txn.Insert(TableNamespaces, ns); err != nil {
		return fmt.Errorf("namespace insert failed: %v", err)
	}

	// Reconcile changed quotas
	return nil
}

// DeleteNamespaces is used to remove a set of namespaces
func (s *StateStore) DeleteNamespaces(index uint64, names []string) error {
	txn := s.db.WriteTxn(index)
	defer txn.Abort()

	for _, name := range names {
		// Lookup the namespace
		existing, err := txn.First(TableNamespaces, "id", name)
		if err != nil {
			return fmt.Errorf("namespace lookup failed: %v", err)
		}
		if existing == nil {
			return fmt.Errorf("namespace not found")
		}

		ns := existing.(*structs.Namespace)
		if ns.Name == constant.DefaultNamespace {
			return fmt.Errorf("default namespace can not be deleted")
		}

		// Delete the namespace
		if err := txn.Delete(TableNamespaces, existing); err != nil {
			return fmt.Errorf("namespace deletion failed: %v", err)
		}
	}

	if err := txn.Insert("index", &IndexEntry{TableNamespaces, index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return txn.Commit()
}