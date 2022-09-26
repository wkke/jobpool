package state

import (
	"fmt"
	"github.com/hashicorp/go-memdb"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
)

// Nodes returns an iterator over all the nodes
func (s *StateStore) Nodes(ws memdb.WatchSet) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	// Walk the entire nodes table
	iter, err := txn.Get("nodes", "id")
	if err != nil {
		return nil, err
	}
	ws.Add(iter.WatchCh())
	return iter, nil
}

// UpsertNode is used to register a node or update a node definition
// This is assumed to be triggered by the client, so we retain the value
// of drain/eligibility which is set by the scheduler.
func (s *StateStore) UpsertNode(msgType constant.MessageType, index uint64, node *structs.Node) error {
	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()

	err := upsertNodeTxn(txn, index, node)
	if err != nil {
		return nil
	}
	return txn.Commit()
}

func upsertNodeTxn(txn *txn, index uint64, node *structs.Node) error {
	// Check if the node already exists
	existing, err := txn.First("nodes", "id", node.ID)
	if err != nil {
		return fmt.Errorf("node lookup failed: %v", err)
	}

	// Setup the indexes correctly
	if existing != nil {
		exist := existing.(*structs.Node)
		node.CreateIndex = exist.CreateIndex
		node.ModifyIndex = index

		// Retain node events that have already been set on the node
		node.Events = exist.Events

		// If we are transitioning from down, record the re-registration
		if exist.Status == constant.NodeStatusDown && node.Status != constant.NodeStatusDown {
			appendNodeEvents(index, node, []*structs.NodeEvent{
				structs.NewNodeEvent().SetSubsystem(structs.NodeEventSubsystemCluster).
					SetMessage(NodeRegisterEventReregistered).
					SetTimestamp(time.Unix(node.StatusUpdatedAt, 0))})
		}

		node.SchedulingEligibility = exist.SchedulingEligibility // Retain the eligibility
		node.DrainStrategy = exist.DrainStrategy                 // Retain the drain strategy
		node.LastDrain = exist.LastDrain                         // Retain the drain metadata
	} else {
		nodeEvent := structs.NewNodeEvent().SetSubsystem(structs.NodeEventSubsystemCluster).
			SetMessage(NodeRegisterEventRegistered).
			SetTimestamp(time.Unix(node.StatusUpdatedAt, 0))
		node.Events = []*structs.NodeEvent{nodeEvent}
		node.CreateIndex = index
		node.ModifyIndex = index
	}

	// Insert the node
	if err := txn.Insert("nodes", node); err != nil {
		return fmt.Errorf("node insert failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"nodes", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return nil
}

// DeleteNode deregisters a batch of nodes
func (s *StateStore) DeleteNode(msgType constant.MessageType, index uint64, nodes []string) error {
	txn := s.db.WriteTxn(index)
	defer txn.Abort()

	err := deleteNodeTxn(txn, index, nodes)
	if err != nil {
		return nil
	}
	return txn.Commit()
}

func deleteNodeTxn(txn *txn, index uint64, nodes []string) error {
	if len(nodes) == 0 {
		return fmt.Errorf("node ids missing")
	}

	for _, nodeID := range nodes {
		existing, err := txn.First("nodes", "id", nodeID)
		if err != nil {
			return fmt.Errorf("node lookup failed: %s: %v", nodeID, err)
		}
		if existing == nil {
			return fmt.Errorf("node not found: %s", nodeID)
		}

		// Delete the node
		if err := txn.Delete("nodes", existing); err != nil {
			return fmt.Errorf("node delete failed: %s: %v", nodeID, err)
		}
	}

	if err := txn.Insert("index", &IndexEntry{"nodes", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return nil
}

// UpdateNodeStatus is used to update the status of a node
func (s *StateStore) UpdateNodeStatus(msgType constant.MessageType, index uint64, nodeID, status string, updatedAt int64, event *structs.NodeEvent) error {
	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()

	if err := s.updateNodeStatusTxn(txn, nodeID, status, updatedAt, event); err != nil {
		return err
	}

	return txn.Commit()
}

func (s *StateStore) updateNodeStatusTxn(txn *txn, nodeID, status string, updatedAt int64, event *structs.NodeEvent) error {

	// Lookup the node
	existing, err := txn.First("nodes", "id", nodeID)
	if err != nil {
		return fmt.Errorf("node lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("node not found")
	}

	// Copy the existing node
	existingNode := existing.(*structs.Node)
	copyNode := existingNode.Copy()
	copyNode.StatusUpdatedAt = updatedAt

	// Add the event if given
	if event != nil {
		appendNodeEvents(txn.Index, copyNode, []*structs.NodeEvent{event})
	}

	// Update the status in the copy
	copyNode.Status = status
	copyNode.ModifyIndex = txn.Index

	// Insert the node
	if err := txn.Insert("nodes", copyNode); err != nil {
		return fmt.Errorf("node update failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"nodes", txn.Index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}
	return nil
}

// UpdateNodeDrain is used to update the drain of a node
func (s *StateStore) UpdateNodeDrain(msgType constant.MessageType, index uint64, nodeID string,
	drain *structs.DrainStrategy, markEligible bool, updatedAt int64,
	event *structs.NodeEvent, drainMeta map[string]string, accessorId string) error {

	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()
	if err := s.updateNodeDrainImpl(txn, index, nodeID, drain, markEligible, updatedAt, event,
		drainMeta, accessorId, false); err != nil {

		return err
	}
	return txn.Commit()
}

func (s *StateStore) updateNodeDrainImpl(txn *txn, index uint64, nodeID string,
	drain *structs.DrainStrategy, markEligible bool, updatedAt int64,
	event *structs.NodeEvent, drainMeta map[string]string, accessorId string,
	drainCompleted bool) error {

	// Lookup the node
	existing, err := txn.First("nodes", "id", nodeID)
	if err != nil {
		return fmt.Errorf("node lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("node not found")
	}

	// Copy the existing node
	existingNode := existing.(*structs.Node)
	updatedNode := existingNode.Copy()
	updatedNode.StatusUpdatedAt = updatedAt

	// Add the event if given
	if event != nil {
		appendNodeEvents(index, updatedNode, []*structs.NodeEvent{event})
	}

	// Update the drain in the copy
	updatedNode.DrainStrategy = drain
	if drain != nil {
		updatedNode.SchedulingEligibility = constant.NodeSchedulingIneligible
	} else if markEligible {
		updatedNode.SchedulingEligibility = constant.NodeSchedulingEligible
	}

	// Update LastDrain
	updateTime := time.Unix(updatedAt, 0)

	// if drain strategy isn't set before or after, this wasn't a drain operation
	// in that case, we don't care about .LastDrain
	drainNoop := existingNode.DrainStrategy == nil && updatedNode.DrainStrategy == nil
	// otherwise, when done with this method, updatedNode.LastDrain should be set
	// if starting a new drain operation, create a new LastDrain. otherwise, update the existing one.
	startedDraining := existingNode.DrainStrategy == nil && updatedNode.DrainStrategy != nil
	if !drainNoop {
		if startedDraining {
			updatedNode.LastDrain = &structs.DrainMetadata{
				StartedAt: updateTime,
				Meta:      drainMeta,
			}
		} else if updatedNode.LastDrain == nil {
			// if already draining and LastDrain doesn't exist, we need to create a new one
			// this could happen if we upgraded to 1.1.x during a drain
			updatedNode.LastDrain = &structs.DrainMetadata{
				// we don't have sub-second accuracy on these fields, so truncate this
				StartedAt: time.Unix(existingNode.DrainStrategy.StartedAt.Unix(), 0),
				Meta:      drainMeta,
			}
		}

		updatedNode.LastDrain.UpdatedAt = updateTime

		// won't have new metadata on drain complete; keep the existing operator-provided metadata
		// also, keep existing if they didn't provide it
		if len(drainMeta) != 0 {
			updatedNode.LastDrain.Meta = drainMeta
		}

		// we won't have an accessor ID on drain complete, so don't overwrite the existing one
		if accessorId != "" {
			updatedNode.LastDrain.AccessorID = accessorId
		}

		if updatedNode.DrainStrategy != nil {
			updatedNode.LastDrain.Status = constant.DrainStatusDraining
		} else if drainCompleted {
			updatedNode.LastDrain.Status = constant.DrainStatusComplete
		} else {
			updatedNode.LastDrain.Status = constant.DrainStatusCanceled
		}
	}

	updatedNode.ModifyIndex = index

	// Insert the node
	if err := txn.Insert("nodes", updatedNode); err != nil {
		return fmt.Errorf("node update failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"nodes", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return nil
}

// UpdateNodeEligibility is used to update the scheduling eligibility of a node
func (s *StateStore) UpdateNodeEligibility(msgType constant.MessageType, index uint64, nodeID string, eligibility string, updatedAt int64, event *structs.NodeEvent) error {

	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()

	// Lookup the node
	existing, err := txn.First("nodes", "id", nodeID)
	if err != nil {
		return fmt.Errorf("node lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("node not found")
	}

	// Copy the existing node
	existingNode := existing.(*structs.Node)
	copyNode := existingNode.Copy()
	copyNode.StatusUpdatedAt = updatedAt

	// Add the event if given
	if event != nil {
		appendNodeEvents(index, copyNode, []*structs.NodeEvent{event})
	}

	// Check if this is a valid action
	if copyNode.DrainStrategy != nil && eligibility == constant.NodeSchedulingEligible {
		return fmt.Errorf("can not set node's scheduling eligibility to eligible while it is draining")
	}

	// Update the eligibility in the copy
	copyNode.SchedulingEligibility = eligibility
	copyNode.ModifyIndex = index

	// Insert the node
	if err := txn.Insert("nodes", copyNode); err != nil {
		return fmt.Errorf("node update failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"nodes", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return txn.Commit()
}

// UpsertNodeEvents adds the node events to the nodes, rotating events as
// necessary.
func (s *StateStore) UpsertNodeEvents(msgType constant.MessageType, index uint64, nodeEvents map[string][]*structs.NodeEvent) error {
	txn := s.db.WriteTxnMsgT(msgType, index)
	defer txn.Abort()

	for nodeID, events := range nodeEvents {
		if err := s.upsertNodeEvents(index, nodeID, events, txn); err != nil {
			return err
		}
	}

	return txn.Commit()
}

// upsertNodeEvent upserts a node event for a respective node. It also maintains
// that a fixed number of node events are ever stored simultaneously, deleting
// older events once this bound has been reached.
func (s *StateStore) upsertNodeEvents(index uint64, nodeID string, events []*structs.NodeEvent, txn *txn) error {
	// Lookup the node
	existing, err := txn.First("nodes", "id", nodeID)
	if err != nil {
		return fmt.Errorf("node lookup failed: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("node not found")
	}

	// Copy the existing node
	existingNode := existing.(*structs.Node)
	copyNode := existingNode.Copy()
	appendNodeEvents(index, copyNode, events)

	// Insert the node
	if err := txn.Insert("nodes", copyNode); err != nil {
		return fmt.Errorf("node update failed: %v", err)
	}
	if err := txn.Insert("index", &IndexEntry{"nodes", index}); err != nil {
		return fmt.Errorf("index update failed: %v", err)
	}

	return nil
}

// appendNodeEvents is a helper that takes a node and new events and appends
// them, pruning older events as needed.
func appendNodeEvents(index uint64, node *structs.Node, events []*structs.NodeEvent) {
	// Add the events, updating the indexes
	for _, e := range events {
		e.CreateIndex = index
		node.Events = append(node.Events, e)
	}

	// Keep node events pruned to not exceed the max allowed
	if l := len(node.Events); l > constant.MaxRetainedNodeEvents {
		delta := l - constant.MaxRetainedNodeEvents
		node.Events = node.Events[delta:]
	}
}

// NodeByID is used to lookup a node by ID
func (s *StateStore) NodeByID(ws memdb.WatchSet, nodeID string) (*structs.Node, error) {
	txn := s.db.ReadTxn()

	watchCh, existing, err := txn.FirstWatch("nodes", "id", nodeID)
	if err != nil {
		return nil, fmt.Errorf("node lookup failed: %v", err)
	}
	ws.Add(watchCh)

	if existing != nil {
		return existing.(*structs.Node), nil
	}
	return nil, nil
}

// NodesByIDPrefix is used to lookup nodes by prefix
func (s *StateStore) NodesByIDPrefix(ws memdb.WatchSet, nodeID string) (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	iter, err := txn.Get("nodes", "id_prefix", nodeID)
	if err != nil {
		return nil, fmt.Errorf("node lookup failed: %v", err)
	}
	ws.Add(iter.WatchCh())

	return iter, nil
}

// NodeBySecretID is used to lookup a node by SecretID
func (s *StateStore) NodeBySecretID(ws memdb.WatchSet, secretID string) (*structs.Node, error) {
	txn := s.db.ReadTxn()

	watchCh, existing, err := txn.FirstWatch("nodes", "secret_id", secretID)
	if err != nil {
		return nil, fmt.Errorf("node lookup by SecretID failed: %v", err)
	}
	ws.Add(watchCh)

	if existing != nil {
		return existing.(*structs.Node), nil
	}
	return nil, nil
}
