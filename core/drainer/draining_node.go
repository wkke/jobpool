package drainer

import (
	"fmt"
	"sync"
	"time"

	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/structs"
)

type drainingNode struct {
	state *state.StateStore
	node  *structs.Node
	l     sync.RWMutex
}

func NewDrainingNode(node *structs.Node, state *state.StateStore) *drainingNode {
	return &drainingNode{
		state: state,
		node:  node,
	}
}

func (n *drainingNode) GetNode() *structs.Node {
	n.l.Lock()
	defer n.l.Unlock()
	return n.node
}

func (n *drainingNode) Update(node *structs.Node) {
	n.l.Lock()
	defer n.l.Unlock()
	n.node = node
}

// DeadlineTime returns if the node has a deadline and if so what it is
func (n *drainingNode) DeadlineTime() (bool, time.Time) {
	n.l.RLock()
	defer n.l.RUnlock()

	// Should never happen
	if n.node == nil || n.node.DrainStrategy == nil {
		return false, time.Time{}
	}

	return n.node.DrainStrategy.DeadlineTime()
}

// IsDone returns if the node is done draining batch and service allocs. System
// allocs must be stopped before marking drain complete unless they're being
// ignored.
func (n *drainingNode) IsDone() (bool, error) {
	n.l.RLock()
	defer n.l.RUnlock()

	// Should never happen
	if n.node == nil || n.node.DrainStrategy == nil {
		return false, fmt.Errorf("node doesn't have a drain strategy set")
	}
	return true, nil
}


// RemainingAllocs returns the set of allocations remaining on a node that
// still need to be drained.
func (n *drainingNode) RemainingAllocs() ([]*structs.Allocation, error) {
	n.l.RLock()
	defer n.l.RUnlock()

	// Should never happen
	if n.node == nil || n.node.DrainStrategy == nil {
		return nil, fmt.Errorf("node doesn't have a drain strategy set")
	}

	// Retrieve the allocs on the node
	allocs, err := n.state.AllocsByNode(nil, n.node.ID)
	if err != nil {
		return nil, err
	}

	var drain []*structs.Allocation
	for _, alloc := range allocs {
		// Nothing to do on a terminal allocation
		if alloc.TerminalStatus() {
			continue
		}

		drain = append(drain, alloc)
	}

	return drain, nil
}


// DrainingJobs returns the set of jobs on the node that can block a drain.
// These include batch and service jobs.
func (n *drainingNode) DrainingJobs() ([]structs.NamespacedID, error) {
	n.l.RLock()
	defer n.l.RUnlock()

	// Should never happen
	if n.node == nil || n.node.DrainStrategy == nil {
		return nil, fmt.Errorf("node doesn't have a drain strategy set")
	}

	// Retrieve the allocs on the node
	allocs, err := n.state.AllocsByNode(nil, n.node.ID)
	if err != nil {
		return nil, err
	}

	jobIDs := make(map[structs.NamespacedID]struct{})
	var jobs []structs.NamespacedID
	for _, alloc := range allocs {
		if alloc.TerminalStatus() {
			continue
		}

		jns := structs.NamespacedID{Namespace: alloc.Namespace, ID: alloc.PlanID}
		if _, ok := jobIDs[jns]; ok {
			continue
		}
		jobIDs[jns] = struct{}{}
		jobs = append(jobs, jns)
	}

	return jobs, nil
}