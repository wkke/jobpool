package drainer

import (
	"context"
	"fmt"
	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"golang.org/x/time/rate"
	"sync"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper"
	xtime "yunli.com/jobpool/helper/time"
	"yunli.com/jobpool/helper/uuid"
)

var (
	// stateReadErrorDelay is the delay to apply before retrying reading state
	// when there is an error
	stateReadErrorDelay = 1 * time.Second
)

const (
	// LimitStateQueriesPerSecond is the number of state queries allowed per
	// second
	LimitStateQueriesPerSecond = 100.0

	// BatchUpdateInterval is how long we wait to batch updates
	BatchUpdateInterval = 1 * time.Second

	// NodeDeadlineCoalesceWindow is the duration in which deadlining nodes will
	// be coalesced together
	NodeDeadlineCoalesceWindow = 5 * time.Second

	// NodeDrainEventComplete is used to indicate that the node drain is
	// finished.
	NodeDrainEventComplete = "Node drain complete"

	// NodeDrainEventDetailDeadlined is the key to use when the drain is
	// complete because a deadline. The acceptable values are "true" and "false"
	NodeDrainEventDetailDeadlined = "deadline_reached"
)

// RaftApplier contains methods for applying the raft requests required by the
// NodeDrainer.
type RaftApplier interface {
	AllocUpdateDesiredTransition(allocs map[string]*structs.DesiredTransition, evals []*structs.Evaluation) (uint64, error)
	NodesDrainComplete(nodes []string, event *structs.NodeEvent) (uint64, error)
	AllocUpdateInDrainNode(allocs []*structs.Allocation) (uint64, error)
}

// NodeTracker is the interface to notify an object that is tracking draining
// nodes of changes
type NodeTracker interface {
	// TrackedNodes returns all the nodes that are currently tracked as
	// draining.
	TrackedNodes() map[string]*structs.Node

	// Remove removes a node from the draining set.
	Remove(nodeID string)

	// Update either updates the specification of a draining node or tracks the
	// node as draining.
	Update(node *structs.Node)
}

// DrainingJobWatcherFactory returns a new DrainingJobWatcher
type DrainingJobWatcherFactory func(context.Context, *rate.Limiter, *state.StateStore, log.Logger) DrainingJobWatcher

// DrainDeadlineNotifierFactory returns a new DrainDeadlineNotifier
type DrainDeadlineNotifierFactory func(context.Context) DrainDeadlineNotifier

// DrainingNodeWatcherFactory returns a new DrainingNodeWatcher
type DrainingNodeWatcherFactory func(context.Context, *rate.Limiter, *state.StateStore, log.Logger, NodeTracker) DrainingNodeWatcher

func GetDrainingJobWatcher(ctx context.Context, limiter *rate.Limiter, state *state.StateStore, logger log.Logger) DrainingJobWatcher {
	return NewDrainingJobWatcher(ctx, limiter, state, logger)
}

// allocMigrateBatcher is used to batch allocation updates.
type allocMigrateBatcher struct {
	updates []*structs.Allocation

	updateFuture *structs.BatchFuture

	// updateTimer is the timer that will trigger the next batch
	// update, and may be nil if there is no batch pending.
	updateTimer *time.Timer

	batchWindow time.Duration

	// synchronizes access to the updates list, the future and the timer.
	sync.Mutex
}

// NodeDrainerConfig is used to configure a new node drainer.
type NodeDrainerConfig struct {
	Logger               log.Logger
	Raft                 RaftApplier
	JobFactory           DrainingJobWatcherFactory
	NodeFactory          DrainingNodeWatcherFactory
	DrainDeadlineFactory DrainDeadlineNotifierFactory

	// StateQueriesPerSecond configures the query limit against the state store
	// that is allowed by the node drainer.
	StateQueriesPerSecond float64

	// BatchUpdateInterval is the interval in which allocation updates are
	// batched.
	BatchUpdateInterval time.Duration
}

// NodeDrainer is used to orchestrate migrating allocations off of draining
// nodes.
type NodeDrainer struct {
	enabled bool
	logger  log.Logger

	// nodes is the set of draining nodes
	nodes map[string]*drainingNode

	// nodeWatcher watches for nodes to transition in and out of drain state.
	nodeWatcher DrainingNodeWatcher
	nodeFactory DrainingNodeWatcherFactory

	jobWatcher DrainingJobWatcher
	jobFactory DrainingJobWatcherFactory

	// deadlineNotifier notifies when nodes reach their drain deadline.
	deadlineNotifier        DrainDeadlineNotifier
	deadlineNotifierFactory DrainDeadlineNotifierFactory

	// state is the state that is watched for state changes.
	state *state.StateStore

	// queryLimiter is used to limit the rate of blocking queries
	queryLimiter *rate.Limiter

	// raft is a shim around the raft messages necessary for draining
	raft RaftApplier

	// batcher is used to batch alloc migrations.
	batcher allocMigrateBatcher

	// ctx and exitFn are used to cancel the watcher
	ctx    context.Context
	exitFn context.CancelFunc

	l sync.RWMutex
}

// GetNodeWatcherFactory returns a DrainingNodeWatcherFactory
func GetNodeWatcherFactory() DrainingNodeWatcherFactory {
	return func(ctx context.Context, limiter *rate.Limiter, state *state.StateStore, logger log.Logger, tracker NodeTracker) DrainingNodeWatcher {
		return NewNodeDrainWatcher(ctx, limiter, state, logger, tracker)
	}
}

// GetDeadlineNotifier returns a node deadline notifier with default coalescing.
func GetDeadlineNotifier(ctx context.Context) DrainDeadlineNotifier {
	return NewDeadlineHeap(ctx, NodeDeadlineCoalesceWindow)
}

// NewNodeDrainer returns a new new node drainer. The node drainer is
// responsible for marking allocations on draining nodes with a desired
// migration transition, updating the drain strategy on nodes when they are
// complete and creating evaluations for the system to react to these changes.
func NewNodeDrainer(c *NodeDrainerConfig) *NodeDrainer {
	return &NodeDrainer{
		raft:                    c.Raft,
		logger:                  c.Logger.Named("drain"),
		jobFactory:              c.JobFactory,
		nodeFactory:             c.NodeFactory,
		deadlineNotifierFactory: c.DrainDeadlineFactory,
		queryLimiter:            rate.NewLimiter(rate.Limit(c.StateQueriesPerSecond), 100),
		batcher: allocMigrateBatcher{
			batchWindow: c.BatchUpdateInterval,
		},
	}
}

// SetEnabled will start or stop the node draining goroutine depending on the
// enabled boolean.
func (n *NodeDrainer) SetEnabled(enabled bool, state *state.StateStore) {
	n.l.Lock()
	defer n.l.Unlock()

	// If we are starting now or have a new state, init state and start the
	// run loop
	n.enabled = enabled
	if enabled {
		n.flush(state)
		go n.run(n.ctx)
		// 扩展逻辑，解决历史节点down且没有监听到，之后alloc没有程序处理的问题
		go n.runExtension(n.ctx)
	} else if !enabled && n.exitFn != nil {
		n.exitFn()
	}
}

// flush is used to clear the state of the watcher
func (n *NodeDrainer) flush(state *state.StateStore) {
	// Cancel anything that may be running.
	if n.exitFn != nil {
		n.exitFn()
	}

	// Store the new state
	if state != nil {
		n.state = state
	}

	n.ctx, n.exitFn = context.WithCancel(context.Background())
	n.jobWatcher = n.jobFactory(n.ctx, n.queryLimiter, n.state, n.logger)
	n.nodeWatcher = n.nodeFactory(n.ctx, n.queryLimiter, n.state, n.logger, n)
	n.deadlineNotifier = n.deadlineNotifierFactory(n.ctx)
	n.nodes = make(map[string]*drainingNode, 32)
}

// run is a long lived event handler that receives changes from the relevant
// watchers and takes action based on them.
func (n *NodeDrainer) run(ctx context.Context) {
	for {
		select {
		case <-n.ctx.Done():
			return
		case nodes := <-n.deadlineNotifier.NextBatch():
			n.handleDeadlinedNodes(nodes)
		case req := <-n.jobWatcher.Drain():
			n.handleJobAllocDrain(req)
		case allocs := <-n.jobWatcher.Migrated():
			n.handleMigratedAllocs(allocs)
		}
	}
}

// handleDeadlinedNodes handles a set of nodes reaching their drain deadline.
// The handler detects the remaining allocations on the nodes and immediately
// marks them for migration.
func (n *NodeDrainer) handleDeadlinedNodes(nodes []string) {
	// Retrieve the set of allocations that will be force stopped.
	var forceStop []*structs.Allocation
	n.l.RLock()
	for _, node := range nodes {
		draining, ok := n.nodes[node]
		if !ok {
			n.logger.Debug("skipping untracked deadlined node", "node_id", node)
			continue
		}
		allocs, err := draining.RemainingAllocs()
		if err != nil {
			n.logger.Error("failed to retrieve allocs on deadlined node", "node_id", node, "error", err)
			continue
		}

		n.logger.Debug("node deadlined causing allocs to be force stopped", "node_id", node, "num_allocs", len(allocs))
		forceStop = append(forceStop, allocs...)
	}
	n.l.RUnlock()
	n.batchDrainAllocs(forceStop)

	// Create the node event
	event := structs.NewNodeEvent().
		SetSubsystem(structs.NodeEventSubsystemDrain).
		SetMessage(NodeDrainEventComplete).
		AddDetail(NodeDrainEventDetailDeadlined, "true")

	// Submit the node transitions in a sharded form to ensure a reasonable
	// Raft transaction size.
	for _, nodes := range partitionIds(defaultMaxIdsPerTxn, nodes) {
		if _, err := n.raft.NodesDrainComplete(nodes, event); err != nil {
			n.logger.Error("failed to unset drain for nodes", "error", err)
		}
	}
}

func (n *NodeDrainer) handleJobAllocDrain(req *DrainRequest) {
	index, err := n.batchDrainAllocs(req.Allocs)
	req.Resp.Respond(index, err)
}

func (n *NodeDrainer) handleMigratedAllocs(allocs []*structs.Allocation) {
	// Determine the set of nodes that were effected
	nodes := make(map[string]struct{})
	for _, alloc := range allocs {
		nodes[alloc.NodeID] = struct{}{}
	}

	var done []string
	var remainingAllocs []*structs.Allocation

	// For each node, check if it is now done
	n.l.RLock()
	for node := range nodes {
		draining, ok := n.nodes[node]
		if !ok {
			continue
		}

		isDone, err := draining.IsDone()
		if err != nil {
			n.logger.Error("error checking if node is done draining", "node_id", node, "error", err)
			continue
		}

		if !isDone {
			continue
		}

		done = append(done, node)

		remaining, err := draining.RemainingAllocs()
		if err != nil {
			n.logger.Error("node is done draining but encountered an error getting remaining allocs", "node_id", node, "error", err)
			continue
		}

		remainingAllocs = append(remainingAllocs, remaining...)
	}
	n.l.RUnlock()

	// Stop any running system jobs on otherwise done nodes
	if len(remainingAllocs) > 0 {
		future := structs.NewBatchFuture()
		n.drainAllocs(future, remainingAllocs)
		if err := future.Wait(); err != nil {
			n.logger.Error("failed to drain remaining allocs from done nodes", "num_allocs", len(remainingAllocs), "error", err)
		}
	}

	// Create the node event
	event := structs.NewNodeEvent().
		SetSubsystem(structs.NodeEventSubsystemDrain).
		SetMessage(NodeDrainEventComplete)

	// Submit the node transitions in a sharded form to ensure a reasonable
	// Raft transaction size.
	for _, nodes := range partitionIds(defaultMaxIdsPerTxn, done) {
		if _, err := n.raft.NodesDrainComplete(nodes, event); err != nil {
			n.logger.Error("failed to unset drain for nodes", "error", err)
		}
	}
}

// batchDrainAllocs is used to batch the draining of allocations. It will block
// until the batch is complete.
func (n *NodeDrainer) batchDrainAllocs(allocs []*structs.Allocation) (uint64, error) {
	// Add this to the batch
	n.batcher.Lock()
	n.batcher.updates = append(n.batcher.updates, allocs...)
	n.logger.Info("--需要迁移的alloc数量", "size", len(allocs))

	// Start a new batch if none
	future := n.batcher.updateFuture
	if future == nil {
		future = structs.NewBatchFuture()
		n.batcher.updateFuture = future
		n.batcher.updateTimer = time.AfterFunc(n.batcher.batchWindow, func() {
			// Get the pending updates
			n.batcher.Lock()
			updates := n.batcher.updates
			future := n.batcher.updateFuture
			n.batcher.updates = nil
			n.batcher.updateFuture = nil
			n.batcher.updateTimer = nil
			n.batcher.Unlock()

			// Perform the batch update
			n.drainAllocs(future, updates)
		})
	}
	n.batcher.Unlock()

	if err := future.Wait(); err != nil {
		return 0, err
	}

	return future.Index(), nil
}

// drainAllocs is a non batch, marking of the desired transition to migrate for
// the set of allocations. It will also create the necessary evaluations for the
// affected jobs.
func (n *NodeDrainer) drainAllocs(future *structs.BatchFuture, allocs []*structs.Allocation) {
	// Compute the effected jobs and make the transition map
	jobs := make(map[structs.NamespacedID]*structs.Allocation, 4)
	transitions := make(map[string]*structs.DesiredTransition, len(allocs))
	for _, alloc := range allocs {
		transitions[alloc.ID] = &structs.DesiredTransition{
			Migrate: helper.BoolToPtr(true),
		}
		jobs[alloc.PlanNamespacedID()] = alloc
	}

	evals := make([]*structs.Evaluation, 0, len(jobs))
	now := xtime.NewFormatTime(time.Now())
	for _, alloc := range jobs {
		evals = append(evals, &structs.Evaluation{
			ID:          uuid.Generate(),
			Namespace:   alloc.Namespace,
			Priority:    alloc.Plan.Priority,
			Type:        alloc.Plan.Type,
			TriggeredBy: constant.EvalTriggerNodeDrain,
			PlanID:      alloc.PlanID,
			JobID:       alloc.JobId,
			Status:      constant.EvalStatusPending,
			CreateTime:  now,
			UpdateTime:  now,
		})
	}

	// Commit this update via Raft
	var finalIndex uint64
	for _, u := range partitionAllocDrain(defaultMaxIdsPerTxn, transitions, evals) {
		index, err := n.raft.AllocUpdateDesiredTransition(u.Transitions, u.Evals)
		if err != nil {
			future.Respond(0, err)
			return
		}
		finalIndex = index
	}

	future.Respond(finalIndex, nil)
}

// 处理极特殊情况的alloc
func (n *NodeDrainer) runExtension(ctx context.Context) {
	timer, stop := helper.NewSafeTimer(stateReadErrorDelay)
	defer stop()
	nindex := uint64(1)
	aindex := uint64(1)
	for {
		timer.Reset(stateReadErrorDelay)
		nodes, index, err := n.getNodes(nindex)
		if err != nil {
			if err == context.Canceled {
				return
			}
			n.logger.Error("error get node updates at index", "index", nindex, "error", err)
			select {
			case <-n.ctx.Done():
				return
			case <-timer.C:
				continue
			}
		}
		nindex = index
		var downNodes []string
		for nodeID, node := range nodes {
			if node.Status == constant.NodeStatusDown {
				downNodes = append(downNodes, nodeID)
			}
		}
		allocs, index, err := n.getPendingAllocBefore(aindex)
		if err != nil {
			n.logger.Error("error get alloc by down nodes", "nodes", downNodes, "error", err)
			select {
			case <-n.ctx.Done():
				return
			case <-timer.C:
				continue
			}
		}
		aindex = index
		// 还要获取
		// n.logger.Info(fmt.Sprintf("----------大家注意啊------------%d", aindex))
		if len(allocs) == 0 {
			continue
		}
		var nodeDownAllocs []*structs.Allocation
		var notMatchNodeAllocs []*structs.Allocation
		for _, alloc := range allocs {
			matchedNode := false
			for _, nodeId := range downNodes {
				if alloc.NodeID == nodeId {
					matchedNode = true
					n.logger.Info("the pending alloc in down node", "jobId", alloc.JobId, "allocId", alloc.ID, "status", alloc.ClientStatus, "node", alloc.NodeID)
					nodeDownAllocs = append(nodeDownAllocs, alloc)
				}
			}
			if !matchedNode {
				notMatchNodeAllocs = append(notMatchNodeAllocs, alloc)
			}
		}
		for _, alloc := range notMatchNodeAllocs {
			n.logger.Info("the pending alloc in none node", "jobId", alloc.JobId, "allocId", alloc.ID, "status", alloc.ClientStatus, "node", alloc.NodeID)
		}
		if len(notMatchNodeAllocs) > 0 {
			nodeDownAllocs = append(nodeDownAllocs, notMatchNodeAllocs...)
		}
		for _, alloc := range nodeDownAllocs {
			alloc.ClientStatus = constant.AllocClientStatusFailed
			alloc.ClientDescription = fmt.Sprintf("allocation failed because the node: %s is down", alloc.NodeID)
		}
		// 这些alloc不迁移，直接failed即可
		_, err = n.raft.AllocUpdateInDrainNode(nodeDownAllocs)
		if err != nil {
			n.logger.Warn("update alloc need failed", "error", err)
		}
	}
}

// getNodes returns all nodes blocking until the nodes are after the given index.
func (n *NodeDrainer) getNodes(minIndex uint64) (map[string]*structs.Node, uint64, error) {
	if err := n.queryLimiter.Wait(n.ctx); err != nil {
		return nil, 0, err
	}
	resp, index, err := n.state.BlockingQuery(n.getNodesImpl, minIndex, n.ctx)
	if err != nil {
		return nil, 0, err
	}
	return resp.(map[string]*structs.Node), index, nil
}

// getNodesImpl is used to get nodes from the state store, returning the set of
// nodes and the given index.
func (n *NodeDrainer) getNodesImpl(ws memdb.WatchSet, state *state.StateStore) (interface{}, uint64, error) {
	iter, err := state.Nodes(ws)
	if err != nil {
		return nil, 0, err
	}

	index, err := state.Index("nodes")
	if err != nil {
		return nil, 0, err
	}

	var maxIndex uint64 = 0
	resp := make(map[string]*structs.Node, 64)
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}

		node := raw.(*structs.Node)
		resp[node.ID] = node
		if maxIndex < node.ModifyIndex {
			maxIndex = node.ModifyIndex
		}
	}

	// Prefer using the actual max index of affected nodes since it means less
	// unblocking
	if maxIndex != 0 {
		index = maxIndex
	}

	return resp, index, nil
}

func (n *NodeDrainer) getPendingAllocBefore(minIndex uint64) ([]*structs.Allocation, uint64, error) {
	ws := memdb.NewWatchSet()
	// 1分钟之前的pending状态alloc
	minuteBefore := time.Now().Add(time.Minute * -1)
	if minIndex == 1 {
		// 第一次就把之前pending的全failed
		minuteBefore = time.Now()
	}

	allocs, maxIndex, err := n.state.AllocsByStatusBefore(ws, minIndex, minuteBefore, constant.AllocClientStatusPending)
	if err != nil {
		return nil, maxIndex, err
	}
	return allocs, maxIndex, nil
}
