package core

import (
	"fmt"
	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"net/http"
	"sync"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/state/paginator"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
	"yunli.com/jobpool/helper/uuid"
)

const (
	// batchUpdateInterval is how long we wait to batch updates
	batchUpdateInterval = 50 * time.Millisecond

	// maxParallelRequestsPerDerive  is the maximum number of parallel Vault
	// create token requests that may be outstanding per derive request
	maxParallelRequestsPerDerive = 16

	// NodeDrainEvents are the various drain messages
	NodeDrainEventDrainSet      = "Node drain strategy set"
	NodeDrainEventDrainDisabled = "Node drain disabled"
	NodeDrainEventDrainUpdated  = "Node drain strategy updated"

	// NodeEligibilityEventEligible is used when the nodes eligiblity is marked
	// eligible
	NodeEligibilityEventEligible = "Node marked as eligible for scheduling"

	// NodeEligibilityEventIneligible is used when the nodes eligiblity is marked
	// ineligible
	NodeEligibilityEventIneligible = "Node marked as ineligible for scheduling"

	// NodeHeartbeatEventReregistered is the message used when the node becomes
	// reregistered by the heartbeat.
	NodeHeartbeatEventReregistered = "Node reregistered by heartbeat"
)

// Node endpoint is used for client interactions
type Node struct {
	srv    *Server
	logger hclog.Logger

	// ctx provides context regarding the underlying connection
	ctx *RPCContext

	// evals holds pending rescheduling eval updates triggered by failed allocations
	evals []*structs.Evaluation

	// updateTimer is the timer that will trigger the next batch
	// update, and may be nil if there is no batch pending.
	updateTimer *time.Timer

	// updatesLock synchronizes access to the updates list,
	// the future and the timer.
	updatesLock sync.Mutex
}

// Register is used to upsert a client that is available for scheduling
func (n *Node) Register(args *dto.NodeRegisterRequest, reply *dto.NodeUpdateResponse) error {
	isForwarded := args.IsForwarded()
	if done, err := n.srv.forward("Node.Register", args, args, reply); done {
		// We have a valid node connection since there is no error from the
		// forwarded server, so add the mapping to cache the
		// connection and allow the server to send RPCs to the client.
		if err == nil && n.ctx != nil && n.ctx.NodeID == "" && !isForwarded {
			n.ctx.NodeID = args.Node.ID
			n.srv.addNodeConn(n.ctx)
		}

		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "client", "register"}, time.Now())
	// Validate the arguments
	if args.Node == nil {
		return fmt.Errorf("missing node for client registration")
	}
	if args.Node.ID == "" {
		return fmt.Errorf("missing node ID for client registration")
	}
	if args.Node.Datacenter == "" {
		return fmt.Errorf("missing datacenter for client registration")
	}
	if args.Node.Name == "" {
		return fmt.Errorf("missing node name for client registration")
	}
	//if len(args.Node.Attributes) == 0 {
	//	return fmt.Errorf("missing attributes for client registration")
	//}
	if args.Node.SecretID == "" {
		return fmt.Errorf("missing node secret ID for client registration")
	}

	// Default the status if none is given
	if args.Node.Status == "" {
		args.Node.Status = constant.NodeStatusInit
	}
	if !structs.ValidNodeStatus(args.Node.Status) {
		return fmt.Errorf("invalid status for node")
	}

	// Default to eligible for scheduling if unset
	if args.Node.SchedulingEligibility == "" {
		args.Node.SchedulingEligibility = constant.NodeSchedulingEligible
	}

	// Set the timestamp when the node is registered
	args.Node.StatusUpdatedAt = time.Now().Unix()

	// Compute the node class
	if err := args.Node.ComputeClass(); err != nil {
		return fmt.Errorf("failed to computed node class: %v", err)
	}

	// Look for the node so we can detect a state transition
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}

	ws := memdb.NewWatchSet()
	originalNode, err := snap.NodeByID(ws, args.Node.ID)
	if err != nil {
		return err
	}

	// Check if the SecretID has been tampered with
	if originalNode != nil {
		if args.Node.SecretID != originalNode.SecretID && originalNode.SecretID != "" {
			return fmt.Errorf("node secret ID does not match. Not registering node.")
		}
	}

	// We have a valid node connection, so add the mapping to cache the
	// connection and allow the server to send RPCs to the client. We only cache
	// the connection if it is not being forwarded from another server.
	if n.ctx != nil && n.ctx.NodeID == "" && !args.IsForwarded() {
		n.ctx.NodeID = args.Node.ID
		n.srv.addNodeConn(n.ctx)
	}

	// Commit this update via Raft
	_, index, err := n.srv.raftApply(constant.NodeRegisterRequestType, args)
	if err != nil {
		n.logger.Error("register failed", "error", err)
		return err
	}
	reply.NodeModifyIndex = index

	// Check if we need to setup a heartbeat
	if !args.Node.TerminalStatus() {
		ttl, err := n.srv.resetHeartbeatTimer(args.Node.ID)
		if err != nil {
			n.logger.Error("heartbeat reset failed", "error", err)
			return err
		}
		reply.HeartbeatTTL = ttl
	}

	// Set the reply index
	reply.Index = index
	snap, err = n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}

	n.srv.peerLock.RLock()
	defer n.srv.peerLock.RUnlock()
	if err := n.constructNodeServerInfoResponse(snap, reply); err != nil {
		n.logger.Error("failed to populate NodeUpdateResponse", "error", err)
		return err
	}

	return nil
}

// UpdateStatus is used to update the status of a client node
func (n *Node) UpdateStatus(args *dto.NodeUpdateStatusRequest, reply *dto.NodeUpdateResponse) error {
	isForwarded := args.IsForwarded()
	if done, err := n.srv.forward("Node.UpdateStatus", args, args, reply); done {
		// We have a valid node connection since there is no error from the
		// forwarded server, so add the mapping to cache the
		// connection and allow the server to send RPCs to the client.
		if err == nil && n.ctx != nil && n.ctx.NodeID == "" && !isForwarded {
			n.ctx.NodeID = args.NodeID
			n.srv.addNodeConn(n.ctx)
		}

		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "client", "update_status"}, time.Now())

	// Verify the arguments
	if args.NodeID == "" {
		return fmt.Errorf("missing node ID for client status update")
	}
	if !structs.ValidNodeStatus(args.Status) {
		return fmt.Errorf("invalid status for node")
	}

	// Look for the node
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}

	ws := memdb.NewWatchSet()
	node, err := snap.NodeByID(ws, args.NodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node not found")
	}

	// We have a valid node connection, so add the mapping to cache the
	// connection and allow the server to send RPCs to the client. We only cache
	// the connection if it is not being forwarded from another server.
	if n.ctx != nil && n.ctx.NodeID == "" && !args.IsForwarded() {
		n.ctx.NodeID = args.NodeID
		n.srv.addNodeConn(n.ctx)
	}

	// XXX: Could use the SecretID here but have to update the heartbeat system
	// to track SecretIDs.

	// Update the timestamp of when the node status was updated
	args.UpdatedAt = time.Now().Unix()

	// Commit this update via Raft
	var index uint64
	if node.Status != args.Status {
		// Attach an event if we are updating the node status to ready when it
		// is down via a heartbeat
		if node.Status == constant.NodeStatusDown && args.NodeEvent == nil {
			args.NodeEvent = structs.NewNodeEvent().
				SetSubsystem(structs.NodeEventSubsystemCluster).
				SetMessage(NodeHeartbeatEventReregistered)
		}

		_, index, err = n.srv.raftApply(constant.NodeUpdateStatusRequestType, args)
		if err != nil {
			n.logger.Error("status update failed", "error", err)
			return err
		}
		reply.NodeModifyIndex = index
	}

	// Check if we should trigger evaluations
	if structs.ShouldDrainNode(args.Status) ||
		nodeStatusTransitionRequiresEval(args.Status, node.Status) {
		evalIDs, evalIndex, err := n.createNodeEvals(args.NodeID, index)
		if err != nil {
			n.logger.Error("eval creation failed", "error", err)
			return err
		}
		reply.EvalIDs = evalIDs
		reply.EvalCreateIndex = evalIndex
	}

	// Check if we need to setup a heartbeat
	switch args.Status {
	case constant.NodeStatusDown:
		// Determine if there are any Vault accessors on the node to cleanup
		n.logger.Debug("TODO when node down delete the service and other")
		n.logger.Debug("需要解注册一些内容")

	default:
		ttl, err := n.srv.resetHeartbeatTimer(args.NodeID)
		if err != nil {
			n.logger.Error("heartbeat reset failed", "error", err)
			return err
		}
		reply.HeartbeatTTL = ttl
	}

	// Set the reply index and leader
	reply.Index = index
	n.srv.peerLock.RLock()
	defer n.srv.peerLock.RUnlock()
	if err := n.constructNodeServerInfoResponse(snap, reply); err != nil {
		n.logger.Error("failed to populate NodeUpdateResponse", "error", err)
		return err
	}
	return nil
}

// nodeStatusTransitionRequiresEval is a helper that takes a nodes new and old status and
// returns whether it has transitioned to ready.
func nodeStatusTransitionRequiresEval(newStatus, oldStatus string) bool {
	initToReady := oldStatus == constant.NodeStatusInit && newStatus == constant.NodeStatusReady
	terminalToReady := oldStatus == constant.NodeStatusDown && newStatus == constant.NodeStatusReady
	disconnectedToOther := oldStatus == constant.NodeStatusDisconnected && newStatus != constant.NodeStatusDisconnected
	otherToDisconnected := oldStatus != constant.NodeStatusDisconnected && newStatus == constant.NodeStatusDisconnected
	return initToReady || terminalToReady || disconnectedToOther || otherToDisconnected
}

// updateNodeUpdateResponse assumes the n.srv.peerLock is held for reading.
func (n *Node) constructNodeServerInfoResponse(snap *state.StateSnapshot, reply *dto.NodeUpdateResponse) error {
	reply.LeaderRPCAddr = string(n.srv.raft.Leader())

	// Reply with cfg information required for future RPC requests
	reply.Servers = make([]*structs.NodeServerInfo, 0, len(n.srv.localPeers))
	for _, v := range n.srv.localPeers {
		reply.Servers = append(reply.Servers,
			&structs.NodeServerInfo{
				RPCAdvertiseAddr: v.RPCAddr.String(),
				Datacenter:       v.Datacenter,
			})
	}

	// TODO(sean@): Use an indexed node count instead
	//
	// Snapshot is used only to iterate over all nodes to create a node
	// count to send back to Jobpool Clients in their heartbeat so Clients
	// can estimate the size of the cluster.
	ws := memdb.NewWatchSet()
	iter, err := snap.Nodes(ws)
	if err == nil {
		for {
			raw := iter.Next()
			if raw == nil {
				break
			}
			reply.NumNodes++
		}
	}
	return nil
}

func (n *Node) EmitEvents(args *dto.EmitNodeEventsRequest, reply *dto.EmitNodeEventsResponse) error {
	// Ensure the connection was initiated by another client if TLS is used.
	err := validateTLSCertificateLevel(n.srv, n.ctx, tlsCertificateLevelClient)
	if err != nil {
		return err
	}

	if done, err := n.srv.forward("Node.EmitEvents", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "client", "emit_events"}, time.Now())

	if len(args.NodeEvents) == 0 {
		return fmt.Errorf("no node events given")
	}
	for nodeID, events := range args.NodeEvents {
		if len(events) == 0 {
			return fmt.Errorf("no node events given for node %q", nodeID)
		}
	}

	_, index, err := n.srv.raftApply(constant.UpsertNodeEventsType, args)
	if err != nil {
		n.logger.Error("upserting node events failed", "error", err)
		return err
	}

	reply.Index = index
	return nil
}

// List is used to list the available nodes
func (n *Node) List(args *dto.NodeListRequest,
	reply *dto.NodeListResponse) error {
	if done, err := n.srv.forward("Node.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "client", "list"}, time.Now())

	// Set up the blocking query.
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {

			var err error
			var iter memdb.ResultIterator
			if prefix := args.QueryOptions.Prefix; prefix != "" {
				iter, err = state.NodesByIDPrefix(ws, prefix)
			} else {
				iter, err = state.Nodes(ws)
			}
			if err != nil {
				return err
			}

			var nodes []*structs.NodeListStub

			// Build the paginator. This includes the function that is
			// responsible for appending a node to the nodes array.
			paginatorImpl, err := paginator.NewPaginator(iter, nil, args.QueryOptions,
				func(raw interface{}) error {
					nodes = append(nodes, raw.(*structs.Node).Stub(args.Fields))
					return nil
				})
			if err != nil {
				return structs.NewErrRPCCodedf(
					http.StatusBadRequest, "failed to create result paginator: %v", err)
			}

			// Calling page populates our output nodes array as well as returns
			// the next token.
			nextToken, totalElements, totalPages, err := paginatorImpl.Page()
			if err != nil {
				return structs.NewErrRPCCodedf(
					http.StatusBadRequest, "failed to read result page: %v", err)
			}

			// Populate the reply.
			reply.Nodes = nodes
			reply.NextToken = nextToken
			reply.TotalElements = totalElements
			reply.TotalPages = totalPages

			// Use the last index that affected the plans table
			index, err := state.Index("nodes")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			n.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return n.srv.blockingRPC(&opts)
}

// -- 获取节点上对应的alloc信息

// GetClientAllocs is used to request a lightweight list of alloc modify indexes
// per allocation.
func (n *Node) GetClientAllocs(args *dto.NodeSpecificRequest,
	reply *dto.NodeClientAllocsResponse) error {
	isForwarded := args.IsForwarded()
	if done, err := n.srv.forward("Node.GetClientAllocs", args, args, reply); done {
		// We have a valid node connection since there is no error from the
		// forwarded server, so add the mapping to cache the
		// connection and allow the server to send RPCs to the client.
		if err == nil && n.ctx != nil && n.ctx.NodeID == "" && !isForwarded {
			n.ctx.NodeID = args.NodeID
			n.srv.addNodeConn(n.ctx)
		}

		return err
	}
	defer metrics.MeasureSince([]string{"jobpool", "client", "get_client_allocs"}, time.Now())

	// Verify the arguments
	if args.NodeID == "" {
		return fmt.Errorf("missing node ID")
	}

	// numOldAllocs is used to detect if there is a garbage collection event
	// that effects the node. When an allocation is garbage collected, that does
	// not change the modify index changes and thus the query won't unblock,
	// even though the set of allocations on the node has changed.
	var numOldAllocs int

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {
			// Look for the node
			node, err := state.NodeByID(ws, args.NodeID)
			if err != nil {
				return err
			}

			var allocs []*structs.Allocation
			if node != nil {
				if args.SecretID == "" {
					return fmt.Errorf("missing node secret ID for client status update")
				} else if args.SecretID != node.SecretID {
					return fmt.Errorf("node secret ID does not match")
				}

				// We have a valid node connection, so add the mapping to cache the
				// connection and allow the server to send RPCs to the client. We only cache
				// the connection if it is not being forwarded from another server.
				if n.ctx != nil && n.ctx.NodeID == "" && !args.IsForwarded() {
					n.ctx.NodeID = args.NodeID
					n.srv.addNodeConn(n.ctx)
				}

				var err error
				allocs, err = state.AllocsByNode(ws, args.NodeID)
				if err != nil {
					return err
				}
			}

			reply.Allocs = make(map[string]uint64)

			// preferTableIndex is used to determine whether we should build the
			// response index based on the full table indexes versus the modify
			// indexes of the allocations on the specific node. This is
			// preferred in the case that the node doesn't yet have allocations
			// or when we detect a GC that effects the node.
			preferTableIndex := true

			// Setup the output
			if numAllocs := len(allocs); numAllocs != 0 {
				preferTableIndex = false

				for _, alloc := range allocs {
					reply.Allocs[alloc.ID] = alloc.AllocModifyIndex
					reply.Index = maxUint64(reply.Index, alloc.ModifyIndex)
				}

				// Determine if we have less allocations than before. This
				// indicates there was a garbage collection
				if numAllocs < numOldAllocs {
					preferTableIndex = true
				}

				// Store the new number of allocations
				numOldAllocs = numAllocs
			}

			if preferTableIndex {
				// Use the last index that affected the nodes table
				index, err := state.Index("allocs")
				if err != nil {
					return err
				}

				// Must provide non-zero index to prevent blocking
				// Index 1 is impossible anyways (due to Raft internals)
				if index == 0 {
					reply.Index = 1
				} else {
					reply.Index = index
				}
			}
			return nil
		}}
	return n.srv.blockingRPC(&opts)
}

// createNodeEvals is used to create evaluations for each alloc on a node.
// Each Eval is scoped to a job, so we need to potentially trigger many evals.
func (n *Node) createNodeEvals(nodeID string, nodeIndex uint64) ([]string, uint64, error) {
	// Snapshot the state
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to snapshot state: %v", err)
	}

	// Find all the allocations for this node
	ws := memdb.NewWatchSet()
	allocs, err := snap.AllocsByNode(ws, nodeID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to find allocs for '%s': %v", nodeID, err)
	}

	// Fast-path if nothing to do
	if len(allocs) == 0 {
		return nil, 0, nil
	}

	//if true {
	//	// TODO 2022-09-01
	//	// 经过验证，一次性的这种任务不需要迁移，否则会重复运行
	//	return nil, 0, nil
	//}

	// Create an eval for each JobID affected
	var evals []*structs.Evaluation
	var evalIDs []string
	planIDs := map[structs.NamespacedID]struct{}{}
	now := xtime.NewFormatTime(time.Now())

	for _, alloc := range allocs {
		// Deduplicate on PlanID
		if _, ok := planIDs[alloc.PlanNamespacedID()]; ok {
			continue
		}
		if !alloc.MigrateStatus() {
			continue
		}

		planIDs[alloc.PlanNamespacedID()] = struct{}{}

		// Create a new eval
		eval := &structs.Evaluation{
			ID:              uuid.Generate(),
			Namespace:       alloc.Namespace,
			Priority:        alloc.Plan.Priority,
			Type:            alloc.Plan.Type,
			TriggeredBy:     constant.EvalTriggerNodeUpdate,
			PlanID:          alloc.PlanID,
			JobID:           alloc.JobId,
			NodeID:          nodeID,
			NodeModifyIndex: nodeIndex,
			Status:          constant.EvalStatusPending,
			CreateTime:      now,
			UpdateTime:      now,
		}
		evals = append(evals, eval)
		evalIDs = append(evalIDs, eval.ID)
	}

	// Create the Raft transaction
	update := &dto.EvalUpdateRequest{
		Evals:        evals,
		WriteRequest: dto.WriteRequest{Region: n.srv.config.Region},
	}

	// Commit this evaluation via Raft
	// XXX: There is a risk of partial failure where the node update succeeds
	// but that the EvalUpdate does not.
	_, evalIndex, err := n.srv.raftApply(constant.EvalUpdateRequestType, update)
	if err != nil {
		return nil, 0, err
	}
	return evalIDs, evalIndex, nil
}
