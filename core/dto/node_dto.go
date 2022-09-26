package dto

import (
	"time"
	"yunli.com/jobpool/core/structs"
)

// NodeRegisterRequest is used for Node.Register endpoint
// to register a node as being a schedulable entity.
type NodeRegisterRequest struct {
	Node      *structs.Node
	NodeEvent *structs.NodeEvent
	WriteRequest
}

// NodeDeregisterRequest is used for Node.Deregister endpoint
// to deregister a node as being a schedulable entity.
type NodeDeregisterRequest struct {
	NodeID string
	WriteRequest
}

// NodeBatchDeregisterRequest is used for Node.BatchDeregister endpoint
// to deregister a batch of nodes from being schedulable entities.
type NodeBatchDeregisterRequest struct {
	NodeIDs []string
	WriteRequest
}

// NodeUpdateStatusRequest is used for Node.UpdateStatus endpoint
// to update the status of a node.
type NodeUpdateStatusRequest struct {
	NodeID    string
	Status    string
	NodeEvent *structs.NodeEvent
	UpdatedAt int64
	WriteRequest
}

// EmitNodeEventsRequest is a request to update the node events source
// with a new client-side event
type EmitNodeEventsRequest struct {
	// NodeEvents are a map where the key is a node id, and value is a list of
	// events for that node
	NodeEvents map[string][]*structs.NodeEvent

	WriteRequest
}

// BatchNodeUpdateDrainRequest is used for updating the drain strategy for a
// batch of nodes
type BatchNodeUpdateDrainRequest struct {
	// Updates is a mapping of nodes to their updated drain strategy
	Updates map[string]*structs.DrainUpdate

	// NodeEvents is a mapping of the node to the event to add to the node
	NodeEvents map[string]*structs.NodeEvent

	// UpdatedAt represents server time of receiving request
	UpdatedAt int64

	WriteRequest
}

// NodeUpdateResponse is used to respond to a node update
type NodeUpdateResponse struct {
	HeartbeatTTL    time.Duration
	EvalIDs         []string
	EvalCreateIndex uint64
	NodeModifyIndex uint64

	// Features informs clients what enterprise features are allowed
	Features uint64

	// LeaderRPCAddr is the RPC address of the current Raft Leader.  If
	// empty, the current Jobpool Server is in the minority of a partition.
	LeaderRPCAddr string

	// NumNodes is the number of Jobpool nodes attached to this quorum of
	// Jobpool Servers at the time of the response.  This value can
	// fluctuate based on the health of the cluster between heartbeats.
	NumNodes int32

	// Servers is the full list of known Jobpool servers in the local
	// region.
	Servers []*structs.NodeServerInfo

	QueryMeta
}

// EmitNodeEventsResponse is a response to the client about the status of
// the node event source update.
type EmitNodeEventsResponse struct {
	WriteMeta
}

// NodeListRequest is used to parameterize a list request
type NodeListRequest struct {
	QueryOptions

	Fields *structs.NodeStubFields
}

// NodeListResponse is used for a list request
type NodeListResponse struct {
	Nodes []*structs.NodeListStub `json:"data"`
	QueryMeta
	ExcludePageResponse
}

type NodeHealthResponse struct {
	Master int
	Client int
	Leader int
}
