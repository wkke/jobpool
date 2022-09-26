package structs

import (
	"fmt"
	"net"
	"strings"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/helper"
)

// Node is a representation of a schedulable client node
type Node struct {
	// ID is a unique identifier for the node. It can be constructed
	// by doing a concatenation of the Name and Datacenter as a simple
	// approach. Alternatively a UUID may be used.
	ID string

	// SecretID is an ID that is only known by the Node and the set of Servers.
	// It is not accessible via the API and is used to authenticate nodes
	// conducting privileged activities.
	SecretID string

	// Datacenter for this node
	Datacenter string

	// Node name
	Name string

	// HTTPAddr is the address on which the Jobpool client is listening for http
	// requests
	HTTPAddr string

	// TLSEnabled indicates if the Agent has TLS enabled for the HTTP API
	TLSEnabled bool

	// Attributes is an arbitrary set of key/value
	// data that can be used for constraints. Examples
	// include "kernel.name=linux", "arch=386", "driver.docker=1",
	// "docker.runtime=1.8.3"
	Attributes map[string]string


	// Links are used to 'link' this client to external
	// systems.
	Links map[string]string

	// Meta is used to associate arbitrary metadata with this
	// client. This is opaque to Jobpool.
	Meta map[string]string

	// NodeClass is an opaque identifier used to group nodes
	// together for the purpose of determining scheduling pressure.
	NodeClass string

	// ComputedClass is a unique id that identifies nodes with a common set of
	// attributes and capabilities.
	ComputedClass string

	// DrainStrategy determines the node's draining behavior.
	// Will be non-nil only while draining.
	DrainStrategy *DrainStrategy

	// SchedulingEligibility determines whether this node will receive new
	// placements.
	SchedulingEligibility string

	// Status of this node
	Status string

	// StatusDescription is meant to provide more human useful information
	StatusDescription string

	// StatusUpdatedAt is the time stamp at which the state of the node was
	// updated
	StatusUpdatedAt int64

	// Events is the most recent set ofnetwork.go events generated for the node,
	// retaining only MaxRetainedNodeEvents number at a time
	Events []*NodeEvent

	// HostNetworks is a map of host host_network names to their configuration
	HostNetworks map[string]*ClientHostNetworkConfig

	// LastDrain contains metadata about the most recent drain operation
	LastDrain *DrainMetadata

	// Raft Indexes
	CreateIndex uint64
	ModifyIndex uint64
}



// NodeEvent is a single unit representing a node’s state change
type NodeEvent struct {
	Message     string
	Subsystem   string
	Details     map[string]string
	Timestamp   time.Time
	CreateIndex uint64
}

func (ne *NodeEvent) String() string {
	var details []string
	for k, v := range ne.Details {
		details = append(details, fmt.Sprintf("%s: %s", k, v))
	}

	return fmt.Sprintf("Message: %s, Subsystem: %s, Details: %s, Timestamp: %s", ne.Message, ne.Subsystem, strings.Join(details, ","), ne.Timestamp.String())
}

func (ne *NodeEvent) Copy() *NodeEvent {
	c := new(NodeEvent)
	*c = *ne
	c.Details = helper.CopyMapStringString(ne.Details)
	return c
}

// NewNodeEvent generates a new node event storing the current time as the
// timestamp
func NewNodeEvent() *NodeEvent {
	return &NodeEvent{Timestamp: time.Now()}
}


// SetMessage is used to set the message on the node event
func (ne *NodeEvent) SetMessage(msg string) *NodeEvent {
	ne.Message = msg
	return ne
}

// SetSubsystem is used to set the subsystem on the node event
func (ne *NodeEvent) SetSubsystem(sys string) *NodeEvent {
	ne.Subsystem = sys
	return ne
}

// SetTimestamp is used to set the timestamp on the node event
func (ne *NodeEvent) SetTimestamp(ts time.Time) *NodeEvent {
	ne.Timestamp = ts
	return ne
}

// AddDetail is used to add a detail to the node event
func (ne *NodeEvent) AddDetail(k, v string) *NodeEvent {
	if ne.Details == nil {
		ne.Details = make(map[string]string, 1)
	}
	ne.Details[k] = v
	return ne
}

// Sanitize returns a copy of the Node omitting confidential fields
// It only returns a copy if the Node contains the confidential fields
func (n *Node) Sanitize() *Node {
	if n == nil {
		return nil
	}
	if n.SecretID == "" {
		return n
	}
	clean := n.Copy()
	clean.SecretID = ""
	return clean
}


func (n *Node) Copy() *Node {
	if n == nil {
		return nil
	}
	nn := new(Node)
	*nn = *n
	nn.Attributes = helper.CopyMapStringString(nn.Attributes)
	nn.Links = helper.CopyMapStringString(nn.Links)
	nn.Meta = helper.CopyMapStringString(nn.Meta)
	nn.DrainStrategy = nn.DrainStrategy.Copy()
	nn.Events = copyNodeEvents(n.Events)
	nn.HostNetworks = copyNodeHostNetworks(n.HostNetworks)
	nn.LastDrain = nn.LastDrain.Copy()
	return nn
}


// DrainMetadata contains information about the most recent drain operation for a given Node.
type DrainMetadata struct {
	// StartedAt is the time that the drain operation started. This is equal to Node.DrainStrategy.StartedAt,
	// if it exists
	StartedAt time.Time

	// UpdatedAt is the time that that this struct was most recently updated, either via API action
	// or drain completion
	UpdatedAt time.Time

	// Status reflects the status of the drain operation.
	Status constant.DrainStatus

	// AccessorID is the accessor ID of the ACL token used in the most recent API operation against this drain
	AccessorID string

	// Meta includes the operator-submitted metadata about this drain operation
	Meta map[string]string
}

func (m *DrainMetadata) Copy() *DrainMetadata {
	if m == nil {
		return nil
	}
	c := new(DrainMetadata)
	*c = *m
	c.Meta = helper.CopyMapStringString(m.Meta)
	return c
}


// copyNodeEvents is a helper to copy a list of NodeEvent's
func copyNodeEvents(events []*NodeEvent) []*NodeEvent {
	l := len(events)
	if l == 0 {
		return nil
	}

	c := make([]*NodeEvent, l)
	for i, event := range events {
		c[i] = event.Copy()
	}
	return c
}

// copyNodeHostVolumes is a helper to copy a map of string to HostNetwork
func copyNodeHostNetworks(networks map[string]*ClientHostNetworkConfig) map[string]*ClientHostNetworkConfig {
	l := len(networks)
	if l == 0 {
		return nil
	}

	c := make(map[string]*ClientHostNetworkConfig, l)
	for network, v := range networks {
		c[network] = v.Copy()
	}

	return c
}

func (n *Node) Canonicalize() {
	if n == nil {
		return
	}

	// Ensure SchedulingEligibility is correctly set whenever draining so the plan applier and other scheduling logic
	// only need to check SchedulingEligibility when determining whether a placement is feasible on a node.
	if n.DrainStrategy != nil {
		n.SchedulingEligibility = constant.NodeSchedulingIneligible
	} else if n.SchedulingEligibility == "" {
		n.SchedulingEligibility = constant.NodeSchedulingEligible
	}
}



// DrainUpdate is used to update the drain of a node
type DrainUpdate struct {
	// DrainStrategy is the new strategy for the node
	DrainStrategy *DrainStrategy

	// MarkEligible marks the node as eligible if removing the drain strategy.
	MarkEligible bool
}


// NodeServerInfo is used to in NodeUpdateResponse to return Jobpool server
// information used in RPC server lists.
type NodeServerInfo struct {
	// RPCAdvertiseAddr is the IP endpoint that a Jobpool Server wishes to
	// be contacted at for RPCs.
	RPCAdvertiseAddr string

	// RpcMajorVersion is the major version number the Jobpool Server
	// supports
	RPCMajorVersion int32

	// RpcMinorVersion is the minor version number the Jobpool Server
	// supports
	RPCMinorVersion int32

	// Datacenter is the datacenter that a Jobpool server belongs to
	Datacenter string
}

// TerminalStatus returns if the current status is terminal and
// will no longer transition.
func (n *Node) TerminalStatus() bool {
	switch n.Status {
	case constant.NodeStatusDown:
		return true
	default:
		return false
	}
}


// ValidNodeStatus is used to check if a node status is valid
func ValidNodeStatus(status string) bool {
	switch status {
	case constant.NodeStatusInit, constant.NodeStatusReady, constant.NodeStatusDown, constant.NodeStatusDisconnected:
		return true
	default:
		return false
	}
}


// ShouldDrainNode checks if a given node status should trigger an
// evaluation. Some states don't require any further action.
func ShouldDrainNode(status string) bool {
	switch status {
	case constant.NodeStatusInit, constant.NodeStatusReady, constant.NodeStatusDisconnected:
		return false
	case constant.NodeStatusDown:
		return true
	default:
		panic(fmt.Sprintf("unhandled node status %s", status))
	}
}


// NodeStubFields defines which fields are included in the NodeListStub.
type NodeStubFields struct {
	Resources bool
	OS        bool
}


// Stub returns a summarized version of the node
func (n *Node) Stub(fields *NodeStubFields) *NodeListStub {

	addr, _, _ := net.SplitHostPort(n.HTTPAddr)

	s := &NodeListStub{
		Address:               addr,
		ID:                    n.ID,
		Datacenter:            n.Datacenter,
		Name:                  n.Name,
		NodeClass:             n.NodeClass,
		Version:               n.Attributes["jobpool.version"],
		Drain:                 n.DrainStrategy != nil,
		SchedulingEligibility: n.SchedulingEligibility,
		Status:                n.Status,
		StatusDescription:     n.StatusDescription,
		LastDrain:             n.LastDrain,
		CreateIndex:           n.CreateIndex,
		ModifyIndex:           n.ModifyIndex,
	}

	if fields != nil {

		// Fetch key attributes from the main Attributes map.
		if fields.OS {
			m := make(map[string]string)
			m["os.name"] = n.Attributes["os.name"]
			s.Attributes = m
		}
	}

	return s
}
// NodeListStub is used to return a subset of plan information
// for the plan list
type NodeListStub struct {
	Address               string
	ID                    string
	Attributes            map[string]string `json:",omitempty"`
	Datacenter            string
	Name                  string
	NodeClass             string
	Version               string
	Drain                 bool
	SchedulingEligibility string
	Status                string
	StatusDescription     string
	LastDrain             *DrainMetadata
	CreateIndex           uint64
	ModifyIndex           uint64
}

func (n *Node) Ready() bool {
	return n.Status == constant.NodeStatusReady && n.DrainStrategy == nil && n.SchedulingEligibility ==constant.NodeSchedulingEligible
}