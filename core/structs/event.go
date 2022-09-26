package structs

import (
	"yunli.com/jobpool/core/constant"
)

type EventStreamWrapper struct {
	Error *RpcError
	Event *EventJson
}

const (
	NodeEventSubsystemDrain     = "Drain"
	NodeEventSubsystemDriver    = "Driver"
	NodeEventSubsystemHeartbeat = "Heartbeat"
	NodeEventSubsystemCluster   = "Cluster"
	NodeEventSubsystemStorage   = "Storage"
)

// Event represents a change in Jobpools state.
type Event struct {
	// Topic represents the primary object for the event
	Topic constant.Topic

	// Type is a short string representing the reason for the event
	Type string

	// Key is the primary identifier of the Event, The involved objects ID
	Key string

	// Namespace is the namespace of the object, If the object is not namespace
	// aware (Node) it is left blank
	Namespace string

	// FilterKeys are a set of additional related keys that are used to include
	// events during filtering.
	FilterKeys []string

	// Index is the raft index that corresponds to the event
	Index uint64

	// Payload is the Event itself see state/events.go for a list of events
	Payload interface{}
}

// Events is a wrapper that contains a set of events for a given index.
type Events struct {
	Index  uint64
	Events []Event
}

// EventJson is a wrapper for a JSON object
type EventJson struct {
	Data []byte
}

func (j *EventJson) Copy() *EventJson {
	n := new(EventJson)
	*n = *j
	n.Data = make([]byte, len(j.Data))
	copy(n.Data, j.Data)
	return n
}

// NodeStreamEvent holds a newly updated Node
type NodeStreamEvent struct {
	Node *Node
}

