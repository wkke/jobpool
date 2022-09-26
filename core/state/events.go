package state

import (
	memdb "github.com/hashicorp/go-memdb"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
)

var MsgTypeEvents = map[constant.MessageType]string{
	constant.NodeRegisterRequestType:                 constant.TypeNodeRegistration,
	constant.NodeDeregisterRequestType:               constant.TypeNodeDeregistration,
	constant.UpsertNodeEventsType:                    constant.TypeNodeEvent,
	constant.NodeUpdateStatusRequestType:             constant.TypeNodeEvent,
	constant.NodeUpdateDrainRequestType:              constant.TypeNodeDrain,
	constant.BatchNodeUpdateDrainRequestType:         constant.TypeNodeDrain,
}

func eventsFromChanges(tx ReadTxn, changes Changes) *structs.Events {
	eventType, ok := MsgTypeEvents[changes.MsgType]
	if !ok {
		return nil
	}

	var events []structs.Event
	for _, change := range changes.Changes {
		if event, ok := eventFromChange(change); ok {
			event.Type = eventType
			event.Index = changes.Index
			events = append(events, event)
		}
	}

	return &structs.Events{Index: changes.Index, Events: events}
}

func eventFromChange(change memdb.Change) (structs.Event, bool) {
	if change.Deleted() {
		switch change.Table {
		case "nodes":
			before, ok := change.Before.(*structs.Node)
			if !ok {
				return structs.Event{}, false
			}

			before = before.Sanitize()
			return structs.Event{
				Topic: constant.TopicNode,
				Key:   before.ID,
				Payload: &structs.NodeStreamEvent{
					Node: before,
				},
			}, true
		}
		return structs.Event{}, false
	}

	switch change.Table {
	case "nodes":
		after, ok := change.After.(*structs.Node)
		if !ok {
			return structs.Event{}, false
		}

		after = after.Sanitize()
		return structs.Event{
			Topic: constant.TopicNode,
			Key:   after.ID,
			Payload: &structs.NodeStreamEvent{
				Node: after,
			},
		}, true
	}

	return structs.Event{}, false
}
