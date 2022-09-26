package dto

import (
	"yunli.com/jobpool/core/constant"
)

// EventStreamRequest is used to stream events from a servers EventBroker
type EventStreamRequest struct {
	Topics map[constant.Topic][]string
	Index  int

	QueryOptions
}
