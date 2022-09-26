package constant

type MessageType uint8

// note: new raft message types need to be added to the end of this
// list of contents
const (
	NodeRegisterRequestType          MessageType = 0
	NodeDeregisterRequestType        MessageType = 1
	NodeUpdateStatusRequestType      MessageType = 2
	NodeUpdateDrainRequestType       MessageType = 3
	UpsertNodeEventsType             MessageType = 4
	BatchNodeUpdateDrainRequestType  MessageType = 6
	NodeBatchDeregisterRequestType   MessageType = 7
	ClusterMetadataRequestType       MessageType = 8
	EventSinkUpsertRequestType       MessageType = 9
	EventSinkDeleteRequestType       MessageType = 10
	BatchEventSinkUpdateProgressType MessageType = 11
	NamespaceUpsertRequestType       MessageType = 12
	NamespaceDeleteRequestType       MessageType = 13
	KvUpsertRequestType              MessageType = 14
	KvDeleteRequestType              MessageType = 15
	PlanRegisterRequestType          MessageType = 16
	PlanDeregisterRequestType        MessageType = 17
	PlanUpdateStatusRequestType      MessageType = 18
	AllocUpdateRequestType           MessageType = 19
	EvalUpdateRequestType            MessageType = 20
	ApplyPlanResultsRequestType      MessageType = 21
	JobRegisterRequestType           MessageType = 30
	JobUpdateStatusRequestType       MessageType = 31
	JobDeleteRequestType             MessageType = 33
	AllocClientUpdateRequestType     MessageType = 35
	// the alloc is not implement
	AllocUpdateDesiredTransitionRequestType MessageType = 99
)
