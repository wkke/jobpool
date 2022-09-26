package interfaces

import "yunli.com/jobpool/core/structs"

type Client interface {
	AllocStateHandler
}

// AllocStateHandler exposes a handler to be called when an allocation's state changes
type AllocStateHandler interface {
	// AllocStateUpdated is used to emit an updated allocation. This allocation
	// is stripped to only include client settable fields.
	AllocStateUpdated(alloc *structs.Allocation)

	// PutAllocation is used to persist an updated allocation in the local state store.
	PutAllocation(*structs.Allocation) error

	// 获取当前job的状态，用于阻止重复运行
	GetAllocJobStatus(*structs.Allocation) string
}

type JobStateHandler interface {
	JobStateUpdated()
}
