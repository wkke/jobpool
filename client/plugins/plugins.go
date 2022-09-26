package plugins

type PluginRunner interface {
	// 同步任务
	StartSynchroJob(*JobRequest) (*JobResult, error)

	// 异步任务
	StartAsynchroJob(*JobRequest) (*JobResult, error)
}

type PluginJobDispatcher interface {
	// 跳过任务信息外发
	DispatchSkipJob(planId string, jobId string, parameter []byte) error

	// 无槽位失败任务信息外发
	DispatchNoSlotJob(planId string, jobId string, parameter []byte) error
}

type JobRequest struct {
	JobId   string
	Params  map[string]string
	Setting []byte
}

type JobResult struct {
	JobId   string
	State   string
	Message string
}
