package constant

const (
	EvalStatusBlocked   = "blocked"
	EvalStatusPending   = "pending"
	EvalStatusComplete  = "complete"
	EvalStatusFailed    = "failed"
	EvalStatusCancelled = "canceled"
)

const (
	QueueNameFailed = "_failed"
)


const (
	EvalTriggerJobRegister          = "job-register"
	EvalTriggerJobDeregister        = "job-deregister"
	EvalTriggerPeriodicJob          = "periodic-job"
	EvalTriggerNodeDrain            = "node-drain"
	EvalTriggerNodeUpdate           = "node-update"
	EvalTriggerAllocStop            = "alloc-stop"
	EvalTriggerScheduled            = "scheduled"
	EvalTriggerRollingUpdate        = "rolling-update"
	EvalTriggerDeploymentWatcher    = "deployment-watcher"
	EvalTriggerFailedFollowUp       = "failed-follow-up"
	EvalTriggerMaxPlans             = "max-plan-attempts"
	EvalTriggerRetryFailedAlloc     = "alloc-failure"
	EvalTriggerQueuedAllocs         = "queued-allocs"
	EvalTriggerPreemption           = "preemption"
	EvalTriggerScaling              = "job-scaling"
	EvalTriggerMaxDisconnectTimeout = "max-disconnect-timeout"
	EvalTriggerReconnect            = "reconnect"
)
