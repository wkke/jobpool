package runner

import (
	"context"
	"fmt"
	log "github.com/hashicorp/go-hclog"
	"sync"
	"yunli.com/jobpool/client/config"
	cinterfaces "yunli.com/jobpool/client/interfaces"
	"yunli.com/jobpool/client/plugins"
	"yunli.com/jobpool/client/plugins/rest"
	"yunli.com/jobpool/client/service/runner/state"
	cstate "yunli.com/jobpool/client/state"
	cstructs "yunli.com/jobpool/client/structs"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
)

type RPCer interface {
	RPC(method string, args interface{}, reply interface{}) error
}

// allocRunner is used to run all the tasks in a given allocation
type allocRunner struct {
	// id is the ID of the allocation. Can be accessed without a lock
	id string

	// Logger is the logger for the alloc runner.
	logger log.Logger

	clientConfig *config.Config

	// stateUpdater is used to emit updated alloc state
	stateUpdater cinterfaces.AllocStateHandler

	// jobStateUpdatedCh is ticked whenever job state as changed. Must
	// have len==1 to allow nonblocking notification of state updates while
	// the goroutine is already processing a previous update.
	jobStateUpdatedCh chan struct{}

	// taskStateUpdateHandlerCh is closed when the task state handling
	// goroutine exits. It is unsafe to destroy the local allocation state
	// before this goroutine exits.
	taskStateUpdateHandlerCh chan struct{}

	// allocUpdatedCh is a channel that is used to stream allocation updates into
	// the allocUpdate handler. Must have len==1 to allow nonblocking notification
	// of new allocation updates while the goroutine is processing a previous
	// update.
	allocUpdatedCh chan *structs.Allocation

	// waitCh is closed when the Run loop has exited
	waitCh chan struct{}

	// destroyed is true when the Run loop has exited, postrun hooks have
	// run, and alloc runner has been destroyed. Must acquire destroyedLock
	// to access.
	destroyed bool

	// destroyCh is closed when the Run loop has exited, postrun hooks have
	// run, and alloc runner has been destroyed.
	destroyCh chan struct{}

	// shutdown is true when the Run loop has exited, and shutdown hooks have
	// run. Must acquire destroyedLock to access.
	shutdown bool

	// shutdownCh is closed when the Run loop has exited, and shutdown hooks
	// have run.
	shutdownCh chan struct{}

	// destroyLaunched is true if Destroy has been called. Must acquire
	// destroyedLock to access.
	destroyLaunched bool

	// shutdownLaunched is true if Shutdown has been called. Must acquire
	// destroyedLock to access.
	shutdownLaunched bool

	// destroyedLock guards destroyed, destroyLaunched, shutdownLaunched,
	// and serializes Shutdown/Destroy calls.
	destroyedLock sync.Mutex

	// Alloc captures the allocation being run.
	alloc     *structs.Allocation
	allocLock sync.RWMutex

	// state is the alloc runner's state
	state     *state.State
	stateLock sync.RWMutex

	stateDB cstate.StateDB

	flowName string

	// allocBroadcaster sends client allocation updates to all listeners
	allocBroadcaster *cstructs.AllocBroadcaster

	// serversContactedCh is passed to TaskRunners so they can detect when
	// servers have been contacted for the first time in case of a failed
	// restore.
	serversContactedCh chan struct{}

	shutdownDelayCtx      context.Context
	shutdownDelayCancelFn context.CancelFunc

	region string
	// rpcClient is the RPC Client that should be used by the allocrunner and its
	// hooks to communicate with Jobpool Servers.
	rpcClient RPCer

	runner plugins.PluginRunner
}

// NewAllocRunner returns a new allocation runner.
func NewAllocRunner(config *Config) (*allocRunner, error) {
	alloc := config.Alloc

	ar := &allocRunner{
		id:                       alloc.ID,
		alloc:                    alloc,
		clientConfig:             config.ClientConfig,
		waitCh:                   make(chan struct{}),
		destroyCh:                make(chan struct{}),
		shutdownCh:               make(chan struct{}),
		state:                    &state.State{},
		stateDB:                  config.StateDB,
		stateUpdater:             config.StateUpdater,
		jobStateUpdatedCh:        make(chan struct{}, 1),
		taskStateUpdateHandlerCh: make(chan struct{}),
		allocUpdatedCh:           make(chan *structs.Allocation, 1),
		serversContactedCh:       config.ServersContactedCh,
		region:                   config.Region,
		rpcClient:                config.RPCClient,
	}

	// Create the logger based on the allocation ID
	ar.logger = config.Logger.Named("alloc_runner").With("alloc_id", alloc.ID)

	// Create alloc broadcaster
	ar.allocBroadcaster = cstructs.NewAllocBroadcaster(ar.logger)

	shutdownDelayCtx, shutdownDelayCancel := context.WithCancel(context.Background())
	ar.shutdownDelayCtx = shutdownDelayCtx
	ar.shutdownDelayCancelFn = shutdownDelayCancel
	ar.runner = rest.NewPlugin(nil, ar.logger)
	return ar, nil
}

func (ar *allocRunner) Listener() *cstructs.AllocListener {
	return ar.allocBroadcaster.Listen()
}

func (ar *allocRunner) Alloc() *structs.Allocation {
	ar.allocLock.RLock()
	defer ar.allocLock.RUnlock()
	return ar.alloc
}

func (ar *allocRunner) AllocState() *state.State {
	return ar.state
}

func (ar *allocRunner) Shutdown() {
	panic("implement me")
}

func (ar *allocRunner) Run() {
	defer close(ar.waitCh)
	// 增加监听
	go ar.handleJobStateUpdates()

	go ar.handleAllocUpdates()

	// If task update chan has been closed, that means we've been shutdown.
	select {
	case <-ar.taskStateUpdateHandlerCh:
		return
	default:
	}

	if ar.shouldRun() {
		ar.logger.Debug("should run the job")
		// run之前需要确认这个job状态（避免同一个job多次执行的问题）
		jobStatus := ar.stateUpdater.GetAllocJobStatus(ar.alloc)
		if constant.JobStatusPending != jobStatus {
			ar.logger.Debug("the job status is not pending, won't run this job", "jobId", ar.alloc.JobId, "namespace", ar.alloc.Namespace, "status", jobStatus)
			return
		}

		jobRequest := &plugins.JobRequest{
			JobId:   ar.alloc.JobId,
			Params:  make(map[string]string),
			Setting: ar.alloc.Plan.Parameters,
		}
		var result *plugins.JobResult
		if "curl" == ar.alloc.Plan.BusinessType {
			jobResult, err := ar.runner.StartSynchroJob(jobRequest)
			if err != nil {
				ar.logger.Warn("run the job failed", "error", err)
			}
			result = jobResult
		} else {
			ar.logger.Debug("start run asyn job", "jobId", jobRequest.JobId, "jobrequest_setting", string(jobRequest.Setting))
			body := fmt.Sprintf(`{"planId": "%s", "jobId":"%s"}`, ar.alloc.PlanID, ar.alloc.JobId)
			jobRequest.Params["body"] = body
			jobResult, err := ar.runner.StartAsynchroJob(jobRequest)
			if err != nil {
				ar.logger.Warn("run the job failed", "error", err)
			}
			result = jobResult
		}
		ar.logger.Debug("the result is", "result", result)
		// storage state
		ar.stateLock.Lock()
		defer ar.stateLock.Unlock()
		ar.state.JobStatus = result.State
		ar.state.Info = result.Message
		ar.JobStateUpdated()
	} else {
		ar.logger.Debug("should not run the job")
	}
}

func (ar *allocRunner) WaitCh() <-chan struct{} {
	return ar.waitCh
}

func (ar *allocRunner) DestroyCh() <-chan struct{} {
	return ar.destroyCh
}

func (ar *allocRunner) ShutdownCh() <-chan struct{} {
	return ar.shutdownCh
}

func (ar *allocRunner) Reconnect(update *structs.Allocation) error {
	panic("implement me")
}

func (ar *allocRunner) shouldRun() bool {
	// Do not run allocs that are terminal
	if ar.Alloc().TerminalStatus() {
		ar.logger.Trace("alloc terminal; not running",
			"desired_status", ar.Alloc().DesiredStatus,
			"client_status", ar.Alloc().ClientStatus,
		)
		return false
	}

	// It's possible that the alloc local state was marked terminal before
	// the server copy of the alloc (checked above) was marked as terminal,
	// so check the local state as well.
	switch clientStatus := ar.AllocState().ClientStatus; clientStatus {
	case constant.AllocClientStatusComplete, constant.AllocClientStatusFailed,constant.AllocClientStatusExpired,constant.AllocClientStatusSkipped, constant.AllocClientStatusUnknown:
		ar.logger.Trace("alloc terminal; updating server and not running", "status", clientStatus)
		return false
	}
	return true
}

func (ar *allocRunner) handleAllocUpdates() {
	for {
		select {
		case update := <-ar.allocUpdatedCh:
			ar.handleAllocUpdate(update)
		case <-ar.waitCh:
			return
		}
	}
}

// This method sends the updated alloc to Run for serially processing updates.
// If there is already a pending update it will be discarded and replaced by
// the latest update.
func (ar *allocRunner) handleAllocUpdate(update *structs.Allocation) {
	// Detect Stop updates
	// stopping := !ar.Alloc().TerminalStatus() && update.TerminalStatus()

	// Update ar.alloc
	ar.setAlloc(update)

}

func (ar *allocRunner) setAlloc(updated *structs.Allocation) {
	ar.allocLock.Lock()
	ar.alloc = updated
	ar.allocLock.Unlock()
}

func (ar *allocRunner) Update(update *structs.Allocation) {
	select {
	// Drain queued update from the channel if possible, and check the modify
	// index
	case oldUpdate := <-ar.allocUpdatedCh:
		// If the old update is newer than the replacement, then skip the new one
		// and return. This case shouldn't happen, but may in the case of a bug
		// elsewhere inside the system.
		if oldUpdate.AllocModifyIndex > update.AllocModifyIndex {
			ar.logger.Debug("Discarding allocation update due to newer alloc revision in queue",
				"old_modify_index", oldUpdate.AllocModifyIndex,
				"new_modify_index", update.AllocModifyIndex)
			ar.allocUpdatedCh <- oldUpdate
			return
		} else {
			ar.logger.Debug("Discarding allocation update",
				"skipped_modify_index", oldUpdate.AllocModifyIndex,
				"new_modify_index", update.AllocModifyIndex)
		}
	case <-ar.waitCh:
		ar.logger.Trace("AllocRunner has terminated, skipping alloc update",
			"modify_index", update.AllocModifyIndex)
		return
	default:
	}

	// Queue the new update
	ar.allocUpdatedCh <- update
}

func (ar *allocRunner) JobStateUpdated() {
	select {
	case ar.jobStateUpdatedCh <- struct{}{}:
	default:
		// already pending updates
	}
}

func (ar *allocRunner) handleJobStateUpdates() {
	defer close(ar.taskStateUpdateHandlerCh)
	for done := false; !done; {
		select {
		case <-ar.jobStateUpdatedCh:
			ar.logger.Debug("receive jobStateUpdatedCh")
		case <-ar.waitCh:
			ar.logger.Debug("receive waitCH")
			// Run has exited, sync once more to ensure final
			// states are collected.
			done = true
		}

		// Task state has been updated; gather the state of the other tasks
		// trNum := len(ar.tasks)
		//trNum := 1
		//states := make(map[string]*structs.TaskState, trNum)
		//
		//if trNum > 0 {
		//	// Get the client allocation
		calloc := ar.clientAlloc()
		// Update the server
		ar.stateUpdater.AllocStateUpdated(calloc)
		//
		//	// Broadcast client alloc to listeners
		ar.allocBroadcaster.Send(calloc)
		//}
	}
}

func (ar *allocRunner) clientAlloc() *structs.Allocation {
	a := &structs.Allocation{
		ID: ar.id,
	}
	if ar.alloc != nil && ar.alloc.JobId != "" {
		a.JobId = ar.alloc.JobId
		a.Namespace = ar.alloc.Namespace
	}
	// Compute the ClientStatus
	if ar.state.ClientStatus != "" {
		// The client status is being forced
		a.ClientStatus, a.ClientDescription = ar.state.ClientStatus, ar.state.Info
	} else {
		a.ClientStatus, a.ClientDescription = getClientStatus(ar.state.JobStatus, ar.state.Info)
	}
	return a
}

func getClientStatus(taskStates string, info string) (status, description string) {
	var pending, running, dead, failed bool
	switch taskStates {
	case constant.JobStatusRunning:
		running = true
	case constant.JobStatusPending:
		pending = true
	case constant.JobStatusSkipped:
		dead = true
	case constant.JobStatusCancelled:
		dead = true
	case constant.JobStatusComplete:
		dead = true
	case constant.JobStatusFailed:
		failed = true
	}
	// Determine the alloc status
	if failed {
		if info == "" {
			return constant.AllocClientStatusFailed, "Failed jobs"
		} else {
			return constant.AllocClientStatusFailed, info
		}
	} else if running {
		return constant.AllocClientStatusRunning, "job are running"
	} else if pending {
		return constant.AllocClientStatusPending, "No job have started"
	} else if dead {
		return constant.AllocClientStatusComplete, "The job have completed"
	}

	// fmt.Println(fmt.Sprintf("status in tasks pending, running, dea, failed: %s, %s, %s, %s", pending, running, dead, failed))
	return "", ""
}
