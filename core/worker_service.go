package core

import (
	"context"
	"fmt"
	"github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	"strings"
	"sync"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/scheduler"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper/uuid"
)

const (
	// backoffBaselineFast is the baseline time for exponential backoff
	backoffBaselineFast = 20 * time.Millisecond

	// backoffBaselineSlow is the baseline time for exponential backoff
	// but that is much slower than backoffBaselineFast
	backoffBaselineSlow = 500 * time.Millisecond

	// backoffLimitSlow is the limit of the exponential backoff for
	// the slower backoff
	backoffLimitSlow = 10 * time.Second

	// backoffSchedulerVersionMismatch is the backoff between retries when the
	// scheduler version mismatches that of the leader.
	backoffSchedulerVersionMismatch = 30 * time.Second

	// dequeueTimeout is used to timeout an evaluation dequeue so that
	// we can check if there is a shutdown event
	dequeueTimeout = 500 * time.Millisecond

	// raftSyncLimit is the limit of time we will wait for Raft replication
	// to catch up to the evaluation. This is used to fast Nack and
	// allow another scheduler to pick it up.
	raftSyncLimit = 5 * time.Second

	// dequeueErrGrace is the grace period where we don't log about
	// dequeue errors after start. This is to improve the user experience
	// in dev mode where the leader isn't elected for a few seconds.
	dequeueErrGrace = 10 * time.Second
)

type Worker struct {
	srv            *Server
	logger         log.Logger
	start          time.Time
	id             string
	status         WorkerStatus
	statusLock     sync.Mutex
	workloadStatus SchedulerWorkerStatus

	pauseFlag bool
	pauseLock sync.Mutex
	pauseCond *sync.Cond
	ctx       context.Context
	cancelFn  context.CancelFunc

	enabledSchedulers []string

	failures      uint
	evalToken     string
	snapshotIndex uint64
}

func NewWorker(ctx context.Context, srv *Server, args *structs.SchedulerWorkerPoolArgs) (*Worker, error) {
	w := newWorker(ctx, srv, args)
	w.Start()
	return w, nil
}

func newWorker(ctx context.Context, srv *Server, args *structs.SchedulerWorkerPoolArgs) *Worker {
	w := &Worker{
		id:                uuid.Generate(),
		srv:               srv,
		start:             time.Now(),
		status:            WorkerStarting,
		enabledSchedulers: make([]string, len(args.EnabledSchedulers)),
	}
	copy(w.enabledSchedulers, args.EnabledSchedulers)

	w.logger = srv.logger.ResetNamed("worker").With("worker_id", w.id)
	w.pauseCond = sync.NewCond(&w.pauseLock)
	w.ctx, w.cancelFn = context.WithCancel(ctx)

	return w
}

func (w *Worker) Start() {
	w.setStatus(WorkerStarting)
	go w.run()
}

// run is the long-lived goroutine which is used to run the worker
func (w *Worker) run() {
	defer func() {
		w.markStopped()
	}()
	w.setStatuses(WorkerStarted, WorkloadRunning)
	w.logger.Debug("running")
	for {
		// Check to see if the context has been cancelled. Server shutdown and Shutdown()
		// should do this.
		if w.workerShuttingDown() {
			return
		}
		// Dequeue a pending evaluation
		eval, token, waitIndex, shutdown := w.dequeueEvaluation(dequeueTimeout)
		if shutdown {
			return
		}

		// since dequeue takes time, we could have shutdown the server after getting an eval that
		// needs to be nacked before we exit. Explicitly checking the server to allow this eval
		// to be processed on worker shutdown.
		if w.srv.IsShutdown() {
			w.logger.Error("nacking eval because the server is shutting down", "eval", log.Fmt("%#v", eval))
			w.sendNack(eval, token)
			return
		}

		// Wait for the raft log to catchup to the evaluation
		w.setWorkloadStatus(WorkloadWaitingForRaft)
		snap, err := w.snapshotMinIndex(waitIndex, raftSyncLimit)
		if err != nil {
			w.logger.Error("error waiting for Raft index", "error", err, "index", waitIndex)
			w.sendNack(eval, token)
			continue
		}

		// Invoke the scheduler to determine placements
		w.setWorkloadStatus(WorkloadScheduling)
		if err := w.invokeScheduler(snap, eval, token); err != nil {
			w.logger.Error("error invoking scheduler", "error", err)
			w.sendNack(eval, token)
			continue
		}
		// Complete the evaluation
		w.sendAck(eval, token)
	}
}

// invokeScheduler is used to invoke the business logic of the scheduler
func (w *Worker) invokeScheduler(snap *state.StateSnapshot, eval *structs.Evaluation, token string) error {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "worker", "invoke_scheduler", eval.Type}, time.Now())

	w.evalToken = token
	// Store the snapshot's index
	var err error
	w.snapshotIndex, err = snap.LatestIndex()
	if err != nil {
		return fmt.Errorf("failed to determine snapshot's index: %v", err)
	}

	// Create the scheduler, or use the special core scheduler
	var sched scheduler.Scheduler
	if eval.Type == constant.PlanTypeCore {
		return nil
	} else {
		// TODO 按照type 创建scheduler
		sched, err = scheduler.NewScheduler(constant.PlanTypeService, w.logger, snap, w)
		if err != nil {
			return fmt.Errorf("failed to instantiate scheduler: %v", err)
		}
	}

	if eval.JobID == "" {
		return nil
	}

	// Process the evaluation
	err = sched.Process(eval)
	if err != nil {
		return fmt.Errorf("failed to process evaluation: %v", err)
	}
	return nil
}

// sendNack makes a best effort to nack the evaluation.
// Any errors are logged but swallowed.
func (w *Worker) sendNack(eval *structs.Evaluation, token string) {
	w.sendAcknowledgement(eval, token, false)
}

// sendAck makes a best effort to ack the evaluation.
// Any errors are logged but swallowed.
func (w *Worker) sendAck(eval *structs.Evaluation, token string) {
	w.sendAcknowledgement(eval, token, true)
}

// dequeueEvaluation is used to fetch the next ready evaluation.
// This blocks until an evaluation is available or a timeout is reached.
func (w *Worker) dequeueEvaluation(timeout time.Duration) (
	eval *structs.Evaluation, token string, waitIndex uint64, shutdown bool) {
	// Setup the request
	req := dto.EvalDequeueRequest{
		Schedulers: w.enabledSchedulers,
		Timeout:    timeout,
		WriteRequest: dto.WriteRequest{
			Region: w.srv.config.Region,
		},
	}
	var resp dto.EvalDequeueResponse

REQ:
	// Wait inside this function if the worker is paused.
	w.maybeWait()
	// Immediately check to see if the worker has been shutdown.
	if w.workerShuttingDown() {
		return nil, "", 0, true
	}

	// Make a blocking RPC
	start := time.Now()
	w.setWorkloadStatus(WorkloadWaitingToDequeue)
	err := w.srv.RPC("Eval.Dequeue", &req, &resp)
	metrics.MeasureSince([]string{constant.JobPoolName, "worker", "dequeue_eval"}, start)
	if err != nil {
		if time.Since(w.start) > dequeueErrGrace && !w.workerShuttingDown() {
			if w.shouldResubmit(err) {
				w.logger.Debug("failed to dequeue evaluation", "error", err)
			} else {
				w.logger.Error("failed to dequeue evaluation", "error", err)
			}
		}

		// Adjust the backoff based on the error. If it is a scheduler version
		// mismatch we increase the baseline.
		base, limit := backoffBaselineFast, backoffLimitSlow
		if strings.Contains(err.Error(), "calling scheduler version") {
			base = backoffSchedulerVersionMismatch
			limit = backoffSchedulerVersionMismatch
		}

		if w.backoffErr(base, limit) {
			return nil, "", 0, true
		}
		goto REQ
	}
	w.backoffReset()

	// Check if we got a response
	if resp.Eval != nil {
		w.logger.Debug("dequeued evaluation", "eval_id", resp.Eval.ID, "type", resp.Eval.Type, "namespace", resp.Eval.Namespace, "plan_id", resp.Eval.PlanID, "node_id", resp.Eval.NodeID, "triggered_by", resp.Eval.TriggeredBy)
		return resp.Eval, resp.Token, resp.GetWaitIndex(), false
	}

	goto REQ
}

// sendAcknowledgement should not be called directly. Call `sendAck` or `sendNack` instead.
// This function implements `ack`ing or `nack`ing the evaluation generally.
// Any errors are logged but swallowed.
func (w *Worker) sendAcknowledgement(eval *structs.Evaluation, token string, ack bool) {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "worker", "send_ack"}, time.Now())
	// Setup the request
	req := dto.EvalAckRequest{
		EvalID: eval.ID,
		JobID:  eval.JobID,
		Token:  token,
		WriteRequest: dto.WriteRequest{
			Region: w.srv.config.Region,
		},
	}
	var resp dto.GenericResponse

	// Determine if this is an Ack or Nack
	verb := "ack"
	endpoint := "Eval.Ack"
	if !ack {
		verb = "nack"
		endpoint = "Eval.Nack"
	}

	// Make the RPC call
	err := w.srv.RPC(endpoint, &req, &resp)
	if err != nil {
		w.logger.Error(fmt.Sprintf("failed to %s evaluation", verb), "eval_id", eval.ID, "error", err)
	} else {
		w.logger.Debug(fmt.Sprintf("%s evaluation", verb), "eval_id", eval.ID, "type", eval.Type, "namespace", eval.Namespace, "plan_id", eval.PlanID, "node_id", eval.NodeID, "triggered_by", eval.TriggeredBy)
	}
}

// snapshotMinIndex times calls to StateStore.SnapshotAfter which may block.
func (w *Worker) snapshotMinIndex(waitIndex uint64, timeout time.Duration) (*state.StateSnapshot, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	snap, err := w.srv.fsm.State().SnapshotMinIndex(ctx, waitIndex)
	cancel()
	metrics.MeasureSince([]string{constant.JobPoolName, "worker", "wait_for_index"}, start)

	// Wrap error to ensure callers don't disregard timeouts.
	if err == context.DeadlineExceeded {
		err = fmt.Errorf("timed out after %s waiting for index=%d", timeout, waitIndex)
	}

	return snap, err
}

func (w *Worker) setStatus(newStatus WorkerStatus) {
	w.statusLock.Lock()
	defer w.statusLock.Unlock()
	w.setWorkerStatusLocked(newStatus)
}

func (w *Worker) setWorkerStatusLocked(newStatus WorkerStatus) {
	if newStatus == w.status {
		return
	}
	w.logger.Trace("changed worker status", "from", w.status, "to", newStatus)
	w.status = newStatus
}

func (w *Worker) markStopped() {
	w.setStatuses(WorkerStopped, WorkloadStopped)
	w.logger.Debug("stopped")
}

func (w *Worker) workerShuttingDown() bool {
	select {
	case <-w.ctx.Done():
		return true
	default:
		return false
	}
}

func (w *Worker) setWorkloadStatusLocked(newStatus SchedulerWorkerStatus) {
	if newStatus == w.workloadStatus {
		return
	}
	w.logger.Trace("changed workload status", "from", w.workloadStatus, "to", newStatus)
	w.workloadStatus = newStatus
}

// setStatuses is used internally to the worker to update the
// status of the worker and workload at one time, since some
// transitions need to update both values using the same lock.
func (w *Worker) setStatuses(newWorkerStatus WorkerStatus, newWorkloadStatus SchedulerWorkerStatus) {
	w.statusLock.Lock()
	defer w.statusLock.Unlock()
	w.setWorkerStatusLocked(newWorkerStatus)
	w.setWorkloadStatusLocked(newWorkloadStatus)
}

func (w *Worker) setWorkloadStatus(newStatus SchedulerWorkerStatus) {
	w.statusLock.Lock()
	defer w.statusLock.Unlock()
	w.setWorkloadStatusLocked(newStatus)
}

func (w *Worker) maybeWait() {
	w.pauseLock.Lock()
	defer w.pauseLock.Unlock()

	if !w.pauseFlag {
		return
	}

	w.statusLock.Lock()
	w.status = WorkerPaused
	originalWorkloadStatus := w.workloadStatus
	w.workloadStatus = WorkloadPaused
	w.logger.Trace("changed workload status", "from", originalWorkloadStatus, "to", w.workloadStatus)

	w.statusLock.Unlock()

	for w.pauseFlag {
		w.pauseCond.Wait()
	}

	w.statusLock.Lock()

	w.logger.Trace("changed workload status", "from", w.workloadStatus, "to", originalWorkloadStatus)
	w.workloadStatus = originalWorkloadStatus

	// only reset the worker status if the worker is not resuming to stop the paused workload.
	if w.status != WorkerStopping {
		w.logger.Trace("changed worker status", "from", w.status, "to", WorkerStarted)
		w.status = WorkerStarted
	}
	w.statusLock.Unlock()
}

type WorkerStatus int

const (
	WorkerUnknownStatus WorkerStatus = iota // Unknown
	WorkerStarting
	WorkerStarted
	WorkerPausing
	WorkerPaused
	WorkerResuming
	WorkerStopping
	WorkerStopped
)

type SchedulerWorkerStatus int

const (
	WorkloadUnknownStatus SchedulerWorkerStatus = iota
	WorkloadRunning
	WorkloadWaitingToDequeue
	WorkloadWaitingForRaft
	WorkloadScheduling
	WorkloadSubmitting
	WorkloadBackoff
	WorkloadStopped
	WorkloadPaused
)

func (w *Worker) ID() string {
	return w.id
}

func (w *Worker) backoffErr(base, limit time.Duration) bool {
	w.setWorkloadStatus(WorkloadBackoff)
	backoff := (1 << (2 * w.failures)) * base
	if backoff > limit {
		backoff = limit
	} else {
		w.failures++
	}
	select {
	case <-time.After(backoff):
		return false
	case <-w.ctx.Done():
		return true
	}
}

// backoffReset is used to reset the failure count for
// exponential backoff
func (w *Worker) backoffReset() {
	w.failures = 0
}

// for business

func (w *Worker) CreateEval(eval *structs.Evaluation) error {
	// Check for a shutdown before plan submission. This server Shutdown state
	// instead of the worker's to prevent aborting work in flight.
	if w.srv.IsShutdown() {
		return fmt.Errorf("shutdown while planning")
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "worker", "create_eval"}, time.Now())

	// Store the snapshot index in the eval
	eval.SnapshotIndex = w.snapshotIndex

	// Setup the request
	req := dto.EvalUpdateRequest{
		Evals:     []*structs.Evaluation{eval},
		EvalToken: w.evalToken,
		WriteRequest: dto.WriteRequest{
			Region: w.srv.config.Region,
		},
	}
	var resp dto.GenericResponse

SUBMIT:
	// Make the RPC call
	if err := w.srv.RPC("Eval.Create", &req, &resp); err != nil {
		w.logger.Error("failed to create evaluation", "eval", log.Fmt("%#v", eval), "error", err)
		if w.shouldResubmit(err) && !w.backoffErr(backoffBaselineSlow, backoffLimitSlow) {
			goto SUBMIT
		}
		return err
	} else {
		w.logger.Debug("created evaluation", "eval", log.Fmt("%#v", eval))
		w.backoffReset()
	}
	return nil
}

func (w *Worker) UpdateEval(eval *structs.Evaluation) error {
	// Check for a shutdown before plan submission. Checking server state rather than
	// worker state to allow a workers work in flight to complete before stopping.
	if w.srv.IsShutdown() {
		return fmt.Errorf("shutdown while planning")
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "worker", "update_eval"}, time.Now())

	// Store the snapshot index in the eval
	eval.SnapshotIndex = w.snapshotIndex

	// Setup the request
	req := dto.EvalUpdateRequest{
		Evals:     []*structs.Evaluation{eval},
		EvalToken: w.evalToken,
		WriteRequest: dto.WriteRequest{
			Region: w.srv.config.Region,
		},
	}
	var resp dto.GenericResponse

SUBMIT:
	// Make the RPC call
	if err := w.srv.RPC("Eval.Update", &req, &resp); err != nil {
		w.logger.Error("failed to update evaluation", "eval", log.Fmt("%#v", eval), "error", err)
		if w.shouldResubmit(err) && !w.backoffErr(backoffBaselineSlow, backoffLimitSlow) {
			goto SUBMIT
		}
		return err
	} else {
		w.logger.Debug("updated evaluation", "eval", log.Fmt("%#v", eval))
		w.backoffReset()
	}
	return nil
}

func (w *Worker) ReblockEval(evaluation *structs.Evaluation) error {
	// TODO reblock eval
	return nil
}

func (w *Worker) SubmitPlan(plan *structs.PlanAlloc) (*structs.PlanResult, scheduler.State, error) {
	// Check for a shutdown before plan submission. Checking server state rather than
	// worker state to allow work in flight to complete before stopping.
	if w.srv.IsShutdown() {
		return nil, nil, fmt.Errorf("shutdown while planning")
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "worker", "submit_plan"}, time.Now())

	// Add the evaluation token to the plan
	plan.EvalToken = w.evalToken

	// Add SnapshotIndex to ensure leader's StateStore processes the Plan
	// at or after the index it was created.
	plan.SnapshotIndex = w.snapshotIndex

	// Normalize stopped and preempted allocs before RPC
	normalizePlan := ServersMeetMinimumVersion(w.srv.Members(), MinVersionPlanNormalization, true)
	if normalizePlan {
		plan.NormalizeAllocations()
	}

	// Setup the request
	req := dto.PlanAllocRequest{
		PlanAlloc: plan,
		WriteRequest: dto.WriteRequest{
			Region: w.srv.config.Region,
		},
	}
	var resp dto.PlanAllocResponse

SUBMIT:
	// Make the RPC call
	if err := w.srv.RPC("PlanAllocService.Submit", &req, &resp); err != nil {
		w.logger.Error("failed to submit plan for evaluation", "eval_id", plan.EvalID, "error", err)
		if w.shouldResubmit(err) && !w.backoffErr(backoffBaselineSlow, backoffLimitSlow) {
			goto SUBMIT
		}
		return nil, nil, err
	} else {
		w.logger.Debug("submitted plan for evaluation", "eval_id", plan.EvalID)
		w.backoffReset()
	}

	// Look for a result
	result := resp.Result
	if result == nil {
		return nil, nil, fmt.Errorf("missing result")
	}

	// Check if a state update is required. This could be required if we
	// planned based on stale data, which is causing issues. For example, a
	// node failure since the time we've started planning or conflicting task
	// allocations.
	var state scheduler.State
	if result.RefreshIndex != 0 {
		// Wait for the raft log to catchup to the evaluation
		w.logger.Debug("refreshing state", "refresh_index", result.RefreshIndex, "eval_id", plan.EvalID)

		var err error
		state, err = w.snapshotMinIndex(result.RefreshIndex, raftSyncLimit)
		if err != nil {
			return nil, nil, err
		}
	}

	// Return the result and potential state update
	return result, state, nil
}

func (w *Worker) shouldResubmit(err error) bool {
	s := err.Error()
	switch {
	case strings.Contains(s, "No cluster leader"):
		return true
	case strings.Contains(s, "plan queue is disabled"):
		return true
	default:
		return false
	}
}
