package scheduler

import (
	"fmt"
	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-multierror"
	"math/rand"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
	"yunli.com/jobpool/helper/uuid"
)

type GenericScheduler struct {
	logger   log.Logger
	eventsCh chan<- interface{}
	state    State
	planner  Planner
	batch    bool

	eval       *structs.Evaluation
	job        *structs.Job
	plan       *structs.Plan
	planAlloc  *structs.PlanAlloc
	planResult *structs.PlanResult
	ctx        *EvalContext
	// stack      *GenericStack

	// followUpEvals are evals with WaitUntil set, which are delayed until that time
	// before being rescheduled
	followUpEvals []*structs.Evaluation

	blocked      *structs.Evaluation
	queuedAllocs map[string]int
}

func NewGenericScheduler(name string, logger log.Logger, state State, planner Planner) *GenericScheduler {
	s := &GenericScheduler{
		logger:  logger.Named(name),
		state:   state,
		planner: planner,
		batch:   false,
	}
	return s
}

func (s *GenericScheduler) Process(eval *structs.Evaluation) (err error) {
	s.logger.Debug("----in scheduler process-----", "trigger by", eval.TriggeredBy)
	s.eval = eval
	s.logger = s.logger.With("eval_id", eval.ID, "plan_id", eval.PlanID, "namespace", eval.Namespace)

	progress := func() bool { return progressMade(s.planResult) }
	limit := 5
	if s.batch {
		// reset limit
		limit = 5
	}
	if err := retryMax(limit, s.processImpl, progress); err != nil {
		if statusErr, ok := err.(*StatusError); ok {
			// Scheduling was tried but made no forward progress so create a
			// blocked eval to retry once resources become available.
			var mErr multierror.Error
			if err := s.createBlockedEval(true); err != nil {
				mErr.Errors = append(mErr.Errors, err)
			}
			if err := setStatus(s.logger, s.planner, s.eval, nil, s.blocked,
				statusErr.EvalStatus, err.Error(), s.queuedAllocs); err != nil {
				mErr.Errors = append(mErr.Errors, err)
			}
			return mErr.ErrorOrNil()
		}
		return err
	}
	if s.eval.Status == constant.EvalStatusBlocked {
		e := s.ctx.Eligibility()
		newEval := s.eval.Copy()
		newEval.EscapedComputedClass = e.HasEscaped()
		newEval.ClassEligibility = e.GetClasses()
		// newEval.QuotaLimitReached = e.QuotaLimitReached()
		return s.planner.ReblockEval(newEval)
	}

	// Update the status to complete
	return setStatus(s.logger, s.planner, s.eval, nil, s.blocked,
		constant.EvalStatusComplete, "", s.queuedAllocs)
}

// 正常流程执行eval和alloc
func (s *GenericScheduler) processImpl() (bool, error) {
	// Lookup the Plan by ID
	var err error
	ws := memdb.NewWatchSet()

	s.job, err = s.state.JobByID(ws, s.eval.Namespace, s.eval.JobID)
	if err != nil {
		return false, fmt.Errorf("failed to get job %q: %v", s.eval.JobID, err)
	}
	if s.job == nil {
		return false, fmt.Errorf("get job by id empty, namespace:%q, jobId: %q", s.eval.Namespace, s.eval.JobID)
	}

	s.plan, err = s.state.PlanByID(ws, s.eval.Namespace, s.job.PlanID)
	if err != nil {
		return false, fmt.Errorf("failed to get plan %q: %v", s.eval.PlanID, err)
	}
	if s.plan == nil {
		return false, fmt.Errorf("get plan by id empty, namespace:%q, planId:  %q", s.eval.Namespace, s.job.PlanID)
	}
	numTaskGroups := 0
	stopped := s.plan.Stopped()
	if !stopped {
		numTaskGroups = 1
	}

	s.queuedAllocs = make(map[string]int, numTaskGroups)
	s.followUpEvals = nil

	// Create a planAlloc
	s.planAlloc = makePlanAllocByEvalAndPlan(s.eval, s.plan, s.job)

	// Create an evaluation context
	s.ctx = NewEvalContext(s.eventsCh, s.state, s.planAlloc, s.job, s.logger)

	// Compute the target job allocations
	if err := s.computeJobAllocs(); err != nil {
		s.logger.Error("failed to compute job allocations", "error", err)
		return false, err
	}

	// If there are failed allocations, we need to create a blocked evaluation
	// to place the failed allocations when resources become available. If the
	// current evaluation is already a blocked eval, we reuse it. If not, submit
	// a new eval to the planner in createBlockedEval. If rescheduling should
	// be delayed, do that instead.
	delayInstead := len(s.followUpEvals) > 0 && s.eval.WaitUntil.IsZero()

	if s.eval.Status != constant.EvalStatusBlocked && s.blocked == nil &&
		!delayInstead {
		if err := s.createBlockedEval(false); err != nil {
			s.logger.Error("failed to make blocked eval", "error", err)
			return false, err
		}
		s.logger.Debug("failed to place all allocations, blocked eval created", "blocked_eval_id", s.blocked.ID)
	}

	// If the plan is a no-op, we can bail. If AnnotatePlan is set submit the plan
	// anyways to get the annotations.
	if s.planAlloc.IsNoOp() && !s.eval.AnnotatePlan {
		return true, nil
	}

	// Create follow up evals for any delayed reschedule eligible allocations, except in
	// the case that this evaluation was already delayed.
	if delayInstead {
		for _, eval := range s.followUpEvals {
			eval.PreviousEval = s.eval.ID
			// TODO(preetha) this should be batching evals before inserting them
			if err := s.planner.CreateEval(eval); err != nil {
				s.logger.Error("failed to make next eval for rescheduling", "error", err)
				return false, err
			}
			s.logger.Debug("found reschedulable allocs, followup eval created", "followup_eval_id", eval.ID)
		}
	}

	// Submit the plan and store the results.
	result, newState, err := s.planner.SubmitPlan(s.planAlloc)
	s.planResult = result
	if err != nil {
		return false, err
	}

	// If we got a state refresh, try again since we have stale data
	if newState != nil {
		s.logger.Debug("refresh forced")
		s.state = newState
		return false, nil
	}

	// Try again if the plan was not fully committed, potential conflict
	fullCommit, expected, actual := result.FullCommit(s.planAlloc)
	if !fullCommit {
		s.logger.Debug("plan didn't fully commit", "attempted", expected, "placed", actual)
		if newState == nil {
			return false, fmt.Errorf("missing state refresh after partial commit")
		}
		return false, nil
	}

	// Success!
	return true, nil
}

func makePlanAllocByEvalAndPlan(eval *structs.Evaluation, plan *structs.Plan, job *structs.Job) *structs.PlanAlloc {
	p := &structs.PlanAlloc{
		EvalID:          eval.ID,
		Priority:        eval.Priority,
		Plan:            plan,
		JobId:           job.ID,
		NodeUpdate:      make(map[string][]*structs.Allocation),
		NodeAllocation:  make(map[string][]*structs.Allocation),
		NodePreemptions: make(map[string][]*structs.Allocation),
	}
	if plan != nil {
		p.AllAtOnce = plan.AllAtOnce
	}
	return p
}

func progressMade(result *structs.PlanResult) bool {
	return result != nil && (len(result.NodeUpdate) != 0 ||
		len(result.NodeAllocation) != 0)
}

func retryMax(max int, cb func() (bool, error), reset func() bool) error {
	attempts := 0
	for attempts < max {
		done, err := cb()
		if err != nil {
			return err
		}
		if done {
			return nil
		}

		// Check if we should reset the number attempts
		if reset != nil && reset() {
			fmt.Println(" retry chongzhi 0")
			attempts = 0
		} else {
			fmt.Println(fmt.Sprintf("retry times %d", attempts))
			attempts++
		}
	}
	return &StatusError{
		Err:        fmt.Errorf("maximum attempts reached (%d)", max),
		EvalStatus: constant.EvalStatusFailed,
	}
}

type StatusError struct {
	Err        error
	EvalStatus string
}

func (s *StatusError) Error() string {
	return s.Err.Error()
}

func (s *GenericScheduler) createBlockedEval(planFailure bool) error {
	e := s.ctx.Eligibility()
	escaped := e.HasEscaped()

	// Only store the eligible classes if the eval hasn't escaped.
	var classEligibility map[string]bool
	if !escaped {
		classEligibility = e.GetClasses()
	}

	ev := s.eval
	s.blocked = &structs.Evaluation{
		ID:                   uuid.Generate(),
		Namespace:            ev.Namespace,
		Priority:             ev.Priority,
		Type:                 ev.Type,
		TriggeredBy:          constant.EvalTriggerQueuedAllocs,
		JobID:                ev.JobID,
		PlanID:               ev.PlanID,
		PlanModifyIndex:      ev.PlanModifyIndex,
		Status:               constant.EvalStatusBlocked,
		PreviousEval:         ev.ID,
		ClassEligibility:     classEligibility,
		EscapedComputedClass: escaped,
		CreateTime:           xtime.NewFormatTime(time.Now()),
	}
	if planFailure {
		s.blocked.TriggeredBy = constant.EvalTriggerMaxPlans
		s.blocked.StatusDescription = "created reached max retry times"
	} else {
		s.blocked.StatusDescription = "created to place remaining allocations"
	}
	return s.planner.CreateEval(s.blocked)
}

// setStatus is used to update the status of the evaluation
func setStatus(logger log.Logger, planner Planner,
	eval, nextEval, spawnedBlocked *structs.Evaluation,
	status, desc string,
	queuedAllocs map[string]int) error {

	logger.Debug("setting eval status", "status", status)
	newEval := eval.Copy()
	newEval.Status = status
	newEval.StatusDescription = desc
	if nextEval != nil {
		newEval.NextEval = nextEval.ID
	}
	if spawnedBlocked != nil {
		newEval.BlockedEval = spawnedBlocked.ID
	}
	if queuedAllocs != nil {
		newEval.QueuedAllocations = queuedAllocs
	}
	return planner.UpdateEval(newEval)
}

// 核心
func (s *GenericScheduler) computeJobAllocs() error {
	ws := memdb.NewWatchSet()
	allocs, err := s.state.AllocsByPlan(ws, s.eval.Namespace, s.eval.PlanID, true)
	if err != nil {
		return fmt.Errorf("failed to get allocs for plan '%s': %v",
			s.eval.PlanID, err)
	}
	tainted, err := taintedNodes(s.state, allocs)
	if err != nil {
		return fmt.Errorf("failed to get tainted nodes for plan '%s': %v",
			s.eval.PlanID, err)
	}
	// Update the allocations which are in pending/running state on tainted
	// nodes to lost, but only if the scheduler has already marked them
	updateNonTerminalAllocsToLost(s.planAlloc, tainted, allocs)
	runNode, err := s.getRandomNodeForSelect(s.state)
	if err != nil {
		return fmt.Errorf("failed to fetch node for scheduler '%s': %v",
			s.eval.PlanID, err)
	}
	if runNode == nil {
		return structs.NewErr1107FailExecute("客户端选取", "随机", "无可用客户端")
	}
	s.logger.Trace("select running plan node ", "plan", s.plan, "planAlloc", s.planAlloc, "node", runNode)
	updateAlloc := &structs.Allocation{
		ID:           uuid.Generate(),
		Name:         s.plan.Name,
		Namespace:    s.plan.Namespace,
		EvalID:       s.eval.ID,
		PlanID:       s.plan.ID,
		Plan:         s.plan,
		JobId:        s.job.ID,
		NodeID:       runNode.ID,
		ClientStatus: constant.AllocClientStatusPending,
	}
	s.planAlloc.AppendAlloc(updateAlloc, s.planAlloc.Plan)
	return nil
}

func taintedNodes(state State, allocs []*structs.Allocation) (map[string]*structs.Node, error) {
	out := make(map[string]*structs.Node)
	for _, alloc := range allocs {
		if _, ok := out[alloc.NodeID]; ok {
			continue
		}
		ws := memdb.NewWatchSet()
		node, err := state.NodeByID(ws, alloc.NodeID)
		if err != nil {
			return nil, err
		}

		// If the node does not exist, we should migrate
		if node == nil {
			out[alloc.NodeID] = nil
			continue
		}
		if structs.ShouldDrainNode(node.Status) || node.DrainStrategy != nil {
			out[alloc.NodeID] = node
		}

		// Disconnected nodes are included in the tainted set so that their
		// MaxClientDisconnect configuration can be included in the
		// timeout calculation.
		if node.Status == constant.NodeStatusDisconnected {
			out[alloc.NodeID] = node
		}
	}
	return out, nil
}

// updateNonTerminalAllocsToLost updates the allocations which are in pending/running state
// on tainted node to lost, but only for allocs already DesiredStatus stop or evict
func updateNonTerminalAllocsToLost(plan *structs.PlanAlloc, tainted map[string]*structs.Node, allocs []*structs.Allocation) {
	for _, alloc := range allocs {
		node, ok := tainted[alloc.NodeID]
		if !ok {
			continue
		}

		// Only handle down nodes or nodes that are gone (node == nil)
		if node != nil && node.Status != constant.NodeStatusDown {
			continue
		}
		allocLost := "alloc is lost since its node is down"

		// If the alloc is already correctly marked lost, we're done
		if (alloc.DesiredStatus == constant.AllocDesiredStatusStop ||
			alloc.DesiredStatus == constant.AllocDesiredStatusEvict) &&
			(alloc.ClientStatus == constant.AllocClientStatusRunning ||
				alloc.ClientStatus == constant.AllocClientStatusPending) {
			plan.AppendStoppedAlloc(alloc, allocLost, constant.AllocClientStatusUnknown, "")
		}
	}
}

// 先随便取个node作为计算使用 (多客户端使用随机数选择)
func (s *GenericScheduler) getRandomNodeForSelect(state State) (*structs.Node, error) {
	ws := memdb.NewWatchSet()
	iter, err := state.Nodes(ws)
	if err != nil {
		return nil, err
	}
	var maxIndex uint64 = 0
	var nodeSelectedId string

	resp := make(map[string]*structs.Node, 64)
	var nodes []*structs.Node
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		node := raw.(*structs.Node)
		// node信息中有down状态和其他状态的不适合用于运行任务
		if node.Status != constant.NodeStatusReady {
			continue
		}
		nodes = append(nodes, node)
		resp[node.ID] = node
		if maxIndex < node.ModifyIndex {
			maxIndex = node.ModifyIndex
			nodeSelectedId = node.ID
		}
	}
	if len(nodes) == 1 {
		nodeSelectedId = nodes[0].ID
	} else if len(nodes) > 1 {
		// random
		num := rand.Intn(len(nodes) - 1)
		if num < len(nodes) && nodes[num] != nil {
			nodeSelectedId = nodes[num].ID
		}
	}

	if resp[nodeSelectedId] == nil {
		return nil, fmt.Errorf("no avaliable node exist")
	}
	return resp[nodeSelectedId], nil
}
