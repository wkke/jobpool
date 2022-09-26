package core

import (
	"fmt"
	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-memdb"
	"math/rand"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
	"yunli.com/jobpool/helper/uuid"
)

// 这个的实现现在很简单，后续需要结合策略和资源
func (s *Server) DispatchPlan(plan *structs.Plan) (*structs.Evaluation, error) {
	s.logger.Debug("将Plan发送给client或队列用于下一步执行----")
	job, fsmErr, index, err := s.createJobByPeriodic(plan, constant.JobStatusPending)
	if err, ok := fsmErr.(error); ok && err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	s.logger.Debug("the job id when create job finished", "jobid", job.ID)

	// raft apply eval only
	eval := &structs.Evaluation{
		ID:          uuid.Generate(),
		Namespace:   plan.Namespace,
		Priority:    plan.Priority,
		Type:        plan.Type,
		TriggeredBy: constant.EvalTriggerPeriodicJob,
		JobID:       job.ID,
		PlanID:      plan.ID,
		Status:      constant.EvalStatusPending,
	}
	timeNow := xtime.NewFormatTime(time.Now())
	plan.SubmitTime = timeNow
	eval.CreateTime = timeNow
	eval.UpdateTime = timeNow
	eval.CreateIndex = index
	eval.ModifyIndex = index
	// 将eval入db，然后worker中获取并写入Queue然后按照策略分配
	eval.PlanModifyIndex = index
	update := &dto.EvalUpdateRequest{
		Evals: []*structs.Evaluation{eval},
	}
	_, evalIndex, err := s.raftApply(constant.EvalUpdateRequestType, update)
	if err != nil {
		return nil, err
	}

	// Update its indexes.
	eval.CreateIndex = evalIndex
	eval.ModifyIndex = evalIndex
	return eval, nil
}

func taintedNodes(state *state.StateSnapshot, allocs []*structs.Allocation) (map[string]*structs.Node, error) {
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

func (s *Server) RunningChildren(plan *structs.Plan) (bool, error) {
	state, err := s.fsm.State().Snapshot()
	if err != nil {
		return false, err
	}
	ws := memdb.NewWatchSet()
	// prefix := fmt.Sprintf("%s____%s", plan.ID, constant.PeriodicLaunchSuffix)
	// s.logger.Info("fetch jobs by plan", "prefix", prefix)
	iter, err := state.JobsByPlanID(ws, plan.Namespace, plan.ID)
	if err != nil {
		return false, err
	}
	var jobInfo *structs.Job
	for i := iter.Next(); i != nil; i = iter.Next() {
		jobInfo = i.(*structs.Job)
		if !jobInfo.TerminalStatus() {
			return true, nil
		}
	}
	// There are no jobs or tasks that aren't terminal.
	return false, nil
}

// schedulePeriodic is used to do periodic plan dispatch while we are leader
func (s *Server) schedulePeriodic(stopCh chan struct{}) {
	// TODO do something like gc
	evalGC := time.NewTicker(s.config.EvalGCInterval)
	defer evalGC.Stop()
	getLatest := func() (uint64, bool) {
		snapshotIndex, err := s.fsm.State().LatestIndex()
		if err != nil {
			s.logger.Error("failed to determine state store's index", "error", err)
			return 0, false
		}

		return snapshotIndex, true
	}
	for {

		select {
		case <-stopCh:
			return
		case <-evalGC.C:
			if index, ok := getLatest(); ok {
				s.logger.Debug("the latest index : %d", index)
				// s.evalBroker.Enqueue(s.corePlanEval(structs.CorePlanEvalGC, index))
			}
		}
	}
}

func (s *Server) restorePeriodicDispatcher() error {
	logger := s.logger.Named("periodic")
	ws := memdb.NewWatchSet()
	iter, err := s.fsm.State().PlansByPeriodic(ws, true)
	if err != nil {
		return fmt.Errorf("failed to get periodic plans: %v", err)
	}

	now := time.Now()
	for i := iter.Next(); i != nil; i = iter.Next() {
		plan := i.(*structs.Plan)

		// We skip adding parameterized plans because they themselves aren't
		// tracked, only the dispatched children are.
		if plan.IsParameterized() {
			continue
		}

		if err := s.periodicDispatcher.Add(plan); err != nil {
			logger.Error("failed to add plan to periodic dispatcher", "error", err)
			continue
		}

		// We do not need to force run the plan since it isn't active.
		if !plan.IsPeriodicActive() {
			continue
		}

		// If the periodic plan has never been launched before, launch will hold
		// the time the periodic plan was added. Otherwise it has the last launch
		// time of the periodic plan.
		launch, err := s.fsm.State().PeriodicLaunchByID(ws, plan.Namespace, plan.ID)
		if err != nil {
			return fmt.Errorf("failed to get periodic launch time: %v", err)
		}
		if launch == nil {
			s.logger.Warn("no recorded periodic launch time","plan", plan.ID, "namespace",
				 plan.Namespace)
			// TODO 确认？这里应该是返回错误使其跑不起来，改为忽略 20220831
			continue
			//return fmt.Errorf("no recorded periodic launch time for plan %q in namespace %q",
			//	plan.ID, plan.Namespace)
		}

		// nextLaunch is the next launch that should occur.
		nextLaunch, err := plan.Periodic.Next(launch.Launch.In(plan.Periodic.GetLocation()))
		if err != nil {
			logger.Error("failed to determine next periodic launch for plan", "plan", plan.NamespacedID(), "error", err)
			continue
		}

		// We skip force launching the plan if  there should be no next launch
		// (the zero case) or if the next launch time is in the future. If it is
		// in the future, it will be handled by the periodic dispatcher.
		if nextLaunch.IsZero() || !nextLaunch.Before(now) {
			continue
		}

		if _, err := s.periodicDispatcher.ForceRun(plan.Namespace, plan.ID); err != nil {
			logger.Error("force run of periodic plan failed", "plan", plan.NamespacedID(), "error", err)
			return fmt.Errorf("force run of periodic plan %q failed: %v", plan.NamespacedID(), err)
		}
		logger.Debug("periodic plan force runned during leadership establishment", "plan", plan.NamespacedID())
	}

	return nil
}

func (s *Server) getAllNodes() ([]*structs.Node, error) {
	// Scan the nodes
	ws := memdb.NewWatchSet()
	var out []*structs.Node
	iter, err := s.State().Nodes(ws)
	if err != nil {
		return nil, err
	}
	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		// Filter on datacenter and status
		node := raw.(*structs.Node)
		if !node.Ready() {
			continue
		}
		out = append(out, node)
	}
	return out, nil
}

func (s *Server) createJobByPeriodic(plan *structs.Plan, status string) (*structs.Job, interface{}, uint64, error) {
	job := &structs.Job{
		ID:            uuid.Generate(),
		PlanID:        plan.ParentID,
		DerivedPlanId: plan.ID,
		Name:          plan.Name,
		Type:          constant.JobTypeAuto,
		Namespace:     plan.Namespace,
		OperatorId:    plan.CreatorId,
		Timeout:       3600000,
		Status:        status,
		Parameters:    plan.Parameters,

		CreateTime: xtime.NewFormatTime(time.Now()),
		UpdateTime: xtime.NewFormatTime(time.Now()),
	}
	if constant.JobStatusSkipped == status && job.PlanID == "" {
		job.PlanID = plan.ID
	}
	req := dto.JobRegisterRequest{
		Job:  job,
		Plan: plan,
		WriteRequest: dto.WriteRequest{
			Namespace: job.Namespace,
		},
	}
	fsmErr, index, err := s.raftApply(constant.JobRegisterRequestType, req)
	return job, fsmErr, index, err
}

// 先随便取个node作为计算使用 (多客户端使用随机数选择)
func (s *Server) getRandomNodeForSelect(state *state.StateSnapshot) (*structs.Node, error) {
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
	if len(nodes) > 0 {
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

// 为该plan创建跳过任务记录
// 仅当周期配置为不允许并行时记录
// TODO 调用插件请求添加跳过计划任务
func (s *Server) SkipPlan(plan *structs.Plan) error {
	job, fsmErr, _, err := s.createJobByPeriodic(plan, constant.JobStatusSkipped)
	if err, ok := fsmErr.(error); ok && err != nil {
		s.logger.Warn("create job for skiped failed", "error", err)
		return err
	}
	if err != nil {
		s.logger.Warn("create job for skiped failed", "error", err)
		return err
	}
	s.jobDispatcher.DispatchSkipJob(plan.ID, job.ID, plan.Parameters)
	return nil
}

// 调度将在有效日期内自动生效，反之，在有效期外的任务将不会自动调度
func (s *Server) ExpirePlan(planRequest *structs.Plan) error {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "plan", "offline"}, time.Now())
	// Lookup the plan
	snap, err := s.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	plan, err := snap.PlanByID(ws, planRequest.Namespace, planRequest.ID)
	if err != nil {
		return err
	}
	if plan == nil {
		s.logger.Warn("unknown plan id", planRequest.ID)
		return structs.NewErr1101None("计划", planRequest.ID)
	}
	if constant.PlanStatusDead == plan.Status || constant.PlanStatusDead == planRequest.Status {
		return structs.NewErr1107FailExecute("计划", planRequest.ID, "该计划已下线")
	}
	// Commit the plan update via Raft
	args := &dto.PlanOnlineOfflineRequest{
		PlanID: planRequest.ID,
		Status: constant.PlanStatusDead,
		Reason: fmt.Sprintf("计划：[%s]调度有效时间[%s]已到，该计划自动过期", plan.Name, plan.Periodic.EndTime),
	}
	args.Namespace = planRequest.Namespace
	_, _, err = s.raftApply(constant.PlanUpdateStatusRequestType, args)
	if err != nil {
		s.logger.Error("offline plan failed", "error", err)
		return err
	}
	return nil
}
