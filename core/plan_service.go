package core

import (
	"fmt"
	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"net/http"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/state/paginator"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
	"yunli.com/jobpool/helper/uuid"
)

type Plan struct {
	srv    *Server
	logger hclog.Logger
}

func NewPlanEndpoints(s *Server) *Plan {
	return &Plan{
		srv:    s,
		logger: s.logger.Named("plan"),
	}
	// TODO 分配规则检查
}

func (j *Plan) Add(args *dto.PlanAddRequest, reply *dto.PlanAddResponse) error {
	if done, err := j.srv.forward("Plan.Add", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "plan", "register"}, time.Now())

	// Validate the arguments - 更新不需要校验这些参数
	if args.ID == "" {
		args.ID = uuid.Generate()
		if args.Name == "" {
			return fmt.Errorf("missing plan name for add")
		}
		if args.PlanAddDto.Parameters == "" {
			return fmt.Errorf("missing plan parameters for add")
		}
		if args.Priority == 0 {
			// 默认为50
			args.Priority = 50
		}

		// defensive check; http layer and RPC requester should ensure namespaces are set consistently
		//if args.RequestNamespace() != args.PlanAddDto.NamespaceId {
		//	return fmt.Errorf("mismatched request namespace in request: %q, %q", args.RequestNamespace(), args.PlanAddDto.NamespaceId)
		//}
		if args.Type != "" {
			if constant.PlanTypeCore != args.Type && constant.PlanTypeService != args.Type {
				return structs.NewErr1002LimitValue("计划", fmt.Sprintf("%s,%s", constant.PlanTypeCore, constant.PlanTypeService))
			}
		} else {
			args.Type = constant.PlanTypeService
		}
		if args.BusinessType != "" {
			if !constant.PlanBusinessTypeSet[args.BusinessType] {
				return structs.NewErr1004Invalid("参数businessType", args.BusinessType)
			}
		}

		// 当 namespace 不存在的时候，先创建
		snap, err := j.srv.State().Snapshot()
		if err != nil {
			return err
		}
		ws := memdb.NewWatchSet()
		existingNamespace, err := snap.NamespaceByName(ws, args.PlanAddDto.NamespaceId)
		if err != nil {
			return err
		}
		if existingNamespace == nil {
			var raftArgs = dto.NamespaceUpsertRequest{
				Namespaces:   []*structs.Namespace{{Name: args.PlanAddDto.NamespaceId}},
				WriteRequest: args.WriteRequest,
			}
			fsmErr, _, err := j.srv.raftApply(constant.NamespaceUpsertRequestType, raftArgs)
			if err, ok := fsmErr.(error); ok && err != nil {
				j.logger.Error("namespace upsert failed", "error", err, "fsm", true)
				return err
			}
			if err != nil {
				j.logger.Error("namespace upsert failed", "error", err, "raft", true)
				return err
			}
		}
	}
	// If the plan is periodic or parameterized, we don't create an eval.
	//if !(args.PlanAddDto.IsPeriodic()) {
	//	// 非周期性任务
	//	j.logger.Warn("the plan is not periodic -------!!!")
	//	return structs.NewErr1001Blank("周期配置")
	//}

	// Lookup the plan
	snap, err := j.srv.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	existingPlan, err := snap.PlanByID(ws, args.RequestNamespace(), args.PlanAddDto.ID)
	if err != nil {
		return err
	}
	// Submit a multiregion plan to other regions (enterprise only).
	// The plan will have its region interpolated.
	var timeNow = xtime.NewFormatTime(time.Now())
	if existingPlan != nil {
		newVersion := existingPlan.Version + 1
		args.PlanAddDto.Version = newVersion
	} else {
		args.PlanAddDto.SubmitTime = timeNow
		args.PlanAddDto.CreateTime = timeNow
	}
	args.PlanAddDto.UpdateTime = timeNow

	// Commit this update via Raft
	fsmErr, index, err := j.srv.raftApply(constant.PlanRegisterRequestType, args)
	if err, ok := fsmErr.(error); ok && err != nil {
		j.logger.Error("registering plan failed", "error", err, "fsm", true)
		return err
	}
	if err != nil {
		j.logger.Error("registering plan failed", "error", err, "raft", true)
		return err
	}

	// Populate the reply with plan information
	reply.PlanModifyIndex = index
	reply.Index = index
	reply.ID = args.ID
	return nil
}

// 查询job
func (j *Plan) List(args *dto.PlanListRequest, reply *dto.PlanListResponse) error {
	if done, err := j.srv.forward("Plan.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "plan", "list"}, time.Now())
	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {
			// Capture all the plans
			var err error
			var iter memdb.ResultIterator
			if prefix := args.QueryOptions.Prefix; prefix != "" {
				iter, err = state.PlansByIDPrefix(ws, args.Namespace, prefix)
			} else {
				iter, err = state.Plans(ws)
			}
			if err != nil {
				return err
			}

			//filters := []paginator.Filter{
			//	paginator.NamespaceFilter{
			//		AllowableNamespaces: allowableNamespaces,
			//	},
			//}
			var jobs []*dto.PlanDto
			paginator, err := paginator.NewPaginator(iter, nil, args.QueryOptions,
				func(raw interface{}) error {
					plan := raw.(*structs.Plan)
					jobDto := &dto.PlanDto{
						Plan: plan.Copy(),
					}
					// launch id
					launch, err := state.PeriodicLaunchByID(ws, args.Namespace, plan.ID)
					if err == nil && launch != nil {
						jobDto.LaunchTime = xtime.NewFormatTime(launch.Launch)
					}

					jobs = append(jobs, jobDto)
					return nil
				})
			if err != nil {
				return structs.NewErrRPCCodedf(
					http.StatusBadRequest, "failed to create result paginator: %v", err)
			}

			nextToken, totalElements, totalPages, err := paginator.Page()
			if err != nil {
				return structs.NewErrRPCCodedf(
					http.StatusBadRequest, "failed to read result page: %v", err)
			}

			reply.QueryMeta.NextToken = nextToken
			reply.Plans = jobs
			reply.TotalPages = totalPages
			reply.TotalElements = totalElements

			// Use the last index that affected the plans table or summary
			jindex, err := state.Index("plans")
			if err != nil {
				return err
			}
			reply.Index = jindex

			// Set the query response
			j.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return j.srv.blockingRPC(&opts)
}

// Deregister is used to remove a plan the cluster.
func (j *Plan) Deregister(args *dto.PlanDeregisterRequest, reply *dto.PlanDeregisterResponse) error {
	if done, err := j.srv.forward("Plan.Deregister", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "plan", "deregister"}, time.Now())
	// Validate the arguments
	if args.PlanID == "" {
		return structs.NewErr1001Blank("planID")
	}

	// Lookup the plan
	snap, err := j.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	plan, err := snap.PlanByID(ws, args.RequestNamespace(), args.PlanID)
	if err != nil {
		return err
	}
	if plan == nil {
		j.logger.Warn("unknown plan id", args.PlanID)
		return structs.NewErr1101None("计划", args.PlanID)
	}

	// Commit the plan update via Raft
	_, index, err := j.srv.raftApply(constant.PlanDeregisterRequestType, args)
	if err != nil {
		j.logger.Error("deregister failed", "error", err)
		return err
	}

	// Populate the reply with plan information
	reply.PlanID = args.PlanID
	reply.PlanModifyIndex = index
	reply.EvalCreateIndex = index
	reply.Index = index
	return nil
}

// online the plan
func (j *Plan) Online(args *dto.PlanOnlineOfflineRequest, reply *dto.PlanOnlineOfflineResponse) error {
	if done, err := j.srv.forward("Plan.Online", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "plan", "online"}, time.Now())
	// Validate the arguments
	if args.PlanID == "" {
		return fmt.Errorf("missing plan ID for online")
	}

	// Lookup the plan
	snap, err := j.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	plan, err := snap.PlanByID(ws, args.RequestNamespace(), args.PlanID)
	if err != nil {
		return err
	}
	if plan == nil {
		j.logger.Warn("unknown plan id", args.PlanID)
		return structs.NewErr1101None("计划", args.PlanID)
	}
	args.Status = constant.PlanStatusRunning
	if constant.PlanStatusRunning == plan.Status {
		j.logger.Warn("计划", args.PlanID, "该计划已运行中")
		reply.PlanID = args.PlanID
		reply.Status = args.Status
		return nil
	}
	// Commit the plan update via Raft
	_, index, err := j.srv.raftApply(constant.PlanUpdateStatusRequestType, args)
	if err != nil {
		j.logger.Error("online plan failed", "error", err)
		return err
	}

	reply.PlanID = args.PlanID
	reply.Status = args.Status
	reply.Index = index
	return nil
}

// offline the plan
func (j *Plan) Offline(args *dto.PlanOnlineOfflineRequest, reply *dto.PlanOnlineOfflineResponse) error {
	if done, err := j.srv.forward("Plan.Offline", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "plan", "offline"}, time.Now())
	// Validate the arguments
	if args.PlanID == "" {
		return fmt.Errorf("missing plan ID for online")
	}

	// Lookup the plan
	snap, err := j.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	plan, err := snap.PlanByID(ws, args.RequestNamespace(), args.PlanID)
	if err != nil {
		return err
	}
	if plan == nil {
		j.logger.Warn("unknown plan id", args.PlanID)
		return structs.NewErr1101None("计划", args.PlanID)
	}
	args.Status = constant.PlanStatusDead
	if constant.PlanStatusDead == plan.Status {
		// 重复下线不算错误，只算是一个操作失误，给一个日志警告
		j.logger.Warn("计划", args.PlanID, "该计划已下线")
		reply.PlanID = args.PlanID
		reply.Status = args.Status
		return nil
	}
	// Commit the plan update via Raft
	_, index, err := j.srv.raftApply(constant.PlanUpdateStatusRequestType, args)
	if err != nil {
		j.logger.Error("offline plan failed", "error", err)
		return err
	}

	reply.PlanID = args.PlanID
	reply.Status = args.Status
	reply.Index = index
	return nil
}

func (j *Plan) Detail(args *dto.PlanDetailRequest, reply *dto.PlanDetailResponse) error {
	if done, err := j.srv.forward("Plan.Detail", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "plan", "detail"}, time.Now())
	// Validate the arguments
	if args.PlanID == "" {
		return fmt.Errorf("missing plan ID for online")
	}

	// Lookup the plan
	snap, err := j.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	plan, err := snap.PlanByID(ws, args.RequestNamespace(), args.PlanID)
	if err != nil {
		return err
	}
	if plan == nil {
		j.logger.Warn("unknown plan id", args.PlanID)
		return structs.NewErr1101None("计划", args.PlanID)
	}
	reply.Plan = plan
	return nil
}
