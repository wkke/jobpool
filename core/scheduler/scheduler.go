package scheduler

import (
	"fmt"
	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
)

var SchedulerFactory = map[string]Factory{
	constant.PlanTypeService: NewServiceScheduler,
}

func NewScheduler(name string, logger log.Logger, state State, planner Planner) (Scheduler, error) {
	// Lookup the factory function
	factory, ok := SchedulerFactory[name]
	if !ok {
		return nil, fmt.Errorf("unknown scheduler '%s'", name)
	}
	// Instantiate the scheduler
	sched := factory(logger, state, planner)
	return sched, nil
}

type Scheduler interface {
	// Process is used to handle a new evaluation. The scheduler is free to
	// apply any logic necessary to make the task placements. The state and
	// planner will be provided prior to any invocations of process.
	Process(*structs.Evaluation) error
}

type Factory func(log.Logger, State, Planner) Scheduler

type State interface {
	PlanByID(ws memdb.WatchSet, namespace string, id string) (*structs.Plan, error)

	JobByID(ws memdb.WatchSet, namespace string, id string) (*structs.Job, error)

	AllocsByPlan(ws memdb.WatchSet, namespace string, id string, b bool) ([]*structs.Allocation, error)

	NodeByID(ws memdb.WatchSet, id string) (*structs.Node, error)

	Nodes(ws memdb.WatchSet) (memdb.ResultIterator, error)
}

type Planner interface {
	UpdateEval(eval *structs.Evaluation) error

	CreateEval(eval *structs.Evaluation) error

	SubmitPlan(alloc *structs.PlanAlloc) (*structs.PlanResult, State, error)

	ReblockEval(*structs.Evaluation) error
}

func NewServiceScheduler(logger log.Logger, state State, planner Planner) Scheduler {
	return NewGenericScheduler("service_scheduler", logger, state, planner)
}
