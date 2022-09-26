package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	log "github.com/hashicorp/go-hclog"
	"strconv"
	"strings"
	"sync"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
)

// PlanEvalDispatcher is an interface to submit plans and have evaluations created
// for them.
type PlanEvalDispatcher interface {
	// DispatchPlan takes a Plan a new, untracked Plan and creates an evaluation
	// for it and returns the eval.
	DispatchPlan(plan *structs.Plan) (*structs.Evaluation, error)

	// RunningChildren returns whether the passed Plan has any running children.
	RunningChildren(plan *structs.Plan) (bool, error)

	// create skipped job and task to save this periodic
	SkipPlan(plan *structs.Plan) error

	// 调度将在有效日期内自动生效，反之，在有效期外的任务将不会自动调度
	ExpirePlan(plan *structs.Plan) error
}

type PeriodicDispatch struct {
	dispatcher PlanEvalDispatcher
	enabled    bool

	tracked map[structs.NamespacedID]*structs.Plan
	heap    *periodicHeap

	updateCh chan struct{}
	stopFn   context.CancelFunc
	logger   log.Logger
	l        sync.RWMutex
}

// NewPeriodicDispatch returns a periodic dispatcher that is used to track and
// launch periodic plans.
func NewPeriodicDispatch(logger log.Logger, dispatcher PlanEvalDispatcher) *PeriodicDispatch {
	return &PeriodicDispatch{
		dispatcher: dispatcher,
		tracked:    make(map[structs.NamespacedID]*structs.Plan),
		heap:       NewPeriodicHeap(),
		updateCh:   make(chan struct{}, 1),
		logger:     logger.Named("periodic"),
	}
}

func (p *PeriodicDispatch) SetEnabled(enabled bool) {
	p.l.Lock()
	defer p.l.Unlock()
	wasRunning := p.enabled
	p.enabled = enabled

	// If we are transitioning from enabled to disabled, stop the daemon and
	// flush.
	if !enabled && wasRunning {
		p.stopFn()
		p.flush()
	} else if enabled && !wasRunning {
		// If we are transitioning from disabled to enabled, run the daemon.
		ctx, cancel := context.WithCancel(context.Background())
		p.stopFn = cancel
		go p.run(ctx, p.updateCh)
	}
}

// run is a long-lived function that waits till a Plan's periodic spec is met and
// then creates an evaluation to run the Plan.
func (p *PeriodicDispatch) run(ctx context.Context, updateCh <-chan struct{}) {
	var launchCh <-chan time.Time
	for p.shouldRun() {
		plan, launch := p.nextLaunch()
		// get start time and end time and deal with the plan expired
		skipThisPlan := false
		if plan != nil {
			// plan的创建时间在启动时间之后，则不参与这次调度
			if plan.CreateTime.TimeValue().After(launch) {
				skipThisPlan = true
			} else if plan.Periodic.StartTime != "" && !plan.Periodic.StartTime.TimeValue().IsZero() {
				if time.Now().Before(plan.Periodic.StartTime.TimeValue()) {
					// 时间没到不需要调度
					skipThisPlan = true
				}
			}

			if launch.IsZero() {
				launchCh = nil
			} else {
				launchDur := launch.Sub(time.Now().In(plan.Periodic.GetLocation()))
				launchCh = time.After(launchDur)
				p.logger.Debug("scheduled periodic Plan launch", "launch_delay", launchDur, "Plan", plan.NamespacedID())
			}
		} else {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-updateCh:
			continue
		case <-launchCh:
			p.dispatchLaunch(plan, launch, skipThisPlan)
		}
	}
}

// periodicHeap wraps a heap and gives operations other than Push/Pop.
type periodicHeap struct {
	index map[structs.NamespacedID]*periodicPlan
	heap  periodicHeapImp
}

func (p *periodicHeap) Length() int {
	return len(p.heap)
}

func (p *periodicHeap) Push(plan *structs.Plan, next time.Time) error {
	tuple := structs.NamespacedID{
		ID:        plan.ID,
		Namespace: plan.Namespace,
	}
	if _, ok := p.index[tuple]; ok {
		return fmt.Errorf("Plan %q (%s) already exists", plan.ID, plan.Namespace)
	}

	pPlan := &periodicPlan{plan, next, 0}
	p.index[tuple] = pPlan
	heap.Push(&p.heap, pPlan)
	return nil
}

func (p *periodicHeap) Pop() *periodicPlan {
	if len(p.heap) == 0 {
		return nil
	}

	pPlan := heap.Pop(&p.heap).(*periodicPlan)
	tuple := structs.NamespacedID{
		ID:        pPlan.plan.ID,
		Namespace: pPlan.plan.Namespace,
	}
	delete(p.index, tuple)
	return pPlan
}

func (p *periodicHeap) Peek() *periodicPlan {
	if len(p.heap) == 0 {
		return nil
	}

	return p.heap[0]
}

func (p *periodicHeap) Contains(plan *structs.Plan) bool {
	tuple := structs.NamespacedID{
		ID:        plan.ID,
		Namespace: plan.Namespace,
	}
	_, ok := p.index[tuple]
	return ok
}

func (p *periodicHeap) Update(plan *structs.Plan, next time.Time) error {
	tuple := structs.NamespacedID{
		ID:        plan.ID,
		Namespace: plan.Namespace,
	}
	if pPlan, ok := p.index[tuple]; ok {
		// Need to update the Plan as well because its spec can change.
		pPlan.plan = plan
		pPlan.next = next
		heap.Fix(&p.heap, pPlan.index)
		return nil
	}

	return fmt.Errorf("heap doesn't contain Plan %q (%s)", plan.ID, plan.Namespace)
}

func (p *periodicHeap) Remove(plan *structs.Plan) error {
	tuple := structs.NamespacedID{
		ID:        plan.ID,
		Namespace: plan.Namespace,
	}
	if pPlan, ok := p.index[tuple]; ok {
		heap.Remove(&p.heap, pPlan.index)
		delete(p.index, tuple)
		return nil
	}

	return fmt.Errorf("heap doesn't contain Plan %q (%s)", plan.ID, plan.Namespace)
}

type periodicPlan struct {
	plan  *structs.Plan
	next  time.Time
	index int
}

func NewPeriodicHeap() *periodicHeap {
	return &periodicHeap{
		index: make(map[structs.NamespacedID]*periodicPlan),
		heap:  make(periodicHeapImp, 0),
	}
}

type periodicHeapImp []*periodicPlan

func (h periodicHeapImp) Len() int { return len(h) }

func (h periodicHeapImp) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	// Sort such that zero times are at the end of the list.
	iZero, jZero := h[i].next.IsZero(), h[j].next.IsZero()
	if iZero && jZero {
		return false
	} else if iZero {
		return false
	} else if jZero {
		return true
	}

	return h[i].next.Before(h[j].next)
}

func (h periodicHeapImp) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *periodicHeapImp) Push(x interface{}) {
	n := len(*h)
	plan := x.(*periodicPlan)
	plan.index = n
	*h = append(*h, plan)
}

func (h *periodicHeapImp) Pop() interface{} {
	old := *h
	n := len(old)
	plan := old[n-1]
	plan.index = -1 // for safety
	*h = old[0 : n-1]
	return plan
}

func (p *PeriodicDispatch) flush() {
	p.updateCh = make(chan struct{}, 1)
	p.tracked = make(map[structs.NamespacedID]*structs.Plan)
	p.heap = NewPeriodicHeap()
	p.stopFn = nil
}

// shouldRun returns whether the long lived run function should run.
func (p *PeriodicDispatch) shouldRun() bool {
	p.l.RLock()
	defer p.l.RUnlock()
	return p.enabled
}

// nextLaunch returns the next Plan to launch and when it should be launched. If
// the next Plan can't be determined, an error is returned. If the dispatcher is
// stopped, a nil Plan will be returned.
func (p *PeriodicDispatch) nextLaunch() (*structs.Plan, time.Time) {
	// If there is nothing wait for an update.
	p.l.RLock()
	defer p.l.RUnlock()
	if p.heap.Length() == 0 {
		return nil, time.Time{}
	}

	nextPlan := p.heap.Peek()
	if nextPlan == nil {
		return nil, time.Time{}
	}

	return nextPlan.plan, nextPlan.next
}

func (p *PeriodicDispatch) dispatchLaunch(plan *structs.Plan, launchTime time.Time, skipThisPlan bool) {
	p.l.Lock()
	nextLaunch, err := plan.Periodic.Next(launchTime)
	if err != nil {
		p.logger.Error("failed to parse next periodic launch", "Plan", plan.NamespacedID(), "error", err)
	}
	p.l.Unlock()
	// 下次调度需要看是否超出了endtime
	skipNextPlan := false
	if plan.Periodic.EndTime != "" && !plan.Periodic.EndTime.TimeValue().IsZero() {
		// TODO 优化点，应该知道下次过期就直接不要放heap里了，这里防止最后一个任务不运行才判断当前时间
		if nextLaunch.After(plan.Periodic.EndTime.TimeValue()) && time.Now().After(plan.Periodic.EndTime.TimeValue()) {
			skipNextPlan = true
			skipThisPlan = true
			err := p.dispatcher.ExpirePlan(plan)
			if err != nil {
				p.logger.Warn("the plan has expired and can't delete from periodic", "endtime", plan.Periodic.EndTime, "plan", plan)
			}
		}
	}
	if !skipNextPlan {
		p.l.Lock()
		if err := p.heap.Update(plan, nextLaunch); err != nil {
			p.logger.Error("failed to update next launch of periodic Plan", "Plan", plan.NamespacedID(), "error", err)
		}
		p.l.Unlock()
	}
	p.logger.Debug(" launching Plan", "Plan", plan.NamespacedID(), "launch_time", launchTime)
	if !skipThisPlan {
		p.dispatch(plan, launchTime)
	}
}

// dispatch creates an evaluation for the Plan and updates its next launchtime
// based on the passed launch time.
func (p *PeriodicDispatch) dispatch(plan *structs.Plan, launchTime time.Time) {
	p.l.Lock()
	// If the Plan prohibits overlapping and there are running children, we skip
	// the launch.
	if plan.Periodic.ProhibitOverlap {
		running, err := p.dispatcher.RunningChildren(plan)
		if err != nil {
			p.logger.Error("failed to determine if periodic Plan has running children", "Plan", plan.NamespacedID(), "error", err)
			p.l.Unlock()
			return
		}

		if running {
			// 创建job和task，状态均为跳过
			p.logger.Debug("skipping launch of periodic Plan because Plan prohibits overlap", "Plan", plan.NamespacedID())
			p.l.Unlock()
			err = p.dispatcher.SkipPlan(plan)
			if err != nil {
				p.logger.Error("create skipped plan failed", "plan", plan, "error", err)
				return
			}
			return
		}
	}
	p.l.Unlock()
	p.createEval(plan, launchTime)
}

// createEval instantiates a Plan based on the passed periodic Plan and submits an
// evaluation for it. This should not be called with the lock held.
func (p *PeriodicDispatch) createEval(periodicPlan *structs.Plan, time time.Time) (*structs.Evaluation, error) {
	derived, err := p.derivePlan(periodicPlan, time)
	if err != nil {
		return nil, err
	}

	eval, err := p.dispatcher.DispatchPlan(derived)
	if err != nil {
		p.logger.Error("failed to dispatch Plan", "Plan", periodicPlan.NamespacedID(), "error", err)
		return nil, err
	}

	return eval, nil
}

// derivePlan instantiates a new Plan based on the passed periodic Plan and the
// launch time.
func (p *PeriodicDispatch) derivePlan(periodicPlan *structs.Plan, time time.Time) (
	derived *structs.Plan, err error) {

	// Have to recover in case the Plan copy panics.
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("deriving child Plan from periodic Plan failed; deregistering from periodic runner",
				"Plan", periodicPlan.NamespacedID(), "error", r)

			p.Remove(periodicPlan.Namespace, periodicPlan.ID)
			derived = nil
			err = fmt.Errorf("Failed to create a copy of the periodic Plan %q (%s): %v",
				periodicPlan.ID, periodicPlan.Namespace, r)
		}
	}()

	// Create a copy of the periodic Plan, give it a derived ID/Name and make it
	// non-periodic in initial status
	derived = periodicPlan.Copy()
	derived.ParentID = periodicPlan.ID
	derived.ID = p.derivedPlanID(periodicPlan, time)
	derived.Name = periodicPlan.Name
	derived.Periodic = nil
	derived.Status = ""
	return
}

func (p *PeriodicDispatch) derivedPlanID(periodicPlan *structs.Plan, time time.Time) string {
	return fmt.Sprintf("%s%s%d", periodicPlan.ID, constant.PeriodicLaunchSuffix, time.Unix())
}

// Remove stops tracking the passed Plan. If the Plan is not tracked, it is a
// no-op.
func (p *PeriodicDispatch) Remove(namespace, planID string) error {
	p.l.Lock()
	defer p.l.Unlock()
	return p.removeLocked(structs.NamespacedID{
		ID:        planID,
		Namespace: namespace,
	})
}

// Remove stops tracking the passed Plan. If the Plan is not tracked, it is a
// no-op. It assumes this is called while a lock is held.
func (p *PeriodicDispatch) removeLocked(planID structs.NamespacedID) error {
	// Do nothing if not enabled
	if !p.enabled {
		return nil
	}

	plan, tracked := p.tracked[planID]
	if !tracked {
		return nil
	}

	delete(p.tracked, planID)
	if err := p.heap.Remove(plan); err != nil {
		return fmt.Errorf("failed to remove tracked Plan %q (%s): %v", planID.ID, planID.Namespace, err)
	}

	// Signal an update.
	select {
	case p.updateCh <- struct{}{}:
	default:
	}

	p.logger.Debug("deregistered periodic Plan", "Plan", plan.NamespacedID())
	return nil
}

func (p *PeriodicDispatch) ForceRun(namespace string, planID string) (*structs.Evaluation, error) {
	p.l.Lock()
	p.logger.Info("force run the Plan: %s", planID)
	// Do nothing if not enabled
	if !p.enabled {
		p.l.Unlock()
		return nil, fmt.Errorf("periodic dispatch disabled")
	}

	tuple := structs.NamespacedID{
		ID:        planID,
		Namespace: namespace,
	}
	plan, tracked := p.tracked[tuple]
	if !tracked {
		p.l.Unlock()
		return nil, fmt.Errorf("can't force run non-tracked Plan %q (%s)", planID, namespace)
	}

	p.l.Unlock()
	return p.createEval(plan, time.Now().In(plan.Periodic.GetLocation()))
}

func (p *PeriodicDispatch) Add(plan *structs.Plan) error {
	p.l.Lock()
	defer p.l.Unlock()

	// Do nothing if not enabled
	if !p.enabled {
		return nil
	}

	// If we were tracking a Plan and it has been disabled, made non-periodic,
	// stopped or is parameterized, remove it
	disabled := !plan.IsPeriodicActive()

	tuple := structs.NamespacedID{
		ID:        plan.ID,
		Namespace: plan.Namespace,
	}
	_, tracked := p.tracked[tuple]
	if disabled {
		if tracked {
			p.removeLocked(tuple)
		}

		// If the Plan is disabled and we aren't tracking it, do nothing.
		return nil
	}

	// Add or update the Plan.
	p.tracked[tuple] = plan
	next, err := plan.Periodic.Next(time.Now().In(plan.Periodic.GetLocation()))
	if err != nil {
		return fmt.Errorf("failed adding Plan %s: %v", plan.NamespacedID(), err)
	}
	if tracked {
		if err := p.heap.Update(plan, next); err != nil {
			return fmt.Errorf("failed to update Plan %q (%s) launch time: %v", plan.ID, plan.Namespace, err)
		}
		p.logger.Debug("updated periodic Plan", "Plan", plan.NamespacedID())
	} else {
		if err := p.heap.Push(plan, next); err != nil {
			return fmt.Errorf("failed to add Plan %v: %v", plan.ID, err)
		}
		p.logger.Debug("registered periodic Plan", "Plan", plan.NamespacedID())
	}

	// Signal an update.
	select {
	case p.updateCh <- struct{}{}:
	default:
	}

	return nil
}

func (p *PeriodicDispatch) LaunchTime(jobID string) (time.Time, error) {
	index := strings.LastIndex(jobID, constant.PeriodicLaunchSuffix)
	if index == -1 {
		return time.Time{}, fmt.Errorf("couldn't parse launch time from eval: %v", jobID)
	}

	launch, err := strconv.Atoi(jobID[index+len(constant.PeriodicLaunchSuffix):])
	if err != nil {
		return time.Time{}, fmt.Errorf("couldn't parse launch time from eval: %v", jobID)
	}

	return time.Unix(int64(launch), 0), nil
}
