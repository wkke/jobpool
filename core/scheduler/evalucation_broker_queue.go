package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/scheduler/delayheap"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper/uuid"
)

type EvalBroker struct {
	enabled bool
	// 状态统计
	stats *structs.BrokerStats
	// 已在broker中的eval及分配次数
	evalMapCounter map[string]int

	planEvals map[structs.NamespacedID]string

	timeWait map[string]*time.Timer
	requeue  map[string]*structs.Evaluation
	unack    map[string]*unackEval
	// blocked tracks the blocked evaluations by JobID in a priority queue
	blocked map[structs.NamespacedID]PendingEvaluations
	// ready tracks the ready plans by scheduler in a priority queue
	ready               map[string]PendingEvaluations
	waiting             map[string]chan struct{}
	deliveryLimit       int
	initialNackDelay    time.Duration
	subsequentNackDelay time.Duration
	nackTimeout         time.Duration

	// delayedEvalCancelFunc is used to stop the long running go routine
	// that processes delayed evaluations
	delayedEvalCancelFunc context.CancelFunc

	// delayHeap is a heap used to track incoming evaluations that are
	// not eligible to enqueue until their WaitTime
	delayHeap *delayheap.DelayHeap

	// delayedEvalsUpdateCh is used to trigger notifications for updates
	// to the delayHeap
	delayedEvalsUpdateCh chan struct{}

	l sync.RWMutex
}

func NewEvalBroker(timeout, initialNackDelay, subsequentNackDelay time.Duration, deliveryLimit int) (*EvalBroker, error) {
	if timeout < 0 {
		return nil, fmt.Errorf("timeout cannot be negative")
	}
	b := &EvalBroker{
		enabled:              false,
		evalMapCounter:       make(map[string]int),
		planEvals:            make(map[structs.NamespacedID]string),
		blocked:              make(map[structs.NamespacedID]PendingEvaluations),
		ready:                make(map[string]PendingEvaluations),
		unack:                make(map[string]*unackEval),
		waiting:              make(map[string]chan struct{}),
		requeue:              make(map[string]*structs.Evaluation),
		timeWait:             make(map[string]*time.Timer),
		nackTimeout:          timeout,
		initialNackDelay:     initialNackDelay,
		deliveryLimit:        deliveryLimit,
		subsequentNackDelay:  subsequentNackDelay,
		stats:                new(structs.BrokerStats),
		delayHeap:            delayheap.NewDelayHeap(),
		delayedEvalsUpdateCh: make(chan struct{}, 1),
	}
	b.stats.ByScheduler = make(map[string]*structs.SchedulerStats)
	b.stats.DelayedEvals = make(map[string]*structs.Evaluation)
	return b, nil
}

type unackEval struct {
	Eval      *structs.Evaluation
	Token     string
	NackTimer *time.Timer
}

type PendingEvaluations []*structs.Evaluation

func (b *EvalBroker) SetEnabled(enabled bool) {
	b.l.Lock()
	defer b.l.Unlock()

	prevEnabled := b.enabled
	b.enabled = enabled
	if !prevEnabled && enabled {
		// start the go routine for delayed evals
		ctx, cancel := context.WithCancel(context.Background())
		b.delayedEvalCancelFunc = cancel
		go b.runDelayedEvalsWatcher(ctx, b.delayedEvalsUpdateCh)
	}

	if !enabled {
		b.flush()
	}
}

func (b *EvalBroker) Dequeue(schedulers []string, timeout time.Duration) (*structs.Evaluation, string, error) {
	var timeoutTimer *time.Timer
	var timeoutCh <-chan time.Time
SCAN:
	// Scan for work
	eval, token, err := b.scanForSchedulers(schedulers)
	if err != nil {
		if timeoutTimer != nil {
			timeoutTimer.Stop()
		}
		return nil, "", err
	}

	// Check if we have something
	if eval != nil {
		if timeoutTimer != nil {
			timeoutTimer.Stop()
		}
		return eval, token, nil
	}

	// Setup the timeout channel the first time around
	if timeoutTimer == nil && timeout != 0 {
		timeoutTimer = time.NewTimer(timeout)
		timeoutCh = timeoutTimer.C
	}

	// Block until we get work
	scan := b.waitForSchedulers(schedulers, timeoutCh)
	if scan {
		goto SCAN
	}
	return nil, "", nil
}

// Enqueue is used to enqueue a new evaluation
func (b *EvalBroker) Enqueue(eval *structs.Evaluation) {
	b.l.Lock()
	defer b.l.Unlock()
	b.processEnqueue(eval, "")
}

func (b *EvalBroker) EnqueueAll(evals map[*structs.Evaluation]string) {
	// The lock needs to be held until all evaluations are enqueued. This is so
	// that when Dequeue operations are unblocked they will pick the highest
	// priority evaluations.
	b.l.Lock()
	defer b.l.Unlock()
	for eval, token := range evals {
		b.processEnqueue(eval, token)
	}
}

func (b *EvalBroker) processEnqueue(eval *structs.Evaluation, token string) {
	// If we're not enabled, don't enable more queuing.
	if !b.enabled {
		return
	}

	// Check if already enqueued
	if _, ok := b.evalMapCounter[eval.ID]; ok {
		if token == "" {
			return
		}

		// If the token has been passed, the evaluation is being reblocked by
		// the scheduler and should be processed once the outstanding evaluation
		// is Acked or Nacked.
		if unack, ok := b.unack[eval.ID]; ok && unack.Token == token {
			b.requeue[token] = eval
		}
		return
	} else if b.enabled {
		b.evalMapCounter[eval.ID] = 0
	}
	// Check if we need to enforce a wait
	if eval.Wait > 0 {
		b.processWaitingEnqueue(eval)
		return
	}

	if !eval.WaitUntil.IsZero() {
		b.delayHeap.Push(&evalWrapper{eval}, eval.WaitUntil)
		b.stats.TotalWaiting += 1
		b.stats.DelayedEvals[eval.ID] = eval
		// Signal an update.
		select {
		case b.delayedEvalsUpdateCh <- struct{}{}:
		default:
		}
		return
	}
	b.enqueueLocked(eval, eval.Type)
}

// enqueueLocked is used to enqueue with the lock held
func (b *EvalBroker) enqueueLocked(eval *structs.Evaluation, queue string) {
	// Do nothing if not enabled
	if !b.enabled {
		return
	}
	// Check if there is an evaluation for this JobID pending
	namespacedID := structs.NamespacedID{
		ID:        eval.PlanID,
		Namespace: eval.Namespace,
	}
	pendingEval := b.planEvals[namespacedID]
	if pendingEval == "" {
		b.planEvals[namespacedID] = eval.ID
	} else if pendingEval != eval.ID {
		blocked := b.blocked[namespacedID]
		heap.Push(&blocked, eval)
		b.blocked[namespacedID] = blocked
		b.stats.TotalBlocked += 1
		return
	}

	// Find the pending by scheduler class
	pending, ok := b.ready[queue]
	if !ok {
		pending = make([]*structs.Evaluation, 0, 16)
		if _, ok := b.waiting[queue]; !ok {
			b.waiting[queue] = make(chan struct{}, 1)
		}
	}
	// Push onto the heap
	heap.Push(&pending, eval)
	b.ready[queue] = pending

	// Unblock any blocked dequeues
	b.stats.TotalReady += 1
	bySched, ok := b.stats.ByScheduler[queue]
	if !ok {
		bySched = &structs.SchedulerStats{}
		b.stats.ByScheduler[queue] = bySched
	}
	bySched.Ready += 1

	select {
	case b.waiting[queue] <- struct{}{}:
	default:
	}
}

// Len is for the sorting interface
func (p PendingEvaluations) Len() int {
	return len(p)
}

// Less is for the sorting interface. We flip the check
// so that the "min" in the min-heap is the element with the
// highest priority
func (p PendingEvaluations) Less(i, j int) bool {
	if p[i].PlanID != p[j].PlanID && p[i].Priority != p[j].Priority {
		return !(p[i].Priority < p[j].Priority)
	}
	return p[i].CreateIndex < p[j].CreateIndex
}

// Swap is for the sorting interface
func (p PendingEvaluations) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Push is used to add a new evaluation to the slice
func (p *PendingEvaluations) Push(e interface{}) {
	*p = append(*p, e.(*structs.Evaluation))
}

// Pop is used to remove an evaluation from the slice
func (p *PendingEvaluations) Pop() interface{} {
	n := len(*p)
	e := (*p)[n-1]
	(*p)[n-1] = nil
	*p = (*p)[:n-1]
	return e
}

// Pick is used to peek at the next element that would be popped
func (p PendingEvaluations) Peek() *structs.Evaluation {
	n := len(p)
	if n == 0 {
		return nil
	}
	return p[n-1]
}

// scanForSchedulers scans for work on any of the schedulers. The highest priority work
// is dequeued first. This may return nothing if there is no work waiting.
func (b *EvalBroker) scanForSchedulers(schedulers []string) (*structs.Evaluation, string, error) {
	b.l.Lock()
	defer b.l.Unlock()

	// Do nothing if not enabled
	if !b.enabled {
		return nil, "", fmt.Errorf("eval broker disabled")
	}

	// Scan for eligible work
	var eligibleSched []string
	var eligiblePriority int
	for _, sched := range schedulers {
		// Get the pending queue
		pending, ok := b.ready[sched]
		if !ok {
			continue
		}

		// Pick at the next item
		ready := pending.Peek()
		if ready == nil {
			continue
		}

		// Add to eligible if equal or greater priority
		if len(eligibleSched) == 0 || ready.Priority > eligiblePriority {
			eligibleSched = []string{sched}
			eligiblePriority = ready.Priority

		} else if eligiblePriority > ready.Priority {
			continue

		} else if eligiblePriority == ready.Priority {
			eligibleSched = append(eligibleSched, sched)
		}
	}

	// Determine behavior based on eligible work
	switch n := len(eligibleSched); n {
	case 0:
		// No work to do!
		return nil, "", nil

	case 1:
		// Only a single task, dequeue
		return b.dequeueForSched(eligibleSched[0])

	default:
		// Multiple tasks. We pick a random task so that we fairly
		// distribute work.
		offset := rand.Intn(n)
		return b.dequeueForSched(eligibleSched[offset])
	}
}

// dequeueForSched is used to dequeue the next work item for a given scheduler.
// This assumes locks are held and that this scheduler has work
func (b *EvalBroker) dequeueForSched(sched string) (*structs.Evaluation, string, error) {
	// Get the pending queue
	pending := b.ready[sched]
	raw := heap.Pop(&pending)
	b.ready[sched] = pending
	eval := raw.(*structs.Evaluation)

	// Generate a UUID for the token
	token := uuid.Generate()

	// Setup Nack timer
	nackTimer := time.AfterFunc(b.nackTimeout, func() {
		b.Nack(eval.ID, token)
	})

	// Add to the unack queue
	b.unack[eval.ID] = &unackEval{
		Eval:      eval,
		Token:     token,
		NackTimer: nackTimer,
	}

	// Increment the dequeue count
	b.evalMapCounter[eval.ID] += 1

	// Update the stats
	b.stats.TotalReady -= 1
	b.stats.TotalUnacked += 1
	bySched := b.stats.ByScheduler[sched]
	bySched.Ready -= 1
	bySched.Unacked += 1

	return eval, token, nil
}

// waitForSchedulers is used to wait for work on any of the scheduler or until a timeout.
// Returns if there is work waiting potentially.
func (b *EvalBroker) waitForSchedulers(schedulers []string, timeoutCh <-chan time.Time) bool {
	doneCh := make(chan struct{})
	readyCh := make(chan struct{}, 1)
	defer close(doneCh)

	// Start all the watchers
	b.l.Lock()
	for _, sched := range schedulers {
		waitCh, ok := b.waiting[sched]
		if !ok {
			waitCh = make(chan struct{}, 1)
			b.waiting[sched] = waitCh
		}

		// Start a goroutine that either waits for the waitCh on this scheduler
		// to unblock or for this waitForSchedulers call to return
		go func() {
			select {
			case <-waitCh:
				select {
				case readyCh <- struct{}{}:
				default:
				}
			case <-doneCh:
			}
		}()
	}
	b.l.Unlock()

	// Block until we have ready work and should scan, or until we timeout
	// and should not make an attempt to scan for work
	select {
	case <-readyCh:
		return true
	case <-timeoutCh:
		return false
	}
}

// Ack is used to positively acknowledge handling an evaluation
func (b *EvalBroker) Ack(evalID, token string) error {
	b.l.Lock()
	defer b.l.Unlock()

	// Always delete the requeued evaluation. Either the Ack is successful and
	// we requeue it or it isn't and we want to remove it.
	defer delete(b.requeue, token)

	// Lookup the unack'd eval
	unack, ok := b.unack[evalID]
	if !ok {
		return fmt.Errorf("Evaluation ID not found")
	}
	if unack.Token != token {
		return fmt.Errorf("Token does not match for Evaluation ID")
	}
	planId := unack.Eval.PlanID

	// Ensure we were able to stop the timer
	if !unack.NackTimer.Stop() {
		return fmt.Errorf("Evaluation ID Ack'd after Nack timer expiration")
	}

	// Update the stats
	b.stats.TotalUnacked -= 1
	queue := unack.Eval.Type
	if b.evalMapCounter[evalID] > b.deliveryLimit {
		queue = constant.QueueNameFailed
	}
	bySched := b.stats.ByScheduler[queue]
	bySched.Unacked -= 1

	// Cleanup
	delete(b.unack, evalID)
	delete(b.evalMapCounter, evalID)

	namespacedID := structs.NamespacedID{
		ID:        planId,
		Namespace: unack.Eval.Namespace,
	}
	delete(b.planEvals, namespacedID)

	// Check if there are any blocked evaluations
	if blocked := b.blocked[namespacedID]; len(blocked) != 0 {
		raw := heap.Pop(&blocked)
		if len(blocked) > 0 {
			b.blocked[namespacedID] = blocked
		} else {
			delete(b.blocked, namespacedID)
		}
		eval := raw.(*structs.Evaluation)
		b.stats.TotalBlocked -= 1
		b.enqueueLocked(eval, eval.Type)
	}

	// Re-enqueue the evaluation.
	if eval, ok := b.requeue[token]; ok {
		b.processEnqueue(eval, "")
	}

	return nil
}

// Nack is used to negatively acknowledge handling an evaluation
func (b *EvalBroker) Nack(evalID, token string) error {
	b.l.Lock()
	defer b.l.Unlock()

	// Always delete the requeued evaluation since the Nack means the requeue is
	// invalid.
	delete(b.requeue, token)

	// Lookup the unack'd eval
	unack, ok := b.unack[evalID]
	if !ok {
		return fmt.Errorf("Evaluation ID not found")
	}
	if unack.Token != token {
		return fmt.Errorf("Token does not match for Evaluation ID")
	}

	// Stop the timer, doesn't matter if we've missed it
	unack.NackTimer.Stop()

	// Cleanup
	delete(b.unack, evalID)

	// Update the stats
	b.stats.TotalUnacked -= 1
	bySched := b.stats.ByScheduler[unack.Eval.Type]
	bySched.Unacked -= 1

	// Check if we've hit the delivery limit, and re-enqueue
	// in the failedQueue
	if dequeues := b.evalMapCounter[evalID]; dequeues >= b.deliveryLimit {
		b.enqueueLocked(unack.Eval, constant.QueueNameFailed)
	} else {
		e := unack.Eval
		e.Wait = b.nackReenqueueDelay(e, dequeues)

		// See if there should be a delay before re-enqueuing
		if e.Wait > 0 {
			b.processWaitingEnqueue(e)
		} else {
			b.enqueueLocked(e, e.Type)
		}
	}

	return nil
}

func (b *EvalBroker) processWaitingEnqueue(eval *structs.Evaluation) {
	timer := time.AfterFunc(eval.Wait, func() {
		b.enqueueWaiting(eval)
	})
	b.timeWait[eval.ID] = timer
	b.stats.TotalWaiting += 1
}

// enqueueWaiting is used to enqueue a waiting evaluation
func (b *EvalBroker) enqueueWaiting(eval *structs.Evaluation) {
	b.l.Lock()
	defer b.l.Unlock()

	delete(b.timeWait, eval.ID)
	b.stats.TotalWaiting -= 1
	b.enqueueLocked(eval, eval.Type)
}

// nackReenqueueDelay is used to determine the delay that should be applied on
// the evaluation given the number of previous attempts
func (b *EvalBroker) nackReenqueueDelay(eval *structs.Evaluation, prevDequeues int) time.Duration {
	switch {
	case prevDequeues <= 0:
		return 0
	case prevDequeues == 1:
		return b.initialNackDelay
	default:
		// For each subsequent nack compound a delay
		return time.Duration(prevDequeues-1) * b.subsequentNackDelay
	}
}

// runDelayedEvalsWatcher is a long-lived function that waits till a time deadline is met for
// pending evaluations before enqueuing them
func (b *EvalBroker) runDelayedEvalsWatcher(ctx context.Context, updateCh <-chan struct{}) {
	var timerChannel <-chan time.Time
	var delayTimer *time.Timer
	for {
		eval, waitUntil := b.nextDelayedEval()
		if waitUntil.IsZero() {
			timerChannel = nil
		} else {
			launchDur := waitUntil.Sub(time.Now().UTC())
			if delayTimer == nil {
				delayTimer = time.NewTimer(launchDur)
			} else {
				delayTimer.Reset(launchDur)
			}
			timerChannel = delayTimer.C
		}

		select {
		case <-ctx.Done():
			return
		case <-timerChannel:
			// remove from the heap since we can enqueue it now
			b.l.Lock()
			b.delayHeap.Remove(&evalWrapper{eval})
			b.stats.TotalWaiting -= 1
			delete(b.stats.DelayedEvals, eval.ID)
			b.enqueueLocked(eval, eval.Type)
			b.l.Unlock()
		case <-updateCh:
			continue
		}
	}
}

// Outstanding checks if an EvalID has been delivered but not acknowledged
// and returns the associated token for the evaluation.
func (b *EvalBroker) Outstanding(evalID string) (string, bool) {
	b.l.RLock()
	defer b.l.RUnlock()
	unack, ok := b.unack[evalID]
	if !ok {
		return "", false
	}
	return unack.Token, true
}

func (b *EvalBroker) OutstandingReset(evalID, token string) error {
	b.l.RLock()
	defer b.l.RUnlock()
	unack, ok := b.unack[evalID]
	if !ok {
		return structs.ErrNotOutstanding
	}
	if unack.Token != token {
		return structs.ErrTokenMismatch
	}
	if !unack.NackTimer.Reset(b.nackTimeout) {
		return structs.ErrNackTimeoutReached
	}
	return nil
}

func (b *EvalBroker) nextDelayedEval() (*structs.Evaluation, time.Time) {
	b.l.RLock()
	defer b.l.RUnlock()

	// If there is nothing wait for an update.
	if b.delayHeap.Length() == 0 {
		return nil, time.Time{}
	}
	nextEval := b.delayHeap.Peek()
	if nextEval == nil {
		return nil, time.Time{}
	}
	eval := nextEval.Node.Data().(*structs.Evaluation)
	return eval, nextEval.WaitUntil
}

// Stats is used to query the state of the broker
func (b *EvalBroker) Stats() *structs.BrokerStats {
	// Allocate a new stats struct
	stats := new(structs.BrokerStats)
	stats.DelayedEvals = make(map[string]*structs.Evaluation)
	stats.ByScheduler = make(map[string]*structs.SchedulerStats)

	b.l.RLock()
	defer b.l.RUnlock()

	// Copy all the stats
	stats.TotalReady = b.stats.TotalReady
	stats.TotalUnacked = b.stats.TotalUnacked
	stats.TotalBlocked = b.stats.TotalBlocked
	stats.TotalWaiting = b.stats.TotalWaiting
	for id, eval := range b.stats.DelayedEvals {
		evalCopy := *eval
		stats.DelayedEvals[id] = &evalCopy
	}
	for sched, subStat := range b.stats.ByScheduler {
		subStatCopy := *subStat
		stats.ByScheduler[sched] = &subStatCopy
	}
	stats.TotalFailedQueue = len(b.ready[constant.QueueNameFailed])
	return stats
}

// PauseNackTimeout is used to pause the Nack timeout for an eval that is making
// progress but is in a potentially unbounded operation such as the plan queue.
func (b *EvalBroker) PauseNackTimeout(evalID, token string) error {
	b.l.RLock()
	defer b.l.RUnlock()
	unack, ok := b.unack[evalID]
	if !ok {
		return structs.ErrNotOutstanding
	}
	if unack.Token != token {
		return structs.ErrTokenMismatch
	}
	if !unack.NackTimer.Stop() {
		return structs.ErrNackTimeoutReached
	}
	return nil
}

// ResumeNackTimeout is used to resume the Nack timeout for an eval that was
// paused. It should be resumed after leaving an unbounded operation.
func (b *EvalBroker) ResumeNackTimeout(evalID, token string) error {
	b.l.Lock()
	defer b.l.Unlock()
	unack, ok := b.unack[evalID]
	if !ok {
		return structs.ErrNotOutstanding
	}
	if unack.Token != token {
		return structs.ErrTokenMismatch
	}
	unack.NackTimer.Reset(b.nackTimeout)
	return nil
}

// Flush is used to clear the state of the broker. It must be called from within
// the lock.
func (b *EvalBroker) flush() {
	// Unblock any waiters
	for _, waitCh := range b.waiting {
		close(waitCh)
	}
	b.waiting = make(map[string]chan struct{})

	// Cancel any Nack timers
	for _, unack := range b.unack {
		unack.NackTimer.Stop()
	}

	// Cancel any time wait evals
	for _, wait := range b.timeWait {
		wait.Stop()
	}

	// Cancel the delayed evaluations goroutine
	if b.delayedEvalCancelFunc != nil {
		b.delayedEvalCancelFunc()
	}

	// Clear out the update channel for delayed evaluations
	b.delayedEvalsUpdateCh = make(chan struct{}, 1)

	// Reset the broker
	b.stats.TotalReady = 0
	b.stats.TotalUnacked = 0
	b.stats.TotalBlocked = 0
	b.stats.TotalWaiting = 0
	b.stats.DelayedEvals = make(map[string]*structs.Evaluation)
	b.stats.ByScheduler = make(map[string]*structs.SchedulerStats)
	b.evalMapCounter = make(map[string]int)
	b.planEvals = make(map[structs.NamespacedID]string)
	b.blocked = make(map[structs.NamespacedID]PendingEvaluations)
	b.ready = make(map[string]PendingEvaluations)
	b.unack = make(map[string]*unackEval)
	b.timeWait = make(map[string]*time.Timer)
	b.delayHeap = delayheap.NewDelayHeap()
}

// evalWrapper satisfies the HeapNode interface
type evalWrapper struct {
	eval *structs.Evaluation
}

func (d *evalWrapper) Data() interface{} {
	return d.eval
}

func (d *evalWrapper) ID() string {
	return d.eval.ID
}

func (d *evalWrapper) Namespace() string {
	return d.eval.Namespace
}
