package scheduler

import (
	"fmt"
	log "github.com/hashicorp/go-hclog"
	"sync"
	"time"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper"
	"yunli.com/jobpool/helper/timetable"
)

type BlockedEvals struct {
	plans            map[structs.NamespacedID]string
	logger           log.Logger
	evalBroker       *EvalBroker
	enabled          bool
	duplicates       []*structs.Evaluation
	duplicateCh      chan struct{}
	capacityChangeCh chan *capacityUpdate
	// captured is the set of evaluations that are captured by computed node
	// classes.
	captured map[string]wrappedEval
	// escaped is the set of evaluations that have escaped computed node
	// classes.
	escaped map[string]wrappedEval

	unblockIndexes map[string]uint64
	stopCh         chan struct{}
	// timetable is used to correlate indexes with their insertion time. This
	// allows us to prune based on time.
	timetable *timetable.TimeTable
	stats     *structs.BlockedStats
	l         sync.RWMutex
}

func NewBlockedEvals(evalBroker *EvalBroker, logger log.Logger) *BlockedEvals {
	return &BlockedEvals{
		logger:           logger.Named("blocked_evals"),
		evalBroker:       evalBroker,
		plans:            make(map[structs.NamespacedID]string),
		captured:         make(map[string]wrappedEval),
		escaped:          make(map[string]wrappedEval),
		unblockIndexes:   make(map[string]uint64),
		capacityChangeCh: make(chan *capacityUpdate, 8000),
		duplicateCh:      make(chan struct{}, 1),
		stopCh:           make(chan struct{}),
		stats:            &structs.BlockedStats{},
	}
}

// capacityUpdate stores unblock data.
type capacityUpdate struct {
	computedClass string
	quotaChange   string
	index         uint64
}

type wrappedEval struct {
	eval  *structs.Evaluation
	token string
}

// SetEnabled is used to control if the blocked eval tracker is enabled. The
// tracker should only be enabled on the active leader.
func (b *BlockedEvals) SetEnabled(enabled bool) {
	b.l.Lock()
	if b.enabled == enabled {
		b.l.Unlock()
		return
	} else if enabled {
		go b.watchCapacity(b.stopCh, b.capacityChangeCh)
		go b.prune(b.stopCh)
	} else {
		close(b.stopCh)
	}
	b.enabled = enabled
	b.l.Unlock()
	if !enabled {
		b.Flush()
	}
}

func (b *BlockedEvals) SetTimetable(timetable *timetable.TimeTable) {
	b.l.Lock()
	b.timetable = timetable
	b.l.Unlock()
}

// Block tracks the passed evaluation and enqueues it into the eval broker when
// a suitable node calls unblock.
func (b *BlockedEvals) Block(eval *structs.Evaluation) {
	b.processBlock(eval, "")
}

func (b *BlockedEvals) processBlock(eval *structs.Evaluation, token string) {
	b.l.Lock()
	defer b.l.Unlock()

	// Do nothing if not enabled
	if !b.enabled {
		return
	}

	// block 状态的eval需要恢复
	// Handle the new evaluation being for a job we are already tracking.
	if b.processBlockJobDuplicate(eval) {
		// If process block job duplicate returns true, the new evaluation has
		// been marked as a duplicate and we have nothing to do, so return
		// early.
		return
	}

	// Check if the eval missed an unblock while it was in the scheduler at an
	// older index. The scheduler could have been invoked with a snapshot of
	// state that was prior to additional capacity being added or allocations
	// becoming terminal.
	if b.missedUnblock(eval) {
		// Just re-enqueue the eval immediately. We pass the token so that the
		// eval_broker can properly handle the case in which the evaluation is
		// still outstanding.
		b.evalBroker.EnqueueAll(map[*structs.Evaluation]string{eval: token})
		return
	}
	b.stats.Block(eval)

	// Mark the plan as tracked.
	b.plans[structs.NewNamespacedID(eval.PlanID, eval.Namespace)] = eval.ID

	// Wrap the evaluation, capturing its token.
	wrapped := wrappedEval{
		eval:  eval,
		token: token,
	}
	if eval.EscapedComputedClass {
		b.escaped[eval.ID] = wrapped
		b.stats.TotalEscaped++
		return
	}
	// Add the eval to the set of blocked evals whose plans constraints are
	// captured by computed node class.
	b.captured[eval.ID] = wrapped
}

// Untrack causes any blocked evaluation for the passed job to be no longer
// tracked. Untrack is called when there is a successful evaluation for the job
// and a blocked evaluation is no longer needed.
func (b *BlockedEvals) Untrack(planID, namespace string) {
	b.l.Lock()
	defer b.l.Unlock()

	// Do nothing if not enabled
	if !b.enabled {
		return
	}

	nsID := structs.NewNamespacedID(planID, namespace)

	// Get the evaluation ID to cancel
	evalID, ok := b.plans[nsID]
	if !ok {
		// No blocked evaluation so exit
		return
	}

	// Attempt to delete the evaluation
	if w, ok := b.captured[evalID]; ok {
		delete(b.plans, nsID)
		b.logger.Debug("ready to delete captured ", "captured", b.captured, "evalid", evalID)
		delete(b.captured, evalID)
		b.stats.Unblock(w.eval)
	}

	if w, ok := b.escaped[evalID]; ok {
		delete(b.plans, nsID)
		delete(b.escaped, evalID)
		b.stats.Unblock(w.eval)
	}
}

func (b *BlockedEvals) Unblock(computedClass string, index uint64) {
	b.l.Lock()

	// Do nothing if not enabled
	if !b.enabled {
		b.l.Unlock()
		return
	}

	// Store the index in which the unblock happened. We use this on subsequent
	// block calls in case the evaluation was in the scheduler when a trigger
	// occurred.
	b.unblockIndexes[computedClass] = index
	b.l.Unlock()

	b.capacityChangeCh <- &capacityUpdate{
		computedClass: computedClass,
		index:         index,
	}
}

func (b *BlockedEvals) missedUnblock(eval *structs.Evaluation) bool {
	var max uint64 = 0
	for id, index := range b.unblockIndexes {
		// Calculate the max unblock index
		if max < index {
			max = index
		}

		// The evaluation is blocked because it has hit a quota limit not class
		// eligibility
		if eval.QuotaLimitReached != "" {
			if eval.QuotaLimitReached != id {
				// Not a match
				continue
			} else if eval.SnapshotIndex < index {
				// The evaluation was processed before the quota specification was
				// updated, so unblock the evaluation.
				return true
			}

			// The evaluation was processed having seen all changes to the quota
			return false
		}

		elig, ok := eval.ClassEligibility[id]
		if !ok && eval.SnapshotIndex < index {
			// The evaluation was processed and did not encounter this class
			// because it was added after it was processed. Thus for correctness
			// we need to unblock it.
			return true
		}

		// The evaluation could use the computed node class and the eval was
		// processed before the last unblock.
		if elig && eval.SnapshotIndex < index {
			return true
		}
	}

	// If the evaluation has escaped, and the map contains an index older than
	// the evaluations, it should be unblocked.
	if eval.EscapedComputedClass && eval.SnapshotIndex < max {
		return true
	}

	// The evaluation is ahead of all recent unblocks.
	return false
}

func (b *BlockedEvals) processBlockJobDuplicate(eval *structs.Evaluation) (newCancelled bool) {
	existingID, hasExisting := b.plans[structs.NewNamespacedID(eval.PlanID, eval.Namespace)]
	if !hasExisting {
		return
	}

	var dup *structs.Evaluation
	existingW, ok := b.captured[existingID]
	if ok {
		if latestEvalIndex(existingW.eval) <= latestEvalIndex(eval) {
			b.logger.Info("---ready to delete captured ", "captured", b.captured, "evalid", existingID)
			delete(b.captured, existingID)
			dup = existingW.eval
			b.stats.Unblock(dup)
		} else {
			dup = eval
			newCancelled = true
		}
	} else {
		existingW, ok = b.escaped[existingID]
		if !ok {
			// This is a programming error
			b.logger.Error("existing blocked evaluation is neither tracked as captured or escaped", "existing_id", existingID)
			delete(b.plans, structs.NewNamespacedID(eval.PlanID, eval.Namespace))
			return
		}

		if latestEvalIndex(existingW.eval) <= latestEvalIndex(eval) {
			delete(b.escaped, existingID)
			b.stats.TotalEscaped--
			dup = existingW.eval
		} else {
			dup = eval
			newCancelled = true
		}
	}

	b.duplicates = append(b.duplicates, dup)

	// Unblock any waiter.
	select {
	case b.duplicateCh <- struct{}{}:
	default:
	}

	return
}

func latestEvalIndex(eval *structs.Evaluation) uint64 {
	if eval == nil {
		return 0
	}

	return helper.Uint64Max(eval.CreateIndex, eval.SnapshotIndex)
}

// GetDuplicates returns all the duplicate evaluations and blocks until the
// passed timeout.
func (b *BlockedEvals) GetDuplicates(timeout time.Duration) []*structs.Evaluation {
	var timeoutTimer *time.Timer
	var timeoutCh <-chan time.Time
SCAN:
	b.l.Lock()
	if len(b.duplicates) != 0 {
		dups := b.duplicates
		b.duplicates = nil
		b.l.Unlock()
		return dups
	}

	// Capture chans inside the lock to prevent a race with them getting
	// reset in Flush
	dupCh := b.duplicateCh
	stopCh := b.stopCh
	b.l.Unlock()

	// Create the timer
	if timeoutTimer == nil && timeout != 0 {
		timeoutTimer = time.NewTimer(timeout)
		timeoutCh = timeoutTimer.C
		defer timeoutTimer.Stop()
	}

	select {
	case <-stopCh:
		return nil
	case <-timeoutCh:
		return nil
	case <-dupCh:
		goto SCAN
	}
}

// UnblockFailed unblocks all blocked evaluation that were due to scheduler
// failure.
func (b *BlockedEvals) UnblockFailed() {
	b.l.Lock()
	defer b.l.Unlock()

	// Do nothing if not enabled
	if !b.enabled {
		return
	}

	quotaLimit := 0
	unblocked := make(map[*structs.Evaluation]string, 4)
	for id, wrapped := range b.captured {
		if wrapped.eval.TriggeredBy == "max-plan-attempts" {
			unblocked[wrapped.eval] = wrapped.token
			delete(b.captured, id)
			delete(b.plans, structs.NewNamespacedID(wrapped.eval.PlanID, wrapped.eval.Namespace))
			if wrapped.eval.QuotaLimitReached != "" {
				quotaLimit++
			}
		}
	}

	for id, wrapped := range b.escaped {
		if wrapped.eval.TriggeredBy == "max-plan-attempts" {
			unblocked[wrapped.eval] = wrapped.token
			delete(b.escaped, id)
			delete(b.plans, structs.NewNamespacedID(wrapped.eval.PlanID, wrapped.eval.Namespace))
			b.stats.TotalEscaped -= 1
			if wrapped.eval.QuotaLimitReached != "" {
				quotaLimit++
			}
		}
	}

	if len(unblocked) > 0 {
		b.stats.TotalQuotaLimit -= quotaLimit
		for eval := range unblocked {
			b.stats.Unblock(eval)
		}
		b.evalBroker.EnqueueAll(unblocked)
	}
}

// watchCapacity is a long lived function that watches for capacity changes in
// nodes and unblocks the correct set of evals.
func (b *BlockedEvals) watchCapacity(stopCh <-chan struct{}, changeCh <-chan *capacityUpdate) {
	for {
		select {
		case <-stopCh:
			return
		case update := <-changeCh:
			b.unblock(update.computedClass, update.quotaChange)
		}
	}
}

func (b *BlockedEvals) unblock(computedClass, quota string) {
	b.l.Lock()
	defer b.l.Unlock()

	// Protect against the case of a flush.
	if !b.enabled {
		return
	}

	// Every eval that has escaped computed node class has to be unblocked
	// because any node could potentially be feasible.
	numEscaped := len(b.escaped)
	numQuotaLimit := 0
	unblocked := make(map[*structs.Evaluation]string, helper.MaxInt(numEscaped, 4))

	if numEscaped != 0 && computedClass != "" {
		for id, wrapped := range b.escaped {
			unblocked[wrapped.eval] = wrapped.token
			delete(b.escaped, id)
			delete(b.plans, structs.NewNamespacedID(wrapped.eval.PlanID, wrapped.eval.Namespace))

			if wrapped.eval.QuotaLimitReached != "" {
				numQuotaLimit++
			}
		}
	}

	// We unblock any eval that is explicitly eligible for the computed class
	// and also any eval that is not eligible or uneligible. This signifies that
	// when the evaluation was originally run through the scheduler, that it
	// never saw a node with the given computed class and thus needs to be
	// unblocked for correctness.
	for id, wrapped := range b.captured {
		fmt.Println("in block service-------wrapped")
		if quota != "" && wrapped.eval.QuotaLimitReached != quota {
			fmt.Println("in quota not null")
			// We are unblocking based on quota and this eval doesn't match
			continue
		} else if elig, ok := wrapped.eval.ClassEligibility[computedClass]; ok && !elig {
			fmt.Println("in elig not null")
			// Can skip because the eval has explicitly marked the node class
			// as ineligible.
			continue
		}

		// Unblock the evaluation because it is either for the matching quota,
		// is eligible based on the computed node class, or never seen the
		// computed node class.
		unblocked[wrapped.eval] = wrapped.token
		delete(b.plans, structs.NewNamespacedID(wrapped.eval.PlanID, wrapped.eval.Namespace))
		b.logger.Info("----- start to delete the captured", "captured", b.captured)
		delete(b.captured, id)
		if wrapped.eval.QuotaLimitReached != "" {
			numQuotaLimit++
		}
	}

	if len(unblocked) != 0 {
		// Update the counters
		b.stats.TotalEscaped = 0
		b.stats.TotalQuotaLimit -= numQuotaLimit
		for eval := range unblocked {
			b.stats.Unblock(eval)
		}
		// Enqueue all the unblocked evals into the broker.
		b.evalBroker.EnqueueAll(unblocked)
	}
}

const (
	// unblockBuffer is the buffer size for the unblock channel. The buffer
	// should be large to ensure that the FSM doesn't block when calling Unblock
	// as this would apply back-pressure on Raft.
	unblockBuffer = 8096

	// pruneInterval is the interval at which we prune objects from the
	// BlockedEvals tracker
	pruneInterval = 5 * time.Minute

	// pruneThreshold is the threshold after which objects will be pruned.
	pruneThreshold = 15 * time.Minute
)

// prune is a long lived function that prunes unnecessary objects on a timer.
func (b *BlockedEvals) prune(stopCh <-chan struct{}) {
	ticker := time.NewTicker(pruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case t := <-ticker.C:
			cutoff := t.UTC().Add(-1 * pruneThreshold)
			b.pruneUnblockIndexes(cutoff)
			// b.pruneStats(cutoff)
		}
	}
}

// pruneUnblockIndexes is used to prune any tracked entry that is excessively
// old. This protects againsts unbounded growth of the map.
func (b *BlockedEvals) pruneUnblockIndexes(cutoff time.Time) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.timetable == nil {
		return
	}

	oldThreshold := b.timetable.NearestIndex(cutoff)
	for key, index := range b.unblockIndexes {
		if index < oldThreshold {
			delete(b.unblockIndexes, key)
		}
	}
}

// Flush is used to clear the state of blocked evaluations.
func (b *BlockedEvals) Flush() {
	b.l.Lock()
	defer b.l.Unlock()
	b.stats.TotalEscaped = 0
	b.stats.TotalBlocked = 0
	b.stats.TotalQuotaLimit = 0
	b.captured = make(map[string]wrappedEval)
	b.escaped = make(map[string]wrappedEval)
	b.plans = make(map[structs.NamespacedID]string)
	b.unblockIndexes = make(map[string]uint64)
	b.timetable = nil
	b.duplicates = nil
	b.capacityChangeCh = make(chan *capacityUpdate, unblockBuffer)
	b.stopCh = make(chan struct{})
	b.duplicateCh = make(chan struct{}, 1)
}

func (b *BlockedEvals) Stats() *structs.BlockedStats {
	// Allocate a new stats struct
	stats := &structs.BlockedStats{}
	b.l.RLock()
	defer b.l.RUnlock()
	// Copy all the stats
	stats.TotalEscaped = b.stats.TotalEscaped
	stats.TotalBlocked = b.stats.TotalBlocked
	stats.TotalQuotaLimit = b.stats.TotalQuotaLimit
	stats.TotalCaptured = len(b.captured)
	return stats
}
