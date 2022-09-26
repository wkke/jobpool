package scheduler

import (
	"container/heap"
	log "github.com/hashicorp/go-hclog"
	"sync"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
)

// 任务地图相关逻辑
type JobRoadmap struct {
	enabled     bool
	size        int
	slotPending map[string]SlotJobs
	slotRunning map[string]SlotJobs
	slotRetry   map[string]SlotJobs
	stats       *structs.JobRoadStats
	logger      log.Logger
	l           sync.RWMutex
}

func NewJobRoadmap(defaultSize int, logger log.Logger) (*JobRoadmap, error) {
	if defaultSize == 0 {
		// size 默认先给100，即100个job槽位，超过则拒绝增加任务
		defaultSize = 100
	}
	q := &JobRoadmap{
		enabled:     false,
		logger:      logger,
		size:        defaultSize,
		slotPending: make(map[string]SlotJobs),
		slotRunning: make(map[string]SlotJobs),
		slotRetry:   make(map[string]SlotJobs),
		stats:       new(structs.JobRoadStats),
	}
	q.stats.TotalPending = make(map[string]int)
	q.stats.TotalRetry = make(map[string]int)
	q.stats.TotalRunning = make(map[string]int)
	return q, nil
}

func (q *JobRoadmap) Enabled() bool {
	q.l.RLock()
	defer q.l.RUnlock()
	return q.enabled
}

func (q *JobRoadmap) SetEnabled(enabled bool) {
	q.l.Lock()
	defer q.l.Unlock()

	prevEnabled := q.enabled
	q.enabled = enabled
	if !prevEnabled && enabled {
		// start the go routine for instant jobs
	}
	if !enabled {
		q.flush()
	}
}

func (q *JobRoadmap) Enqueue(eval *structs.Evaluation) {
	q.l.Lock()
	defer q.l.Unlock()
	if !q.enabled {
		return
	}
	if eval == nil || eval.JobID == "" {
		return
	}

	namespacedID := eval.Namespace
	waiting := q.slotPending[namespacedID]
	waitSlot := &structs.JobSlot{
		ID:          eval.JobID,
		CreateIndex: eval.CreateIndex,
	}
	if waiting.ContainsId(eval.JobID) {
		return
	}
	heap.Push(&waiting, waitSlot)
	q.slotPending[namespacedID] = waiting
	q.stats.TotalPending[namespacedID] += 1
}

func (q *JobRoadmap) EnqueueAll(jobs []*structs.Job) {
	q.l.Lock()
	defer q.l.Unlock()
	for _, job := range jobs {
		q.processEnqueue(job)
	}
}

func (q *JobRoadmap) processEnqueue(job *structs.Job) {
	if !q.enabled {
		return
	}
	if job == nil || job.ID == "" {
		return
	}
	if constant.JobStatusSkipped == job.Status {
		return
	}
	// 添加必然是pennding
	if constant.JobStatusPending != job.Status {
		q.logger.Warn("wrong job status in add process", "status", job.Status, "job", job)
		return
	}
	namespacedID := job.Namespace
	pending := q.slotPending[namespacedID]
	if pending.ContainsId(job.ID) {
		return
	}
	heap.Push(&pending, job.ConvertJobSlot())
	q.slotPending[namespacedID] = pending
	q.stats.TotalPending[namespacedID] += 1
}

// 任务启动后将其移动到running中
func (q *JobRoadmap) JobStartRunning(namespace string, jobId string) error {
	q.l.Lock()
	defer q.l.Unlock()
	index, slot := q.slotPending[namespace].Pick(jobId)
	if slot == nil {
		q.logger.Debug("dequeue eval failed in job roadmap", "JobID", jobId)
		return nil
	}
	slotJob := q.slotPending[namespace]
	heap.Remove(&slotJob, index)
	q.slotPending[namespace] = slotJob
	q.stats.TotalPending[namespace] -= 1
	// 将其放入running
	runningJobs := q.slotRunning[namespace]
	heap.Push(&runningJobs, slot)
	q.slotRunning[namespace] = runningJobs
	q.stats.TotalRunning[namespace] += 1
	return nil
}

func (q *JobRoadmap) Dequeue() (*structs.Job, error) {

	return nil, nil
}

func (q *JobRoadmap) HasEmptySlot(namespace string) bool {
	q.l.Lock()
	defer q.l.Unlock()
	slotLeft := q.getSlotLeft(namespace)
	if slotLeft > 0 {
		return true
	}
	return false
}

func (q *JobRoadmap) DequeueEval(namespace string, jobId string) error {
	q.l.Lock()
	defer q.l.Unlock()
	indexPending, evalPending := q.slotPending[namespace].Pick(jobId)
	indexRunning, evalRunning := q.slotRunning[namespace].Pick(jobId)
	if evalPending == nil && evalRunning == nil {
		q.logger.Debug("dequeue evalPending failed in job roadmap", "JobID", jobId)
		return nil
	}
	if evalPending != nil {
		slotJob := q.slotPending[namespace]
		heap.Remove(&slotJob, indexPending)
		q.slotPending[namespace] = slotJob
		q.stats.TotalPending[namespace] -= 1
	}
	if evalRunning != nil {
		slotJob := q.slotRunning[namespace]
		heap.Remove(&slotJob, indexRunning)
		q.slotRunning[namespace] = slotJob
		q.stats.TotalRunning[namespace] -= 1
	}
	return nil
}

func (q *JobRoadmap) flush() {
	// 清空
	q.stats.TotalPending = make(map[string]int)
	q.stats.TotalRetry = make(map[string]int)
	q.stats.TotalRunning = make(map[string]int)
	q.slotPending = make(map[string]SlotJobs)
	q.slotRetry = make(map[string]SlotJobs)
	q.slotRunning = make(map[string]SlotJobs)
}

func (q *JobRoadmap) Stats(namespace string) *structs.JobMapStats {
	stats := new(structs.JobMapStats)
	q.l.RLock()
	defer q.l.RUnlock()
	stats.TotalRunning = q.stats.TotalRunning[namespace]
	stats.TotalRetry = q.stats.TotalRetry[namespace]
	stats.TotalPending = q.stats.TotalPending[namespace]
	stats.TotalUnUsed = q.getSlotLeft(namespace)

	// 将队列中的内容打印出来
	q.logger.Debug("-----in roadmap running queue-----")
	listRunning := q.slotRunning[namespace]
	runningJobs := make([]string, 0)
	if listRunning != nil {
		for _, item := range listRunning {
			q.logger.Debug("running", item.ID)
			runningJobs = append(runningJobs, item.ID)
		}
	}
	stats.RunningJobs = runningJobs
	q.logger.Debug("-----in roadmap pending queue-----")
	listPending := q.slotPending[namespace]
	pendingJobs := make([]string, 0)
	if listPending != nil {
		for _, item := range listPending {
			q.logger.Debug("pending", item.ID)
			pendingJobs = append(pendingJobs, item.ID)
		}
	}
	stats.PendingJobs = pendingJobs
	q.logger.Debug("-----in roadmap retry queue-----")
	listRetry := q.slotRetry[namespace]
	if listRetry != nil {
		for _, item := range listRetry {
			q.logger.Debug("retry", item.ID)
		}
	}

	return stats
}

func (q *JobRoadmap) getSlotLeft(namespace string) int {
	slotLeft := q.size - q.stats.TotalRunning[namespace] - q.stats.TotalRetry[namespace] - q.stats.TotalPending[namespace]
	return slotLeft
}

type SlotJobs []*structs.JobSlot

func (s SlotJobs) Len() int {
	return len(s)
}

func (s SlotJobs) Less(i, j int) bool {
	return s[i].CreateIndex < s[j].CreateIndex
}

func (s SlotJobs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *SlotJobs) Push(e interface{}) {
	*s = append(*s, e.(*structs.JobSlot))
}

func (s *SlotJobs) Pop() interface{} {
	n := len(*s)
	e := (*s)[n-1]
	(*s)[n-1] = nil
	*s = (*s)[:n-1]
	return e
}

func (s SlotJobs) Pick(ID string) (int, *structs.JobSlot) {
	flagIndex := -1
	var selected *structs.JobSlot
	for index, item := range s {
		if item.ID == ID {
			flagIndex = index
			selected = item
		}
	}
	if flagIndex >= 0 {
		return flagIndex, selected
		// s = append(s[:flagIndex], s[(flagIndex+1):]...)
	} else {
		return flagIndex, nil
	}
}

func (s SlotJobs) ContainsId(ID string) bool {
	for _, item := range s {
		if item.ID == ID {
			return true
		}
	}
	return false
}
