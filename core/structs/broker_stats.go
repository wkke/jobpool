package structs

// BrokerStats returns all the stats about the broker
type BrokerStats struct {
	TotalReady       int
	TotalUnacked     int
	TotalBlocked     int
	TotalWaiting     int
	TotalFailedQueue int
	DelayedEvals     map[string]*Evaluation
	ByScheduler      map[string]*SchedulerStats
}

// SchedulerStats returns the stats per scheduler
type SchedulerStats struct {
	Ready   int
	Unacked int
}

// 计数器
type BlockedStats struct {
	// TotalEscaped is the total number of blocked evaluations that have escaped
	// computed node classes.
	TotalEscaped int

	// TotalBlocked is the total number of blocked evaluations.
	TotalBlocked int

	// TotalQuotaLimit is the total number of blocked evaluations that are due
	// to the quota limit being reached.
	TotalQuotaLimit int

	TotalCaptured int
}

func (b *BlockedStats) Block(eval *Evaluation) {
	b.TotalBlocked++
}
func (b *BlockedStats) Unblock(eval *Evaluation) {
	b.TotalBlocked--
}

type JobRoadStats struct {
	TotalPending map[string]int
	TotalRunning map[string]int
	TotalRetry   map[string]int
}

type JobMapStats struct {
	TotalPending int
	TotalRunning int
	TotalRetry   int
	TotalUnUsed  int
	RunningJobs  []string `json:"runningJobs"`
	PendingJobs  []string `json:"pendingJobs"`
}
