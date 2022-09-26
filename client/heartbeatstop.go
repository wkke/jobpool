package client

import (
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

type heartbeatStop struct {
	lastOk        time.Time
	startupGrace  time.Time
	allocInterval map[string]time.Duration
	logger        hclog.InterceptLogger
	shutdownCh    chan struct{}
	lock          *sync.RWMutex
}

func newHeartbeatStop(
	timeout time.Duration,
	logger hclog.InterceptLogger,
	shutdownCh chan struct{}) *heartbeatStop {

	h := &heartbeatStop{
		startupGrace:  time.Now().Add(timeout),
		allocInterval: make(map[string]time.Duration),
		logger:        logger,
		shutdownCh:    shutdownCh,
		lock:          &sync.RWMutex{},
	}

	return h
}


func (h *heartbeatStop) shouldStopAfter(now time.Time, interval time.Duration) bool {
	lastOk := h.getLastOk()
	if lastOk.IsZero() {
		return now.After(h.startupGrace)
	}
	return now.After(lastOk.Add(interval))
}

// watch is a loop that checks for allocations that should be stopped. It also manages the
// registration of allocs to be stopped in a single thread.
func (h *heartbeatStop) watch() {
	// If we never manage to successfully contact the server, we want to stop our allocs
	// after duration + start time
	h.lastOk = time.Now()
	stop := make(chan string, 1)
	var now time.Time
	var interval time.Duration
	checkAllocs := false

	for {
		// minimize the interval
		interval = 5 * time.Second
		for _, t := range h.allocInterval {
			if t < interval {
				interval = t
			}
		}

		checkAllocs = false
		timeout := time.After(interval)

		select {
		case allocID := <-stop:
			// TODO
			delete(h.allocInterval, allocID)

		case <-timeout:
			checkAllocs = true

		case <-h.shutdownCh:
			return
		}

		if !checkAllocs {
			continue
		}

		now = time.Now()
		for allocID, d := range h.allocInterval {
			if h.shouldStopAfter(now, d) {
				stop <- allocID
			}
		}
	}
}

// setLastOk sets the last known good heartbeat time to the current time, and persists that time to disk
func (h *heartbeatStop) setLastOk(t time.Time) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.lastOk = t
}

func (h *heartbeatStop) getLastOk() time.Time {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.lastOk
}
