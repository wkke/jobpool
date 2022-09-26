package scheduler


import (
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/structs"
)

type EvaluateDispatcher interface {
	EvaluateNodePlan(snap *state.StateSnapshot, plan *structs.PlanAlloc, nodeID string) (bool, string, error)
}

// EvaluatePool is used to have a pool of workers that are evaluating
// if a Plan is valid. It can be used to parallelize the evaluation
// of a Plan.
type EvaluatePool struct {
	workers    int
	workerStop []chan struct{}
	req        chan EvaluateRequest
	res        chan evaluateResult
	planner    EvaluateDispatcher
}

type EvaluateRequest struct {
	Snap   *state.StateSnapshot
	Plan   *structs.PlanAlloc
	NodeID string
}

type evaluateResult struct {
	NodeID string
	Fit    bool
	Reason string
	Err    error
}

// NewEvaluatePool returns a pool of the given size.
func NewEvaluatePool(workers, bufSize int, planner EvaluateDispatcher) *EvaluatePool {
	p := &EvaluatePool{
		workers:    workers,
		workerStop: make([]chan struct{}, workers),
		req:        make(chan EvaluateRequest, bufSize),
		res:        make(chan evaluateResult, bufSize),
		planner:    planner,
	}
	for i := 0; i < workers; i++ {
		stopCh := make(chan struct{})
		p.workerStop[i] = stopCh
		go p.run(stopCh)
	}
	return p
}

// Size returns the current size
func (p *EvaluatePool) Size() int {
	return p.workers
}

// SetSize is used to resize the worker pool
func (p *EvaluatePool) SetSize(size int) {
	// Protect against a negative size
	if size < 0 {
		size = 0
	}

	// Handle an upwards resize
	if size >= p.workers {
		for i := p.workers; i < size; i++ {
			stopCh := make(chan struct{})
			p.workerStop = append(p.workerStop, stopCh)
			go p.run(stopCh)
		}
		p.workers = size
		return
	}

	// Handle a downwards resize
	for i := p.workers; i > size; i-- {
		close(p.workerStop[i-1])
		p.workerStop[i-1] = nil
	}
	p.workerStop = p.workerStop[:size]
	p.workers = size
}

// RequestCh is used to push requests
func (p *EvaluatePool) RequestCh() chan<- EvaluateRequest {
	return p.req
}

// ResultCh is used to read the results as they are ready
func (p *EvaluatePool) ResultCh() <-chan evaluateResult {
	return p.res
}

// Shutdown is used to shutdown the pool
func (p *EvaluatePool) Shutdown() {
	p.SetSize(0)
}

// run is a long running go routine per worker
func (p *EvaluatePool) run(stopCh chan struct{}) {
	for {
		select {
		case req := <-p.req:
			fit, reason, err := p.planner.EvaluateNodePlan(req.Snap, req.Plan, req.NodeID)
			p.res <- evaluateResult{req.NodeID, fit, reason, err}

		case <-stopCh:
			return
		}
	}
}
