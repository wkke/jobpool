package scheduler

import (
	"testing"
	"time"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper/ci"
)

func testPlanQueue(t *testing.T) *PlanQueue {
	pq, err := NewPlanQueue()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return pq
}

func PlanAlloc() *structs.PlanAlloc {
	return &structs.PlanAlloc{
		Priority: 50,
	}
}

func PlanResult() *structs.PlanResult {
	return &structs.PlanResult{}
}


func TestPlanQueue_Enqueue_Dequeue(t *testing.T) {
	ci.Parallel(t)
	pq := testPlanQueue(t)
	if pq.Enabled() {
		t.Fatalf("should not be enabled")
	}
	pq.SetEnabled(true)
	if !pq.Enabled() {
		t.Fatalf("should be enabled")
	}

	plan := PlanAlloc()
	future, err := pq.Enqueue(plan)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	stats := pq.Stats()
	if stats.Depth != 1 {
		t.Fatalf("bad: %#v", stats)
	}

	resCh := make(chan *structs.PlanResult, 1)
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		defer close(resCh)

		res, err := future.Wait()
		if err != nil {
			errCh <- err
			return
		}
		resCh <- res
	}()

	pending, err := pq.Dequeue(time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	stats = pq.Stats()
	if stats.Depth != 0 {
		t.Fatalf("bad: %#v", stats)
	}

	if pending == nil || pending.Plan != plan {
		t.Fatalf("bad: %#v", pending)
	}

	result := PlanResult()
	pending.Respond(result, nil)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("error in anonymous goroutine: %s", err)
		}
	case r := <-resCh:
		if r != result {
			t.Fatalf("Bad: %#v", r)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout")
	}
}


// Ensure higher priority dequeued first
func TestPlanQueue_Dequeue_Priority(t *testing.T) {
	ci.Parallel(t)
	pq := testPlanQueue(t)
	pq.SetEnabled(true)

	plan1 := PlanAlloc()
	plan1.Priority = 10
	pq.Enqueue(plan1)

	plan2 := PlanAlloc()
	plan2.Priority = 30
	pq.Enqueue(plan2)

	plan3 := PlanAlloc()
	plan3.Priority = 20
	pq.Enqueue(plan3)

	out1, _ := pq.Dequeue(time.Second)
	if out1.Plan != plan2 {
		t.Fatalf("bad: %#v", out1)
	}

	out2, _ := pq.Dequeue(time.Second)
	if out2.Plan != plan3 {
		t.Fatalf("bad: %#v", out2)
	}

	out3, _ := pq.Dequeue(time.Second)
	if out3.Plan != plan1 {
		t.Fatalf("bad: %#v", out3)
	}
}