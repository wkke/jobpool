package scheduler

import (
	"testing"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper/ci"
	"yunli.com/jobpool/helper/testlog"
)

func testJobRoadmapQueue(t *testing.T) *JobRoadmap {
	logger := testlog.HCLogger(t)
	tr, err := NewJobRoadmap(100, logger)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return tr
}

func Test_Enqueue_Dequeue(t *testing.T) {
	ci.Parallel(t)
	pq := testJobRoadmapQueue(t)
	if pq.Enabled() {
		t.Fatalf("should not be enabled")
	}
	pq.SetEnabled(true)
	if !pq.Enabled() {
		t.Fatalf("should be enabled")
	}
	namespace := "default"
	jobId := "1111-2222-3333-4444"
	eval := &structs.Evaluation{
		ID:          "12345678",
		Namespace:   namespace,
		JobID:       jobId,
		CreateIndex: 1,
	}
	pq.Enqueue(eval)

	eval2 := &structs.Evaluation{
		ID:          "123456789",
		Namespace:   namespace,
		JobID:       jobId,
		CreateIndex: 2,
	}
	pq.Enqueue(eval2)

	err := pq.DequeueEval(namespace, jobId)
	if err != nil {
		panic("dequeue eval failed")
	}
}
