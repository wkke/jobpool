package scheduler

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper/ci"
	xtime "yunli.com/jobpool/helper/time"
	"yunli.com/jobpool/helper/uuid"
)

func EvalMoke() *structs.Evaluation {
	eval := &structs.Evaluation{
		ID:         uuid.Generate(),
		Namespace:  "default",
		Priority:   50,
		Type:       "service",
		PlanID:     uuid.Generate(),
		Status:     constant.EvalStatusPending,
		CreateTime: xtime.NewFormatTime(time.Now()),
		UpdateTime: xtime.NewFormatTime(time.Now()),
	}
	return eval
}

func testBroker(t *testing.T, timeout time.Duration) *EvalBroker {
	b, err := NewEvalBroker(5*time.Second, 1*time.Second, 20*time.Second, 3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return b
}

func TestEnqueueDequeue(t *testing.T) {
	ci.Parallel(t)
	b := testBroker(t, 0)
	b.SetEnabled(true)

	eval := EvalMoke()
	b.Enqueue(eval)

	var sche = []string{"service"}
	out, token, err := b.Dequeue(sche, 5*time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != eval {
		t.Fatalf("bad : %#v", out)
	}
	fmt.Println(token)
}

func TestAck_Nacd(t *testing.T) {
	ci.Parallel(t)
	b := testBroker(t, 0)

	// Enqueue, but broker is disabled!
	eval := EvalMoke()
	b.Enqueue(eval)

	// Verify nothing was done
	stats := b.Stats()
	if stats.TotalReady != 0 {
		t.Fatalf("bad: %#v", stats)
	}
	// Enable the broker, and enqueue
	b.SetEnabled(true)
	b.Enqueue(eval)

	// Double enqueue is a no-op
	b.Enqueue(eval)

	// Verify enqueue is done
	stats = b.Stats()
	if stats.TotalReady != 1 {
		t.Fatalf("bad: %#v", stats)
	}
	if stats.ByScheduler[eval.Type].Ready != 1 {
		t.Fatalf("bad: %#v", stats)
	}

	// Dequeue should work
	out, token, err := b.Dequeue([]string{"service"}, time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != eval {
		t.Fatalf("bad : %#v", out)
	}


	tokenOut, ok := b.Outstanding(out.ID)
	if !ok {
		t.Fatalf("should be outstanding")
	}
	if tokenOut != token {
		t.Fatalf("Bad: %#v %#v", token, tokenOut)
	}


	// OutstandingReset should verify the token
	err = b.OutstandingReset("nope", "foo")
	if err.Error() != "evaluation is not outstanding" {
		t.Fatalf("err: %v", err)
	}
	err = b.OutstandingReset(out.ID, "foo")
	if err.Error()  != "evaluation token does not match" {
		t.Fatalf("err: %v", err)
	}
	err = b.OutstandingReset(out.ID, tokenOut)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the stats
	stats = b.Stats()
	statJson, err := json.Marshal(stats)
	fmt.Println(fmt.Sprintf("the stats is : %s", statJson))

	if stats.TotalReady != 0 {
		t.Fatalf("bad: %#v", stats)
	}
	if stats.TotalUnacked != 1 {
		t.Fatalf("bad: %#v", stats)
	}
	if stats.ByScheduler[eval.Type].Ready != 0 {
		t.Fatalf("bad: %#v", stats)
	}
	if stats.ByScheduler[eval.Type].Unacked != 1 {
		t.Fatalf("bad: %#v", stats)
	}

	err = b.Nack(eval.ID, token)
	if err != nil {
		t.Fatalf("err in nack: %#v",err)
	}

	stats = b.Stats()
	statJson, _ = json.Marshal(stats)
	fmt.Println(fmt.Sprintf("the new stats is : %s", statJson))


	if _, ok := b.Outstanding(out.ID); ok {
		t.Fatalf("should not be outstanding")
	}

	stats = b.Stats()
	statJson, _ = json.Marshal(stats)
	fmt.Println(fmt.Sprintf("after out stats is : %s", statJson))

	//core.WaitForResult(func() (bool, error) {
	//	stats = b.Stats()
	//	if stats.TotalReady != 1 {
	//		return false, fmt.Errorf("bad: %#v", stats)
	//	}
	//	if stats.TotalUnacked != 0 {
	//		return false, fmt.Errorf("bad: %#v", stats)
	//	}
	//	if stats.TotalWaiting != 0 {
	//		return false, fmt.Errorf("bad: %#v", stats)
	//	}
	//	if stats.ByScheduler[eval.Type].Ready != 1 {
	//		return false, fmt.Errorf("bad: %#v", stats)
	//	}
	//	if stats.ByScheduler[eval.Type].Unacked != 0 {
	//		return false, fmt.Errorf("bad: %#v", stats)
	//	}
	//
	//	return true, nil
	//}, func(e error) {
	//	t.Fatal(e)
	//})

	// Dequeue should work again
	out2, token2, err := b.Dequeue([]string{"service"}, time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out2 != eval {
		t.Fatalf("bad : %#v", out2)
	}
	if token2 == token {
		t.Fatalf("should get a new token")
	}
	stats = b.Stats()
	statJson, _ = json.Marshal(stats)
	fmt.Println(fmt.Sprintf("after dequeue stats is : %s", statJson))

	// Ack finally
	err = b.Ack(eval.ID, token2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	stats = b.Stats()
	statJson, _ = json.Marshal(stats)
	fmt.Println(fmt.Sprintf("after ack stats is : %s", statJson))

}
