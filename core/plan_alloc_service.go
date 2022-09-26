package core

import (
	"fmt"
	"github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
)

type PlanAllocService struct {
	srv    *Server
	logger log.Logger
	// ctx provides context regarding the underlying connection
	ctx *RPCContext
}


// Submit is used to submit a plan to the leader
func (p *PlanAllocService) Submit(args *dto.PlanAllocRequest, reply *dto.PlanAllocResponse) error {
	// Ensure the connection was initiated by another server if TLS is used.
	err := validateTLSCertificateLevel(p.srv, p.ctx, tlsCertificateLevelServer)
	if err != nil {
		return err
	}

	if done, err := p.srv.forward("PlanAllocService.Submit", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "plan", "submit"}, time.Now())

	if args.PlanAlloc == nil {
		return fmt.Errorf("cannot submit nil plan")
	}

	// Pause the Nack timer for the eval as it is making progress as long as it
	// is in the plan queue. We resume immediately after we get a result to
	// handle the case that the receiving worker dies.
	plan := args.PlanAlloc
	id := plan.EvalID
	token := plan.EvalToken
	if err := p.srv.evalBroker.PauseNackTimeout(id, token); err != nil {
		return err
	}
	defer p.srv.evalBroker.ResumeNackTimeout(id, token)

	// Submit the plan to the queue
	future, err := p.srv.planQueue.Enqueue(plan)
	if err != nil {
		return err
	}

	// Wait for the results
	result, err := future.Wait()
	if err != nil {
		return err
	}

	// Package the result
	reply.Result = result
	reply.Index = result.AllocIndex
	return nil
}
