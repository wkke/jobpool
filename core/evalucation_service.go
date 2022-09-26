package core

import (
	"fmt"
	"github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-multierror"
	"net/http"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/state/paginator"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
)

type Eval struct {
	srv    *Server
	logger log.Logger

	// ctx provides context regarding the underlying connection
	ctx *RPCContext
}

// Dequeue is used to dequeue a pending evaluation
func (e *Eval) Dequeue(args *dto.EvalDequeueRequest,
	reply *dto.EvalDequeueResponse) error {

	// Ensure the connection was initiated by another server if TLS is used.
	err := validateTLSCertificateLevel(e.srv, e.ctx, tlsCertificateLevelServer)
	if err != nil {
		return err
	}

	if done, err := e.srv.forward("Eval.Dequeue", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "eval", "dequeue"}, time.Now())

	// Ensure there is at least one scheduler
	if len(args.Schedulers) == 0 {
		return fmt.Errorf("dequeue requires at least one scheduler type")
	}

	// Ensure there is a default timeout
	if args.Timeout <= 0 {
		args.Timeout = time.Second
	}

	// Attempt the dequeue
	eval, token, err := e.srv.evalBroker.Dequeue(args.Schedulers, args.Timeout)
	if err != nil {
		return err
	}

	if eval != nil {
		e.logger.Debug("--- finish eval dequeue in eval service----", "eval", eval)
	}

	// Provide the output if any
	if eval != nil {
		// Get the index that the worker should wait until before scheduling.
		waitIndex, err := e.getWaitIndex(eval.Namespace, eval.PlanID, eval.ModifyIndex)
		if err != nil {
			var mErr multierror.Error
			_ = multierror.Append(&mErr, err)

			// We have dequeued the evaluation but won't be returning it to the
			// worker so Nack the eval.
			if err := e.srv.evalBroker.Nack(eval.ID, token); err != nil {
				_ = multierror.Append(&mErr, err)
			}

			return &mErr
		}

		reply.Eval = eval
		reply.Token = token
		reply.WaitIndex = waitIndex
	}

	// Set the query response
	e.srv.setQueryMeta(&reply.QueryMeta)
	return nil
}

// Ack is used to acknowledge completion of a dequeued evaluation
func (e *Eval) Ack(args *dto.EvalAckRequest,
	reply *dto.GenericResponse) error {

	// Ensure the connection was initiated by another server if TLS is used.
	err := validateTLSCertificateLevel(e.srv, e.ctx, tlsCertificateLevelServer)
	if err != nil {
		return err
	}

	if done, err := e.srv.forward("Eval.Ack", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "eval", "ack"}, time.Now())

	// Ack the EvalID
	if err := e.srv.evalBroker.Ack(args.EvalID, args.Token); err != nil {
		return err
	}
	//if err := e.srv.jobRoadMap.DequeueEval(args.Namespace, args.JobID); err != nil {
	//	return err
	//}
	return nil
}

func (e *Eval) Nack(args *dto.EvalAckRequest,
	reply *dto.GenericResponse) error {

	// Ensure the connection was initiated by another server if TLS is used.
	err := validateTLSCertificateLevel(e.srv, e.ctx, tlsCertificateLevelServer)
	if err != nil {
		return err
	}

	if done, err := e.srv.forward("Eval.Nack", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "eval", "nack"}, time.Now())

	// Nack the EvalID
	if err := e.srv.evalBroker.Nack(args.EvalID, args.Token); err != nil {
		return err
	}
	return nil
}

func (e *Eval) getWaitIndex(namespace, planId string, evalModifyIndex uint64) (uint64, error) {
	snap, err := e.srv.State().Snapshot()
	if err != nil {
		return 0, err
	}

	evals, err := snap.EvalsByPlan(nil, namespace, planId)
	if err != nil {
		return 0, err
	}

	// Since dequeueing evals is concurrent with applying Raft messages to
	// the state store, initialize to the currently dequeued eval's index
	// in case it isn't in the snapshot used by EvalsByJob yet.
	max := evalModifyIndex
	for _, eval := range evals {
		if max < eval.ModifyIndex {
			max = eval.ModifyIndex
		}
	}

	return max, nil
}

// Create is used to make a new evaluation
func (e *Eval) Create(args *dto.EvalUpdateRequest,
	reply *dto.GenericResponse) error {

	// Ensure the connection was initiated by another server if TLS is used.
	err := validateTLSCertificateLevel(e.srv, e.ctx, tlsCertificateLevelServer)
	if err != nil {
		return err
	}

	if done, err := e.srv.forward("Eval.Create", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "eval", "create"}, time.Now())

	// Ensure there is only a single update with token
	if len(args.Evals) != 1 {
		return fmt.Errorf("only a single eval can be created")
	}
	eval := args.Evals[0]

	// Verify the parent evaluation is outstanding, and that the tokens match.
	if err := e.srv.evalBroker.OutstandingReset(eval.PreviousEval, args.EvalToken); err != nil {
		return err
	}

	// Look for the eval
	snap, err := e.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}

	ws := memdb.NewWatchSet()
	out, err := snap.EvalByID(ws, eval.ID)
	if err != nil {
		return err
	}
	if out != nil {
		return fmt.Errorf("evaluation already exists")
	}

	eval.CreateTime = xtime.NewFormatTime(time.Now())
	eval.UpdateTime = xtime.NewFormatTime(time.Now())

	// Update via Raft
	_, index, err := e.srv.raftApply(constant.EvalUpdateRequestType, args)
	if err != nil {
		return err
	}

	// Update the index
	reply.Index = index
	return nil
}

// Update is used to perform an update of an Eval if it is outstanding.
func (e *Eval) Update(args *dto.EvalUpdateRequest,
	reply *dto.GenericResponse) error {

	// Ensure the connection was initiated by another server if TLS is used.
	err := validateTLSCertificateLevel(e.srv, e.ctx, tlsCertificateLevelServer)
	if err != nil {
		return err
	}

	if done, err := e.srv.forward("Eval.Update", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "eval", "update"}, time.Now())

	// Ensure there is only a single update with token
	if len(args.Evals) != 1 {
		return fmt.Errorf("only a single eval can be updated")
	}
	eval := args.Evals[0]

	// Verify the evaluation is outstanding, and that the tokens match.
	if err := e.srv.evalBroker.OutstandingReset(eval.ID, args.EvalToken); err != nil {
		return err
	}

	// Update via Raft
	_, index, err := e.srv.raftApply(constant.EvalUpdateRequestType, args)
	if err != nil {
		return err
	}

	// Update the index
	reply.Index = index
	return nil
}

// Update is used to perform an update of an Eval if it is outstanding.
func (e *Eval) Stat(args *dto.EvalStatRequest, reply *dto.EvalStatResponse) error {
	if done, err := e.srv.forward("Eval.Stat", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "eval", "update"}, time.Now())

	stat := e.srv.evalBroker.Stats()
	blockStat := e.srv.blockedEvals.Stats()

	reply.TotalReady = stat.TotalReady
	reply.TotalBlocked = stat.TotalBlocked
	reply.TotalWaiting = stat.TotalWaiting
	reply.TotalUnacked = stat.TotalUnacked
	reply.TotalFailedQueue = stat.TotalFailedQueue
	reply.DelayedEvals = len(stat.DelayedEvals)
	reply.ByScheduler = len(stat.ByScheduler)

	reply.TotalCaptured = blockStat.TotalCaptured
	reply.TotalBlockedQueue = blockStat.TotalBlocked
	reply.TotalEscaped = blockStat.TotalEscaped
	reply.TotalQuotaLimit = blockStat.TotalQuotaLimit
	return nil
}

// List is used to get a list of the evaluations in the system
func (e *Eval) List(args *dto.EvalListRequest, reply *dto.EvalListResponse) error {
	if done, err := e.srv.forward("Eval.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "eval", "list"}, time.Now())

	namespace := args.RequestNamespace()
	// Setup the blocking query
	sort := state.SortOption(args.Reverse)
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, store *state.StateStore) error {
			// Scan all the evaluations
			var err error
			var iter memdb.ResultIterator

			if prefix := args.QueryOptions.Prefix; prefix != "" {
				iter, err = store.EvalsByIDPrefix(ws, namespace, prefix, sort)
			} else {
				iter, err = store.Evals(ws, sort)
			}
			if err != nil {
				return err
			}

			iter = memdb.NewFilterIterator(iter, func(raw interface{}) bool {
				if eval := raw.(*structs.Evaluation); eval != nil {
					return args.ShouldBeFiltered(eval)
				}
				return false
			})

			var evals []*structs.Evaluation
			paginator, err := paginator.NewPaginator(iter, nil, args.QueryOptions,
				func(raw interface{}) error {
					eval := raw.(*structs.Evaluation)
					evals = append(evals, eval)
					return nil
				})
			if err != nil {
				return structs.NewErrRPCCodedf(
					http.StatusBadRequest, "failed to create result paginator: %v", err)
			}

			nextToken, totalElements, totalPages, err := paginator.Page()
			if err != nil {
				return structs.NewErrRPCCodedf(
					http.StatusBadRequest, "failed to read result page: %v", err)
			}

			reply.QueryMeta.NextToken = nextToken
			reply.Evaluations = evals
			reply.TotalPages = totalPages
			reply.TotalElements = totalElements

			// Use the last index that affected the plans table
			index, err := store.Index("evals")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			e.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return e.srv.blockingRPC(&opts)
}
