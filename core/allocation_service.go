package core

import (
	"fmt"
	"github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-multierror"
	"net/http"
	"sync"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/state/paginator"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
	"yunli.com/jobpool/helper/uuid"
)

// Alloc endpoint is used for manipulating allocations
type Alloc struct {
	srv    *Server
	logger log.Logger

	// ctx provides context regarding the underlying connection
	ctx *RPCContext

	// updates holds pending client status updates for allocations
	updates []*structs.Allocation

	// evals holds pending rescheduling eval updates triggered by failed allocations
	evals []*structs.Evaluation

	// updateFuture is used to wait for the pending batch update
	// to complete. This may be nil if no batch is pending.
	updateFuture *structs.BatchFuture

	// updateTimer is the timer that will trigger the next batch
	// update, and may be nil if there is no batch pending.
	updateTimer *time.Timer

	// updatesLock synchronizes access to the updates list,
	// the future and the timer.
	updatesLock sync.Mutex
}

// GetAllocs is used to lookup a set of allocations
func (a *Alloc) GetAllocs(args *dto.AllocsGetRequest,
	reply *dto.AllocsGetResponse) error {

	// Ensure the connection was initiated by a client if TLS is used.
	err := validateTLSCertificateLevel(a.srv, a.ctx, tlsCertificateLevelClient)
	if err != nil {
		return err
	}

	if done, err := a.srv.forward("Alloc.GetAllocs", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"jobpool", "alloc", "get_allocs"}, time.Now())

	allocs := make([]*structs.Allocation, len(args.AllocIDs))

	// Setup the blocking query. We wait for at least one of the requested
	// allocations to be above the min query index. This guarantees that the
	// server has received that index.
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {
			// Lookup the allocation
			thresholdMet := false
			maxIndex := uint64(0)
			for i, alloc := range args.AllocIDs {
				out, err := state.AllocByID(ws, alloc)
				if err != nil {
					return err
				}
				if out == nil {
					// We don't have the alloc yet
					thresholdMet = false
					break
				}

				// Store the pointer
				allocs[i] = out

				// Check if we have passed the minimum index
				if out.ModifyIndex > args.QueryOptions.MinQueryIndex {
					thresholdMet = true
				}

				if maxIndex < out.ModifyIndex {
					maxIndex = out.ModifyIndex
				}
			}

			// Setup the output
			if thresholdMet {
				reply.Allocs = allocs
				reply.Index = maxIndex
			} else {
				// Use the last index that affected the nodes table
				index, err := state.Index("allocs")
				if err != nil {
					return err
				}
				reply.Index = index
			}

			// Set the query response
			a.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		},
	}
	return a.srv.blockingRPC(&opts)
}

// GetAllocs is used to lookup a set of allocations
func (a *Alloc) List(args *dto.AllocsGetRequest,
	reply *dto.AllocsGetResponse) error {
	if done, err := a.srv.forward("Alloc.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"jobpool", "alloc", "get_allocs"}, time.Now())
	// Setup the blocking query. We wait for at least one of the requested
	// allocations to be above the min query index. This guarantees that the
	// server has received that index.
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {
			// Capture all the plans
			var err error
			var iter memdb.ResultIterator
			iter, err = state.Allocs(ws)
			if err != nil {
				return err
			}
			var allocs []*structs.Allocation
			paginator, err := paginator.NewPaginator(iter, nil, args.QueryOptions,
				func(raw interface{}) error {
					alloc := raw.(*structs.Allocation)
					allocs = append(allocs, alloc)
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
			reply.Allocs = allocs
			reply.TotalPages = totalPages
			reply.TotalElements = totalElements
			return nil
		},
	}
	return a.srv.blockingRPC(&opts)
}

func (a *Alloc) GetAllocsByJobId(args *dto.AllocsGetByJobIdRequest, reply *dto.AllocsGetByJobIdResponse) error {
	if done, err := a.srv.forward("Alloc.GetAllocsByJobId", args, args, reply); done {
		return err
	}
	state := a.srv.State()
	snap, _ := state.Snapshot()
	ws := snap.NewWatchSet()
	allocs, err := state.AllocsByJob(ws, args.Namespace, args.JobId)
	if err != nil {
		return err
	}
	reply.Allocs = allocs
	return nil
}

func (a *Alloc) UpdateAlloc(args *dto.AllocUpdateRequest, reply *dto.GenericResponse) error {
	if done, err := a.srv.forward("Alloc.UpdateAlloc", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "client", "update_alloc"}, time.Now())
	if len(args.Alloc) == 0 {
		return fmt.Errorf("must update at least one allocation")
	}

	// Update modified timestamp for client initiated allocation updates
	now := time.Now()
	var evals []*structs.Evaluation
	for _, allocToUpdate := range args.Alloc {
		evalTriggerBy := ""
		allocToUpdate.ModifyTime = xtime.NewFormatTime(now)

		alloc, _ := a.srv.State().AllocByID(nil, allocToUpdate.ID)
		if alloc == nil {
			continue
		}

		if allocToUpdate.ClientStatus == "" {
			continue
		}

		if !allocToUpdate.TerminalStatus() && alloc.ClientStatus != constant.AllocClientStatusUnknown {
			continue
		}

		var plan *structs.Plan
		var planType string
		var planPriority int

		plan, err := a.srv.State().PlanByID(nil, alloc.Namespace, alloc.PlanID)
		if err != nil {
			a.logger.Debug("UpdateAlloc unable to find plan", "plan", alloc.PlanID, "error", err)
			continue
		}

		// If the plan is nil it means it has been de-registered.
		if plan == nil {
			planType = alloc.Plan.Type
			planPriority = alloc.Plan.Priority
			evalTriggerBy = constant.EvalTriggerJobDeregister
			allocToUpdate.DesiredStatus = constant.AllocDesiredStatusStop
			a.logger.Debug("UpdateAlloc unable to find plan - shutting down alloc", "plan", alloc.PlanID)
		}

		if plan != nil {
			planType = plan.Type
			planPriority = plan.Priority
		}

		// If we cannot find the task group for a failed alloc we cannot continue, unless it is an orphan.
		if evalTriggerBy != constant.EvalTriggerJobDeregister &&
			allocToUpdate.ClientStatus == constant.AllocClientStatusFailed &&
			alloc.FollowupEvalID == "" {
		}

		var eval *structs.Evaluation
		// If unknown, and not an orphan, set the trigger by.
		if evalTriggerBy != constant.EvalTriggerJobDeregister &&
			alloc.ClientStatus == constant.AllocClientStatusUnknown {
			evalTriggerBy = constant.EvalTriggerReconnect
		}

		// If we weren't able to determine one of our expected eval triggers,
		// continue and don't create an eval.
		if evalTriggerBy == "" {
			continue
		}

		eval = &structs.Evaluation{
			ID:          uuid.Generate(),
			Namespace:   alloc.Namespace,
			TriggeredBy: evalTriggerBy,
			PlanID:      alloc.PlanID,
			Type:        planType,
			Priority:    planPriority,
			Status:      constant.EvalStatusPending,
			CreateTime:  xtime.NewFormatTime(time.Now()),
			UpdateTime:  xtime.NewFormatTime(time.Now()),
		}
		evals = append(evals, eval)
	}

	// Add this to the batch
	a.updatesLock.Lock()
	a.updates = append(a.updates, args.Alloc...)
	a.evals = append(a.evals, evals...)

	// Start a new batch if none
	future := a.updateFuture
	if future == nil {
		future = structs.NewBatchFuture()
		a.updateFuture = future
		a.updateTimer = time.AfterFunc(batchUpdateInterval, func() {
			// Get the pending updates
			a.updatesLock.Lock()
			updates := a.updates
			evals := a.evals
			future := a.updateFuture

			// Assume future update patterns will be similar to
			// current batch and set cap appropriately to avoid
			// slice resizing.
			a.updates = make([]*structs.Allocation, 0, len(updates))
			a.evals = make([]*structs.Evaluation, 0, len(evals))

			a.updateFuture = nil
			a.updateTimer = nil
			a.updatesLock.Unlock()

			// Perform the batch update
			a.batchUpdate(future, updates, evals)
		})
	}
	a.updatesLock.Unlock()
	// Wait for the future
	if err := future.Wait(); err != nil {
		return err
	}
	// Setup the response
	reply.Index = future.Index()
	return nil
}

// batchUpdate is used to update all the allocations
func (n *Alloc) batchUpdate(future *structs.BatchFuture, updates []*structs.Allocation, evals []*structs.Evaluation) {
	var mErr multierror.Error
	// Group pending evals by jobID to prevent creating unnecessary evals
	evalsByJobId := make(map[structs.NamespacedID]struct{})
	var trimmedEvals []*structs.Evaluation
	for _, eval := range evals {
		namespacedID := structs.NamespacedID{
			ID:        eval.JobID,
			Namespace: eval.Namespace,
		}
		_, exists := evalsByJobId[namespacedID]
		if !exists {
			now := xtime.NewFormatTime(time.Now())
			eval.CreateTime = now
			eval.UpdateTime = now
			trimmedEvals = append(trimmedEvals, eval)
			evalsByJobId[namespacedID] = struct{}{}
		}
	}

	if len(trimmedEvals) > 0 {
		n.logger.Debug("adding evaluations for rescheduling failed allocations", "num_evals", len(trimmedEvals))
	}
	// Prepare the batch update
	batch := &dto.AllocUpdateRequest{
		Alloc:        updates,
		Evals:        trimmedEvals,
		WriteRequest: dto.WriteRequest{Region: n.srv.config.Region},
	}

	// Commit this update via Raft
	_, index, err := n.srv.raftApply(constant.AllocClientUpdateRequestType, batch)
	if err != nil {
		n.logger.Error("alloc update failed", "error", err)
		mErr.Errors = append(mErr.Errors, err)
	}

	for _, alloc := range updates {
		// Skip any allocation that isn't dead on the client
		if !alloc.Terminated() {
			continue
		}
	}
	// Respond to the future
	future.Respond(index, mErr.ErrorOrNil())
}
