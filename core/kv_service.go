package core

import (
	"fmt"
	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-memdb"
	"net/http"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/state/paginator"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
)

type Kv struct {
	srv *Server
}

// ListNamespaces is used to list the namespaces
func (n *Kv) ListKvs(args *dto.KvListRequest, reply *dto.KvListResponse) error {
	if done, err := n.srv.forward("Kv.ListKvs", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "kv", "list_kv"}, time.Now())

	// Set up the blocking query.
	sort := state.SortOption(args.Reverse)
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {

			var err error
			var iter memdb.ResultIterator
			iter, err = state.Kvs(ws, sort)
			if err != nil {
				return err
			}

			var kvs []*structs.Kv
			// Build the paginator. This includes the function that is
			// responsible for appending a node to the nodes array.
			paginatorImpl, err := paginator.NewPaginator(iter, nil, args.QueryOptions,
				func(raw interface{}) error {
					kvs = append(kvs, raw.(*structs.Kv))
					return nil
				})
			if err != nil {
				return structs.NewErrRPCCodedf(
					http.StatusBadRequest, "failed to create result paginator: %v", err)
			}

			// Calling page populates our output nodes array as well as returns
			// the next token.
			nextToken, totalElements, totalPages, err := paginatorImpl.Page()
			if err != nil {
				return structs.NewErrRPCCodedf(
					http.StatusBadRequest, "failed to read result page: %v", err)
			}

			// Populate the reply.
			reply.Kvs = kvs
			reply.NextToken = nextToken
			reply.TotalElements = totalElements
			reply.TotalPages = totalPages

			// Use the last index that affected the plans table
			index, err := state.Index("kvs")
			if err != nil {
				return err
			}
			reply.Index = index

			// Set the query response
			n.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return n.srv.blockingRPC(&opts)
}

func (n *Kv) AddKv(args *dto.KvUpsertRequest, reply *dto.KvAddResponse) error {
	if done, err := n.srv.forward("Kv.AddKv", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "kv", "add"}, time.Now())

	if args.Kv == nil {
		return fmt.Errorf("missing kv for add")
	}
	// if it is exist
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	out, err := snap.KvByKey(ws, args.Kv.Key)
	if err != nil {
		return err
	}
	if out != nil {
		return fmt.Errorf("the kv key [%s] already exists", args.Kv.Key)
	}
	args.Kv.CreateTime = xtime.NewFormatTime(time.Now())
	args.Kv.UpdateTime = xtime.NewFormatTime(time.Now())

	_, index, err := n.srv.raftApply(constant.KvUpsertRequestType, args)
	if err != nil {
		n.srv.logger.Error("add kv failed", "error", err)
		return err
	}
	reply.Index = index
	reply.Key = args.Kv.Key
	reply.Value = args.Kv.Value
	return nil
}

func (n *Kv) UpdateKv(args *dto.KvUpdateRequest, reply *dto.KvAddResponse) error {
	if done, err := n.srv.forward("Kv.UpdateKv", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "kv", "update"}, time.Now())

	if args.Key == "" {
		return fmt.Errorf("missing key for update")
	}
	if args.Value == "" {
		return fmt.Errorf("missing key for update")
	}
	// if it is exist
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	out, err := snap.KvByKey(ws, args.Key)
	if err != nil {
		return err
	}
	if out == nil {
		return fmt.Errorf("the kv key [%s] not exists", args.Key)
	}
	if out.Value == args.Value {
		return fmt.Errorf("the kv key [%s] and value [%s] not changed", args.Key, args.Value)
	}

	upsertArgs := &dto.KvUpsertRequest{
		Kv: &structs.Kv{
			Key:   args.Key,
			Value: args.Value,
		},
		WriteRequest: args.WriteRequest,
	}

	_, index, err := n.srv.raftApply(constant.KvUpsertRequestType, upsertArgs)
	if err != nil {
		n.srv.logger.Error("update kv failed", "error", err)
		return err
	}
	reply.Index = index
	reply.Key = args.Key
	reply.Value = args.Value
	return nil
}

func (n *Kv) GetKv(args *dto.KvDetailRequest, reply *dto.KvDetailResponse) error {
	if done, err := n.srv.forward("Kv.GetKv", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "kv", "detail"}, time.Now())

	if args.Key == "" {
		return fmt.Errorf("missing key for detail")
	}
	// if it is exist
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	out, err := snap.KvByKey(ws, args.Key)
	if err != nil {
		return err
	}
	if out == nil {
		return fmt.Errorf("the kv key [%s] not exists", args.Key)
	}
	reply.Kv = out
	return nil
}

func (n *Kv) DeleteKv(args *dto.KvDetailRequest, reply *dto.KvDeleteResponse) error {
	if done, err := n.srv.forward("Kv.DeleteKv", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "kv", "delete"}, time.Now())

	if args.Key == "" {
		return fmt.Errorf("missing key for detail")
	}
	// if it is exist
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	out, err := snap.KvByKey(ws, args.Key)
	if err != nil {
		return err
	}
	if out == nil {
		return fmt.Errorf("the kv key [%s] not exists", args.Key)
	}
	_, _, err = n.srv.raftApply(constant.KvDeleteRequestType, args)
	if err != nil {
		n.srv.logger.Error("delete kv failed", "error", err)
		return err
	}
	return nil
}
