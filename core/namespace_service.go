package core

import (
	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/structs"
)

// Namespace endpoint is used for manipulating namespaces
type Namespace struct {
	srv    *Server
	logger hclog.Logger
}

// ListNamespaces is used to list the namespaces
func (n *Namespace) ListNamespaces(args *dto.NamespaceListRequest, reply *dto.NamespaceListResponse) error {
	if done, err := n.srv.forward("Namespace.ListNamespaces", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "namespace", "list_namespace"}, time.Now())

	// Setup the blocking query
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, s *state.StateStore) error {
			// Iterate over all the namespaces
			var err error
			var iter memdb.ResultIterator
			if prefix := args.QueryOptions.Prefix; prefix != "" {
				iter, err = s.NamespacesByNamePrefix(ws, prefix)
			} else {
				iter, err = s.Namespaces(ws)
			}
			if err != nil {
				return err
			}

			reply.Namespaces = nil
			for {
				raw := iter.Next()
				if raw == nil {
					break
				}
				ns := raw.(*structs.Namespace)

				// Only return namespaces allowed by acl
				reply.Namespaces = append(reply.Namespaces, ns)
			}

			// Use the last index that affected the namespace table
			index, err := s.Index(state.TableNamespaces)
			if err != nil {
				return err
			}

			// Ensure we never set the index to zero, otherwise a blocking query cannot be used.
			// We floor the index at one, since realistically the first write must have a higher index.
			if index == 0 {
				index = 1
			}
			reply.Index = index
			return nil
		}}
	return n.srv.blockingRPC(&opts)
}

func (n *Namespace) AddNamespace(args *dto.NamespaceAddRequest, reply *dto.SingleNamespaceResponse) error {
	if done, err := n.srv.forward("Namespace.AddNamespace", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "namespace", "add_namespace"}, time.Now())

	snap, err := n.srv.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	existingNamespace, err := snap.NamespaceByName(ws, args.Name)
	if err != nil {
		return err
	}
	if existingNamespace != nil {
		return structs.NewErr1100Exist("namespace", args.Name)
	}

	var raftArgs = dto.NamespaceUpsertRequest{
		Namespaces:   []*structs.Namespace{args.Namespace},
		WriteRequest: args.WriteRequest,
	}
	fsmErr, _, err := n.srv.raftApply(constant.NamespaceUpsertRequestType, raftArgs)
	if err, ok := fsmErr.(error); ok && err != nil {

		n.logger.Error("namespace upsert failed", "error", err, "fsm", true)
		return err
	}
	if err != nil {
		n.logger.Error("namespace upsert failed", "error", err, "raft", true)
		return err
	}

	snapa, err := n.srv.State().Snapshot()
	if err != nil {
		return err
	}
	wsa := memdb.NewWatchSet()
	ns, err := snapa.NamespaceByName(wsa, args.Name)

	reply.Namespace = ns

	return nil
}
