package core

import (
	"fmt"
	metrics "github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	memdb "github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"io"
	"sync"
	"time"
	"yunli.com/jobpool/client/plugins"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/scheduler"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/structs"
	timetable "yunli.com/jobpool/helper/timetable"
)

const (
	// timeTableGranularity is the granularity of index to time tracking
	timeTableGranularity = 5 * time.Minute

	// timeTableLimit is the maximum limit of our tracking
	timeTableLimit = 72 * time.Hour
)

// LogApplier is the definition of a function that can apply a Raft log
type LogApplier func(buf []byte, index uint64) interface{}

// LogAppliers is a mapping of the Raft MessageType to the appropriate log
// applier
type LogAppliers map[constant.MessageType]LogApplier

// SnapshotRestorer is the definition of a function that can apply a Raft log
type SnapshotRestorer func(restore *state.StateRestore, dec *codec.Decoder) error

// SnapshotRestorers is a mapping of the SnapshotType to the appropriate
// snapshot restorer.
type SnapshotRestorers map[constant.SnapshotType]SnapshotRestorer

// coreFSM implements a finite state machine that is used
// along with Raft to provide strong consistency. We implement
// this outside the Server to avoid exposing this outside the package.
type coreFSM struct {
	logger log.Logger

	evalBroker         *scheduler.EvalBroker
	blockedEvals       *scheduler.BlockedEvals
	jobRoadMap         *scheduler.JobRoadmap
	periodicDispatcher *scheduler.PeriodicDispatch
	jobDispatcher      plugins.PluginJobDispatcher

	state     *state.StateStore
	timetable *timetable.TimeTable

	// cfg is the FSM cfg
	config *FSMConfig

	// enterpriseAppliers holds the set of enterprise only LogAppliers
	enterpriseAppliers LogAppliers

	// enterpriseRestorers holds the set of enterprise only snapshot restorers
	enterpriseRestorers SnapshotRestorers

	// stateLock is only used to protect outside callers to State() from
	// racing with Restore(), which is called by Raft (it puts in a totally
	// new state store). Everything internal here is synchronized by the
	// Raft side, so doesn't need to lock this.
	stateLock sync.RWMutex
}

// stateSnapshot is used to provide a snapshot of the current
// state in a way that can be accessed concurrently with operations
// that may modify the live state.
type stateSnapshot struct {
	snap      *state.StateSnapshot
	timetable *timetable.TimeTable
}

// snapshotHeader is the first entry in our snapshot
type snapshotHeader struct {
}

// FSMConfig is used to configure the FSM
type FSMConfig struct {
	EvalBroker *scheduler.EvalBroker

	Blocked *scheduler.BlockedEvals

	JobRoadmap *scheduler.JobRoadmap

	// Logger is the logger used by the FSM
	Logger log.Logger

	// Region is the region of the server embedding the FSM
	Region string

	// EnableEventBroker specifies if the FSMs state store should enable
	// it's event publisher.
	EnableEventBroker bool

	Periodic *scheduler.PeriodicDispatch

	JobDispatcher plugins.PluginJobDispatcher

	// EventBufferSize is the amount of messages to hold in memory
	EventBufferSize int64
}

// NewFSM is used to construct a new FSM with a blank state.
func NewFSM(config *FSMConfig) (*coreFSM, error) {
	// Create a state store
	sconfig := &state.StateStoreConfig{
		Logger:          config.Logger,
		Region:          config.Region,
		EnablePublisher: config.EnableEventBroker,
		EventBufferSize: config.EventBufferSize,
	}
	state, err := state.NewStateStore(sconfig)
	if err != nil {
		return nil, err
	}

	fsm := &coreFSM{
		evalBroker:          config.EvalBroker,
		blockedEvals:        config.Blocked,
		jobRoadMap:          config.JobRoadmap,
		logger:              config.Logger.Named("fsm"),
		config:              config,
		periodicDispatcher:  config.Periodic,
		jobDispatcher:       config.JobDispatcher,
		state:               state,
		timetable:           timetable.NewTimeTable(timeTableGranularity, timeTableLimit),
		enterpriseAppliers:  make(map[constant.MessageType]LogApplier, 8),
		enterpriseRestorers: make(map[constant.SnapshotType]SnapshotRestorer, 8),
	}

	return fsm, nil
}

// Close is used to cleanup resources associated with the FSM
func (n *coreFSM) Close() error {
	n.state.StopEventBroker()
	return nil
}

func (n *coreFSM) Apply(log *raft.Log) interface{} {
	buf := log.Data
	msgType := constant.MessageType(buf[0])

	// Witness this write
	n.timetable.Witness(log.Index, time.Now().UTC())

	// Check if this message type should be ignored when unknown. This is
	// used so that new commands can be added with developer control if older
	// versions can safely ignore the command, or if they should crash.
	ignoreUnknown := false
	if msgType&constant.IgnoreUnknownTypeFlag == constant.IgnoreUnknownTypeFlag {
		msgType &= ^constant.IgnoreUnknownTypeFlag
		ignoreUnknown = true
	}

	switch msgType {
	case constant.NodeRegisterRequestType:
		return n.applyUpsertNode(msgType, buf[1:], log.Index)
	case constant.NodeDeregisterRequestType:
		return n.applyDeregisterNode(msgType, buf[1:], log.Index)
	case constant.NodeUpdateStatusRequestType:
		return n.applyStatusUpdate(msgType, buf[1:], log.Index)
	case constant.UpsertNodeEventsType:
		return n.applyUpsertNodeEvent(msgType, buf[1:], log.Index)
	case constant.NodeBatchDeregisterRequestType:
		return n.applyDeregisterNodeBatch(msgType, buf[1:], log.Index)
	case constant.ClusterMetadataRequestType:
		return n.applyClusterMetadata(buf[1:], log.Index)
	case constant.NamespaceUpsertRequestType:
		return n.applyNamespaceUpsert(buf[1:], log.Index)
	case constant.NamespaceDeleteRequestType:
		return n.applyNamespaceDelete(buf[1:], log.Index)
	case constant.KvUpsertRequestType:
		return n.applyKvUpsert(msgType, buf[1:], log.Index)
	case constant.KvDeleteRequestType:
		return n.applyKvDelete(buf[1:], log.Index)
	case constant.AllocUpdateRequestType:
		return n.applyAllocUpdate(msgType, buf[1:], log.Index)
	case constant.EventSinkUpsertRequestType,
		constant.EventSinkDeleteRequestType,
		constant.BatchEventSinkUpdateProgressType:
		return nil
	// start business
	case constant.PlanRegisterRequestType:
		return n.applyUpsertPlan(msgType, buf[1:], log.Index)
	case constant.PlanDeregisterRequestType:
		return n.applyDeregisterPlan(msgType, buf[1:], log.Index)
	case constant.PlanUpdateStatusRequestType:
		return n.applyUpdatePlanStatus(msgType, buf[1:], log.Index)
	case constant.EvalUpdateRequestType:
		return n.applyUpdateEval(msgType, buf[1:], log.Index)
	case constant.ApplyPlanResultsRequestType:
		return n.applyPlanResults(msgType, buf[1:], log.Index)
	case constant.JobRegisterRequestType:
		return n.applyUpsertJob(msgType, buf[1:], log.Index)
	case constant.JobUpdateStatusRequestType:
		return n.applyUpdateJobStatus(msgType, buf[1:], log.Index)
	case constant.JobDeleteRequestType:
		return n.applyDeleteJob(msgType, buf[1:], log.Index)
	case constant.AllocClientUpdateRequestType:
		return n.applyAllocClientUpdate(msgType, buf[1:], log.Index)
	case constant.AllocUpdateDesiredTransitionRequestType:
		return n.applyAllocUpdateDesiredTransition(msgType, buf[1:], log.Index)
	}

	n.logger.Info("mess type not known", "messageType", msgType)

	// Check enterprise only message types.
	if applier, ok := n.enterpriseAppliers[msgType]; ok {
		return applier(buf[1:], log.Index)
	}

	// We didn't match anything, either panic or ignore
	if ignoreUnknown {
		n.logger.Warn("ignoring unknown message type, upgrade to newer version", "msg_type", msgType)
		return nil
	}

	panic(fmt.Errorf("failed to apply request: %#v", buf))
}

func (n *coreFSM) applyClusterMetadata(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "cluster_meta"}, time.Now())

	var req structs.ClusterMetadata
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.ClusterSetMetadata(index, &req); err != nil {
		n.logger.Error("ClusterSetMetadata failed", "error", err)
		return err
	}

	n.logger.Trace("ClusterSetMetadata", "cluster_id", req.ClusterID, "create_time", req.CreateTime)

	return nil
}

func (n *coreFSM) applyUpsertNode(reqType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "register_node"}, time.Now())
	var req dto.NodeRegisterRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	// Handle upgrade paths
	req.Node.Canonicalize()

	if err := n.state.UpsertNode(reqType, index, req.Node); err != nil {
		n.logger.Error("UpsertNode failed", "error", err)
		return err
	}
	if req.Node.Status == constant.NodeStatusReady {
		n.blockedEvals.Unblock(req.Node.ComputedClass, index)
	}

	return nil
}

func (n *coreFSM) applyKvUpsert(reqType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "apply_kv_upsert"}, time.Now())
	var req dto.KvUpsertRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}
	if err := n.state.UpsertKv(reqType, index, req.Kv); err != nil {
		n.logger.Error("UpsertKv failed", "error", err)
		return err
	}
	return nil
}

func (n *coreFSM) applyKvDelete(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "apply_kv_delete"}, time.Now())
	var req dto.KvDetailRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.DeleteKv(index, req.Key); err != nil {
		n.logger.Error("DeleteKv failed", "error", err)
	}

	return nil
}

func (n *coreFSM) applyDeregisterNode(reqType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "deregister_node"}, time.Now())
	var req dto.NodeDeregisterRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.DeleteNode(reqType, index, []string{req.NodeID}); err != nil {
		n.logger.Error("DeleteNode failed", "error", err)
		return err
	}

	return nil
}

func (n *coreFSM) applyDeregisterNodeBatch(reqType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "batch_deregister_node"}, time.Now())
	var req dto.NodeBatchDeregisterRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.DeleteNode(reqType, index, req.NodeIDs); err != nil {
		n.logger.Error("DeleteNode failed", "error", err)
		return err
	}

	return nil
}

func (n *coreFSM) applyStatusUpdate(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "node_status_update"}, time.Now())
	var req dto.NodeUpdateStatusRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.UpdateNodeStatus(msgType, index, req.NodeID, req.Status, req.UpdatedAt, req.NodeEvent); err != nil {
		n.logger.Error("UpdateNodeStatus failed", "error", err)
		return err
	}

	// Unblock evals for the nodes computed node class if it is in a ready
	// state.
	if req.Status == constant.NodeStatusReady {
		ws := memdb.NewWatchSet()
		node, err := n.state.NodeByID(ws, req.NodeID)
		if err != nil {
			n.logger.Error("looking up node failed", "node_id", req.NodeID, "error", err)
			return err

		}
		n.blockedEvals.Unblock(node.ComputedClass, index)
	}

	return nil
}

// applyUpsertNodeEvent tracks the given node events.
func (n *coreFSM) applyUpsertNodeEvent(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "upsert_node_events"}, time.Now())
	var req dto.EmitNodeEventsRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode EmitNodeEventsRequest: %v", err))
	}

	if err := n.state.UpsertNodeEvents(msgType, index, req.NodeEvents); err != nil {
		n.logger.Error("failed to add node events", "error", err)
		return err
	}

	return nil
}

// applyNamespaceUpsert is used to upsert a set of namespaces
func (n *coreFSM) applyNamespaceUpsert(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "apply_namespace_upsert"}, time.Now())
	var req dto.NamespaceUpsertRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}
	if err := n.state.UpsertNamespaces(index, req.Namespaces); err != nil {
		n.logger.Error("UpsertNamespaces failed", "error", err)
		return err
	}
	return nil
}

// applyNamespaceDelete is used to delete a set of namespaces
func (n *coreFSM) applyNamespaceDelete(buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "apply_namespace_delete"}, time.Now())
	var req dto.NamespaceDeleteRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.DeleteNamespaces(index, req.Namespaces); err != nil {
		n.logger.Error("DeleteNamespaces failed", "error", err)
	}

	return nil
}

func (n *coreFSM) Snapshot() (raft.FSMSnapshot, error) {
	// Create a new snapshot
	snap, err := n.state.Snapshot()
	if err != nil {
		return nil, err
	}

	ns := &stateSnapshot{
		snap:      snap,
		timetable: n.timetable,
	}
	return ns, nil
}

func (n *coreFSM) Restore(old io.ReadCloser) error {
	defer old.Close()

	// Create a new state store
	config := &state.StateStoreConfig{
		Logger:          n.config.Logger,
		Region:          n.config.Region,
		EnablePublisher: n.config.EnableEventBroker,
		EventBufferSize: n.config.EventBufferSize,
	}
	newState, err := state.NewStateStore(config)
	if err != nil {
		return err
	}

	// Start the state restore
	restore, err := newState.Restore()
	if err != nil {
		return err
	}
	defer restore.Abort()

	// Create a decoder
	dec := codec.NewDecoder(old, structs.MsgpackHandle)

	// Read in the header
	var header snapshotHeader
	if err := dec.Decode(&header); err != nil {
		return err
	}

	// Populate the new state
	msgType := make([]byte, 1)
	for {
		// Read the message type
		_, err := old.Read(msgType)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Decode
		snapType := constant.SnapshotType(msgType[0])
		switch snapType {
		case constant.TimeTableSnapshot:
			if err := n.timetable.Deserialize(dec); err != nil {
				return fmt.Errorf("time table deserialize failed: %v", err)
			}

		case constant.NodeSnapshot:
			node := new(structs.Node)
			if err := dec.Decode(node); err != nil {
				return err
			}

			// Handle upgrade paths
			node.Canonicalize()

			if err := restore.NodeRestore(node); err != nil {
				return err
			}

		case constant.IndexSnapshot:
			idx := new(state.IndexEntry)
			if err := dec.Decode(idx); err != nil {
				return err
			}
			if err := restore.IndexRestore(idx); err != nil {
				return err
			}

		case constant.ClusterMetadataSnapshot:
			meta := new(structs.ClusterMetadata)
			if err := dec.Decode(meta); err != nil {
				return err
			}
			if err := restore.ClusterMetadataRestore(meta); err != nil {
				return err
			}

		case constant.NamespaceSnapshot:
			namespace := new(structs.Namespace)
			if err := dec.Decode(namespace); err != nil {
				return err
			}
			if err := restore.NamespaceRestore(namespace); err != nil {
				return err
			}
		case constant.KvSnapshot:
			kv := new(structs.Kv)
			if err := dec.Decode(kv); err != nil {
				return err
			}
			if err := restore.KvRestore(kv); err != nil {
				return err
			}

		case constant.PeriodicLaunchSnapshot:
			launch := new(structs.PeriodicLaunch)
			if err := dec.Decode(launch); err != nil {
				return err
			}
			if err := restore.PeriodicLaunchRestore(launch); err != nil {
				return err
			}
		case constant.PlanSnapshot:
			plan := new(structs.Plan)
			if err := dec.Decode(plan); err != nil {
				return err
			}
			plan.Canonicalize()
			if err := restore.PlanRestore(plan); err != nil {
				return err
			}
		case constant.EvalSnapshot:
			eval := new(structs.Evaluation)
			if err := dec.Decode(eval); err != nil {
				return err
			}
			if err := restore.EvalRestore(eval); err != nil {
				return err
			}
		case constant.AllocSnapshot:
			alloc := new(structs.Allocation)
			if err := dec.Decode(alloc); err != nil {
				return err
			}
			// Handle upgrade path
			alloc.Canonicalize()
			if err := restore.AllocRestore(alloc); err != nil {
				return err
			}
		case constant.JobSnapshot:
			job := new(structs.Job)
			if err := dec.Decode(job); err != nil {
				return err
			}
			if err := restore.JobRestore(job); err != nil {
				return err
			}
		case constant.EventSinkSnapshot:
			return nil
		default:
			// Check if this is an enterprise only object being restored
			restorer, ok := n.enterpriseRestorers[snapType]
			if !ok {
				return fmt.Errorf("Unrecognized snapshot type: %v", msgType)
			}

			// Restore the enterprise only object
			if err := restorer(restore, dec); err != nil {
				return err
			}
		}
	}

	if err := restore.Commit(); err != nil {
		return err
	}

	// External code might be calling State(), so we need to synchronize
	// here to make sure we swap in the new state store atomically.
	n.stateLock.Lock()
	stateOld := n.state
	n.state = newState
	n.stateLock.Unlock()

	// Signal that the old state store has been abandoned. This is required
	// because we don't operate on it any more, we just throw it away, so
	// blocking queries won't see any changes and need to be woken up.
	stateOld.Abandon()

	return nil
}

func (s *stateSnapshot) Persist(sink raft.SnapshotSink) error {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "persist"}, time.Now())
	// Register the nodes
	encoder := codec.NewEncoder(sink, structs.MsgpackHandle)

	// Write the header
	header := snapshotHeader{}
	if err := encoder.Encode(&header); err != nil {
		sink.Cancel()
		return err
	}

	// Write the time table
	sink.Write([]byte{byte(constant.TimeTableSnapshot)})
	if err := s.timetable.Serialize(encoder); err != nil {
		sink.Cancel()
		return err
	}

	// Write all the data out
	if err := s.persistIndexes(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistNodes(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistNamespaces(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistKvs(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistClusterMetadata(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistPlans(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistJobs(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistEvals(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistAllocs(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	if err := s.persistPeriodicLaunches(sink, encoder); err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

func (s *stateSnapshot) persistIndexes(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	// Get all the indexes
	iter, err := s.snap.Indexes()
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := iter.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		idx := raw.(*state.IndexEntry)

		// Write out a node registration
		sink.Write([]byte{byte(constant.IndexSnapshot)})
		if err := encoder.Encode(idx); err != nil {
			return err
		}
	}
	return nil
}

func (s *stateSnapshot) persistNodes(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	// Get all the nodes
	ws := memdb.NewWatchSet()
	nodes, err := s.snap.Nodes(ws)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := nodes.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		node := raw.(*structs.Node)

		// Write out a node registration
		sink.Write([]byte{byte(constant.NodeSnapshot)})
		if err := encoder.Encode(node); err != nil {
			return err
		}
	}
	return nil
}

// persistNamespaces persists all the namespaces.
func (s *stateSnapshot) persistNamespaces(sink raft.SnapshotSink, encoder *codec.Encoder) error {
	ws := memdb.NewWatchSet()
	namespaces, err := s.snap.Namespaces(ws)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := namespaces.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		namespace := raw.(*structs.Namespace)

		// Write out a namespace registration
		sink.Write([]byte{byte(constant.NamespaceSnapshot)})
		if err := encoder.Encode(namespace); err != nil {
			return err
		}
	}
	return nil
}

func (s *stateSnapshot) persistKvs(sink raft.SnapshotSink, encoder *codec.Encoder) error {
	ws := memdb.NewWatchSet()
	kvs, err := s.snap.Kvs(ws, state.SortDefault)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := kvs.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		kv := raw.(*structs.Kv)
		// Write out a namespace registration
		sink.Write([]byte{byte(constant.KvSnapshot)})
		if err := encoder.Encode(kv); err != nil {
			return err
		}
	}
	return nil
}

func (s *stateSnapshot) persistClusterMetadata(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {

	// Get the cluster metadata
	ws := memdb.NewWatchSet()
	clusterMetadata, err := s.snap.ClusterMetadata(ws)
	if err != nil {
		return err
	}
	if clusterMetadata == nil {
		return nil
	}

	// Write out the cluster metadata
	sink.Write([]byte{byte(constant.ClusterMetadataSnapshot)})
	if err := encoder.Encode(clusterMetadata); err != nil {
		return err
	}

	return nil
}

func (s *stateSnapshot) persistPlans(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	ws := memdb.NewWatchSet()
	plans, err := s.snap.Plans(ws)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := plans.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		plan := raw.(*structs.Plan)

		// Write out a plan registration
		sink.Write([]byte{byte(constant.PlanSnapshot)})
		if err := encoder.Encode(plan); err != nil {
			return err
		}
	}
	return nil
}

func (s *stateSnapshot) persistJobs(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	ws := memdb.NewWatchSet()
	sort := state.SortOption(true)
	jobs, err := s.snap.Jobs(ws, sort)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := jobs.Next()
		if raw == nil {
			break
		}
		// Prepare the request struct
		job := raw.(*structs.Job)
		sink.Write([]byte{byte(constant.JobSnapshot)})
		if err := encoder.Encode(job); err != nil {
			return err
		}
	}
	return nil
}

func (s *stateSnapshot) persistEvals(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	// Get all the evaluations
	ws := memdb.NewWatchSet()
	evals, err := s.snap.Evals(ws, false)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := evals.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		eval := raw.(*structs.Evaluation)

		// Write out the evaluation
		sink.Write([]byte{byte(constant.EvalSnapshot)})
		if err := encoder.Encode(eval); err != nil {
			return err
		}
	}
	return nil
}

func (s *stateSnapshot) persistAllocs(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	// Get all the allocations
	ws := memdb.NewWatchSet()
	allocs, err := s.snap.Allocs(ws)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := allocs.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		alloc := raw.(*structs.Allocation)

		// Write out the evaluation
		sink.Write([]byte{byte(constant.AllocSnapshot)})
		if err := encoder.Encode(alloc); err != nil {
			return err
		}
	}
	return nil
}
func (s *stateSnapshot) persistPeriodicLaunches(sink raft.SnapshotSink,
	encoder *codec.Encoder) error {
	// Get all the jobs
	ws := memdb.NewWatchSet()
	launches, err := s.snap.PeriodicLaunches(ws)
	if err != nil {
		return err
	}

	for {
		// Get the next item
		raw := launches.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		launch := raw.(*structs.PeriodicLaunch)

		// Write out a job registration
		sink.Write([]byte{byte(constant.PeriodicLaunchSnapshot)})
		if err := encoder.Encode(launch); err != nil {
			return err
		}
	}
	return nil
}

// State is used to return a handle to the current state
func (n *coreFSM) State() *state.StateStore {
	n.stateLock.RLock()
	defer n.stateLock.RUnlock()
	return n.state
}

// TimeTable returns the time table of transactions
func (n *coreFSM) TimeTable() *timetable.TimeTable {
	return n.timetable
}

func (n *coreFSM) applyUpsertPlan(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "register_plan"}, time.Now())
	var req dto.PlanAddRequest

	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	// info, _ := json.Marshal(req)
	// n.logger.Info(fmt.Sprintf("the plan json is : %s", info))
	plan := req.PlanAddDto.ConvertPlan()
	if err := n.state.UpsertPlan(msgType, index, plan); err != nil {
		n.logger.Error("UpsertPlan failed", "error", err)
		return err
	}

	// We always add the plan to the periodic dispatcher because there is the
	// possibility that the periodic spec was removed and then we should stop
	// tracking it.
	ws := memdb.NewWatchSet()
	dispatcherPlan, err := n.state.PlanByID(ws, plan.Namespace, plan.ID)
	if err != nil {
		return err
	}
	if err := n.periodicDispatcher.Add(dispatcherPlan); err != nil {
		n.logger.Error("periodicDispatcher.Add failed", "error", err)
		return fmt.Errorf("failed adding plan to periodic dispatcher: %v", err)
	}

	// If it is an active periodic plan, record the time it was inserted. This is
	// necessary for recovering during leader election. It is possible that from
	// the time it is added to when it was suppose to launch, leader election
	// occurs and the plan was not launched. In this case, we use the insertion
	// time to determine if a launch was missed.
	if dispatcherPlan.IsPeriodicActive() {
		prevLaunch, err := n.state.PeriodicLaunchByID(ws, plan.Namespace, plan.ID)
		if err != nil {
			n.logger.Error("PeriodicLaunchByID failed", "error", err)
			return err
		}

		// Record the insertion time as a launch. We overload the launch table
		// such that the first entry is the insertion time.
		if prevLaunch == nil {
			launch := &structs.PeriodicLaunch{
				ID:        plan.ID,
				Namespace: plan.Namespace,
				Launch:    time.Now(),
			}
			if err := n.state.UpsertPeriodicLaunch(index, launch); err != nil {
				n.logger.Error("UpsertPeriodicLaunch failed", "error", err)
				return err
			}
		}
	}
	return nil
}

func (n *coreFSM) applyDeregisterPlan(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "deregister_plan"}, time.Now())
	var req dto.PlanDeregisterRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	err := n.state.WithWriteTransaction(msgType, index, func(tx state.Txn) error {
		err := n.handlePlanDeregister(index, req.PlanID, req.Namespace, tx)
		if err != nil {
			n.logger.Error("deregistering plan failed",
				"error", err, "plan", req.PlanID, "namespace", req.Namespace)
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (n *coreFSM) applyUpdatePlanStatus(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "update_plan"}, time.Now())
	var req dto.PlanOnlineOfflineRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	err := n.state.WithWriteTransaction(msgType, index, func(tx state.Txn) error {
		err := n.handlePlanStatusUpdate(index, req, tx)
		if err != nil {
			n.logger.Error("update plan failed",
				"error", err, "plan", req.PlanID, "namespace", req.Namespace)
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// handlePlanDeregister is used to deregister a plan. Leaves error logging up to
// caller.
func (n *coreFSM) handlePlanDeregister(index uint64, planID, namespace string, tx state.Txn) error {
	// If it is periodic remove it from the dispatcher
	if err := n.periodicDispatcher.Remove(namespace, planID); err != nil {
		return fmt.Errorf("periodicDispatcher.Remove failed: %w", err)
	}
	if err := n.state.DeletePlanTxn(index, namespace, planID, tx); err != nil {
		return fmt.Errorf("DeleteJob failed: %w", err)
	}
	// We always delete from the periodic launch table because it is possible that
	// the plan was updated to be non-periodic, thus checking if it is periodic
	// doesn't ensure we clean it up properly.
	n.state.DeletePeriodicLaunchTxn(index, namespace, planID, tx)
	return nil
}

func (n *coreFSM) handlePlanStatusUpdate(index uint64, req dto.PlanOnlineOfflineRequest, tx state.Txn) error {
	// Get the current plan and mark it as stopped and re-insert it.
	ws := memdb.NewWatchSet()
	current, err := n.state.PlanByIDTxn(ws, req.Namespace, req.PlanID, tx)
	if err != nil {
		return fmt.Errorf("PlanByID lookup failed: %w", err)
	}

	if current == nil {
		return fmt.Errorf("plan %q in namespace %q doesn't exist to be update", req.PlanID, req.Namespace)
	}
	planExisted := current.Copy()
	if constant.PlanStatusDead == req.Status {
		// If it is periodic remove it from the dispatcher
		if err := n.periodicDispatcher.Remove(req.Namespace, req.PlanID); err != nil {
			return fmt.Errorf("periodicDispatcher.Remove failed: %w", err)
		}
		planExisted.Stop = true
		if req.Reason != "" && planExisted.Description == "" {
			planExisted.Description = fmt.Sprintf("%s%s", planExisted.Description, req.Reason)
		}
	} else if constant.PlanStatusRunning == req.Status {
		planExisted.Stop = false
		if err := n.periodicDispatcher.Add(planExisted); err != nil {
			n.logger.Error("periodicDispatcher.Add failed", "error", err)
			return fmt.Errorf("failed adding plan to periodic dispatcher: %v", err)
		}
	}

	if err := n.state.UpsertPlanTxn(index, planExisted, tx); err != nil {
		return fmt.Errorf("UpsertPlan failed: %w", err)
	}
	return nil
}

// Release is a no-op, as we just need to GC the pointer
// to the state store snapshot. There is nothing to explicitly
// cleanup.
func (s *stateSnapshot) Release() {}

func (n *coreFSM) applyAllocUpdate(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"jobpool", "fsm", "alloc_update"}, time.Now())
	var req dto.AllocUpdateRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	// Attach the plan to all the allocations. It is pulled out in the
	// payload to avoid the redundancy of encoding, but should be denormalized
	// prior to being inserted into MemDB.
	structs.DenormalizeAllocationPlans(req.Plan, req.Alloc)
	if err := n.state.UpsertAllocs(msgType, index, req.Alloc); err != nil {
		n.logger.Error("UpsertAllocs failed", "error", err)
		return err
	}
	return nil
}

func (n *coreFSM) applyUpdateEval(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"jobpool", "fsm", "update_eval"}, time.Now())
	var req dto.EvalUpdateRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	return n.upsertEvals(msgType, index, req.Evals)
}

func (n *coreFSM) upsertEvals(msgType constant.MessageType, index uint64, evals []*structs.Evaluation) error {
	if err := n.state.UpsertEvals(msgType, index, evals); err != nil {
		n.logger.Error("UpsertEvals failed", "error", err)
		return err
	}
	n.handleUpsertedEvals(evals, msgType, index)
	return nil
}

func (n *coreFSM) handleUpsertedEval(eval *structs.Evaluation) {
	if eval == nil {
		return
	}

	// n.logger.Info("---- handleUpsertedEval----", "eval", eval, "ShouldEnqueue", eval.ShouldEnqueue())
	if eval.ShouldEnqueue() {
		n.evalBroker.Enqueue(eval)
		n.jobRoadMap.Enqueue(eval)
	} else if eval.ShouldBlock() {
		n.blockedEvals.Block(eval)
		n.jobRoadMap.Enqueue(eval)
	} else if eval.Status == constant.EvalStatusComplete {
		// n.logger.Info("todo handle upsert eval to deal with complete evals")
		n.blockedEvals.Untrack(eval.PlanID, eval.Namespace)
	}
}

// applyPlanApply applies the results of a plan application
func (n *coreFSM) applyPlanResults(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{"jobpool", "fsm", "apply_plan_results"}, time.Now())
	var req dto.ApplyPlanResultsRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.UpsertPlanResults(msgType, index, &req); err != nil {
		n.logger.Error("ApplyPlan failed", "error", err)
		return err
	}

	// Add evals for plans that were preempted
	n.handleUpsertedEvals(req.PreemptionEvals, msgType, index)
	return nil
}

func (n *coreFSM) handleUpsertedEvals(evals []*structs.Evaluation, msgType constant.MessageType, index uint64) {
	ws := memdb.NewWatchSet()
	for _, eval := range evals {
		// 如果槽位不足直接拒绝请求，不进入队列
		if !n.jobRoadMap.HasEmptySlot(eval.Namespace) {
			n.logger.Warn("the slot of jobRoadMap is full, reject the new job")
			// update status of eval and alloc and so on
			plan, err := n.state.PlanByID(ws, eval.Namespace, eval.PlanID)
			if err != nil {
				n.logger.Error("get plan info failed", "error", err)
				continue
			}
			if plan == nil {
				n.logger.Error("get plan info failed, the plan is empty", "plan id", eval.PlanID)
				continue
			}
			n.jobDispatcher.DispatchNoSlotJob(eval.PlanID, eval.JobID, plan.Parameters)
			// update the eval status
			if err := n.state.UpdateAllocsStatusFailed(msgType, index, eval); err != nil {
				n.logger.Error("update eval status failed", "error", err)
				return
			}
			continue
		}
		n.handleUpsertedEval(eval)
	}
}

func (n *coreFSM) applyUpsertJob(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "register_job"}, time.Now())
	var req dto.JobRegisterRequest

	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	//info, _ := json.Marshal(req)
	//n.logger.Info(fmt.Sprintf("the plan json is : %s", info))

	if err := n.state.UpsertJob(msgType, index, req.Job); err != nil {
		n.logger.Error("Upsert Job failed", "error", err)
		return err
	}

	// We always add the plan to the periodic dispatcher because there is the
	// possibility that the periodic spec was removed and then we should stop
	// tracking it.
	if err := n.periodicDispatcher.Add(req.Plan); err != nil {
		n.logger.Error("periodicDispatcher.Add failed", "error", err)
		return fmt.Errorf("failed adding plan to periodic dispatcher: %v", err)
	}

	// Create a watch set
	ws := memdb.NewWatchSet()

	// If it is an active periodic plan, record the time it was inserted. This is
	// necessary for recovering during leader election. It is possible that from
	// the time it is added to when it was suppose to launch, leader election
	// occurs and the plan was not launched. In this case, we use the insertion
	// time to determine if a launch was missed.
	if req.Plan.IsPeriodicActive() {
		prevLaunch, err := n.state.PeriodicLaunchByID(ws, req.Namespace, req.Plan.ID)
		if err != nil {
			n.logger.Error("PeriodicLaunchByID failed", "error", err)
			return err
		}

		// Record the insertion time as a launch. We overload the launch table
		// such that the first entry is the insertion time.
		if prevLaunch == nil {
			launch := &structs.PeriodicLaunch{
				ID:        req.Plan.ID,
				Namespace: req.Namespace,
				Launch:    time.Now(),
			}
			if err := n.state.UpsertPeriodicLaunch(index, launch); err != nil {
				n.logger.Error("UpsertPeriodicLaunch failed", "error", err)
				return err
			}
		}
	}

	// TODO 解决流程依赖问题
	// Check if the parent plan is periodic and mark the launch time.
	parentID := req.Plan.ParentID
	if parentID != "" {
		parent, err := n.state.PlanByID(ws, req.Namespace, parentID)
		if err != nil {
			n.logger.Error("PlanByID lookup for parent failed", "parent_id", parentID, "namespace", req.Namespace, "error", err)
			return err
		} else if parent == nil {
			// The parent has been deregistered.
			return nil
		}

		if parent.IsPeriodic() && !parent.IsParameterized() {
			t, err := n.periodicDispatcher.LaunchTime(req.Plan.ID)
			if err != nil {
				n.logger.Error("LaunchTime failed", "plan", req.Plan.NamespacedID(), "error", err)
				return err
			}

			launch := &structs.PeriodicLaunch{
				ID:        parentID,
				Namespace: req.Namespace,
				Launch:    t,
			}
			if err := n.state.UpsertPeriodicLaunch(index, launch); err != nil {
				n.logger.Error("UpsertPeriodicLaunch failed", "error", err)
				return err
			}
		}
	}
	return nil
}

func (n *coreFSM) applyDeleteJob(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "delete_job"}, time.Now())
	var req dto.JobDeleteRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	err := n.state.WithWriteTransaction(msgType, index, func(tx state.Txn) error {
		err := n.handleJobDelete(index, req.JobID, req.Namespace, tx)
		if err != nil {
			n.logger.Error("deregistering job failed",
				"error", err, "job", req.JobID, "namespace", req.Namespace)
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (n *coreFSM) handleJobDelete(index uint64, jobId, namespace string, tx state.Txn) error {
	if err := n.state.DeleteJobTxn(index, namespace, jobId, tx); err != nil {
		return fmt.Errorf("DeleteJob failed: %w", err)
	}
	return nil
}

func (n *coreFSM) applyUpdateJobStatus(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "job_status_update"}, time.Now())
	var req dto.JobStatusUpdateRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}
	err := n.state.WithWriteTransaction(msgType, index, func(tx state.Txn) error {
		if err := n.state.UpdateJobStatus(index, req.Namespace, req.JobID, req.Status, req.Info, tx); err != nil {
			n.logger.Error("update job status failed", "error", err)
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// applyAllocUpdateDesiredTransition is used to update the desired transitions
// of a set of allocations.
func (n *coreFSM) applyAllocUpdateDesiredTransition(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "alloc_update_desired_transition"}, time.Now())
	var req dto.AllocUpdateDesiredTransitionRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := n.state.UpdateAllocsDesiredTransitions(msgType, index, req.Allocs, req.Evals); err != nil {
		n.logger.Error("UpdateAllocsDesiredTransitions failed", "error", err)
		return err
	}

	n.handleUpsertedEvals(req.Evals, msgType, index)
	return nil
}

func (n *coreFSM) applyAllocClientUpdate(msgType constant.MessageType, buf []byte, index uint64) interface{} {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "fsm", "alloc_client_update"}, time.Now())
	var req dto.AllocUpdateRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}
	if len(req.Alloc) == 0 {
		return nil
	}

	// Create a watch set
	ws := memdb.NewWatchSet()

	// Updating the allocs with the plan id and job group name
	for _, alloc := range req.Alloc {
		if existing, _ := n.state.AllocByID(ws, alloc.ID); existing != nil {
			alloc.PlanID = existing.PlanID
		}
	}

	// Update all the client allocations
	if err := n.state.UpdateAllocsFromClient(msgType, index, req.Alloc); err != nil {
		n.logger.Error("UpdateAllocFromClient failed", "error", err)
		return err
	}

	// Update any evals
	if len(req.Evals) > 0 {
		if err := n.upsertEvals(msgType, index, req.Evals); err != nil {
			n.logger.Error("applyAllocClientUpdate failed to update evaluations", "error", err)
			return err
		}
	}

	// Unblock evals for the nodes computed node class if the client has
	// finished running an allocation.
	for _, alloc := range req.Alloc {
		if alloc.ClientStatus == constant.AllocClientStatusComplete ||
			alloc.ClientStatus == constant.AllocClientStatusFailed ||
			alloc.ClientStatus == constant.AllocClientStatusExpired {
			nodeID := alloc.NodeID
			node, err := n.state.NodeByID(ws, nodeID)
			if err != nil || node == nil {
				n.logger.Error("looking up node failed", "node_id", nodeID, "error", err)
				return err
			}
			if err := n.jobRoadMap.DequeueEval(alloc.Namespace, alloc.JobId); err != nil {
				n.logger.Error("dequeue eval in fsm failed", "jobId", alloc.JobId, "error", err)
			}
			// Unblock any associated quota
			// TODO
			//quota, err := n.allocQuota(alloc.ID)
			//if err != nil {
			//	n.logger.Error("looking up quota associated with alloc failed", "alloc_id", alloc.ID, "error", err)
			//	return err
			//}
			//n.blockedEvals.UnblockClassAndQuota(node.ComputedClass, quota, index)
			//n.blockedEvals.UnblockNode(node.ID, index)
		}
		if alloc.ClientStatus == constant.AllocClientStatusRunning {
			if err := n.jobRoadMap.JobStartRunning(alloc.Namespace, alloc.JobId); err != nil {
				n.logger.Error("change running eval in fsm failed", "jobId", alloc.JobId, "error", err)
			}
		}
	}

	return nil
}
