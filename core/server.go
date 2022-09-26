package core

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/hashicorp/serf/serf"
	"go.etcd.io/bbolt"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"yunli.com/jobpool/client/plugins"
	"yunli.com/jobpool/client/plugins/rest"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/drainer"
	"yunli.com/jobpool/core/scheduler"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper"
	"yunli.com/jobpool/helper/codec"
	"yunli.com/jobpool/helper/pool"
)

type Server struct {
	config *Config

	logger log.InterceptLogger

	// Connection pool to other Jobpool servers
	connPool *pool.ConnPool

	raft          *raft.Raft
	raftLayer     *RaftLayer
	raftStore     *raftboltdb.BoltStore
	raftInmem     *raft.InmemStore
	raftTransport *raft.NetworkTransport

	// reassertLeaderCh is used to signal that the leader loop must
	// re-establish leadership.
	//
	// This might be relevant in snapshot restores, where leader in-memory
	// state changed significantly such that leader state (e.g. periodic
	// plans, eval brokers) need to be recomputed.
	reassertLeaderCh chan chan error

	// fsm is the state machine used with Raft
	fsm *coreFSM

	// rpcListener is used to listen for incoming connections
	rpcListener net.Listener
	listenerCh  chan struct{}

	// rpcHandler is used to serve and handle RPCs
	*rpcHandler

	// rpcServer is the static RPC server that is used by the local agent.
	rpcServer *rpc.Server

	// clientRpcAdvertise is the advertised RPC address for Jobpool clients to connect
	// to this server
	clientRpcAdvertise net.Addr

	// serverRpcAdvertise is the advertised RPC address for Jobpool servers to connect
	// to this server
	serverRpcAdvertise net.Addr

	// rpcTLS is the TLS cfg for incoming TLS requests
	rpcTLS    *tls.Config
	rpcCancel context.CancelFunc

	// staticEndpoints is the set of static endpoints that can be reused across
	// all RPC connections
	staticEndpoints endpoints

	// streamingRpcs is the registry holding our streaming RPC handlers.
	streamingRpcs *structs.StreamingRpcRegistry

	// nodeConns is the set of multiplexed node connections we have keyed by
	// NodeID
	nodeConns     map[string][]*nodeConnState
	nodeConnsLock sync.RWMutex

	// peers is used to track the known Jobpool servers. This is
	// used for region forwarding and clustering.
	peers      map[string][]*serverParts
	localPeers map[raft.ServerAddress]*serverParts
	peerLock   sync.RWMutex

	// serf is the Serf cluster containing only Jobpool
	// servers. This is used for multi-region federation
	// and automatic clustering within regions.
	serf *serf.Serf

	nodeDrainer *drainer.NodeDrainer

	periodicDispatcher *scheduler.PeriodicDispatch

	jobDispatcher plugins.PluginJobDispatcher

	// reconcileCh is used to pass events from the serf handler
	// into the leader manager. Mostly used to handle when servers
	// join/leave from the region.
	reconcileCh chan serf.Member

	// used to track when the server is ready to serve consistent reads, updated atomically
	readyForConsistentReads int32

	// eventCh is used to receive events from the serf cluster
	eventCh chan serf.Event

	blockedEvals *scheduler.BlockedEvals

	*planner

	workerLock sync.RWMutex
	workers    []*Worker

	// evalBroker is used to manage the in-progress evaluations
	// that are waiting to be brokered to a sub-scheduler
	evalBroker *scheduler.EvalBroker

	// 任务地图
	jobRoadMap *scheduler.JobRoadmap

	// nodeHeartbeater is used to track expiration times of node heartbeats. If it
	// detects an expired node, the node status is updated to be 'down'.
	*nodeHeartbeater

	// clusterIDLock ensures the server does not try to concurrently establish
	// a cluster ID, racing against itself in calls of ClusterID
	clusterIDLock sync.Mutex

	left         bool
	shutdown     bool
	shutdownLock sync.Mutex

	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	shutdownCh     <-chan struct{}
}

// Holds the RPC endpoints
type endpoints struct {
	Status    *Status
	Node      *Node
	Region    *Region
	Event     *Event
	Namespace *Namespace
	Kv        *Kv
	Plan      *Plan
	Job       *Job
	Alloc     *Alloc
	Operator  *Operator
	// Client endpoints
	ClientStats *ClientStats
}

func NewServer(config *Config) (*Server, error) {

	// Create the logger
	logger := config.Logger.ResetNamedIntercept(constant.JobPoolName)
	// Create the server
	s := &Server{
		config:           config,
		connPool:         pool.NewPool(logger, constant.ServerRPCCache, constant.ServerMaxStreams),
		logger:           logger,
		rpcServer:        rpc.NewServer(),
		streamingRpcs:    structs.NewStreamingRpcRegistry(),
		nodeConns:        make(map[string][]*nodeConnState),
		peers:            make(map[string][]*serverParts),
		localPeers:       make(map[raft.ServerAddress]*serverParts),
		reassertLeaderCh: make(chan chan error),
		reconcileCh:      make(chan serf.Member, 32),
		eventCh:          make(chan serf.Event, 256),
	}

	// Create an eval broker
	evalBroker, err := scheduler.NewEvalBroker(
		config.EvalNackTimeout,
		config.EvalNackInitialReenqueueDelay,
		config.EvalNackSubsequentReenqueueDelay,
		config.EvalDeliveryLimit)
	if err != nil {
		return nil, err
	}
	s.evalBroker = evalBroker
	s.blockedEvals = scheduler.NewBlockedEvals(evalBroker, logger)

	// TODO 配置默认size
	jobRoadMap, err := scheduler.NewJobRoadmap(config.JobMapSize, logger)
	if err != nil {
		return nil, err
	}
	s.jobRoadMap = jobRoadMap

	s.shutdownCtx, s.shutdownCancel = context.WithCancel(context.Background())
	s.shutdownCh = s.shutdownCtx.Done()

	// Create the RPC handler
	s.rpcHandler = newRpcHandler(s)

	// Create the planner
	planner, err := newPlanner(s)
	if err != nil {
		return nil, err
	}
	s.planner = planner

	// Create the node heartbeater
	s.nodeHeartbeater = newNodeHeartbeater(s)

	// Initialize the RPC layer
	err = s.setupRPC()
	if err != nil {
		s.Shutdown()
		s.logger.Error("failed to start RPC layer", "error", err)
		return nil, fmt.Errorf("Failed to start RPC layer: %v", err)
	}

	// 用来生成调度job
	s.periodicDispatcher = scheduler.NewPeriodicDispatch(s.logger, s)

	s.jobDispatcher = rest.NewDispatcher(nil, s.logger)

	// Initialize the Raft server
	if err := s.setupRaft(); err != nil {
		s.Shutdown()
		s.logger.Error("failed to start Raft", "error", err)
		return nil, fmt.Errorf("Failed to start Raft: %v", err)
	}
	// Initialize the wan Serf
	s.serf, err = s.setupSerf(config.SerfConfig, s.eventCh, constant.SerfSnapshot)
	if err != nil {
		s.Shutdown()
		s.logger.Error("failed to start serf WAN", "error", err)
		return nil, fmt.Errorf("Failed to start serf: %v", err)
	}

	// TODO worker 放在这里
	// Initialize the scheduling workers
	if err := s.setupWorkers(s.shutdownCtx); err != nil {
		s.Shutdown()
		s.logger.Error("failed to start workers", "error", err)
		return nil, fmt.Errorf("Failed to start workers: %v", err)
	}

	// Setup the node drainer.
	s.setupNodeDrainer()

	// Monitor leadership changes
	go s.monitorLeadership()

	// Start ingesting events for Serf
	go s.serfEventHandler()

	// start the RPC listener for the server
	s.startRPCListener()

	// Emit metrics for the eval broker
	// go evalBroker.EmitStats(time.Second, s.shutdownCh)

	// Emit metrics for the blocked eval tracker.
	// go s.blockedEvals.EmitStats(time.Second, s.shutdownCh)

	// Emit metrics
	go s.heartbeatStats()

	// Emit raft and state store metrics
	go s.EmitRaftStats(10*time.Second, s.shutdownCh)

	// Done
	return s, nil
}

// Join is used to have Jobpool join the gossip ring
// The target address should be another node listening on the
// Serf address
func (s *Server) Join(addrs []string) (int, error) {
	return s.serf.Join(addrs, true)
}

// LocalMember is used to return the local node
func (s *Server) LocalMember() serf.Member {
	return s.serf.LocalMember()
}

// Members is used to return the members of the serf cluster
func (s *Server) Members() []serf.Member {
	return s.serf.Members()
}

// RemoveFailedNode is used to remove a failed node from the cluster
func (s *Server) RemoveFailedNode(node string) error {
	return s.serf.RemoveFailedNode(node)
}

func (s *Server) Leave() error {
	s.logger.Info("server starting leave")
	s.left = true

	// Check the number of known peers
	numPeers, err := s.numPeers()
	if err != nil {
		s.logger.Error("failed to check raft peers during leave", "error", err)
		return err
	}

	addr := s.raftTransport.LocalAddr()

	// If we are the current leader, and we have any other peers (cluster has multiple
	// servers), we should do a RemovePeer to safely reduce the quorum size. If we are
	// not the leader, then we should issue our leave intention and wait to be removed
	// for some sane period of time.
	isLeader := s.IsLeader()
	if isLeader && numPeers > 1 {
		future := s.raft.RemoveServer(raft.ServerID(s.config.NodeID), 0, 0)
		if err := future.Error(); err != nil {
			s.logger.Error("failed to remove ourself as raft peer", "error", err)
		}
	}
	// Leave the gossip pool
	if s.serf != nil {
		if err := s.serf.Leave(); err != nil {
			s.logger.Error("failed to leave Serf cluster", "error", err)
		}
	}

	// If we were not leader, wait to be safely removed from the cluster.
	// We must wait to allow the raft replication to take place, otherwise
	// an immediate shutdown could cause a loss of quorum.
	if !isLeader {
		left := false
		limit := time.Now().Add(5 * time.Second)
		for !left && time.Now().Before(limit) {
			// Sleep a while before we check.
			time.Sleep(50 * time.Millisecond)

			// Get the latest configuration.
			future := s.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				s.logger.Error("failed to get raft configuration", "error", err)
				break
			}

			// See if we are no longer included.
			left = true
			for _, server := range future.Configuration().Servers {
				if server.Address == addr {
					left = false
					break
				}
			}
		}
		if !left {
			s.logger.Warn("failed to leave raft configuration gracefully, timeout")
		}
	}
	return nil
}

func (s *Server) numPeers() (int, error) {
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0, err
	}
	configuration := future.Configuration()
	return len(configuration.Servers), nil
}

// Regions returns the known regions in the cluster.
func (s *Server) Regions() []string {
	s.peerLock.RLock()
	defer s.peerLock.RUnlock()

	regions := make([]string, 0, len(s.peers))
	for region := range s.peers {
		regions = append(regions, region)
	}
	sort.Strings(regions)
	return regions
}

// RPC is used to make a local RPC call
func (s *Server) RPC(method string, args interface{}, reply interface{}) error {
	codec := &codec.InmemCodec{
		Method: method,
		Args:   args,
		Reply:  reply,
	}
	if err := s.rpcServer.ServeRequest(codec); err != nil {
		return err
	}
	return codec.Err
}

// Region returns the region of the server
func (s *Server) Region() string {
	return s.config.Region
}

// State returns the underlying state store. This should *not*
// be used to modify state directly.
func (s *Server) State() *state.StateStore {
	return s.fsm.State()
}

// setupRpcServer is used to populate an RPC server with endpoints
func (s *Server) setupRpcServer(server *rpc.Server, ctx *RPCContext) {
	// Add the static endpoints to the RPC server.
	if s.staticEndpoints.Status == nil {
		// Initialize the list just once
		s.staticEndpoints.Region = &Region{srv: s, logger: s.logger.Named("region")}
		s.staticEndpoints.Status = &Status{srv: s, logger: s.logger.Named("status")}
		s.staticEndpoints.Namespace = &Namespace{srv: s}
		s.staticEndpoints.Kv = &Kv{srv: s}
		s.staticEndpoints.Operator = &Operator{srv: s, logger: s.logger.Named("operator")}

		// These endpoints are dynamic because they need access to the
		// RPCContext, but they also need to be called directly in some cases,
		// so store them into staticEndpoints for later access, but don't
		// register them as static.
		s.staticEndpoints.Node = &Node{srv: s, logger: s.logger.Named("client")}

		// Client endpoints
		s.staticEndpoints.ClientStats = &ClientStats{srv: s, logger: s.logger.Named("client_stats")}

		// 暂时没有用到event服务
		s.staticEndpoints.Event = &Event{srv: s}
		s.staticEndpoints.Event.register()

		// 业务逻辑相关
		s.staticEndpoints.Plan = NewPlanEndpoints(s)
		s.staticEndpoints.Job = NewJobEndpoints(s)

	}

	// Register the static handlers
	server.Register(s.staticEndpoints.Region)
	server.Register(s.staticEndpoints.Status)
	// s.staticEndpoints.Enterprise.Register(server)
	server.Register(s.staticEndpoints.ClientStats)
	server.Register(s.staticEndpoints.Namespace)
	server.Register(s.staticEndpoints.Kv)
	server.Register(s.staticEndpoints.Operator)
	// Create new dynamic endpoints and add them to the RPC server.
	node := &Node{srv: s, ctx: ctx, logger: s.logger.Named("client")}
	// Register the dynamic endpoints
	server.Register(node)

	server.Register(s.staticEndpoints.Plan)
	server.Register(s.staticEndpoints.Job)
	alloc := &Alloc{srv: s, ctx: ctx, logger: s.logger.Named("alloc")}
	server.Register(alloc)
	eval := &Eval{srv: s, ctx: ctx, logger: s.logger.Named("eval")}
	server.Register(eval)

	planAllocService := &PlanAllocService{srv: s, ctx: ctx, logger: s.logger.Named("plan_alloc")}
	server.Register(planAllocService)

}

// IsLeader checks if this server is the cluster leader
func (s *Server) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// Atomically sets a readiness state flag when leadership is obtained, to indicate that server is past its barrier write
func (s *Server) setConsistentReadReady() {
	atomic.StoreInt32(&s.readyForConsistentReads, 1)
}

// Atomically reset readiness state flag on leadership revoke
func (s *Server) resetConsistentReadReady() {
	atomic.StoreInt32(&s.readyForConsistentReads, 0)
}

// Returns true if this server is ready to serve consistent reads
func (s *Server) isReadyForConsistentReads() bool {
	return atomic.LoadInt32(&s.readyForConsistentReads) == 1
}

// setupRPC is used to setup the RPC listener
func (s *Server) setupRPC() error {
	// Populate the static RPC server
	s.setupRpcServer(s.rpcServer, nil)

	listener, err := s.createRPCListener()
	if err != nil {
		listener.Close()
		return err
	}

	if s.config.ClientRPCAdvertise != nil {
		s.clientRpcAdvertise = s.config.ClientRPCAdvertise
	} else {
		s.clientRpcAdvertise = s.rpcListener.Addr()
	}

	// Verify that we have a usable advertise address
	clientAddr, ok := s.clientRpcAdvertise.(*net.TCPAddr)
	if !ok {
		listener.Close()
		return fmt.Errorf("Client RPC advertise address is not a TCP Address: %v", clientAddr)
	}
	if clientAddr.IP.IsUnspecified() {
		listener.Close()
		return fmt.Errorf("Client RPC advertise address is not advertisable: %v", clientAddr)
	}

	if s.config.ServerRPCAdvertise != nil {
		s.serverRpcAdvertise = s.config.ServerRPCAdvertise
	} else {
		// Default to the Serf Advertise + RPC Port
		serfIP := s.config.SerfConfig.MemberlistConfig.AdvertiseAddr
		if serfIP == "" {
			serfIP = s.config.SerfConfig.MemberlistConfig.BindAddr
		}

		addr := net.JoinHostPort(serfIP, fmt.Sprintf("%d", clientAddr.Port))
		resolved, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return fmt.Errorf("Failed to resolve Server RPC advertise address: %v", err)
		}

		s.serverRpcAdvertise = resolved
	}

	// Verify that we have a usable advertise address
	serverAddr, ok := s.serverRpcAdvertise.(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("Server RPC advertise address is not a TCP Address: %v", serverAddr)
	}
	if serverAddr.IP.IsUnspecified() {
		listener.Close()
		return fmt.Errorf("Server RPC advertise address is not advertisable: %v", serverAddr)
	}

	s.raftLayer = NewRaftLayer(s.serverRpcAdvertise)
	return nil
}

// createRPCListener creates the server's RPC listener
func (s *Server) createRPCListener() (*net.TCPListener, error) {
	s.listenerCh = make(chan struct{})
	listener, err := net.ListenTCP("tcp", s.config.RPCAddr)
	if err != nil {
		s.logger.Error("failed to initialize TLS listener", "error", err)
		return listener, err
	}

	s.rpcListener = listener
	return listener, nil
}

// Shutdown is used to shutdown the server
func (s *Server) Shutdown() error {
	s.logger.Info("shutting down server")
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.shutdown {
		return nil
	}

	s.shutdown = true
	s.shutdownCancel()

	if s.serf != nil {
		s.serf.Shutdown()
	}

	if s.raft != nil {
		s.raftTransport.Close()
		s.raftLayer.Close()
		future := s.raft.Shutdown()
		if err := future.Error(); err != nil {
			s.logger.Warn("error shutting down raft", "error", err)
		}
		if s.raftStore != nil {
			s.raftStore.Close()
		}
	}

	// Shutdown the RPC listener
	if s.rpcListener != nil {
		s.rpcListener.Close()
	}

	// Close the connection pool
	s.connPool.Shutdown()

	// Close the fsm
	if s.fsm != nil {
		s.fsm.Close()
	}
	return nil
}

// IsShutdown checks if the server is shutdown
func (s *Server) IsShutdown() bool {
	select {
	case <-s.shutdownCh:
		return true
	default:
		return false
	}
}

func (s *Server) isSingleServerCluster() bool {
	return s.config.BootstrapExpect == 1
}

// setupSerf is used to setup and initialize a Serf
func (s *Server) setupSerf(conf *serf.Config, ch chan serf.Event, path string) (*serf.Serf, error) {
	conf.Init()
	conf.NodeName = fmt.Sprintf("%s.%s", s.config.NodeName, s.config.Region)
	conf.Tags["role"] = constant.JobPoolName
	conf.Tags["region"] = s.config.Region
	conf.Tags["dc"] = s.config.Datacenter
	conf.Tags["build"] = s.config.Build
	conf.Tags["vsn"] = deprecatedAPIMajorVersionStr // for Jobpool <= v1.2 compat
	conf.Tags["raft_vsn"] = fmt.Sprintf("%d", s.config.RaftConfig.ProtocolVersion)
	conf.Tags["id"] = s.config.NodeID
	conf.Tags["rpc_addr"] = s.clientRpcAdvertise.(*net.TCPAddr).IP.String()         // Address that clients will use to RPC to servers
	conf.Tags["port"] = fmt.Sprintf("%d", s.serverRpcAdvertise.(*net.TCPAddr).Port) // Port servers use to RPC to one and another
	if s.isSingleServerCluster() {
		conf.Tags["bootstrap"] = "1"
	}
	bootstrapExpect := s.config.BootstrapExpect
	if bootstrapExpect != 0 {
		conf.Tags["expect"] = fmt.Sprintf("%d", bootstrapExpect)
	}
	if s.config.NonVoter {
		conf.Tags["nonvoter"] = "1"
	}
	logger := s.logger.StandardLogger(&log.StandardLoggerOptions{InferLevels: true})
	conf.MemberlistConfig.Logger = logger
	conf.Logger = logger
	conf.MemberlistConfig.LogOutput = nil
	conf.LogOutput = nil
	conf.EventCh = ch
	if !s.config.DevMode {
		conf.SnapshotPath = filepath.Join(s.config.DataDir, path)
		if err := ensurePath(conf.SnapshotPath, false); err != nil {
			return nil, err
		}
	}
	conf.RejoinAfterLeave = true
	// LeavePropagateDelay is used to make sure broadcasted leave intents propagate
	// This value was tuned using https://www.serf.io/docs/internals/simulator.html to
	// allow for convergence in 99.9% of nodes in a 10 node cluster
	conf.LeavePropagateDelay = 1 * time.Second
	conf.Merge = &serfMergeDelegate{}

	// Until Jobpool supports this fully, we disable automatic resolution.
	// When enabled, the Serf gossip may just turn off if we are the minority
	// node which is rather unexpected.
	conf.EnableNameConflictResolution = false
	return serf.Create(conf)
}

// setupNodeDrainer creates a node drainer which will be enabled when a server
// becomes a leader.
func (s *Server) setupNodeDrainer() {
	// Create a shim around Raft requests
	shim := drainerShim{s}
	c := &drainer.NodeDrainerConfig{
		Logger:                s.logger,
		Raft:                  shim,
		JobFactory:            drainer.GetDrainingJobWatcher,
		NodeFactory:           drainer.GetNodeWatcherFactory(),
		DrainDeadlineFactory:  drainer.GetDeadlineNotifier,
		StateQueriesPerSecond: drainer.LimitStateQueriesPerSecond,
		BatchUpdateInterval:   drainer.BatchUpdateInterval,
	}
	s.nodeDrainer = drainer.NewNodeDrainer(c)
}

// ClusterID returns the unique ID for this cluster.
//
// Any Jobpool server agent may call this method to get at the ID.
// If we are the leader and the ID has not yet been created, it will
// be created now. Otherwise an error is returned.
//
// The ID will not be created until all participating servers have reached
// a minimum version (0.10.4).
func (s *Server) ClusterID() (string, error) {
	s.clusterIDLock.Lock()
	defer s.clusterIDLock.Unlock()

	// try to load the cluster ID from state store
	fsmState := s.fsm.State()
	existingMeta, err := fsmState.ClusterMetadata(nil)
	if err != nil {
		s.logger.Named("core").Error("failed to get cluster ID", "error", err)
		return "", err
	}

	// got the cluster ID from state store, cache that and return it
	if existingMeta != nil && existingMeta.ClusterID != "" {
		return existingMeta.ClusterID, nil
	}

	// if we are not the leader, nothing more we can do
	if !s.IsLeader() {
		return "", errors.New("cluster ID not ready yet")
	}

	// we are the leader, try to generate the ID now
	generatedID, err := s.generateClusterID()
	if err != nil {
		return "", err
	}

	return generatedID, nil
}

// startRPCListener starts the server's the RPC listener
func (s *Server) startRPCListener() {
	ctx, cancel := context.WithCancel(context.Background())
	s.rpcCancel = cancel
	go s.listen(ctx)
}

// GetConfig returns the cfg of the server for testing purposes only
func (s *Server) GetConfig() *Config {
	return s.config
}

// EmitRaftStats is used to export metrics about raft indexes and state store snapshot index
func (s *Server) EmitRaftStats(period time.Duration, stopCh <-chan struct{}) {
	timer, stop := helper.NewSafeTimer(period)
	defer stop()

	for {
		timer.Reset(period)

		select {
		case <-timer.C:
			lastIndex := s.raft.LastIndex()
			metrics.SetGauge([]string{"raft", "lastIndex"}, float32(lastIndex))
			appliedIndex := s.raft.AppliedIndex()
			metrics.SetGauge([]string{"raft", "appliedIndex"}, float32(appliedIndex))
			stateStoreSnapshotIndex, err := s.State().LatestIndex()
			if err != nil {
				s.logger.Warn("Unable to read snapshot index from statestore, metric will not be emitted", "error", err)
			} else {
				metrics.SetGauge([]string{"state", "snapshotIndex"}, float32(stateStoreSnapshotIndex))
			}
		case <-stopCh:
			return
		}
	}
}

// setupRaft is used to setup and initialize Raft
func (s *Server) setupRaft() error {
	// If we have an unclean exit then attempt to close the Raft store.
	defer func() {
		if s.raft == nil && s.raftStore != nil {
			if err := s.raftStore.Close(); err != nil {
				s.logger.Error("failed to close Raft store", "error", err)
			}
		}
	}()

	// Create the FSM
	fsmConfig := &FSMConfig{
		EvalBroker:        s.evalBroker,
		Periodic:          s.periodicDispatcher,
		JobDispatcher:     s.jobDispatcher,
		Blocked:           s.blockedEvals,
		JobRoadmap:        s.jobRoadMap,
		Logger:            s.logger,
		Region:            s.Region(),
		EnableEventBroker: s.config.EnableEventBroker,
		EventBufferSize:   s.config.EventBufferSize,
	}
	var err error
	s.fsm, err = NewFSM(fsmConfig)
	if err != nil {
		return err
	}

	// Create a transport layer
	trans := raft.NewNetworkTransport(s.raftLayer, 3, s.config.RaftTimeout,
		s.config.LogOutput)
	s.raftTransport = trans

	// Make sure we set the Logger.
	s.config.RaftConfig.Logger = s.logger.Named("raft")
	s.config.RaftConfig.LogOutput = nil

	// Our version of Raft protocol 2 requires the LocalID to match the network
	// address of the transport. Raft protocol 3 uses permanent ids.
	s.config.RaftConfig.LocalID = raft.ServerID(trans.LocalAddr())
	if s.config.RaftConfig.ProtocolVersion >= 3 {
		s.config.RaftConfig.LocalID = raft.ServerID(s.config.NodeID)
	}

	// Build an all in-memory setup for dev mode, otherwise prepare a full
	// disk-based setup.
	var log raft.LogStore
	var stable raft.StableStore
	var snap raft.SnapshotStore
	if s.config.DevMode {
		store := raft.NewInmemStore()
		s.raftInmem = store
		stable = store
		log = store
		snap = raft.NewDiscardSnapshotStore()

	} else {
		// Create the base raft path
		path := filepath.Join(s.config.DataDir, constant.RaftState)
		if err := ensurePath(path, true); err != nil {
			return err
		}

		// Check Raft version and update the version file.
		raftVersionFilePath := filepath.Join(path, "version")
		raftVersionFileContent := strconv.Itoa(int(s.config.RaftConfig.ProtocolVersion))
		if err := s.checkRaftVersionFile(raftVersionFilePath); err != nil {
			return err
		}
		if err := ioutil.WriteFile(raftVersionFilePath, []byte(raftVersionFileContent), 0644); err != nil {
			return fmt.Errorf("failed to write Raft version file: %v", err)
		}

		// Create the BoltDB backend, with NoFreelistSync option
		store, raftErr := raftboltdb.New(raftboltdb.Options{
			Path:   filepath.Join(path, "raft.db"),
			NoSync: false, // fsync each log write
			BoltOptions: &bbolt.Options{
				NoFreelistSync: s.config.RaftBoltNoFreelistSync,
			},
		})
		if raftErr != nil {
			return raftErr
		}
		s.raftStore = store
		stable = store
		s.logger.Info("setting up raft bolt store", "no_freelist_sync", s.config.RaftBoltNoFreelistSync)

		// Start publishing bboltdb metrics
		go store.RunMetrics(s.shutdownCtx, 0)

		// Wrap the store in a LogCache to improve performance
		cacheStore, err := raft.NewLogCache(constant.RaftLogCacheSize, store)
		if err != nil {
			store.Close()
			return err
		}
		log = cacheStore

		// Create the snapshot store
		snapshots, err := raft.NewFileSnapshotStore(path, constant.SnapshotsRetained, s.config.LogOutput)
		if err != nil {
			if s.raftStore != nil {
				s.raftStore.Close()
			}
			return err
		}
		snap = snapshots

		// For an existing cluster being upgraded to the new version of
		// Raft, we almost never want to run recovery based on the old
		// peers.json file. We create a peers.info file with a helpful
		// note about where peers.json went, and use that as a sentinel
		// to avoid ingesting the old one that first time (if we have to
		// create the peers.info file because it's not there, we also
		// blow away any existing peers.json file).
		peersFile := filepath.Join(path, "peers.json")
		peersInfoFile := filepath.Join(path, "peers.info")
		if _, err := os.Stat(peersInfoFile); os.IsNotExist(err) {
			if err := ioutil.WriteFile(peersInfoFile, []byte(peersInfoContent), 0644); err != nil {
				return fmt.Errorf("failed to write peers.info file: %v", err)
			}

			// Blow away the peers.json file if present, since the
			// peers.info sentinel wasn't there.
			if _, err := os.Stat(peersFile); err == nil {
				if err := os.Remove(peersFile); err != nil {
					return fmt.Errorf("failed to delete peers.json, please delete manually (see peers.info for details): %v", err)
				}
				s.logger.Info("deleted peers.json file (see peers.info for details)")
			}
		} else if _, err := os.Stat(peersFile); err == nil {
			s.logger.Info("found peers.json file, recovering Raft configuration...")
			var configuration raft.Configuration
			if s.config.RaftConfig.ProtocolVersion < 3 {
				configuration, err = raft.ReadPeersJSON(peersFile)
			} else {
				configuration, err = raft.ReadConfigJSON(peersFile)
			}
			if err != nil {
				return fmt.Errorf("recovery failed to parse peers.json: %v", err)
			}
			tmpFsm, err := NewFSM(fsmConfig)
			if err != nil {
				return fmt.Errorf("recovery failed to make temp FSM: %v", err)
			}
			if err := raft.RecoverCluster(s.config.RaftConfig, tmpFsm,
				log, stable, snap, trans, configuration); err != nil {
				return fmt.Errorf("recovery failed: %v", err)
			}
			if err := os.Remove(peersFile); err != nil {
				return fmt.Errorf("recovery failed to delete peers.json, please delete manually (see peers.info for details): %v", err)
			}
			s.logger.Info("deleted peers.json file after successful recovery")
		}
	}

	// If we are a single server cluster and the state is clean then we can
	// bootstrap now.
	if s.isSingleServerCluster() {
		hasState, err := raft.HasExistingState(log, stable, snap)
		if err != nil {
			return err
		}
		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      s.config.RaftConfig.LocalID,
						Address: trans.LocalAddr(),
					},
				},
			}
			if err := raft.BootstrapCluster(s.config.RaftConfig,
				log, stable, snap, trans, configuration); err != nil {
				return err
			}
		}
	}

	// Setup the Raft store
	s.raft, err = raft.NewRaft(s.config.RaftConfig, s.fsm, log, stable, snap, trans)
	if err != nil {
		return err
	}
	return nil
}

// checkRaftVersionFile reads the Raft version file and returns an error if
// the Raft version is incompatible with the current version configured.
// Provide best-effort check if the file cannot be read.
func (s *Server) checkRaftVersionFile(path string) error {
	raftVersion := s.config.RaftConfig.ProtocolVersion
	baseWarning := "use the 'jobpool operator raft list-peers' command to make sure the Raft protocol versions are consistent"

	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		s.logger.Warn(fmt.Sprintf("unable to read Raft version file, %s", baseWarning), "error", err)
		return nil
	}

	v, err := ioutil.ReadFile(path)
	if err != nil {
		s.logger.Warn(fmt.Sprintf("unable to read Raft version file, %s", baseWarning), "error", err)
		return nil
	}

	previousVersion, err := strconv.Atoi(strings.TrimSpace(string(v)))
	if err != nil {
		s.logger.Warn(fmt.Sprintf("invalid Raft protocol version in Raft version file, %s", baseWarning), "error", err)
		return nil
	}

	if raft.ProtocolVersion(previousVersion) > raftVersion {
		return fmt.Errorf("downgrading Raft is not supported, current version is %d, previous version was %d", raftVersion, previousVersion)
	}

	return nil
}

// StreamingRpcHandler is used to make a streaming RPC call.
func (s *Server) StreamingRpcHandler(method string) (structs.StreamingRpcHandler, error) {
	return s.streamingRpcs.GetHandler(method)
}

// setupWorkers is used to start the scheduling workers
func (s *Server) setupWorkers(ctx context.Context) error {
	poolArgs := &structs.SchedulerWorkerPoolArgs{
		NumSchedulers:     s.config.NumSchedulers,
		EnabledSchedulers: s.config.EnabledSchedulers,
	}
	// we will be writing to the worker slice
	s.workerLock.Lock()
	defer s.workerLock.Unlock()
	return s.setupWorkersLocked(ctx, poolArgs)
}

// setupWorkersLocked directly manipulates the server.config, so it is not safe to
// call concurrently. Use setupWorkers() or call this with server.workerLock set.
func (s *Server) setupWorkersLocked(ctx context.Context, poolArgs *structs.SchedulerWorkerPoolArgs) error {
	// Check if all the schedulers are disabled
	if len(poolArgs.EnabledSchedulers) == 0 || poolArgs.NumSchedulers == 0 {
		s.logger.Warn("no enabled schedulers")
		return nil
	}

	// Check if the core scheduler is not enabled
	foundCore := false
	for _, sched := range poolArgs.EnabledSchedulers {
		if sched == constant.PlanTypeCore {
			foundCore = true
			continue
		}

		if _, ok := scheduler.SchedulerFactory[sched]; !ok {
			return fmt.Errorf("invalid configuration: unknown scheduler %q in enabled schedulers", sched)
		}
	}
	if !foundCore {
		return fmt.Errorf("invalid configuration: %q scheduler not enabled", constant.PlanTypeCore)
	}
	s.logger.Info("starting scheduling worker(s)", "num_workers", poolArgs.NumSchedulers, "schedulers", poolArgs.EnabledSchedulers)
	// Start the workers

	for i := 0; i < s.config.NumSchedulers; i++ {
		if w, err := NewWorker(ctx, s, poolArgs); err != nil {
			return err
		} else {
			s.logger.Debug("started scheduling worker", "id", w.ID(), "index", i+1, "of", s.config.NumSchedulers)
			s.workers = append(s.workers, w)
		}
	}
	s.logger.Info("started scheduling worker(s)", "num_workers", s.config.NumSchedulers, "schedulers", s.config.EnabledSchedulers)
	return nil
}
