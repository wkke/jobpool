package agent

import (
	"fmt"
	metrics "github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
	"io/ioutil"
	golog "log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"yunli.com/jobpool/client"
	clientconfig "yunli.com/jobpool/client/config"
	"yunli.com/jobpool/client/state"
	"yunli.com/jobpool/command/cfg"
	"yunli.com/jobpool/core"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/core/structs/config"
	"yunli.com/jobpool/helper"
	"yunli.com/jobpool/helper/uuid"
)

const (
	// DefaultRaftMultiplier is used as a baseline Raft configuration that
	// will be reliable on a very basic server.
	DefaultRaftMultiplier = 1

	// MaxRaftMultiplier is a fairly arbitrary upper bound that limits the
	// amount of performance detuning that's possible.
	MaxRaftMultiplier = 10
)

type Agent struct {
	config     *cfg.Config
	configLock sync.Mutex

	logger     log.InterceptLogger
	httpLogger log.Logger
	logOutput  io.Writer

	// client is the launched Jobpool Client. Can be nil if the agent isn't
	// configured to run a client.
	client *client.Client

	// server is the launched Jobpool Server. Can be nil if the agent isn't
	// configured to run a server.
	server *core.Server

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	InmemSink *metrics.InmemSink
}

// NewAgent is used to create a new agent with the given configuration
func NewAgent(config *cfg.Config, logger log.InterceptLogger, logOutput io.Writer, inmem *metrics.InmemSink) (*Agent, error) {
	a := &Agent{
		config:     config,
		logOutput:  logOutput,
		shutdownCh: make(chan struct{}),
		InmemSink:  inmem,
	}

	// Create the loggers
	a.logger = logger
	a.httpLogger = a.logger.ResetNamed("http")

	// Global logger should match internal logger as much as possible
	golog.SetFlags(golog.LstdFlags | golog.Lmicroseconds)

	if err := a.setupServer(); err != nil {
		return nil, err
	}
	if err := a.setupClient(); err != nil {
		return nil, err
	}

	if a.client == nil && a.server == nil {
		return nil, fmt.Errorf("must have at least client or server mode enabled")
	}

	return a, nil
}

// setupServer is used to setup the server if enabled
func (a *Agent) setupServer() error {
	if !a.config.Server.Enabled {
		return nil
	}

	// Setup the configuration
	conf, err := a.serverConfig()
	if err != nil {
		return fmt.Errorf("server config setup failed: %s", err)
	}

	// Generate a node ID and persist it if it is the first instance, otherwise
	// read the persisted node ID.
	if err := a.setupNodeID(conf); err != nil {
		return fmt.Errorf("setting up server node ID failed: %s", err)
	}

	// Create the server
	server, err := core.NewServer(conf)
	if err != nil {
		return fmt.Errorf("server setup failed: %v", err)
	}
	a.server = server
	return nil
}

// setupClient is used to setup the client if enabled
func (a *Agent) setupClient() error {
	if !a.config.Client.Enabled {
		return nil
	}

	// Setup the configuration
	conf, err := a.clientConfig()
	if err != nil {
		return fmt.Errorf("client setup failed: %v", err)
	}

	// Reserve some ports for the plugins if we are on Windows
	//if runtime.GOOS == "windows" {
	//	if err := a.reservePortsForClient(conf); err != nil {
	//		return err
	//	}
	//}
	if conf.StateDBFactory == nil {
		conf.StateDBFactory = state.GetStateDBFactory(conf.DevMode)
	}

	agentClient, err := client.NewClient(
		conf, nil)
	if err != nil {
		return fmt.Errorf("client setup failed: %v", err)
	}
	a.client = agentClient

	return nil
}

// clientConfig is used to generate a new client configuration struct for
// initializing a Jobpool client.
func (a *Agent) clientConfig() (*clientconfig.Config, error) {
	c, err := convertClientConfig(a.config)
	if err != nil {
		return nil, err
	}

	if err := a.finalizeClientConfig(c); err != nil {
		return nil, err
	}

	return c, nil
}

// convertClientConfig takes an agent config and log output and returns a client
// Config. There may be missing fields that must be set by the agent. To do this
// call finalizeServerConfig
func convertClientConfig(agentConfig *cfg.Config) (*clientconfig.Config, error) {
	// Set up the configuration
	conf := agentConfig.ClientConfig
	if conf == nil {
		conf = clientconfig.DefaultConfig()
	}

	conf.Servers = agentConfig.Client.Servers
	conf.LogLevel = agentConfig.LogLevel
	conf.DevMode = agentConfig.DevMode
	conf.EnableDebug = agentConfig.EnableDebug

	if agentConfig.Region != "" {
		conf.Region = agentConfig.Region
	}
	if agentConfig.DataDir != "" {
		conf.StateDir = filepath.Join(agentConfig.DataDir, "client")
		conf.AllocDir = filepath.Join(agentConfig.DataDir, "alloc")
	}
	if agentConfig.Client.StateDir != "" {
		conf.StateDir = agentConfig.Client.StateDir
	}
	if agentConfig.Client.AllocDir != "" {
		conf.AllocDir = agentConfig.Client.AllocDir
	}
	if agentConfig.Client.NetworkInterface != "" {
		conf.NetworkInterface = agentConfig.Client.NetworkInterface
	}
	conf.Options = agentConfig.Client.Options
	if agentConfig.Client.MaxKillTimeout != "" {
		dur, err := time.ParseDuration(agentConfig.Client.MaxKillTimeout)
		if err != nil {
			return nil, fmt.Errorf("Error parsing max kill timeout: %s", err)
		}
		conf.MaxKillTimeout = dur
	}
	conf.ClientMaxPort = uint(agentConfig.Client.ClientMaxPort)
	conf.ClientMinPort = uint(agentConfig.Client.ClientMinPort)
	conf.MaxDynamicPort = agentConfig.Client.MaxDynamicPort
	conf.MinDynamicPort = agentConfig.Client.MinDynamicPort
	conf.DisableRemoteExec = agentConfig.Client.DisableRemoteExec

	// Setup the node
	conf.Node = new(structs.Node)
	conf.Node.Datacenter = agentConfig.Datacenter
	conf.Node.Name = agentConfig.NodeName
	conf.Node.Meta = agentConfig.Client.Meta
	conf.Node.NodeClass = agentConfig.Client.NodeClass

	// Set up the HTTP advertise address
	conf.Node.HTTPAddr = agentConfig.AdvertiseAddrs.HTTP

	// Canonicalize Node struct
	conf.Node.Canonicalize()

	conf.Version = agentConfig.Version

	// Set the GC related configs
	conf.GCInterval = agentConfig.Client.GCInterval
	conf.GCParallelDestroys = agentConfig.Client.GCParallelDestroys
	conf.GCDiskUsageThreshold = agentConfig.Client.GCDiskUsageThreshold
	conf.GCInodeUsageThreshold = agentConfig.Client.GCInodeUsageThreshold
	conf.GCMaxAllocs = agentConfig.Client.GCMaxAllocs
	if agentConfig.Client.NoHostUUID != nil {
		conf.NoHostUUID = *agentConfig.Client.NoHostUUID
	} else {
		// Default no_host_uuid to true
		conf.NoHostUUID = true
	}

	conf.BridgeNetworkName = agentConfig.Client.BridgeNetworkName
	conf.BridgeNetworkAllocSubnet = agentConfig.Client.BridgeNetworkSubnet

	for _, hn := range agentConfig.Client.HostNetworks {
		conf.HostNetworks[hn.Name] = hn
	}
	conf.BindWildcardDefaultHostNetwork = agentConfig.Client.BindWildcardDefaultHostNetwork

	conf.CgroupParent = agentConfig.Client.CgroupParent

	return conf, nil
}

// finalizeClientConfig sets configuration fields on the client config that are
// not staticly convertable and are from the agent.
func (a *Agent) finalizeClientConfig(c *clientconfig.Config) error {
	// Setup the logging
	c.Logger = a.logger
	c.LogOutput = a.logOutput

	// If we are running a server, append both its bind and advertise address so
	// we are able to at least talk to the local server even if that isn't
	// configured explicitly. This handles both running server and client on one
	// host and -dev mode.
	if a.server != nil {
		advertised := a.config.AdvertiseAddrs
		normalized := a.config.NormalizedAddrs

		if advertised == nil || advertised.RPC == "" {
			return fmt.Errorf("AdvertiseAddrs is nil or empty")
		} else if normalized == nil || normalized.RPC == "" {
			return fmt.Errorf("normalizedAddrs is nil or empty")
		}

		if normalized.RPC == advertised.RPC {
			c.Servers = append(c.Servers, normalized.RPC)
		} else {
			c.Servers = append(c.Servers, normalized.RPC, advertised.RPC)
		}
	}
	return nil
}

func (a *Agent) GetConfig() *cfg.Config {
	a.configLock.Lock()
	defer a.configLock.Unlock()

	return a.config
}

// setupNodeID will pull the persisted node ID, if any, or create a random one
// and persist it.
func (a *Agent) setupNodeID(config *core.Config) error {
	// For dev mode we have no filesystem access so just make a node ID.
	if a.config.DevMode {
		config.NodeID = uuid.Generate()
		return nil
	}

	// Load saved state, if any. Since a user could edit this, we also
	// validate it. Saved state overwrites any configured node id
	fileID := filepath.Join(config.DataDir, "node-id")
	if _, err := os.Stat(fileID); err == nil {
		rawID, err := ioutil.ReadFile(fileID)
		if err != nil {
			return err
		}

		nodeID := strings.TrimSpace(string(rawID))
		nodeID = strings.ToLower(nodeID)
		config.NodeID = nodeID
		return nil
	}

	// If they've configured a node ID manually then just use that, as
	// long as it's valid.
	if config.NodeID != "" {
		config.NodeID = strings.ToLower(config.NodeID)
		// Persist this configured nodeID to our data directory
		if err := helper.EnsurePath(fileID, false); err != nil {
			return err
		}

		if err := ioutil.WriteFile(fileID, []byte(config.NodeID), 0600); err != nil {
			return err
		}
		return nil
	}

	// If we still don't have a valid node ID, make one.
	if config.NodeID == "" {
		id := uuid.Generate()
		//if err := lib.EnsurePath(fileID, false); err != nil {
		//	return err
		//}
		if err := ioutil.WriteFile(fileID, []byte(id), 0600); err != nil {
			return err
		}

		config.NodeID = id
	}
	return nil
}

// convertServerConfig takes an agent config and log output and returns a Jobpool
// Config. There may be missing fields that must be set by the agent. To do this
// call finalizeServerConfig
func convertServerConfig(agentConfig *cfg.Config) (*core.Config, error) {
	conf := agentConfig.CoreConfig
	if conf == nil {
		conf = core.DefaultConfig()
	}

	conf.DevMode = agentConfig.DevMode
	conf.EnableDebug = agentConfig.EnableDebug

	conf.Build = agentConfig.Version.Version
	if agentConfig.Region != "" {
		conf.Region = agentConfig.Region
	}

	// Set the Authoritative Region if set, otherwise default to
	// the same as the local region.
	if agentConfig.Server.AuthoritativeRegion != "" {
		conf.AuthoritativeRegion = agentConfig.Server.AuthoritativeRegion
	} else if agentConfig.Region != "" {
		conf.AuthoritativeRegion = agentConfig.Region
	}

	if agentConfig.Datacenter != "" {
		conf.Datacenter = agentConfig.Datacenter
	}
	if agentConfig.NodeName != "" {
		conf.NodeName = agentConfig.NodeName
	}
	if agentConfig.Server.BootstrapExpect > 0 {
		conf.BootstrapExpect = agentConfig.Server.BootstrapExpect
	}
	if agentConfig.DataDir != "" {
		conf.DataDir = filepath.Join(agentConfig.DataDir, "server")
	}
	if agentConfig.Server.DataDir != "" {
		conf.DataDir = agentConfig.Server.DataDir
	}
	if agentConfig.Server.RaftProtocol != 0 {
		conf.RaftConfig.ProtocolVersion = raft.ProtocolVersion(agentConfig.Server.RaftProtocol)
	}
	raftMultiplier := int(DefaultRaftMultiplier)
	if agentConfig.Server.RaftMultiplier != nil && *agentConfig.Server.RaftMultiplier != 0 {
		raftMultiplier = *agentConfig.Server.RaftMultiplier
		if raftMultiplier < 1 || raftMultiplier > MaxRaftMultiplier {
			return nil, fmt.Errorf("raft_multiplier cannot be %d. Must be between 1 and %d", *agentConfig.Server.RaftMultiplier, MaxRaftMultiplier)
		}
	}
	conf.RaftConfig.ElectionTimeout *= time.Duration(raftMultiplier)
	conf.RaftConfig.HeartbeatTimeout *= time.Duration(raftMultiplier)
	conf.RaftConfig.LeaderLeaseTimeout *= time.Duration(raftMultiplier)
	conf.RaftConfig.CommitTimeout *= time.Duration(raftMultiplier)

	if agentConfig.Server.NumSchedulers != nil {
		conf.NumSchedulers = *agentConfig.Server.NumSchedulers
	}

	if len(agentConfig.Server.EnabledSchedulers) != 0 {
		// Convert to a set and require the core scheduler
		set := make(map[string]struct{}, 4)
		set[constant.PlanTypeCore] = struct{}{}
		for _, sched := range agentConfig.Server.EnabledSchedulers {
			set[sched] = struct{}{}
		}

		schedulers := make([]string, 0, len(set))
		for k := range set {
			schedulers = append(schedulers, k)
		}
		conf.EnabledSchedulers = schedulers
	}

	if agentConfig.Server.NonVotingServer {
		conf.NonVoter = true
	}

	if agentConfig.Server.EnableEventBroker != nil {
		conf.EnableEventBroker = *agentConfig.Server.EnableEventBroker
	}
	if agentConfig.Server.EventBufferSize != nil {
		if *agentConfig.Server.EventBufferSize < 0 {
			return nil, fmt.Errorf("Invalid Config, event_buffer_size must be non-negative")
		}
		conf.EventBufferSize = int64(*agentConfig.Server.EventBufferSize)
	}

	// Set up the bind addresses
	rpcAddr, err := net.ResolveTCPAddr("tcp", agentConfig.NormalizedAddrs.RPC)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse RPC address %q: %v", agentConfig.NormalizedAddrs.RPC, err)
	}
	serfAddr, err := net.ResolveTCPAddr("tcp", agentConfig.NormalizedAddrs.Serf)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse Serf address %q: %v", agentConfig.NormalizedAddrs.Serf, err)
	}
	conf.RPCAddr.Port = rpcAddr.Port
	conf.RPCAddr.IP = rpcAddr.IP
	conf.SerfConfig.MemberlistConfig.BindPort = serfAddr.Port
	conf.SerfConfig.MemberlistConfig.BindAddr = serfAddr.IP.String()

	// Set up the advertise addresses
	rpcAddr, err = net.ResolveTCPAddr("tcp", agentConfig.AdvertiseAddrs.RPC)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse RPC advertise address %q: %v", agentConfig.AdvertiseAddrs.RPC, err)
	}
	serfAddr, err = net.ResolveTCPAddr("tcp", agentConfig.AdvertiseAddrs.Serf)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse Serf advertise address %q: %v", agentConfig.AdvertiseAddrs.Serf, err)
	}

	// Server address is the serf advertise address and rpc port. This is the
	// address that all servers should be able to communicate over RPC with.
	serverAddr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(serfAddr.IP.String(), fmt.Sprintf("%d", rpcAddr.Port)))
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve Serf advertise address %q: %v", agentConfig.AdvertiseAddrs.Serf, err)
	}

	conf.SerfConfig.MemberlistConfig.AdvertiseAddr = serfAddr.IP.String()
	conf.SerfConfig.MemberlistConfig.AdvertisePort = serfAddr.Port
	conf.ClientRPCAdvertise = rpcAddr
	conf.ServerRPCAdvertise = serverAddr

	// Set up gc threshold and heartbeat grace period
	if gcThreshold := agentConfig.Server.NodeGCThreshold; gcThreshold != "" {
		dur, err := time.ParseDuration(gcThreshold)
		if err != nil {
			return nil, err
		}
		conf.NodeGCThreshold = dur
	}

	if gcThreshold := agentConfig.Server.EvalGCThreshold; gcThreshold != "" {
		dur, err := time.ParseDuration(gcThreshold)
		if err != nil {
			return nil, err
		}
		conf.EvalGCThreshold = dur
	}
	if gcThreshold := agentConfig.Server.DeploymentGCThreshold; gcThreshold != "" {
		dur, err := time.ParseDuration(gcThreshold)
		if err != nil {
			return nil, err
		}
		conf.DeploymentGCThreshold = dur
	}

	if heartbeatGrace := agentConfig.Server.HeartbeatGrace; heartbeatGrace != 0 {
		conf.HeartbeatGrace = heartbeatGrace
	}
	if min := agentConfig.Server.MinHeartbeatTTL; min != 0 {
		conf.MinHeartbeatTTL = min
	}
	if maxHPS := agentConfig.Server.MaxHeartbeatsPerSecond; maxHPS != 0 {
		conf.MaxHeartbeatsPerSecond = maxHPS
	}
	if failoverTTL := agentConfig.Server.FailoverHeartbeatTTL; failoverTTL != 0 {
		conf.FailoverHeartbeatTTL = failoverTTL
	}

	if d, err := time.ParseDuration(agentConfig.Limits.RPCHandshakeTimeout); err != nil {
		return nil, fmt.Errorf("error parsing rpc_handshake_timeout: %v", err)
	} else if d < 0 {
		return nil, fmt.Errorf("rpc_handshake_timeout must be >= 0")
	} else {
		conf.RPCHandshakeTimeout = d
	}

	// Set max rpc conns; nil/0 == unlimited
	// Leave a little room for streaming RPCs
	minLimit := config.LimitsNonStreamingConnsPerClient + 5
	if agentConfig.Limits.RPCMaxConnsPerClient == nil || *agentConfig.Limits.RPCMaxConnsPerClient == 0 {
		conf.RPCMaxConnsPerClient = 0
	} else if limit := *agentConfig.Limits.RPCMaxConnsPerClient; limit <= minLimit {
		return nil, fmt.Errorf("rpc_max_conns_per_client must be > %d; found: %d", minLimit, limit)
	} else {
		conf.RPCMaxConnsPerClient = limit
	}

	// Set deployment rate limit
	if rate := agentConfig.Server.DeploymentQueryRateLimit; rate == 0 {
		conf.DeploymentQueryRateLimit = 100.0
	} else if rate > 0 {
		conf.DeploymentQueryRateLimit = rate
	} else {
		return nil, fmt.Errorf("deploy_query_rate_limit must be greater than 0")
	}

	// Add the search configuration
	if search := agentConfig.Server.Search; search != nil {
		conf.SearchConfig = &structs.SearchConfig{
			FuzzyEnabled:  search.FuzzyEnabled,
			LimitQuery:    search.LimitQuery,
			LimitResults:  search.LimitResults,
			MinTermLength: search.MinTermLength,
		}
	}

	// Set the raft bolt parameters
	if bolt := agentConfig.Server.RaftBoltConfig; bolt != nil {
		conf.RaftBoltNoFreelistSync = bolt.NoFreelistSync
	}

	return conf, nil
}

// serverConfig is used to generate a new server configuration struct
// for initializing a jobpool server.
func (a *Agent) serverConfig() (*core.Config, error) {
	c, err := convertServerConfig(a.config)
	if err != nil {
		return nil, err
	}
	a.finalizeServerConfig(c)
	return c, nil
}

// finalizeServerConfig sets configuration fields on the server config that are
// not staticly convertable and are from the agent.
func (a *Agent) finalizeServerConfig(c *core.Config) {
	// Setup the logging
	c.Logger = a.logger
	c.LogOutput = a.logOutput

	c.AgentShutdown = func() error { return a.Shutdown() }
}

// Shutdown is used to terminate the agent.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	a.logger.Info("requesting shutdown")
	if a.client != nil {
		if err := a.client.Shutdown(); err != nil {
			a.logger.Error("client shutdown failed", "error", err)
		}
	}
	if a.server != nil {
		if err := a.server.Shutdown(); err != nil {
			a.logger.Error("server shutdown failed", "error", err)
		}
	}
	a.logger.Info("shutdown complete")
	a.shutdown = true
	close(a.shutdownCh)
	return nil
}

// Leave is used gracefully exit. Clients will inform servers
// of their departure so that allocations can be rescheduled.
func (a *Agent) Leave() error {
	if a.client != nil {
		if err := a.client.Leave(); err != nil {
			a.logger.Error("client leave failed", "error", err)
		}
	}
	if a.server != nil {
		if err := a.server.Leave(); err != nil {
			a.logger.Error("server leave failed", "error", err)
		}
	}
	return nil
}

// RPC is used to make an RPC call to the Jobpool servers
func (a *Agent) RPC(method string, args interface{}, reply interface{}) error {
	if a.server != nil {
		return a.server.RPC(method, args, reply)
	}
	return a.client.RPC(method, args, reply)
}

// Client returns the configured client or nil
func (a *Agent) Client() *client.Client {
	return a.client
}

// Server returns the configured server or nil
func (a *Agent) Server() *core.Server {
	return a.server
}
