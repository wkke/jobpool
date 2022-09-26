package client

import (
	"errors"
	"fmt"
	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
	"github.com/shirou/gopsutil/host"
	"io/ioutil"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"yunli.com/jobpool/client/config"
	"yunli.com/jobpool/client/servers"
	"yunli.com/jobpool/client/state"
	"yunli.com/jobpool/client/stats"
	cstructs "yunli.com/jobpool/client/structs"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper"
	"yunli.com/jobpool/helper/pool"
	"yunli.com/jobpool/helper/uuid"
)

const (
	// clientRPCCache controls how long we keep an idle connection
	// open to a server
	clientRPCCache = 5 * time.Minute

	// clientMaxStreams controls how many idle streams we keep
	// open to a server
	clientMaxStreams = 2

	// datacenterQueryLimit searches through up to this many adjacent
	// datacenters looking for the Jobpool server service.
	datacenterQueryLimit = 9

	// registerRetryIntv is minimum interval on which we retry
	// registration. We pick a value between this and 2x this.
	registerRetryIntv = 15 * time.Second

	// getAllocRetryIntv is minimum interval on which we retry
	// to fetch allocations. We pick a value between this and 2x this.
	getAllocRetryIntv = 30 * time.Second

	// devModeRetryIntv is the retry interval used for development
	devModeRetryIntv = time.Second

	// noServerRetryIntv is the retry interval used when client has not
	// connected to server yet
	noServerRetryIntv = time.Second

	// stateSnapshotIntv is how often the client snapshots state
	stateSnapshotIntv = 60 * time.Second

	// initialHeartbeatStagger is used to stagger the interval between
	// starting and the initial heartbeat. After the initial heartbeat,
	// we switch to using the TTL specified by the servers.
	initialHeartbeatStagger = 10 * time.Second

	// nodeUpdateRetryIntv is how often the client checks for updates to the
	// node attributes or meta map.
	nodeUpdateRetryIntv = 5 * time.Second

	// allocSyncIntv is the batching period of allocation updates before they
	// are synced with the server.
	allocSyncIntv = 200 * time.Millisecond

	// allocSyncRetryIntv is the interval on which we retry updating
	// the status of the allocation
	allocSyncRetryIntv = 5 * time.Second

	// defaultConnectLogLevel is the log level set in the node meta by default
	defaultConnectLogLevel = "info"

	// defaultConnectProxyConcurrency is the default number of worker threads the
	// connect sidecar should be configured to use.
	//
	defaultConnectProxyConcurrency = "1"
)

var (
	// grace period to allow for batch fingerprint processing
	batchFirstFingerprintsProcessingGrace = batchFirstFingerprintsTimeout + 5*time.Second
)
var (
	// noServersErr is returned by the RPC method when the client has no
	// configured servers.
	noServersErr = errors.New("no servers")
)

type Client struct {
	config *config.Config
	start  time.Time

	// stateDB is used to efficiently store client state.
	stateDB state.StateDB

	// configCopy is a copy that should be passed to alloc-runners.
	configCopy *config.Config
	configLock sync.RWMutex

	logger    hclog.InterceptLogger
	rpcLogger hclog.Logger

	connPool *pool.ConnPool

	// servers is the list of jobpool servers
	servers *servers.Manager

	// heartbeat related times for tracking how often to heartbeat
	heartbeatTTL    time.Duration
	haveHeartbeated bool
	heartbeatLock   sync.Mutex
	heartbeatStop   *heartbeatStop

	triggerDiscoveryCh chan struct{}

	// triggerNodeUpdate triggers the client to mark the Node as changed and
	// update it.
	triggerNodeUpdate chan struct{}

	// triggerEmitNodeEvent sends an event and triggers the client to update the
	// server for the node event
	triggerEmitNodeEvent chan *structs.NodeEvent

	// rpcRetryCh is closed when there an event such as server discovery or a
	// successful RPC occurring happens such that a retry should happen. Access
	// should only occur via the getter method
	rpcRetryCh   chan struct{}
	rpcRetryLock sync.Mutex

	// shutdown is true when the Client has been shutdown. Must hold
	// shutdownLock to access.
	shutdown bool

	// shutdownCh is closed to signal the Client is shutting down.
	shutdownCh chan struct{}

	shutdownLock sync.Mutex

	// shutdownGroup are goroutines that exit when shutdownCh is closed.
	// Shutdown() blocks on Wait() after closing shutdownCh.
	shutdownGroup group

	// rpcServer is used to serve RPCs by the local agent.
	rpcServer     *rpc.Server
	endpoints     rpcEndpoints
	streamingRpcs *structs.StreamingRpcRegistry

	// baseLabels are used when emitting tagged metrics. All client metrics will
	// have these tags, and optionally more.
	baseLabels []metrics.Label

	// fpInitialized chan is closed when the first batch of fingerprints are
	// applied to the node
	fpInitialized chan struct{}

	// serversContactedCh is closed when GetClientAllocs and runAllocs have
	// successfully run once.
	serversContactedCh   chan struct{}
	serversContactedOnce sync.Once

	hostStatsCollector *stats.HostStatsCollector

	// allocs maps alloc IDs to their AllocRunner. This map includes all
	// AllocRunners - running and GC'd - until the server GCs them.
	allocs    map[string]AllocRunner
	allocLock sync.RWMutex

	// invalidAllocs is a map that tracks allocations that failed because
	// the client couldn't initialize alloc or task runners for it. This can
	// happen due to driver errors
	invalidAllocs     map[string]struct{}
	invalidAllocsLock sync.Mutex
	allocUpdates      chan *structs.Allocation

}

func NewClient(cfg *config.Config, rpcs map[string]interface{}) (*Client, error) {
	if cfg.StateDBFactory == nil {
		cfg.StateDBFactory = state.GetStateDBFactory(cfg.DevMode)
	}

	// Create the logger
	logger := cfg.Logger.ResetNamedIntercept("client")

	// Create the client
	c := &Client{
		config:               cfg,
		start:                time.Now(),
		connPool:             pool.NewPool(logger, clientRPCCache, clientMaxStreams),
		streamingRpcs:        structs.NewStreamingRpcRegistry(),
		logger:               logger,
		rpcLogger:            logger.Named("rpc"),
		shutdownCh:           make(chan struct{}),
		triggerDiscoveryCh:   make(chan struct{}),
		triggerNodeUpdate:    make(chan struct{}, 8),
		triggerEmitNodeEvent: make(chan *structs.NodeEvent, 8),
		fpInitialized:        make(chan struct{}),
		serversContactedCh:   make(chan struct{}),
		serversContactedOnce: sync.Once{},
		allocs:               make(map[string]AllocRunner),
		allocUpdates:         make(chan *structs.Allocation, 64),
		invalidAllocs:        make(map[string]struct{}),
	}

	// Initialize the server manager
	c.servers = servers.New(c.logger, c.shutdownCh, c)

	// Start server manager rebalancing go routine
	go c.servers.Start()

	// initialize the client
	if err := c.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize client: %v", err)
	}
	// Setup the clients RPC server
	c.setupClientRpc(rpcs)

	// Setup the node
	if err := c.setupNode(); err != nil {
		return nil, fmt.Errorf("node setup failed: %v", err)
	}

	// Store the config copy before restoring state but after it has been
	// initialized.
	c.configLock.Lock()
	c.configCopy = c.config.Copy()
	c.configLock.Unlock()

	// Batching of initial fingerprints is done to reduce the number of node
	// updates sent to the server on startup. This is the first RPC to the servers
	go c.batchFirstFingerprints()

	// create heartbeatStop. We go after the first attempt to connect to the server, so
	// that our grace period for connection goes for the full time
	c.heartbeatStop = newHeartbeatStop(batchFirstFingerprintsTimeout, logger, c.shutdownCh)
	// Watch for disconnection, and heartbeatStopAllocs configured to have a maximum
	// lifetime when out of touch with the server
	go c.heartbeatStop.watch()

	// Add the stats collector
	statsCollector := stats.NewHostStatsCollector(c.logger, c.config.AllocDir)
	c.hostStatsCollector = statsCollector

	// Set the preconfigured list of static servers
	c.configLock.RLock()
	if len(c.configCopy.Servers) > 0 {
		if _, err := c.setServersImpl(c.configCopy.Servers, true); err != nil {
			logger.Warn("none of the configured servers are valid", "error", err)
		}
	}
	c.configLock.RUnlock()
	// Register and then start heartbeating to the servers.
	c.shutdownGroup.Go(c.registerAndHeartbeat)

	// Begin periodic snapshotting of state.
	c.shutdownGroup.Go(c.periodicSnapshot)

	// fresh the status for alloc
	c.shutdownGroup.Go(c.allocSync)

	// Start the client! Don't use the shutdownGroup as run handles
	// shutdowns manually to prevent updates from being applied during
	// shutdown.
	go c.run()

	// Start collecting stats
	c.shutdownGroup.Go(c.emitStats)

	c.logger.Info("started client", "node_id", c.NodeID())
	return c, nil
}

// 运行分配的任务（TODO 分配的策略？）
func (c *Client) run() {
	// Watch for changes in allocations
	allocUpdates := make(chan *cstructs.AllocUpdates, 8)
	go c.watchAllocations(allocUpdates)

	for {
		select {
		case update := <-allocUpdates:
			// Don't apply updates while shutting down.
			c.shutdownLock.Lock()
			if c.shutdown {
				c.shutdownLock.Unlock()
				return
			}
			// c.logger.Debug("run the client which begin run allocs----")
			// Apply updates inside lock to prevent a concurrent
			// shutdown.
			c.runAllocs(update)
			c.shutdownLock.Unlock()

		case <-c.shutdownCh:
			return
		}
	}
}

// emitStats collects host resource usage stats periodically
func (c *Client) emitStats() {
	// Determining NodeClass to be emitted
	var emittedNodeClass string
	if emittedNodeClass = c.Node().NodeClass; emittedNodeClass == "" {
		emittedNodeClass = "none"
	}

	// Assign labels directly before emitting stats so the information expected
	// is ready
	c.baseLabels = []metrics.Label{
		{Name: "node_id", Value: c.NodeID()},
		{Name: "datacenter", Value: c.Datacenter()},
		{Name: "node_class", Value: emittedNodeClass},
	}

	// Start collecting host stats right away and then keep collecting every
	// collection interval
	next := time.NewTimer(0)
	defer next.Stop()
	for {
		select {
		case <-next.C:
			c.logger.Debug("fetching host resource usage stats")
		case <-c.shutdownCh:
			return
		}
	}
}

// init is used to initialize the client and perform any setup
// needed before we begin starting its various components.
func (c *Client) init() error {
	// Ensure the state dir exists if we have one
	if c.config.StateDir != "" {
		if err := os.MkdirAll(c.config.StateDir, 0700); err != nil {
			return fmt.Errorf("failed creating state dir: %s", err)
		}

	} else {
		// Otherwise make a temp directory to use.
		p, err := ioutil.TempDir("", "JobpoolClient")
		if err != nil {
			return fmt.Errorf("failed creating temporary directory for the StateDir: %v", err)
		}

		p, err = filepath.EvalSymlinks(p)
		if err != nil {
			return fmt.Errorf("failed to find temporary directory for the StateDir: %v", err)
		}

		c.config.StateDir = p
	}
	c.logger.Info("using state directory", "state_dir", c.config.StateDir)

	// Open the state database
	db, err := c.config.StateDBFactory(c.logger, c.config.StateDir)
	if err != nil {
		return fmt.Errorf("failed to open state database: %v", err)
	}

	// Upgrade the state database
	if err := db.Upgrade(); err != nil {
		// Upgrade only returns an error on critical persistence
		// failures in which an operator should intervene before the
		// node is accessible. Upgrade drops and logs corrupt state it
		// encounters, so failing to start the agent should be extremely
		// rare.
		return fmt.Errorf("failed to upgrade state database: %v", err)
	}

	c.stateDB = db

	// Ensure the alloc dir exists if we have one
	if c.config.AllocDir != "" {
		if err := os.MkdirAll(c.config.AllocDir, 0711); err != nil {
			return fmt.Errorf("failed creating alloc dir: %s", err)
		}
	} else {
		// Otherwise make a temp directory to use.
		p, err := ioutil.TempDir("", "JobpoolClient")
		if err != nil {
			return fmt.Errorf("failed creating temporary directory for the AllocDir: %v", err)
		}

		p, err = filepath.EvalSymlinks(p)
		if err != nil {
			return fmt.Errorf("failed to find temporary directory for the AllocDir: %v", err)
		}

		// Change the permissions to have the execute bit
		if err := os.Chmod(p, 0711); err != nil {
			return fmt.Errorf("failed to change directory permissions for the AllocDir: %v", err)
		}

		c.config.AllocDir = p
	}

	c.logger.Info("using alloc directory", "alloc_dir", c.config.AllocDir)

	reserved := "<none>"
	c.logger.Info("using dynamic ports",
		"min", c.config.MinDynamicPort,
		"max", c.config.MaxDynamicPort,
		"reserved", reserved,
	)
	return nil
}

// setupNode is used to setup the initial node
func (c *Client) setupNode() error {
	node := c.config.Node
	if node == nil {
		node = &structs.Node{}
		c.config.Node = node
	}
	// Generate an ID and secret for the node
	id, secretID, err := c.nodeID()
	if err != nil {
		return fmt.Errorf("node ID setup failed: %v", err)
	}

	node.ID = id
	node.SecretID = secretID
	if node.Attributes == nil {
		node.Attributes = make(map[string]string)
	}
	if node.Links == nil {
		node.Links = make(map[string]string)
	}
	if node.Meta == nil {
		node.Meta = make(map[string]string)
	}
	if node.Datacenter == "" {
		node.Datacenter = "dc1"
	}
	if node.Name == "" {
		node.Name, _ = os.Hostname()
	}
	if node.HostNetworks == nil {
		if l := len(c.config.HostNetworks); l != 0 {
			node.HostNetworks = make(map[string]*structs.ClientHostNetworkConfig, l)
			for k, v := range c.config.HostNetworks {
				node.HostNetworks[k] = v.Copy()
			}
		}
	}

	if node.Name == "" {
		node.Name = node.ID
	}
	node.Status = constant.NodeStatusInit

	// Setup default meta
	if _, ok := node.Meta["connect.log_level"]; !ok {
		node.Meta["connect.log_level"] = defaultConnectLogLevel
	}
	if _, ok := node.Meta["connect.proxy_concurrency"]; !ok {
		node.Meta["connect.proxy_concurrency"] = defaultConnectProxyConcurrency
	}

	return nil
}

// Leave is used to prepare the client to leave the cluster
func (c *Client) Leave() error {
	// TODO
	return nil
}

// group wraps a func() in a goroutine and provides a way to block until it
// exits. Inspired by https://godoc.org/golang.org/x/sync/errgroup
type group struct {
	wg sync.WaitGroup
}

// Region returns the region for the given client
func (c *Client) Region() string {
	return c.config.Region
}

// Datacenter returns the datacenter for the given client
func (c *Client) Datacenter() string {
	return c.config.Node.Datacenter
}

// NodeID returns the node ID for the given client
func (c *Client) NodeID() string {
	return c.config.Node.ID
}

// nodeID restores, or generates if necessary, a unique node ID and SecretID.
// The node ID is, if available, a persistent unique ID.  The secret ID is a
// high-entropy random UUID.
func (c *Client) nodeID() (id, secret string, err error) {
	var hostID string
	hostInfo, err := host.Info()
	if !c.config.NoHostUUID && err == nil {
		if hashed, ok := helper.HashUUID(hostInfo.HostID); ok {
			hostID = hashed
		}
	}

	if hostID == "" {
		// Generate a random hostID if no constant ID is available on
		// this platform.
		hostID = uuid.Generate()
	}

	// Do not persist in dev mode
	if c.config.DevMode {
		return hostID, uuid.Generate(), nil
	}

	// Attempt to read existing ID
	idPath := filepath.Join(c.config.StateDir, "client-id")
	idBuf, err := ioutil.ReadFile(idPath)
	if err != nil && !os.IsNotExist(err) {
		return "", "", err
	}

	// Attempt to read existing secret ID
	secretPath := filepath.Join(c.config.StateDir, "secret-id")
	secretBuf, err := ioutil.ReadFile(secretPath)
	if err != nil && !os.IsNotExist(err) {
		return "", "", err
	}

	// Use existing ID if any
	if len(idBuf) != 0 {
		id = strings.ToLower(string(idBuf))
	} else {
		id = hostID

		// Persist the ID
		if err := ioutil.WriteFile(idPath, []byte(id), 0700); err != nil {
			return "", "", err
		}
	}

	if len(secretBuf) != 0 {
		secret = string(secretBuf)
	} else {
		// Generate new ID
		secret = uuid.Generate()

		// Persist the ID
		if err := ioutil.WriteFile(secretPath, []byte(secret), 0700); err != nil {
			return "", "", err
		}
	}

	return id, secret, nil
}

// SetServers sets a new list of jobpool servers to connect to. As long as one
// server is resolvable no error is returned.
func (c *Client) SetServers(in []string) (int, error) {
	return c.setServersImpl(in, false)
}

// setServersImpl sets a new list of jobpool servers to connect to. If force is
// set, we add the server to the internal serverlist even if the server could not
// be pinged. An error is returned if no endpoints were valid when non-forcing.
//
// Force should be used when setting the servers from the initial configuration
// since the server may be starting up in parallel and initial pings may fail.
func (c *Client) setServersImpl(in []string, force bool) (int, error) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var merr multierror.Error
	endpoints := make([]*servers.Server, 0, len(in))
	wg.Add(len(in))

	for _, s := range in {
		go func(srv string) {
			defer wg.Done()
			addr, err := resolveServer(srv)
			if err != nil {
				mu.Lock()
				c.logger.Debug("ignoring server due to resolution error", "error", err, "server", srv)
				merr.Errors = append(merr.Errors, err)
				mu.Unlock()
				return
			}

			// Try to ping to check if it is a real server
			if err := c.Ping(addr); err != nil {
				mu.Lock()
				merr.Errors = append(merr.Errors, fmt.Errorf("Server at address %s failed ping: %v", addr, err))
				mu.Unlock()

				// If we are forcing the setting of the servers, inject it to
				// the serverlist even if we can't ping immediately.
				if !force {
					return
				}
			}

			mu.Lock()
			endpoints = append(endpoints, &servers.Server{Addr: addr})
			mu.Unlock()
		}(s)
	}

	wg.Wait()

	// Only return errors if no servers are valid
	if len(endpoints) == 0 {
		if len(merr.Errors) > 0 {
			return 0, merr.ErrorOrNil()
		}
		return 0, noServersErr
	}

	c.servers.SetServers(endpoints)
	return len(endpoints), nil
}

// periodicSnapshot is a long lived goroutine used to periodically snapshot the
// state of the client
func (c *Client) periodicSnapshot() {
	// Create a snapshot timer
	snapshot := time.After(stateSnapshotIntv)

	for {
		select {
		case <-snapshot:
			snapshot = time.After(stateSnapshotIntv)
			if err := c.saveState(); err != nil {
				c.logger.Error("error saving state", "error", err)
			}

		case <-c.shutdownCh:
			return
		}
	}
}

func (c *Client) saveState() error {
	c.logger.Trace("nothing to do in save snapshot right now!")
	return nil
}

func (c *Client) Shutdown() error {
	c.shutdownLock.Lock()
	defer c.shutdownLock.Unlock()
	if c.shutdown {
		c.logger.Info("already shutdown")
		return nil
	}
	c.logger.Info("start client shutting down")
	c.shutdown = true
	close(c.shutdownCh)
	c.connPool.Shutdown()
	c.shutdownGroup.Wait()
	c.saveState()
	return c.stateDB.Close()
}

// Go starts f in a goroutine and must be called before Wait.
func (g *group) Go(f func()) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f()
	}()
}

func (g *group) AddCh(ch <-chan struct{}) {
	g.Go(func() {
		<-ch
	})
}

// Wait for all goroutines to exit. Must be called after all calls to Go
// complete.
func (g *group) Wait() {
	g.wg.Wait()
}
