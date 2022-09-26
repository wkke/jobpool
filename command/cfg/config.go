package cfg

import (
	"fmt"
	"net"
	"strconv"
	"time"
	client "yunli.com/jobpool/client/config"
	"yunli.com/jobpool/core"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/core/structs/config"
	"yunli.com/jobpool/helper"
	"yunli.com/jobpool/version"
)

type Config struct {
	// Region is the region this agent is in. Defaults to global.
	Region string `hcl:"region"`

	// Datacenter is the datacenter this agent is in. Defaults to dc1
	Datacenter string `hcl:"datacenter"`

	// NodeName is the name we register as. Defaults to hostname.
	NodeName string `hcl:"name"`
	// DataDir is the directory to store our state in
	DataDir string `hcl:"data_dir"`
	// LogLevel is the level of the logs to put out
	LogLevel string `hcl:"log_level"`

	// LogJson enables log output in a JSON format
	LogJson bool `hcl:"log_json"`

	// LogFile enables logging to a file
	LogFile string `hcl:"log_file"`

	// BindAddr is the address on which all of jobpool's services will
	// be bound. If not specified, this defaults to 127.0.0.1.
	BindAddr string `hcl:"bind_addr"`

	// EnableDebug is used to enable debugging HTTP endpoints
	EnableDebug bool `hcl:"enable_debug"`

	// Ports is used to control the network ports we bind to.
	Ports *Ports `hcl:"ports"`

	// Addresses is used to override the network addresses we bind to.
	//
	// Use normalizedAddrs if you need the host+port to bind to.
	Addresses *Addresses `hcl:"addresses"`

	EnableHostAddress bool `hcl:"enable_host_address"`

	// sample: %s.jobpool-server-service.jobpool.svc.cluster.local:%s
	AdvertiseAddrsFormat string `hcl:"advertise_format"`

	// normalizedAddr is set to the Address+Port by normalizeAddrs()
	NormalizedAddrs *NormalizedAddrs

	// AdvertiseAddrs is used to control the addresses we advertise.
	AdvertiseAddrs *AdvertiseAddrs `hcl:"advertise"`

	// Client has our client related settings
	Client *ClientConfig `hcl:"client"`

	// Server has our server related settings
	Server *ServerConfig `hcl:"server"`

	// LeaveOnInt is used to gracefully leave on the interrupt signal
	LeaveOnInt bool `hcl:"leave_on_interrupt"`

	// LeaveOnTerm is used to gracefully leave on the terminate signal
	LeaveOnTerm bool `hcl:"leave_on_terminate"`

	// EnableSyslog is used to enable sending logs to syslog
	EnableSyslog bool `hcl:"enable_syslog"`

	SyslogFacility string `hcl:"syslog_facility"`

	DisableUpdateCheck *bool `hcl:"disable_update_check"`

	CoreConfig *core.Config `hcl:"-" json:"-"`

	// ClientConfig is used to override the default config.
	// This is largely used for testing purposes.
	ClientConfig *client.Config `hcl:"-" json:"-"`

	// DevMode is set by the -dev CLI flag.
	DevMode bool `hcl:"-"`
	// Version information is set at compilation time
	Version *version.VersionInfo

	// HTTPAPIResponseHeaders allows users to configure the Jobpool http agent to
	// set arbitrary headers on API responses
	HTTPAPIResponseHeaders map[string]string `hcl:"http_api_response_headers"`

	// List of config files that have been loaded (in order)
	Files []string `hcl:"-"`

	Plugins map[string]*config.PluginConfig `hcl:"plugins"`

	// Limits contains the configuration for timeouts.
	Limits config.Limits `hcl:"limits"`
}

type ServerConfig struct {

	// Enabled controls if we are a server
	Enabled bool `hcl:"enabled"`

	// AuthoritativeRegion is used to control which region is treated as
	// the source of truth for global tokens and ACL policies.
	AuthoritativeRegion string `hcl:"authoritative_region"`

	// BootstrapExpect tries to automatically bootstrap the cluster,
	// by withholding peers until enough servers join.
	BootstrapExpect int `hcl:"bootstrap_expect"`

	// DataDir is the directory to store our state in
	DataDir string `hcl:"data_dir"`

	// ProtocolVersion is the protocol version to speak. This must be between
	// ProtocolVersionMin and ProtocolVersionMax.
	//
	// Deprecated: This has never been used and will emit a warning if nonzero.
	ProtocolVersion int `hcl:"protocol_version" json:"-"`

	// RaftProtocol is the Raft protocol version to speak. This must be from [1-3].
	RaftProtocol int `hcl:"raft_protocol"`

	// RaftMultiplier scales the Raft timing parameters
	RaftMultiplier *int `hcl:"raft_multiplier"`

	// NumSchedulers is the number of scheduler thread that are run.
	// This can be as many as one per core, or zero to disable this server
	// from doing any scheduling work.
	NumSchedulers *int `hcl:"num_schedulers"`

	// EnabledSchedulers controls the set of sub-schedulers that are
	// enabled for this server to handle. This will restrict the evaluations
	// that the workers dequeue for processing.
	EnabledSchedulers []string `hcl:"enabled_schedulers"`

	// NodeGCThreshold controls how "old" a node must be to be collected by GC.
	// Age is not the only requirement for a node to be GCed but the threshold
	// can be used to filter by age.
	NodeGCThreshold string `hcl:"node_gc_threshold"`

	// EvalGCThreshold controls how "old" an eval must be to be collected by GC.
	// Age is not the only requirement for a eval to be GCed but the threshold
	// can be used to filter by age.
	EvalGCThreshold string `hcl:"eval_gc_threshold"`

	// DeploymentGCThreshold controls how "old" a deployment must be to be
	// collected by GC.  Age is not the only requirement for a deployment to be
	// GCed but the threshold can be used to filter by age.
	DeploymentGCThreshold string `hcl:"deployment_gc_threshold"`

	// HeartbeatGrace is the grace period beyond the TTL to account for network,
	// processing delays and clock skew before marking a node as "down".
	HeartbeatGrace    time.Duration
	HeartbeatGraceHCL string `hcl:"heartbeat_grace" json:"-"`

	// MinHeartbeatTTL is the minimum time between heartbeats. This is used as
	// a floor to prevent excessive updates.
	MinHeartbeatTTL    time.Duration
	MinHeartbeatTTLHCL string `hcl:"min_heartbeat_ttl" json:"-"`

	// MaxHeartbeatsPerSecond is the maximum target rate of heartbeats
	// being processed per second. This allows the TTL to be increased
	// to meet the target rate.
	MaxHeartbeatsPerSecond float64 `hcl:"max_heartbeats_per_second"`

	// FailoverHeartbeatTTL is the TTL applied to heartbeats after
	// a new leader is elected, since we no longer know the status
	// of all the heartbeats.
	FailoverHeartbeatTTL    time.Duration
	FailoverHeartbeatTTLHCL string `hcl:"failover_heartbeat_ttl" json:"-"`

	// StartJoin is a list of addresses to attempt to join when the
	// agent starts. If Serf is unable to communicate with any of these
	// addresses, then the agent will error and exit.
	// Deprecated in Jobpool 0.10
	StartJoin []string `hcl:"start_join"`

	// RetryJoin is a list of addresses to join with retry enabled.
	// Deprecated in Jobpool 0.10
	RetryJoin []string `hcl:"retry_join"`

	// RetryMaxAttempts specifies the maximum number of times to retry joining a
	// host on startup. This is useful for cases where we know the node will be
	// online eventually.
	// Deprecated in Jobpool 0.10
	RetryMaxAttempts int `hcl:"retry_max"`

	// RetryInterval specifies the amount of time to wait in between join
	// attempts on agent start. The minimum allowed value is 1 second and
	// the default is 30s.
	// Deprecated in Jobpool 0.10
	RetryInterval    time.Duration
	RetryIntervalHCL string `hcl:"retry_interval" json:"-"`

	// RejoinAfterLeave controls our interaction with the cluster after leave.
	// When set to false (default), a leave causes to not rejoin
	// the cluster until an explicit join is received. If this is set to
	// true, we ignore the leave, and rejoin the cluster on start.
	RejoinAfterLeave bool `hcl:"rejoin_after_leave"`

	// (Enterprise-only) NonVotingServer is whether this server will act as a
	// non-voting member of the cluster to help provide read scalability.
	NonVotingServer bool `hcl:"non_voting_server"`

	// Encryption key to use for the Serf communication
	EncryptKey string `hcl:"encrypt" json:"-"`

	// ServerJoin contains information that is used to attempt to join servers
	ServerJoin *ServerJoin `hcl:"server_join"`

	// EnableEventBroker configures whether this server's state store
	// will generate events for its event stream.
	EnableEventBroker *bool `hcl:"enable_event_broker"`

	// EventBufferSize configure the amount of events to be held in memory.
	// If EnableEventBroker is set to true, the minimum allowable value
	// for the EventBufferSize is 1.
	EventBufferSize *int `hcl:"event_buffer_size"`

	// Search configures UI search features.
	Search *Search `hcl:"search"`

	// DeploymentQueryRateLimit is in queries per second and is used by the
	// DeploymentWatcher to throttle the amount of simultaneously deployments
	DeploymentQueryRateLimit float64 `hcl:"deploy_query_rate_limit"`

	// RaftBoltConfig configures boltdb as used by raft.
	RaftBoltConfig *RaftBoltConfig `hcl:"raft_boltdb"`
}

type ClientConfig struct {
	// Enabled controls if we are a client
	Enabled bool `hcl:"enabled"`

	// StateDir is the state directory
	StateDir string `hcl:"state_dir"`

	// AllocDir is the directory for storing allocation data
	AllocDir string `hcl:"alloc_dir"`

	// Servers is a list of known server addresses. These are as "host:port"
	Servers []string `hcl:"servers"`

	// NodeClass is used to group the node by class
	NodeClass string `hcl:"node_class"`

	// Options is used for configuration of jobpool internals,
	// like fingerprinters and drivers. The format is:
	//
	//  namespace.option = value
	Options map[string]string `hcl:"options"`

	// Metadata associated with the node
	Meta map[string]string `hcl:"meta"`

	// Interface to use for network fingerprinting
	NetworkInterface string `hcl:"network_interface"`

	// MaxKillTimeout allows capping the user-specifiable KillTimeout.
	MaxKillTimeout string `hcl:"max_kill_timeout"`

	// ClientMaxPort is the upper range of the ports that the client uses for
	// communicating with plugin subsystems
	ClientMaxPort int `hcl:"client_max_port"`

	// ClientMinPort is the lower range of the ports that the client uses for
	// communicating with plugin subsystems
	ClientMinPort int `hcl:"client_min_port"`

	// MaxDynamicPort is the upper range of the dynamic ports that the client
	// uses for allocations
	MaxDynamicPort int `hcl:"max_dynamic_port"`

	// MinDynamicPort is the lower range of the dynamic ports that the client
	// uses for allocations
	MinDynamicPort int `hcl:"min_dynamic_port"`

	// GCInterval is the time interval at which the client triggers garbage
	// collection
	GCInterval    time.Duration
	GCIntervalHCL string `hcl:"gc_interval" json:"-"`

	// GCParallelDestroys is the number of parallel destroys the garbage
	// collector will allow.
	GCParallelDestroys int `hcl:"gc_parallel_destroys"`

	// GCDiskUsageThreshold is the disk usage threshold given as a percent
	// beyond which the Jobpool client triggers GC of terminal allocations
	GCDiskUsageThreshold float64 `hcl:"gc_disk_usage_threshold"`

	// GCInodeUsageThreshold is the inode usage threshold beyond which the Jobpool
	// client triggers GC of the terminal allocations
	GCInodeUsageThreshold float64 `hcl:"gc_inode_usage_threshold"`

	// GCMaxAllocs is the maximum number of allocations a node can have
	// before garbage collection is triggered.
	GCMaxAllocs int `hcl:"gc_max_allocs"`

	// NoHostUUID disables using the host's UUID and will force generation of a
	// random UUID.
	NoHostUUID *bool `hcl:"no_host_uuid"`

	// DisableRemoteExec disables remote exec targeting tasks on this client
	DisableRemoteExec bool `hcl:"disable_remote_exec"`

	// ServerJoin contains information that is used to attempt to join servers
	ServerJoin *ServerJoin `hcl:"server_join"`

	// BridgeNetworkName is the name of the bridge to create when using the
	// bridge network mode
	BridgeNetworkName string `hcl:"bridge_network_name"`

	// BridgeNetworkSubnet is the subnet to allocate IP addresses from when
	// creating allocations with bridge networking mode. This range is local to
	// the host
	BridgeNetworkSubnet string `hcl:"bridge_network_subnet"`

	// HostNetworks describes the different host networks available to the host
	// if the host uses multiple interfaces
	HostNetworks []*structs.ClientHostNetworkConfig `hcl:"host_network"`

	BindWildcardDefaultHostNetwork bool `hcl:"bind_wildcard_default_host_network"`

	// CgroupParent sets the parent cgroup for subsystems managed by Jobpool. If the cgroup
	// doest not exist Jobpool will attempt to create it during startup. Defaults to '/jobpool'
	CgroupParent string `hcl:"cgroup_parent"`

	// client driver, used to run task, 有driver 自己用 hcl 把字符串类型的配置转换为需要的结构体
	Driver map[string]interface{} `hcl:"driver"`
}

type RaftBoltConfig struct {
	// NoFreelistSync toggles whether the underlying raft storage should sync its
	// freelist to disk within the bolt .db file. When disabled, IO performance
	// will be improved but at the expense of longer startup times.
	//
	// Default: false.
	NoFreelistSync bool `hcl:"no_freelist_sync"`
}

// Search is used in servers to configure search API options.
type Search struct {
	// FuzzyEnabled toggles whether the FuzzySearch API is enabled. If not
	// enabled, requests to /v1/search/fuzzy will reply with a 404 response code.
	//
	// Default: enabled.
	FuzzyEnabled bool `hcl:"fuzzy_enabled"`

	// LimitQuery limits the number of objects searched in the FuzzySearch API.
	// The results are indicated as truncated if the limit is reached.
	//
	// Lowering this value can reduce resource consumption of Jobpool server when
	// the FuzzySearch API is enabled.
	//
	// Default value: 20.
	LimitQuery int `hcl:"limit_query"`

	// LimitResults limits the number of results provided by the FuzzySearch API.
	// The results are indicated as truncate if the limit is reached.
	//
	// Lowering this value can reduce resource consumption of Jobpool server per
	// fuzzy search request when the FuzzySearch API is enabled.
	//
	// Default value: 100.
	LimitResults int `hcl:"limit_results"`

	// MinTermLength is the minimum length of Text required before the FuzzySearch
	// API will return results.
	//
	// Increasing this value can avoid resource consumption on Jobpool server by
	// reducing searches with less meaningful results.
	//
	// Default value: 2.
	MinTermLength int `hcl:"min_term_length"`
}

// ServerJoin is used in both clients and servers to bootstrap connections to
// servers
type ServerJoin struct {
	// StartJoin is a list of addresses to attempt to join when the
	// agent starts. If Serf is unable to communicate with any of these
	// addresses, then the agent will error and exit.
	StartJoin []string `hcl:"start_join"`

	// RetryJoin is a list of addresses to join with retry enabled, or a single
	// value to find multiple servers using go-discover syntax.
	RetryJoin []string `hcl:"retry_join"`

	// RetryMaxAttempts specifies the maximum number of times to retry joining a
	// host on startup. This is useful for cases where we know the node will be
	// online eventually.
	RetryMaxAttempts int `hcl:"retry_max"`

	// RetryInterval specifies the amount of time to wait in between join
	// attempts on agent start. The minimum allowed value is 1 second and
	// the default is 30s.
	RetryInterval    time.Duration
	RetryIntervalHCL string `hcl:"retry_interval" json:"-"`

	// ExtraKeysHCL is used by hcl to surface unexpected keys
	ExtraKeysHCL []string `hcl:",unusedKeys" json:"-"`
}

type Ports struct {
	HTTP int `hcl:"http"`
	RPC  int `hcl:"rpc"`
	Serf int `hcl:"serf"`
	// ExtraKeysHCL is used by hcl to surface unexpected keys
	ExtraKeysHCL []string `hcl:",unusedKeys" json:"-"`
}

// Addresses encapsulates all of the addresses we bind to for various
// network services. Everything is optional and defaults to BindAddr.
type Addresses struct {
	HTTP string `hcl:"http"`
	RPC  string `hcl:"rpc"`
	Serf string `hcl:"serf"`
	// ExtraKeysHCL is used by hcl to surface unexpected keys
	ExtraKeysHCL []string `hcl:",unusedKeys" json:"-"`
}

type NormalizedAddrs struct {
	HTTP []string
	RPC  string
	Serf string
}

// AdvertiseAddrs is used to control the addresses we advertise out for
// different network services. All are optional and default to BindAddr and
// their default Port.
type AdvertiseAddrs struct {
	HTTP string `hcl:"http"`
	RPC  string `hcl:"rpc"`
	Serf string `hcl:"serf"`
	// ExtraKeysHCL is used by hcl to surface unexpected keys
	ExtraKeysHCL []string `hcl:",unusedKeys" json:"-"`
}

// devModeConfig holds the config for the -dev and -dev-connect flags
type DevModeConfig struct {
	// mode flags are set at the command line via -dev and -dev-connect
	defaultMode bool
	connectMode bool

	bindAddr string
	iface    string
}

func (c *Config) Merge(b *Config) *Config {
	result := *c

	if b.Region != "" {
		result.Region = b.Region
	}
	if b.Datacenter != "" {
		result.Datacenter = b.Datacenter
	}
	if b.NodeName != "" {
		result.NodeName = b.NodeName
	}
	if b.DataDir != "" {
		result.DataDir = b.DataDir
	}
	if b.LogLevel != "" {
		result.LogLevel = b.LogLevel
	}
	if b.LogJson {
		result.LogJson = true
	}
	if b.LogFile != "" {
		result.LogFile = b.LogFile
	}
	if b.BindAddr != "" {
		result.BindAddr = b.BindAddr
	}
	if b.EnableDebug {
		result.EnableDebug = true
	}
	if b.EnableHostAddress {
		result.EnableHostAddress = true
	}
	if b.AdvertiseAddrsFormat != "" {
		result.AdvertiseAddrsFormat = b.AdvertiseAddrsFormat
	}
	if b.LeaveOnInt {
		result.LeaveOnInt = true
	}
	if b.LeaveOnTerm {
		result.LeaveOnTerm = true
	}
	if b.EnableSyslog {
		result.EnableSyslog = true
	}
	if b.SyslogFacility != "" {
		result.SyslogFacility = b.SyslogFacility
	}
	if b.DisableUpdateCheck != nil {
		result.DisableUpdateCheck = helper.BoolToPtr(*b.DisableUpdateCheck)
	}

	// Apply the client config
	if result.Client == nil && b.Client != nil {
		client := *b.Client
		result.Client = &client
	} else if b.Client != nil {
		result.Client = result.Client.Merge(b.Client)
	}

	// Apply the server config
	if result.Server == nil && b.Server != nil {
		server := *b.Server
		result.Server = &server
	} else if b.Server != nil {
		result.Server = result.Server.Merge(b.Server)
	}

	// Apply the ports config
	if result.Ports == nil && b.Ports != nil {
		ports := *b.Ports
		result.Ports = &ports
	} else if b.Ports != nil {
		result.Ports = result.Ports.Merge(b.Ports)
	}

	// Apply the address config
	if result.Addresses == nil && b.Addresses != nil {
		addrs := *b.Addresses
		result.Addresses = &addrs
	} else if b.Addresses != nil {
		result.Addresses = result.Addresses.Merge(b.Addresses)
	}

	// Apply the advertise addrs config
	if result.AdvertiseAddrs == nil && b.AdvertiseAddrs != nil {
		advertise := *b.AdvertiseAddrs
		result.AdvertiseAddrs = &advertise
	} else if b.AdvertiseAddrs != nil {
		result.AdvertiseAddrs = result.AdvertiseAddrs.Merge(b.AdvertiseAddrs)
	}

	if result.Plugins == nil {
		result.Plugins = make(map[string]*config.PluginConfig)
	}
	for k, v := range b.Plugins {
		result.Plugins[k] = v
	}

	if b.Files != nil && len(b.Files) > 0 {
		result.Files = b.Files
	}
	result.Limits = c.Limits.Merge(b.Limits)

	return &result
}

func (s *ServerConfig) Merge(b *ServerConfig) *ServerConfig {
	result := *s

	if b.Enabled {
		result.Enabled = true
	}
	if b.AuthoritativeRegion != "" {
		result.AuthoritativeRegion = b.AuthoritativeRegion
	}
	if b.BootstrapExpect > 0 {
		result.BootstrapExpect = b.BootstrapExpect
	}
	if b.DataDir != "" {
		result.DataDir = b.DataDir
	}
	if b.ProtocolVersion != 0 {
		result.ProtocolVersion = b.ProtocolVersion
	}
	if b.RaftProtocol != 0 {
		result.RaftProtocol = b.RaftProtocol
	}
	if b.RaftMultiplier != nil {
		c := *b.RaftMultiplier
		result.RaftMultiplier = &c
	}
	if b.NumSchedulers != nil {
		result.NumSchedulers = helper.IntToPtr(*b.NumSchedulers)
	}
	if b.NodeGCThreshold != "" {
		result.NodeGCThreshold = b.NodeGCThreshold
	}

	if b.EvalGCThreshold != "" {
		result.EvalGCThreshold = b.EvalGCThreshold
	}
	if b.DeploymentGCThreshold != "" {
		result.DeploymentGCThreshold = b.DeploymentGCThreshold
	}

	if b.HeartbeatGrace != 0 {
		result.HeartbeatGrace = b.HeartbeatGrace
	}
	if b.HeartbeatGraceHCL != "" {
		result.HeartbeatGraceHCL = b.HeartbeatGraceHCL
	}
	if b.MinHeartbeatTTL != 0 {
		result.MinHeartbeatTTL = b.MinHeartbeatTTL
	}
	if b.MinHeartbeatTTLHCL != "" {
		result.MinHeartbeatTTLHCL = b.MinHeartbeatTTLHCL
	}
	if b.MaxHeartbeatsPerSecond != 0.0 {
		result.MaxHeartbeatsPerSecond = b.MaxHeartbeatsPerSecond
	}
	if b.FailoverHeartbeatTTL != 0 {
		result.FailoverHeartbeatTTL = b.FailoverHeartbeatTTL
	}
	if b.FailoverHeartbeatTTLHCL != "" {
		result.FailoverHeartbeatTTLHCL = b.FailoverHeartbeatTTLHCL
	}
	if b.RetryMaxAttempts != 0 {
		result.RetryMaxAttempts = b.RetryMaxAttempts
	}
	if b.RetryInterval != 0 {
		result.RetryInterval = b.RetryInterval
	}
	if b.RetryIntervalHCL != "" {
		result.RetryIntervalHCL = b.RetryIntervalHCL
	}
	if b.RejoinAfterLeave {
		result.RejoinAfterLeave = true
	}
	if b.NonVotingServer {
		result.NonVotingServer = true
	}
	if b.EncryptKey != "" {
		result.EncryptKey = b.EncryptKey
	}
	if b.ServerJoin != nil {
		result.ServerJoin = result.ServerJoin.Merge(b.ServerJoin)
	}

	if b.EnableEventBroker != nil {
		result.EnableEventBroker = b.EnableEventBroker
	}

	if b.EventBufferSize != nil {
		result.EventBufferSize = b.EventBufferSize
	}

	if b.DeploymentQueryRateLimit != 0 {
		result.DeploymentQueryRateLimit = b.DeploymentQueryRateLimit
	}

	if b.Search != nil {
		result.Search = &Search{FuzzyEnabled: b.Search.FuzzyEnabled}
		if b.Search.LimitQuery > 0 {
			result.Search.LimitQuery = b.Search.LimitQuery
		}
		if b.Search.LimitResults > 0 {
			result.Search.LimitResults = b.Search.LimitResults
		}
		if b.Search.MinTermLength > 0 {
			result.Search.MinTermLength = b.Search.MinTermLength
		}
	}

	if b.RaftBoltConfig != nil {
		result.RaftBoltConfig = &RaftBoltConfig{
			NoFreelistSync: b.RaftBoltConfig.NoFreelistSync,
		}
	}

	// Copy the start join addresses
	result.StartJoin = make([]string, 0, len(s.StartJoin)+len(b.StartJoin))
	result.StartJoin = append(result.StartJoin, s.StartJoin...)
	result.StartJoin = append(result.StartJoin, b.StartJoin...)

	// Copy the retry join addresses
	result.RetryJoin = make([]string, 0, len(s.RetryJoin)+len(b.RetryJoin))
	result.RetryJoin = append(result.RetryJoin, s.RetryJoin...)
	result.RetryJoin = append(result.RetryJoin, b.RetryJoin...)

	return &result
}

// Merge is used to merge two client configs together
func (a *ClientConfig) Merge(b *ClientConfig) *ClientConfig {
	result := *a

	if b.Enabled {
		result.Enabled = true
	}
	if b.StateDir != "" {
		result.StateDir = b.StateDir
	}
	if b.AllocDir != "" {
		result.AllocDir = b.AllocDir
	}
	if b.NodeClass != "" {
		result.NodeClass = b.NodeClass
	}
	if b.NetworkInterface != "" {
		result.NetworkInterface = b.NetworkInterface
	}
	if b.MaxKillTimeout != "" {
		result.MaxKillTimeout = b.MaxKillTimeout
	}
	if b.ClientMaxPort != 0 {
		result.ClientMaxPort = b.ClientMaxPort
	}
	if b.ClientMinPort != 0 {
		result.ClientMinPort = b.ClientMinPort
	}
	if b.MaxDynamicPort != 0 {
		result.MaxDynamicPort = b.MaxDynamicPort
	}
	if b.MinDynamicPort != 0 {
		result.MinDynamicPort = b.MinDynamicPort
	}
	if b.GCInterval != 0 {
		result.GCInterval = b.GCInterval
	}
	if b.GCIntervalHCL != "" {
		result.GCIntervalHCL = b.GCIntervalHCL
	}
	if b.GCParallelDestroys != 0 {
		result.GCParallelDestroys = b.GCParallelDestroys
	}
	if b.GCDiskUsageThreshold != 0 {
		result.GCDiskUsageThreshold = b.GCDiskUsageThreshold
	}
	if b.GCInodeUsageThreshold != 0 {
		result.GCInodeUsageThreshold = b.GCInodeUsageThreshold
	}
	if b.GCMaxAllocs != 0 {
		result.GCMaxAllocs = b.GCMaxAllocs
	}
	// NoHostUUID defaults to true, merge if false
	if b.NoHostUUID != nil {
		result.NoHostUUID = b.NoHostUUID
	}

	if b.DisableRemoteExec {
		result.DisableRemoteExec = b.DisableRemoteExec
	}
	// Add the servers
	result.Servers = append(result.Servers, b.Servers...)

	// Add the options map values
	if result.Options == nil {
		result.Options = make(map[string]string)
	}
	for k, v := range b.Options {
		result.Options[k] = v
	}

	// Add the meta map values
	if result.Meta == nil {
		result.Meta = make(map[string]string)
	}
	for k, v := range b.Meta {
		result.Meta[k] = v
	}

	if b.ServerJoin != nil {
		result.ServerJoin = result.ServerJoin.Merge(b.ServerJoin)
	}
	if b.BindWildcardDefaultHostNetwork {
		result.BindWildcardDefaultHostNetwork = true
	}

	if result.Driver == nil {
		result.Driver = make(map[string]interface{})
	}
	for k, v := range b.Driver {
		result.Driver[k] = v
	}
	return &result
}

// Merge is used to merge two port configurations.
func (a *Ports) Merge(b *Ports) *Ports {
	result := *a

	if b.HTTP != 0 {
		result.HTTP = b.HTTP
	}
	if b.RPC != 0 {
		result.RPC = b.RPC
	}
	if b.Serf != 0 {
		result.Serf = b.Serf
	}
	return &result
}

// Merge is used to merge two address configs together.
func (a *Addresses) Merge(b *Addresses) *Addresses {
	result := *a

	if b.HTTP != "" {
		result.HTTP = b.HTTP
	}
	if b.RPC != "" {
		result.RPC = b.RPC
	}
	if b.Serf != "" {
		result.Serf = b.Serf
	}
	return &result
}

// Merge merges two advertise addrs configs together.
func (a *AdvertiseAddrs) Merge(b *AdvertiseAddrs) *AdvertiseAddrs {
	result := *a

	if b.RPC != "" {
		result.RPC = b.RPC
	}
	if b.Serf != "" {
		result.Serf = b.Serf
	}
	if b.HTTP != "" {
		result.HTTP = b.HTTP
	}
	return &result
}

func (s *ServerJoin) Merge(b *ServerJoin) *ServerJoin {
	if s == nil {
		return b
	}

	result := *s

	if b == nil {
		return &result
	}

	if len(b.StartJoin) != 0 {
		result.StartJoin = b.StartJoin
	}
	if len(b.RetryJoin) != 0 {
		result.RetryJoin = b.RetryJoin
	}
	if b.RetryMaxAttempts != 0 {
		result.RetryMaxAttempts = b.RetryMaxAttempts
	}
	if b.RetryInterval != 0 {
		result.RetryInterval = b.RetryInterval
	}

	return &result
}

func (c *Config) Listener(proto, addr string, port int) (net.Listener, error) {
	if addr == "" {
		addr = c.BindAddr
	}

	// Do our own range check to avoid bugs in package net.
	//
	//   golang.org/issue/11715
	//   golang.org/issue/13447
	//
	// Both of the above bugs were fixed by golang.org/cl/12447 which will be
	// included in Go 1.6. The error returned below is the same as what Go 1.6
	// will return.
	if 0 > port || port > 65535 {
		return nil, &net.OpError{
			Op:  "listen",
			Net: proto,
			Err: &net.AddrError{Err: "invalid port", Addr: fmt.Sprint(port)},
		}
	}
	return net.Listen(proto, net.JoinHostPort(addr, strconv.Itoa(port)))
}
