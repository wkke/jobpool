package config

import (
	log "github.com/hashicorp/go-hclog"
	"io"
	"os"
	"time"
	"yunli.com/jobpool/client/state"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper"
	"yunli.com/jobpool/version"
)

// RPCHandler can be provided to the Client if there is a local server
// to avoid going over the network. If not provided, the Client will
// maintain a connection pool to the servers
type RPCHandler interface {
	RPC(method string, args interface{}, reply interface{}) error
}

// Config is used to parameterize and configure the behavior of the client
type Config struct {
	// DevMode controls if we are in a development mode which
	// avoids persistent storage.
	DevMode bool

	// EnableDebug is used to enable debugging RPC endpoints
	// in the absence of ACLs
	EnableDebug bool

	// StateDir is where we store our state
	StateDir string

	// AllocDir is where we store data for allocations
	AllocDir string

	// LogOutput is the destination for logs
	LogOutput io.Writer

	// Logger provides a logger to the client
	Logger log.InterceptLogger

	// Region is the clients region
	Region string

	// Network interface to be used in network fingerprinting
	NetworkInterface string

	// Network speed is the default speed of network interfaces if they can not
	// be determined dynamically.
	NetworkSpeed int

	// CpuCompute is the default total CPU compute if they can not be determined
	// dynamically. It should be given as Cores * MHz (2 Cores * 2 Ghz = 4000)
	CpuCompute int

	// MemoryMB is the default node total memory in megabytes if it cannot be
	// determined dynamically.
	MemoryMB int

	// MaxKillTimeout allows capping the user-specifiable KillTimeout. If the
	// task's KillTimeout is greater than the MaxKillTimeout, MaxKillTimeout is
	// used.
	MaxKillTimeout time.Duration

	// Servers is a list of known server addresses. These are as "host:port"
	Servers []string

	// RPCHandler can be provided to avoid network traffic if the
	// server is running locally.
	RPCHandler RPCHandler

	// Node provides the base node
	Node *structs.Node

	// ClientMaxPort is the upper range of the ports that the client uses for
	// communicating with plugin subsystems over loopback
	ClientMaxPort uint

	// ClientMinPort is the lower range of the ports that the client uses for
	// communicating with plugin subsystems over loopback
	ClientMinPort uint

	// MaxDynamicPort is the largest dynamic port generated
	MaxDynamicPort int

	// MinDynamicPort is the smallest dynamic port generated
	MinDynamicPort int

	// A mapping of directories on the host OS to attempt to embed inside each
	// task's chroot.
	ChrootEnv map[string]string

	// Options provides arbitrary key-value configuration for jobpool internals,
	// like fingerprinters and drivers. The format is:
	//
	//	namespace.option = value
	Options map[string]string

	// Version is the version of the Jobpool client
	Version *version.VersionInfo

	// StatsCollectionInterval is the interval at which the Jobpool client
	// collects resource usage stats
	StatsCollectionInterval time.Duration

	// PublishNodeMetrics determines whether jobpool is going to publish node
	// level metrics to remote Telemetry sinks
	PublishNodeMetrics bool

	// PublishAllocationMetrics determines whether jobpool is going to publish
	// allocation metrics to remote Telemetry sinks
	PublishAllocationMetrics bool

	// GCInterval is the time interval at which the client triggers garbage
	// collection
	GCInterval time.Duration

	// GCParallelDestroys is the number of parallel destroys the garbage
	// collector will allow.
	GCParallelDestroys int

	// GCDiskUsageThreshold is the disk usage threshold given as a percent
	// beyond which the Jobpool client triggers GC of terminal allocations
	GCDiskUsageThreshold float64

	// GCInodeUsageThreshold is the inode usage threshold given as a percent
	// beyond which the Jobpool client triggers GC of the terminal allocations
	GCInodeUsageThreshold float64

	// GCMaxAllocs is the maximum number of allocations a node can have
	// before garbage collection is triggered.
	GCMaxAllocs int

	// LogLevel is the level of the logs to putout
	LogLevel string

	// NoHostUUID disables using the host's UUID and will force generation of a
	// random UUID.
	NoHostUUID bool

	// DisableRemoteExec disables remote exec targeting tasks on this client
	DisableRemoteExec bool

	// RPCHoldTimeout is how long an RPC can be "held" before it is errored.
	// This is used to paper over a loss of leadership by instead holding RPCs,
	// so that the caller experiences a slow response rather than an error.
	// This period is meant to be long enough for a leader election to take
	// place, and a small jitter is applied to avoid a thundering herd.
	RPCHoldTimeout time.Duration

	// StateDBFactory is used to override stateDB implementations,
	StateDBFactory state.NewStateDBFunc

	// BridgeNetworkName is the name to use for the bridge created in bridge
	// networking mode. This defaults to 'jobpool' if not set
	BridgeNetworkName string

	// BridgeNetworkAllocSubnet is the IP subnet to use for address allocation
	// for allocations in bridge networking mode. Subnet must be in CIDR
	// notation
	BridgeNetworkAllocSubnet string

	// HostNetworks is a map of the conigured host networks by name.
	HostNetworks map[string]*structs.ClientHostNetworkConfig

	// BindWildcardDefaultHostNetwork toggles if the default host network should accept all
	// destinations (true) or only filter on the IP of the default host network (false) when
	// port mapping. This allows Jobpool clients with no defined host networks to accept and
	// port forward traffic only matching on the destination port. An example use of this
	// is when a network loadbalancer is utilizing direct server return and the destination
	// address of incomming packets does not match the IP address of the host interface.
	//
	// This configuration is only considered if no host networks are defined.
	BindWildcardDefaultHostNetwork bool

	// CgroupParent is the parent cgroup Jobpool should use when managing any cgroup subsystems.
	// Currently this only includes the 'cpuset' cgroup subsystem.
	CgroupParent string

	// ReservableCores if set overrides the set of reservable cores reported in fingerprinting.
	ReservableCores []uint16

}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		Version:                 version.GetVersion(),
		LogOutput:               os.Stderr,
		Region:                  "global",
		StatsCollectionInterval: 1 * time.Second,
		LogLevel:                "DEBUG",
		GCInterval:              1 * time.Minute,
		GCParallelDestroys:      2,
		GCDiskUsageThreshold:    80,
		GCInodeUsageThreshold:   70,
		GCMaxAllocs:             50,
		NoHostUUID:              true,
		DisableRemoteExec:       false,
		RPCHoldTimeout:          5 * time.Second,
		HostNetworks:            map[string]*structs.ClientHostNetworkConfig{},
		CgroupParent:            "",
		MaxDynamicPort:          20000,
		MinDynamicPort:          32000,
	}
}

func (c *Config) Copy() *Config {
	nc := new(Config)
	*nc = *c
	nc.Node = nc.Node.Copy()
	nc.Servers = helper.CopySliceString(nc.Servers)
	nc.Options = helper.CopyMapStringString(nc.Options)
	if c.ReservableCores != nil {
		nc.ReservableCores = make([]uint16, len(c.ReservableCores))
		copy(nc.ReservableCores, c.ReservableCores)
	}
	return nc
}
