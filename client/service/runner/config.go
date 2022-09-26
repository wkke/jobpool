package runner

import (
	log "github.com/hashicorp/go-hclog"
	clientconfig "yunli.com/jobpool/client/config"
	"yunli.com/jobpool/client/interfaces"
	cstate "yunli.com/jobpool/client/state"
	"yunli.com/jobpool/core/structs"
)

// Config holds the configuration for creating an allocation runner.
type Config struct {
	// Logger is the logger for the allocation runner.
	Logger log.Logger

	// ClientConfig is the clients configuration.
	ClientConfig *clientconfig.Config

	// Alloc captures the allocation that should be run.
	Alloc *structs.Allocation

	// StateDB is used to store and restore state.
	StateDB cstate.StateDB

	// StateUpdater is used to emit updated task state
	StateUpdater interfaces.AllocStateHandler

	// serversContactedCh is passed to TaskRunners so they can detect when
	// servers have been contacted for the first time in case of a failed
	// restore.
	ServersContactedCh chan struct{}

	// RPCClient is the RPC Client that should be used by the allocrunner and its
	// hooks to communicate with Jobpool Servers.
	RPCClient RPCer

	Region string
}
