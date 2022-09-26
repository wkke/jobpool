package agent

import (
	"fmt"
	"strings"
	"time"
	"yunli.com/jobpool/command/cfg"

	log "github.com/hashicorp/go-hclog"
)

// retryJoiner is used to handle retrying a join until it succeeds or all of
// its tries are exhausted.
type retryJoiner struct {
	// serverJoin adds the specified servers to the serf cluster
	serverJoin func([]string) (int, error)

	// serverEnabled indicates whether the jobpool agent will run in server mode
	serverEnabled bool

	// clientJoin adds the specified servers to the serf cluster
	clientJoin func([]string) (int, error)

	// clientEnabled indicates whether the jobpool agent will run in client mode
	clientEnabled bool

	// errCh is used to communicate with the agent when the max retry attempt
	// limit has been reached
	errCh chan struct{}

	// logger is the retry joiners logger
	logger log.Logger
}

// Validate ensures that the configuration passes validity checks for the
// retry_join stanza. If the configuration is not valid, returns an error that
// will be displayed to the operator, otherwise nil.
func (r *retryJoiner) Validate(config *cfg.Config) error {

	// If retry_join is defined for the server, ensure that deprecated
	// fields and the server_join stanza are not both set
	if config.Server != nil && config.Server.ServerJoin != nil && len(config.Server.ServerJoin.RetryJoin) != 0 {
		if len(config.Server.RetryJoin) != 0 {
			return fmt.Errorf("server_join and retry_join cannot both be defined; prefer setting the server_join stanza")
		}
		if len(config.Server.StartJoin) != 0 {
			return fmt.Errorf("server_join and start_join cannot both be defined; prefer setting the server_join stanza")
		}
		if config.Server.RetryMaxAttempts != 0 {
			return fmt.Errorf("server_join and retry_max cannot both be defined; prefer setting the server_join stanza")
		}

		if config.Server.RetryInterval != 0 {
			return fmt.Errorf("server_join and retry_interval cannot both be defined; prefer setting the server_join stanza")
		}

		if len(config.Server.ServerJoin.StartJoin) != 0 {
			return fmt.Errorf("retry_join and start_join cannot both be defined")
		}
	}

	// if retry_join is defined for the client, ensure that start_join is not
	// set as this configuration is only defined for servers.
	if config.Client != nil && config.Client.ServerJoin != nil {
		if config.Client.ServerJoin.StartJoin != nil {
			return fmt.Errorf("start_join is not supported for Jobpool clients")
		}
	}

	return nil
}

// retryJoin is used to handle retrying a join until it succeeds or all retries
// are exhausted.
func (r *retryJoiner) RetryJoin(serverJoin *cfg.ServerJoin) {
	if len(serverJoin.RetryJoin) == 0 {
		return
	}

	attempt := 0

	addrsToJoin := strings.Join(serverJoin.RetryJoin, " ")
	r.logger.Info("starting retry join", "servers", addrsToJoin)

	for {
		var addrs []string
		var n int
		var err error

		for _, addr := range serverJoin.RetryJoin {
			addrs = append(addrs, addr)
		}

		if len(addrs) > 0 {
			if r.serverEnabled && r.serverJoin != nil {
				n, err = r.serverJoin(addrs)
				if err == nil {
					r.logger.Info("retry join completed", "initial_servers", n, "agent_mode", "server")
					return
				}
			}
			if r.clientEnabled && r.clientJoin != nil {
				n, err = r.clientJoin(addrs)
				if err == nil {
					r.logger.Info("retry join completed", "initial_servers", n, "agent_mode", "client")
					return
				}
			}
		}

		attempt++
		if serverJoin.RetryMaxAttempts > 0 && attempt > serverJoin.RetryMaxAttempts {
			r.logger.Error("max join retry exhausted, exiting")
			close(r.errCh)
			return
		}

		if err != nil {
			r.logger.Warn("join failed", "error", err, "retry", serverJoin.RetryInterval)
		}
		time.Sleep(serverJoin.RetryInterval)
	}
}
