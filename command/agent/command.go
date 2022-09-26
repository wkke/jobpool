package agent

import (
	"flag"
	"fmt"
	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	gsyslog "github.com/hashicorp/go-syslog"
	"github.com/hashicorp/logutils"
	"github.com/mitchellh/cli"
	"github.com/posener/complete"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"yunli.com/jobpool/command/cfg"
	"yunli.com/jobpool/core/constant"
	flaghelper "yunli.com/jobpool/helper/flags"
	"yunli.com/jobpool/helper/gatedwriter"
	"yunli.com/jobpool/helper/logging"
	"yunli.com/jobpool/helper/winsvc"
	"yunli.com/jobpool/version"
)

const gracefulTimeout = 5 * time.Second

type Command struct {
	Version    *version.VersionInfo
	Ui         cli.Ui
	ShutdownCh <-chan struct{}

	args           []string
	agent          *Agent
	httpServers    []*HTTPServer
	logFilter      *logutils.LevelFilter
	logOutput      io.Writer
	retryJoinErrCh chan struct{}
}

func (c *Command) AutocompleteArgs() complete.Predictor {
	return nil
}

func (c *Command) Run(args []string) int {
	c.Ui = &cli.PrefixedUi{
		OutputPrefix: "==> ",
		InfoPrefix:   "    ",
		ErrorPrefix:  "==> ",
		Ui:           c.Ui,
	}
	c.args = args
	config := c.readConfig()
	if config == nil {
		return 1
	}
	// reset UI to prevent prefixed json output
	if config.LogJson {
		c.Ui = &cli.BasicUi{
			Reader:      os.Stdin,
			Writer:      os.Stdout,
			ErrorWriter: os.Stderr,
		}
	}
	// Setup the log outputs
	logFilter, logGate, logOutput := SetupLoggers(c.Ui, config)
	c.logFilter = logFilter
	c.logOutput = logOutput
	if logGate == nil {
		return 1
	}

	// Create logger
	logger := hclog.NewInterceptLogger(&hclog.LoggerOptions{
		Name:       "agent",
		Level:      hclog.LevelFromString(config.LogLevel),
		Output:     logOutput,
		JSONFormat: config.LogJson,
	}) // Wrap log messages emitted with the 'log' package.
	// These usually come from external dependencies.
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{
		InferLevels:              true,
		InferLevelsWithTimestamp: true,
	}))
	log.SetPrefix("")
	log.SetFlags(0)
	// Swap out UI implementation if json logging is enabled
	if config.LogJson {
		c.Ui = &logging.HcLogUI{Log: logger}
	}

	// Log config files
	if len(config.Files) > 0 {
		c.Ui.Output(fmt.Sprintf("Loaded configuration from %s", strings.Join(config.Files, ", ")))
	} else {
		c.Ui.Output("No configuration files loaded")
	}

	// Initialize the telemetry
	inmem, err := c.setupTelemetry(config)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error initializing telemetry: %s", err))
		return 1
	}

	// Create the agent
	if err := c.setupAgent(config, logger, logOutput, inmem); err != nil {
		logGate.Flush()
		return 1
	}
	defer func() {
		c.agent.Shutdown()
		// Shutdown the http server at the end, to ease debugging if
		// the agent takes long to shutdown
		if len(c.httpServers) > 0 {
			for _, srv := range c.httpServers {
				srv.Shutdown()
			}
		}
	}()
	// Join startup nodes if specified
	if err := c.startupJoin(config); err != nil {
		c.Ui.Error(err.Error())
		return 1
	}
	// Compile agent information for output later
	info := make(map[string]string)
	info["name"] = config.NodeName
	info["version"] = config.Version.Version
	info["client"] = strconv.FormatBool(config.Client.Enabled)
	info["log level"] = config.LogLevel
	info["server"] = strconv.FormatBool(config.Server.Enabled)
	info["region"] = fmt.Sprintf("%s (DC: %s)", config.Region, config.Datacenter)
	info["bind addrs"] = c.getBindAddrSynopsis()
	info["advertise addrs"] = c.getAdvertiseAddrSynopsis()

	// Sort the keys for output
	infoKeys := make([]string, 0, len(info))
	for key := range info {
		infoKeys = append(infoKeys, key)
	}
	sort.Strings(infoKeys)

	// Agent configuration output
	padding := 18
	c.Ui.Output("jobpool agent configuration:\n")
	for _, k := range infoKeys {
		c.Ui.Info(fmt.Sprintf(
			"%s%s: %s",
			strings.Repeat(" ", padding-len(k)),
			strings.Title(k),
			info[k]))
	}
	c.Ui.Output("")

	// Output the header that the server has started
	c.Ui.Output("jobpool agent started! Log data will stream in below:\n")

	// Enable log streaming
	logGate.Flush()

	// Start retry join process
	if err := c.handleRetryJoin(config); err != nil {
		c.Ui.Error(err.Error())
		return 1
	}

	// Wait for exit
	return c.handleSignals()
}

// handleRetryJoin is used to start retry joining if it is configured.
func (c *Command) handleRetryJoin(config *cfg.Config) error {
	c.retryJoinErrCh = make(chan struct{})

	if config.Server.Enabled && len(config.Server.RetryJoin) != 0 {
		joiner := retryJoiner{
			errCh:         c.retryJoinErrCh,
			logger:        c.agent.logger.Named("joiner"),
			serverJoin:    c.agent.server.Join,
			serverEnabled: true,
		}

		if err := joiner.Validate(config); err != nil {
			return err
		}

		// Remove the duplicate fields
		if len(config.Server.RetryJoin) != 0 {
			config.Server.ServerJoin.RetryJoin = config.Server.RetryJoin
			config.Server.RetryJoin = nil
		}
		if config.Server.RetryMaxAttempts != 0 {
			config.Server.ServerJoin.RetryMaxAttempts = config.Server.RetryMaxAttempts
			config.Server.RetryMaxAttempts = 0
		}
		if config.Server.RetryInterval != 0 {
			config.Server.ServerJoin.RetryInterval = config.Server.RetryInterval
			config.Server.RetryInterval = 0
		}

		c.agent.logger.Warn("using deprecated retry_join fields. Upgrade configuration to use server_join")
	}

	if config.Server.Enabled &&
		config.Server.ServerJoin != nil &&
		len(config.Server.ServerJoin.RetryJoin) != 0 {

		joiner := retryJoiner{
			errCh:         c.retryJoinErrCh,
			logger:        c.agent.logger.Named("joiner"),
			serverJoin:    c.agent.server.Join,
			serverEnabled: true,
		}

		if err := joiner.Validate(config); err != nil {
			return err
		}

		go joiner.RetryJoin(config.Server.ServerJoin)
	}

	if config.Client.Enabled &&
		config.Client.ServerJoin != nil &&
		len(config.Client.ServerJoin.RetryJoin) != 0 {
		joiner := retryJoiner{
			errCh:         c.retryJoinErrCh,
			logger:        c.agent.logger.Named("joiner"),
			clientJoin:    c.agent.client.SetServers,
			clientEnabled: true,
		}

		if err := joiner.Validate(config); err != nil {
			return err
		}

		go joiner.RetryJoin(config.Client.ServerJoin)
	}

	return nil
}

// handleSignals blocks until we get an exit-causing signal
func (c *Command) handleSignals() int {
	signalCh := make(chan os.Signal, 4)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGPIPE)

	// Wait for a signal
WAIT:
	var sig os.Signal
	select {
	case s := <-signalCh:
		sig = s
	case <-winsvc.ShutdownChannel():
		sig = os.Interrupt
	case <-c.ShutdownCh:
		sig = os.Interrupt
	case <-c.retryJoinErrCh:
		return 1
	}

	// Skip any SIGPIPE signal and don't try to log it (See issues #1798, #3554)
	if sig == syscall.SIGPIPE {
		goto WAIT
	}

	c.Ui.Output(fmt.Sprintf("Caught signal: %v", sig))

	// Check if this is a SIGHUP
	/*
		if sig == syscall.SIGHUP {
			c.handleReload()
			goto WAIT
		}
	*/

	// Check if we should do a graceful leave
	graceful := false
	if sig == os.Interrupt && c.agent.GetConfig().LeaveOnInt {
		graceful = true
	} else if sig == syscall.SIGTERM && c.agent.GetConfig().LeaveOnTerm {
		graceful = true
	}

	// Bail fast if not doing a graceful leave
	if !graceful {
		return 1
	}

	// Attempt a graceful leave
	gracefulCh := make(chan struct{})
	c.Ui.Output("Gracefully shutting down agent...")
	go func() {
		if err := c.agent.Leave(); err != nil {
			c.Ui.Error(fmt.Sprintf("Error: %s", err))
			return
		}
		close(gracefulCh)
	}()

	// Wait for leave or another signal
	select {
	case <-signalCh:
		return 1
	case <-time.After(gracefulTimeout):
		return 1
	case <-gracefulCh:
		return 0
	}
}

func SetupLoggers(ui cli.Ui, config *cfg.Config) (*logutils.LevelFilter, *gatedwriter.Writer, io.Writer) {
	// Setup logging. First create the gated log writer, which will
	// store logs until we're ready to show them. Then create the level
	// filter, filtering logs of the specified level.
	logGate := &gatedwriter.Writer{
		Writer: &cli.UiWriter{Ui: ui},
	}

	logFilter := logging.LevelFilter()
	logFilter.MinLevel = logutils.LogLevel(strings.ToUpper(config.LogLevel))
	logFilter.Writer = logGate
	if !logging.ValidateLevelFilter(logFilter.MinLevel, logFilter) {
		ui.Error(fmt.Sprintf(
			"Invalid log level: %s. Valid log levels are: %v",
			logFilter.MinLevel, logFilter.Levels))
		return nil, nil, nil
	}

	// Create a log writer, and wrap a logOutput around it
	writers := []io.Writer{logFilter}

	// Check if syslog is enabled
	if config.EnableSyslog {
		syslogger, err := gsyslog.NewLogger(gsyslog.LOG_NOTICE, config.SyslogFacility, constant.JobPoolName)
		if err != nil {
			ui.Error(fmt.Sprintf("Syslog setup failed: %v", err))
			return nil, nil, nil
		}
		writers = append(writers, &logging.SyslogWrapper{
			L:    syslogger,
			Filt: logFilter,
		})
	}

	// Check if file logging is enabled
	if config.LogFile != "" {
		dir, fileName := filepath.Split(config.LogFile)

		// if a path is provided, but has no filename, then a default is used.
		if fileName == "" {
			fileName = "jobpool.log"
		}

		// Try to enter the user specified log rotation duration first
		var logRotateDuration time.Duration
		logRotateDuration = 24 * time.Hour
		logFile := &logging.LogFile{
			LogFilter: logFilter,
			FileName:  fileName,
			LogPath:   dir,
			Duration:  logRotateDuration,
		}

		writers = append(writers, logFile)
	}

	logOutput := io.MultiWriter(writers...)
	return logFilter, logGate, logOutput
}

// setupAgent is used to start the agent and various interfaces
func (c *Command) setupAgent(config *cfg.Config, logger hclog.InterceptLogger, logOutput io.Writer, inmem *metrics.InmemSink) error {
	c.Ui.Output("Starting jobpool agent...")
	agent, err := NewAgent(config, logger, logOutput, inmem)
	if err != nil {
		// log the error as well, so it appears at the end
		logger.Error("error starting agent", "error", err)
		c.Ui.Error(fmt.Sprintf("Error starting agent: %s", err))
		return err
	}
	c.agent = agent

	// Setup the HTTP server
	httpServers, err := NewHTTPServers(agent, config)
	if err != nil {
		agent.Shutdown()
		c.Ui.Error(fmt.Sprintf("Error starting http server: %s", err))
		return err
	}
	c.httpServers = httpServers
	return nil
}

func (c *Command) startupJoin(config *cfg.Config) error {
	// Nothing to do
	if !config.Server.Enabled {
		return nil
	}

	// TODO to be deleted
	//serverConfig, err2 := json.Marshal(config.Server)
	//if err2 == nil {
	//	c.Ui.Info(fmt.Sprintf("the server config is: %s", serverConfig))
	//}

	// Validate both old and new aren't being set
	old := len(config.Server.StartJoin)
	var new int
	if config.Server.ServerJoin != nil {
		new = len(config.Server.ServerJoin.StartJoin)
	}
	if old != 0 && new != 0 {
		return fmt.Errorf("server_join and start_join cannot both be defined; prefer setting the server_join stanza")
	}
	// Nothing to do
	if old+new == 0 {
		return nil
	}

	// Combine the lists and join
	joining := config.Server.StartJoin
	if new != 0 {
		joining = append(joining, config.Server.ServerJoin.StartJoin...)
	}

	c.Ui.Output("Joining cluster...")
	n, err := c.agent.server.Join(joining)
	if err != nil {
		return err
	}

	c.Ui.Output(fmt.Sprintf("Join completed. Synced with %d initial agents", n))
	return nil
}

func (c *Command) readConfig() *cfg.Config {
	var dev *cfg.DevModeConfig
	var configPath []string
	var servers string
	var meta []string
	cmdConfig := &cfg.Config{
		Client: &cfg.ClientConfig{},
		Ports:  &cfg.Ports{},
		Server: &cfg.ServerConfig{
			ServerJoin: &cfg.ServerJoin{},
		},
	}
	flags := flag.NewFlagSet("agent", flag.ContinueOnError)
	flags.Usage = func() { c.Ui.Error(c.Help()) }
	var devMode bool
	var devConnectMode bool
	flags.BoolVar(&devMode, "dev", false, "")
	flags.BoolVar(&devConnectMode, "dev-connect", false, "")
	flags.BoolVar(&cmdConfig.Server.Enabled, "server", false, "")
	flags.BoolVar(&cmdConfig.Client.Enabled, "client", false, "")

	// Server-only options
	flags.IntVar(&cmdConfig.Server.BootstrapExpect, "bootstrap-expect", 0, "")
	flags.StringVar(&cmdConfig.Server.EncryptKey, "encrypt", "", "gossip encryption key")
	flags.IntVar(&cmdConfig.Server.RaftProtocol, "raft-protocol", 0, "")
	flags.BoolVar(&cmdConfig.Server.RejoinAfterLeave, "rejoin", false, "")
	flags.Var((*flaghelper.StringFlag)(&cmdConfig.Server.ServerJoin.StartJoin), "join", "")
	flags.Var((*flaghelper.StringFlag)(&cmdConfig.Server.ServerJoin.RetryJoin), "retry-join", "")
	flags.IntVar(&cmdConfig.Server.ServerJoin.RetryMaxAttempts, "retry-max", 0, "")
	flags.Var((flaghelper.FuncDurationVar)(func(d time.Duration) error {
		cmdConfig.Server.ServerJoin.RetryInterval = d
		return nil
	}), "retry-interval", "")

	// Client-only options
	flags.StringVar(&cmdConfig.Client.StateDir, "state-dir", "", "")
	flags.StringVar(&cmdConfig.Client.AllocDir, "alloc-dir", "", "")
	flags.StringVar(&cmdConfig.Client.NodeClass, "node-class", "", "")
	flags.StringVar(&servers, "servers", "", "")
	flags.Var((*flaghelper.StringFlag)(&meta), "meta", "")

	// General options
	flags.Var((*flaghelper.StringFlag)(&configPath), "config", "config")
	flags.StringVar(&cmdConfig.BindAddr, "bind", "", "")
	flags.StringVar(&cmdConfig.Region, "region", "", "")
	flags.StringVar(&cmdConfig.DataDir, "data-dir", "", "")
	flags.StringVar(&cmdConfig.Datacenter, "dc", "", "")
	flags.StringVar(&cmdConfig.LogLevel, "log-level", "", "")
	flags.BoolVar(&cmdConfig.LogJson, "log-json", false, "")
	flags.StringVar(&cmdConfig.NodeName, "node", "", "")

	if err := flags.Parse(c.args); err != nil {
		return nil
	}

	// Split the servers.
	if servers != "" {
		cmdConfig.Client.Servers = strings.Split(servers, ",")
	}

	// Parse the meta flags.
	metaLength := len(meta)
	if metaLength != 0 {
		cmdConfig.Client.Meta = make(map[string]string, metaLength)
		for _, kv := range meta {
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) != 2 {
				c.Ui.Error(fmt.Sprintf("Error parsing Client.Meta value: %v", kv))
				return nil
			}
			cmdConfig.Client.Meta[parts[0]] = parts[1]
		}
	}

	// Load the configuration
	dev, err := cfg.NewDevModeConfig(devMode, devConnectMode)
	if err != nil {
		c.Ui.Error(err.Error())
		return nil
	}
	var config *cfg.Config
	if dev != nil {
		config = cfg.DevConfig(dev)
	} else {
		config = cfg.DefaultConfig()
	}

	// Merge in the enterprise overlay
	config = config.Merge(cfg.DefaultEntConfig())

	for _, path := range configPath {
		current, err := cfg.LoadConfig(path)
		if err != nil {
			c.Ui.Error(fmt.Sprintf(
				"Error loading configuration from %s: %s", path, err))
			return nil
		}

		// The user asked us to load some config here but we didn't find any,
		// so we'll complain but continue.
		if current == nil || reflect.DeepEqual(current, &cfg.Config{}) {
			c.Ui.Warn(fmt.Sprintf("No configuration loaded from %s", path))
		}

		if config == nil {
			config = current
		} else {
			config = config.Merge(current)
		}
	}
	//configJsonConfig, _ := json.Marshal(config)
	//c.Ui.Info(fmt.Sprintf("the  all json config is %s", configJsonConfig))

	// Ensure the sub-structs at least exist
	if config.Client == nil {
		config.Client = &cfg.ClientConfig{}
	}

	if config.Server == nil {
		config.Server = &cfg.ServerConfig{}
	}

	// Merge any CLI options over config file options
	config = config.Merge(cmdConfig)

	// Set the version info
	config.Version = c.Version

	// Normalize binds, ports, addresses, and advertise
	if err := config.NormalizeAddrs(); err != nil {
		c.Ui.Error(err.Error())
		return nil
	}

	//if !c.IsValidConfig(config, cmdConfig) {
	//	return nil
	//}

	//
	if config.Client.Servers == nil {
		config.Client.Servers = config.Client.ServerJoin.RetryJoin
	} else {
		config.Client.ServerJoin.RetryJoin = config.Client.Servers
	}

	return config
}

func (c *Command) getBindAddrSynopsis() string {
	if c == nil || c.agent == nil || c.agent.config == nil || c.agent.config.NormalizedAddrs == nil {
		return ""
	}

	b := new(strings.Builder)
	fmt.Fprintf(b, "HTTP: %s", c.agent.config.NormalizedAddrs.HTTP)

	if c.agent.server != nil {
		if c.agent.config.NormalizedAddrs.RPC != "" {
			fmt.Fprintf(b, "; RPC: %s", c.agent.config.NormalizedAddrs.RPC)
		}
		if c.agent.config.NormalizedAddrs.Serf != "" {
			fmt.Fprintf(b, "; Serf: %s", c.agent.config.NormalizedAddrs.Serf)
		}
	}

	return b.String()
}

// getAdvertiseAddrSynopsis returns a string that describes the addresses the agent
// is advertising.
func (c *Command) getAdvertiseAddrSynopsis() string {
	if c == nil || c.agent == nil || c.agent.config == nil || c.agent.config.AdvertiseAddrs == nil {
		return ""
	}

	b := new(strings.Builder)
	fmt.Fprintf(b, "HTTP: %s", c.agent.config.AdvertiseAddrs.HTTP)

	if c.agent.server != nil {
		if c.agent.config.AdvertiseAddrs.RPC != "" {
			fmt.Fprintf(b, "; RPC: %s", c.agent.config.AdvertiseAddrs.RPC)
		}
		if c.agent.config.AdvertiseAddrs.Serf != "" {
			fmt.Fprintf(b, "; Serf: %s", c.agent.config.AdvertiseAddrs.Serf)
		}
	}

	return b.String()
}

// setupTelemetry is used ot setup the telemetry sub-systems
func (c *Command) setupTelemetry(config *cfg.Config) (*metrics.InmemSink, error) {
	/* Setup telemetry
	Aggregate on 10 second intervals for 1 minute. Expose the
	metrics over stderr when there is a SIGUSR1 received.
	*/
	inm := metrics.NewInmemSink(10*time.Second, time.Minute)
	metrics.DefaultInmemSignal(inm)

	metricsConf := metrics.DefaultConfig("jobpool")

	// Prefer the hostname as a label.
	metricsConf.EnableHostnameLabel = true

	metricsConf.HostName = config.NodeName
	metricsConf.EnableHostname = true

	// Configure the statsite sink
	var fanout metrics.FanoutSink
	// TODO 将监控信息写入中间件如普罗米修斯
	// Initialize the global sink
	if len(fanout) > 0 {
		fanout = append(fanout, inm)
		metrics.NewGlobal(metricsConf, fanout)
	} else {
		metricsConf.EnableHostname = false
		metrics.NewGlobal(metricsConf, inm)
	}
	return inm, nil
}

func (c *Command) Synopsis() string {
	return "Runs a agent"
}

func (c *Command) Help() string {
	helpText := `
Usage: jobpool agent [options]
`
	return strings.TrimSpace(helpText)
}
