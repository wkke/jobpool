package cfg

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/hashicorp/go-secure-stdlib/listenerutil"
	"github.com/hashicorp/go-sockaddr"
	"github.com/hashicorp/go-sockaddr/template"
	"github.com/hashicorp/hcl"
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
	"yunli.com/jobpool/core/structs/config"
	"yunli.com/jobpool/helper"
	"yunli.com/jobpool/version"
)

// DefaultConfig is a the baseline configuration for Jobpool
func DefaultConfig() *Config {
	return &Config{
		LogLevel:   "INFO",
		Region:     "global",
		Datacenter: "dc1",
		BindAddr:   "0.0.0.0",
		Ports: &Ports{
			HTTP: 4646,
			RPC:  4647,
			Serf: 4648,
		},
		Addresses:      &Addresses{},
		AdvertiseAddrs: &AdvertiseAddrs{},
		Client: &ClientConfig{
			Enabled:               false,
			MaxKillTimeout:        "30s",
			ClientMinPort:         14000,
			ClientMaxPort:         14512,
			MinDynamicPort:        20000,
			MaxDynamicPort:        32000,
			GCInterval:            1 * time.Minute,
			GCParallelDestroys:    2,
			GCDiskUsageThreshold:  80,
			GCInodeUsageThreshold: 70,
			GCMaxAllocs:           50,
			NoHostUUID:            helper.BoolToPtr(true),
			DisableRemoteExec:     false,
			ServerJoin: &ServerJoin{
				RetryJoin:        []string{},
				RetryInterval:    30 * time.Second,
				RetryMaxAttempts: 0,
			},
			BindWildcardDefaultHostNetwork: true,
		},
		Server: &ServerConfig{
			Enabled:           false,
			EnableEventBroker: helper.BoolToPtr(true),
			EventBufferSize:   helper.IntToPtr(100),
			RaftProtocol:      3,
			StartJoin:         []string{},
			ServerJoin: &ServerJoin{
				RetryJoin:        []string{},
				RetryInterval:    30 * time.Second,
				RetryMaxAttempts: 0,
			},
			Search: &Search{
				FuzzyEnabled:  true,
				LimitQuery:    20,
				LimitResults:  100,
				MinTermLength: 2,
			},
		},
		SyslogFacility:     "LOCAL0",
		Version:            version.GetVersion(),
		DisableUpdateCheck: helper.BoolToPtr(true),
		Limits:             config.DefaultLimits(),
	}
}

// ParseConfigFile returns an agent.Config from parsed from a file.
func ParseConfigFile(path string) (*Config, error) {
	// slurp
	var buf bytes.Buffer
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := io.Copy(&buf, f); err != nil {
		return nil, err
	}

	// parse
	c := &Config{
		Client: &ClientConfig{
			ServerJoin: &ServerJoin{},
		},
		Server: &ServerConfig{ServerJoin: &ServerJoin{}},
	}
	err = hcl.Decode(c, buf.String())
	if err != nil {
		return nil, err
	}

	// convert strings to time.Durations
	tds := []durationConversionMap{
		{"gc_interval", &c.Client.GCInterval, &c.Client.GCIntervalHCL, nil},
		{"client.server_join.retry_interval", &c.Client.ServerJoin.RetryInterval, &c.Client.ServerJoin.RetryIntervalHCL, nil},
		{"server.heartbeat_grace", &c.Server.HeartbeatGrace, &c.Server.HeartbeatGraceHCL, nil},
		{"server.min_heartbeat_ttl", &c.Server.MinHeartbeatTTL, &c.Server.MinHeartbeatTTLHCL, nil},
		{"server.failover_heartbeat_ttl", &c.Server.FailoverHeartbeatTTL, &c.Server.FailoverHeartbeatTTLHCL, nil},
		{"server.retry_interval", &c.Server.RetryInterval, &c.Server.RetryIntervalHCL, nil},
		{"server.server_join.retry_interval", &c.Server.ServerJoin.RetryInterval, &c.Server.ServerJoin.RetryIntervalHCL, nil},
	}

	// convert strings to time.Durations
	err = convertDurations(tds)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// durationConversionMap holds args for one duration conversion
type durationConversionMap struct {
	targetFieldPath string
	targetField     *time.Duration
	sourceField     *string
	setFunc         func(*time.Duration)
}

// convertDurations parses the duration strings specified in the config files
// into time.Durations
func convertDurations(xs []durationConversionMap) error {
	for _, x := range xs {
		// if targetField is not a pointer itself, use the field map.
		if x.targetField != nil && x.sourceField != nil && "" != *x.sourceField {
			d, err := time.ParseDuration(*x.sourceField)
			if err != nil {
				return fmt.Errorf("%s can't parse time duration %s", x.targetFieldPath, *x.sourceField)
			}

			*x.targetField = d
		} else if x.setFunc != nil && x.sourceField != nil && "" != *x.sourceField {
			// if targetField is a pointer itself, use the setFunc closure.
			d, err := time.ParseDuration(*x.sourceField)
			if err != nil {
				return fmt.Errorf("%s can't parse time duration %s", x.targetFieldPath, *x.sourceField)
			}
			x.setFunc(&d)
		}
	}

	return nil
}

// DefaultEntConfig is an empty config in open source
func DefaultEntConfig() *Config {
	return &Config{}
}

// DevConfig is a Config that is used for dev mode of Jobpool.
func DevConfig(mode *DevModeConfig) *Config {
	if mode == nil {
		mode = &DevModeConfig{defaultMode: true}
		mode.networkConfig()
	}
	conf := DefaultConfig()
	conf.BindAddr = mode.bindAddr
	conf.LogLevel = "DEBUG"
	conf.Client.Enabled = true
	conf.Server.Enabled = true
	conf.DevMode = true
	conf.Server.BootstrapExpect = 1
	conf.EnableDebug = true
	conf.Client.NetworkInterface = mode.iface
	conf.Client.Options = map[string]string{
		"driver.raw_exec.enable": "true",
		"driver.docker.volumes":  "true",
	}
	conf.Client.GCInterval = 10 * time.Minute
	conf.Client.GCDiskUsageThreshold = 99
	conf.Client.GCInodeUsageThreshold = 99
	conf.Client.GCMaxAllocs = 50
	conf.Client.BindWildcardDefaultHostNetwork = true

	return conf
}

// newDevModeConfig parses the optional string value of the -dev flag
func NewDevModeConfig(devMode, connectMode bool) (*DevModeConfig, error) {
	if !devMode && !connectMode {
		return nil, nil
	}
	mode := &DevModeConfig{}
	mode.defaultMode = devMode
	if connectMode {
		if runtime.GOOS != "linux" {
			// strictly speaking -dev-connect only binds to the
			// non-localhost interface, but given its purpose
			// is to support a feature with network namespaces
			// we'll return an error here rather than let the agent
			// come up and fail unexpectedly to run jobs
			return nil, fmt.Errorf("-dev-connect is only supported on linux.")
		}
		u, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf(
				"-dev-connect uses network namespaces and is only supported for root: %v", err)
		}
		if u.Uid != "0" {
			return nil, fmt.Errorf(
				"-dev-connect uses network namespaces and is only supported for root.")
		}
		mode.connectMode = true
	}
	err := mode.networkConfig()
	if err != nil {
		return nil, err
	}
	return mode, nil
}

func (mode *DevModeConfig) networkConfig() error {
	if runtime.GOOS == "windows" {
		mode.bindAddr = "127.0.0.1"
		mode.iface = "Loopback Pseudo-Interface 1"
		return nil
	}
	if runtime.GOOS == "darwin" {
		mode.bindAddr = "127.0.0.1"
		mode.iface = "lo0"
		return nil
	}
	if mode != nil && mode.connectMode {
		// if we hit either of the errors here we're in a weird situation
		// where syscalls to get the list of network interfaces are failing.
		// rather than throwing errors, we'll fall back to the default.
		ifAddrs, err := sockaddr.GetDefaultInterfaces()
		errMsg := "-dev=connect uses network namespaces: %v"
		if err != nil {
			return fmt.Errorf(errMsg, err)
		}
		if len(ifAddrs) < 1 {
			return fmt.Errorf(errMsg, "could not find public network interface")
		}
		iface := ifAddrs[0].Name
		mode.iface = iface
		mode.bindAddr = "0.0.0.0" // allows CLI to "just work"
		return nil
	}
	mode.bindAddr = "127.0.0.1"
	mode.iface = "lo"
	return nil
}

func LoadConfig(path string) (*Config, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if fi.IsDir() {
		return LoadConfigDir(path)
	}

	cleaned := filepath.Clean(path)
	config, err := ParseConfigFile(cleaned)
	if err != nil {
		return nil, fmt.Errorf("Error loading %s: %s", cleaned, err)
	}

	config.Files = append(config.Files, cleaned)
	return config, nil
}

// LoadConfigDir loads all the configurations in the given directory
// in alphabetical order.
func LoadConfigDir(dir string) (*Config, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf(
			"configuration path must be a directory: %s", dir)
	}

	var files []string
	err = nil
	for err != io.EOF {
		var fis []os.FileInfo
		fis, err = f.Readdir(128)
		if err != nil && err != io.EOF {
			return nil, err
		}

		for _, fi := range fis {
			// Ignore directories
			if fi.IsDir() {
				continue
			}

			// Only care about files that are valid to load.
			name := fi.Name()
			skip := true
			if strings.HasSuffix(name, ".hcl") {
				skip = false
			} else if strings.HasSuffix(name, ".json") {
				skip = false
			}
			if skip || isTemporaryFile(name) {
				continue
			}

			path := filepath.Join(dir, name)
			files = append(files, path)
		}
	}

	// Fast-path if we have no files
	if len(files) == 0 {
		return &Config{}, nil
	}

	sort.Strings(files)

	var result *Config
	for _, f := range files {
		config, err := ParseConfigFile(f)
		if err != nil {
			return nil, fmt.Errorf("Error loading %s: %s", f, err)
		}
		config.Files = append(config.Files, f)

		if result == nil {
			result = config
		} else {
			result = result.Merge(config)
		}
	}

	return result, nil
}

func (c *Config) NormalizeAddrs() error {
	if c.BindAddr != "" {
		ipStr, err := listenerutil.ParseSingleIPTemplate(c.BindAddr)
		if err != nil {
			return fmt.Errorf("Bind address resolution failed: %v", err)
		}
		c.BindAddr = ipStr
	}

	httpAddrs, err := normalizeMultipleBind(c.Addresses.HTTP, c.BindAddr)
	if err != nil {
		return fmt.Errorf("Failed to parse HTTP address: %v", err)
	}
	c.Addresses.HTTP = strings.Join(httpAddrs, " ")

	addr, err := normalizeBind(c.Addresses.RPC, c.BindAddr)
	if err != nil {
		return fmt.Errorf("Failed to parse RPC address: %v", err)
	}
	c.Addresses.RPC = addr

	addr, err = normalizeBind(c.Addresses.Serf, c.BindAddr)
	if err != nil {
		return fmt.Errorf("Failed to parse Serf address: %v", err)
	}
	c.Addresses.Serf = addr

	c.NormalizedAddrs = &NormalizedAddrs{
		HTTP: joinHostPorts(httpAddrs, strconv.Itoa(c.Ports.HTTP)),
		RPC:  net.JoinHostPort(c.Addresses.RPC, strconv.Itoa(c.Ports.RPC)),
		Serf: net.JoinHostPort(c.Addresses.Serf, strconv.Itoa(c.Ports.Serf)),
	}

	addr, err = normalizeAdvertise(c.AdvertiseAddrs.HTTP, httpAddrs[0], c.Ports.HTTP, c.DevMode)
	if err != nil {
		return fmt.Errorf("Failed to parse HTTP advertise address (%v, %v, %v, %v): %v", c.AdvertiseAddrs.HTTP, c.Addresses.HTTP, c.Ports.HTTP, c.DevMode, err)
	}
	c.AdvertiseAddrs.HTTP = addr

	addr, err = normalizeAdvertise(c.AdvertiseAddrs.RPC, c.Addresses.RPC, c.Ports.RPC, c.DevMode)
	if err != nil {
		return fmt.Errorf("Failed to parse RPC advertise address: %v", err)
	}
	c.AdvertiseAddrs.RPC = addr

	// TODO to be deleted change the advertise address
	if c.EnableHostAddress {
		hostname, err := os.Hostname()
		if err != nil {
			panic(err)
		}
		c.AdvertiseAddrs.HTTP = fmt.Sprintf(c.AdvertiseAddrsFormat, hostname, c.Ports.HTTP)
		c.AdvertiseAddrs.RPC = fmt.Sprintf(c.AdvertiseAddrsFormat, hostname, c.Ports.RPC)
		c.AdvertiseAddrs.Serf = fmt.Sprintf(c.AdvertiseAddrsFormat, hostname, c.Ports.Serf)
	}

	// Skip serf if server is disabled
	if c.Server != nil && c.Server.Enabled {
		addr, err = normalizeAdvertise(c.AdvertiseAddrs.Serf, c.Addresses.Serf, c.Ports.Serf, c.DevMode)
		if err != nil {
			return fmt.Errorf("Failed to parse Serf advertise address: %v", err)
		}
		c.AdvertiseAddrs.Serf = addr
		if c.EnableHostAddress {
			// to be deleted change the advertise address
			hostname, err := os.Hostname()
			if err != nil {
				panic(err)
			}
			c.AdvertiseAddrs.Serf = fmt.Sprintf(c.AdvertiseAddrsFormat, hostname, c.Ports.Serf)
		}
	}

	// Skip network_interface evaluation if not a client
	if c.Client != nil && c.Client.Enabled && c.Client.NetworkInterface != "" {
		parsed, err := parseSingleInterfaceTemplate(c.Client.NetworkInterface)
		if err != nil {
			return fmt.Errorf("Failed to parse network-interface: %v", err)
		}

		c.Client.NetworkInterface = parsed
	}

	return nil
}

func isTemporaryFile(name string) bool {
	return strings.HasSuffix(name, "~") || // vim
		strings.HasPrefix(name, ".#") || // emacs
		(strings.HasPrefix(name, "#") && strings.HasSuffix(name, "#")) // emacs
}

// parseSingleInterfaceTemplate parses a go-sockaddr template and returns an
// error if it doesn't result in a single value.
func parseSingleInterfaceTemplate(tpl string) (string, error) {
	out, err := template.Parse(tpl)
	if err != nil {
		// Typically something like:
		// unable to parse template "{{printfl \"en50\"}}": template: sockaddr.Parse:1: function "printfl" not defined
		return "", err
	}

	// Remove any extra empty space around the rendered result and check if the
	// result is also not empty if the user provided a template.
	out = strings.TrimSpace(out)
	if tpl != "" && out == "" {
		return "", fmt.Errorf("template %q evaluated to empty result", tpl)
	}

	// `template.Parse` returns a space-separated list of results, but on
	// Windows network interfaces are allowed to have spaces, so there is no
	// guaranteed separators that we can use to test if the template returned
	// multiple interfaces.
	// The test below checks if the template results to a single valid interface.
	_, err = net.InterfaceByName(out)
	if err != nil {
		return "", fmt.Errorf("invalid interface name %q", out)
	}

	return out, nil
}

func normalizeBind(addr, bind string) (string, error) {
	if addr == "" {
		return bind, nil
	}
	return listenerutil.ParseSingleIPTemplate(addr)
}

// normalizeMultipleBind returns normalized bind addresses.
//
// If addr is set it is used, if not the default bind address is used.
func normalizeMultipleBind(addr, bind string) ([]string, error) {
	if addr == "" {
		return []string{bind}, nil
	}
	return parseMultipleIPTemplate(addr)
}

func normalizeAdvertise(addr string, bind string, defport int, dev bool) (string, error) {
	addr, err := listenerutil.ParseSingleIPTemplate(addr)
	if err != nil {
		return "", fmt.Errorf("Error parsing advertise address template: %v", err)
	}

	if addr != "" {
		// Default to using manually configured address
		_, _, err = net.SplitHostPort(addr)
		if err != nil {
			if !isMissingPort(err) && !isTooManyColons(err) {
				return "", fmt.Errorf("Error parsing advertise address %q: %v", addr, err)
			}

			// missing port, append the default
			return net.JoinHostPort(addr, strconv.Itoa(defport)), nil
		}

		return addr, nil
	}

	// Fallback to bind address first, and then try resolving the local hostname
	ips, err := net.LookupIP(bind)
	if err != nil {
		return "", fmt.Errorf("Error resolving bind address %q: %v", bind, err)
	}

	// Return the first non-localhost unicast address
	for _, ip := range ips {
		if ip.IsLinkLocalUnicast() || ip.IsGlobalUnicast() {
			return net.JoinHostPort(ip.String(), strconv.Itoa(defport)), nil
		}
		if ip.IsLoopback() {
			if dev {
				// loopback is fine for dev mode
				return net.JoinHostPort(ip.String(), strconv.Itoa(defport)), nil
			}
			return "", fmt.Errorf("Defaulting advertise to localhost is unsafe, please set advertise manually")
		}
	}

	// Bind is not localhost but not a valid advertise IP, use first private IP
	addr, err = listenerutil.ParseSingleIPTemplate("{{ GetPrivateIP }}")
	if err != nil {
		return "", fmt.Errorf("Unable to parse default advertise address: %v", err)
	}
	return net.JoinHostPort(addr, strconv.Itoa(defport)), nil
}

// isMissingPort returns true if an error is a "missing port" error from
// net.SplitHostPort.
func isMissingPort(err error) bool {
	// matches error const in net/ipsock.go
	const missingPort = "missing port in address"
	return err != nil && strings.Contains(err.Error(), missingPort)
}

// isTooManyColons returns true if an error is a "too many colons" error from
// net.SplitHostPort.
func isTooManyColons(err error) bool {
	// matches error const in net/ipsock.go
	const tooManyColons = "too many colons in address"
	return err != nil && strings.Contains(err.Error(), tooManyColons)
}

// joinHostPorts joins every addr in addrs with the specified port
func joinHostPorts(addrs []string, port string) []string {
	localAddrs := make([]string, len(addrs))
	for i, k := range addrs {
		localAddrs[i] = net.JoinHostPort(k, port)

	}
	return localAddrs
}

func parseMultipleIPTemplate(ipTmpl string) ([]string, error) {
	out, err := template.Parse(ipTmpl)
	if err != nil {
		return []string{}, fmt.Errorf("Unable to parse address template %q: %v", ipTmpl, err)
	}

	ips := strings.Split(out, " ")
	if len(ips) == 0 {
		return []string{}, errors.New("No addresses found, please configure one.")
	}

	return deduplicateAddrs(ips), nil
}

func deduplicateAddrs(addrs []string) []string {
	keys := make(map[string]bool)
	list := []string{}

	for _, entry := range addrs {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}
