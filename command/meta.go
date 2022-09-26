package command

import (
	"flag"
	"github.com/mitchellh/cli"
	"github.com/posener/complete"
	"yunli.com/jobpool/command/client"
	"yunli.com/jobpool/command/cfg"
)

type Meta struct {
	Ui          cli.Ui
	flagAddress string
	region      string
	namespace   string
	token       string
}

type FlagSetFlags uint

const (
	FlagSetNone    FlagSetFlags = 0
	FlagSetClient  FlagSetFlags = 1 << iota
	FlagSetDefault              = FlagSetClient
)

func (m *Meta) FlagSet(n string, fs FlagSetFlags) *flag.FlagSet {
	f := flag.NewFlagSet(n, flag.ContinueOnError)

	// FlagSetClient is used to enable the settings for specifying
	// client connectivity options.
	if fs&FlagSetClient != 0 {
		f.StringVar(&m.flagAddress, "address", "", "")
		f.StringVar(&m.region, "region", "", "")
		f.StringVar(&m.namespace, "namespace", "", "")
		f.StringVar(&m.token, "token", "", "")
	}
	f.SetOutput(&uiErrorWriter{ui: m.Ui})
	return f
}


// AutocompleteFlags returns a set of flag completions for the given flag set.
func (m *Meta) AutocompleteFlags(fs FlagSetFlags) complete.Flags {
	if fs&FlagSetClient == 0 {
		return nil
	}

	return complete.Flags{
		"-address":         complete.PredictAnything,
		"-region":          complete.PredictAnything,
		"-namespace":       NamespacePredictor(m.Client, nil),
		"-token":           complete.PredictAnything,
	}
}


// ApiClientFactory is the signature of a API client factory
type ApiClientFactory func() (*client.Client, error)


// Client is used to initialize and return a new API client using
// the default command line arguments and env vars.
func (m *Meta) clientConfig() *cfg.RestClientConfig {
	config := cfg.DefaultRestClientConfig()

	if m.flagAddress != "" {
		config.Address = m.flagAddress
	}
	if m.region != "" {
		config.Region = m.region
	}
	if m.namespace != "" {
		config.Namespace = m.namespace
	}
	return config
}

func (m *Meta) Client() (*client.Client, error) {
	return client.NewClient(m.clientConfig())
}