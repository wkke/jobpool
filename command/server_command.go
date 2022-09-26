package command

import (
	"strings"

	"github.com/mitchellh/cli"
)

type ServerCommand struct {
	Meta
}

func (f *ServerCommand) Help() string {
	helpText := `
Usage: jobpool server <subcommand> [options] [args]

  This command groups subcommands for interacting with Jobpool servers. Users can
  list Servers, join a server to the cluster, and force leave a server.

  List Jobpool servers:

      $ jobpool server members

  Join a new server to another:

      $ jobpool server join "IP:Port"

  Force a server to leave:

      $ jobpool server force-leave <name>

  Please see the individual subcommand help for detailed usage information.
`

	return strings.TrimSpace(helpText)
}

func (f *ServerCommand) Synopsis() string {
	return "Interact with servers"
}

func (f *ServerCommand) Name() string { return "server" }

func (f *ServerCommand) Run(args []string) int {
	return cli.RunResultHelp
}
