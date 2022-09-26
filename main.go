package main

import (
	"bytes"
	"fmt"
	"github.com/mitchellh/cli"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"yunli.com/jobpool/command"
	"yunli.com/jobpool/version"
)

const (
	// the name of app
	appName = "jobpool"
)

var (
	internalCommonds = []string{
		"syslog",
	}

	commonCommands = []string{
		"agent",
	}
)

func main() {
	os.Exit(Run(os.Args[1:]))
}

func Run(args []string) int {
	if len(args) == 0 {
		args = append(args, "agent")
		args = append(args, "--dev")
		fmt.Println("no args load default arg: agent --dev")
	}

	metaPtr := new(command.Meta)
	agentUi := &cli.BasicUi{
		Reader:      os.Stdin,
		Writer:      os.Stdout,
		ErrorWriter: os.Stderr,
	}

	commands := command.Commands(metaPtr, agentUi)
	cli := &cli.CLI{
		Name:                       appName,
		Version:                    version.GetVersion().Version,
		Args:                       args,
		Commands:                   commands,
		HiddenCommands:             internalCommonds,
		Autocomplete:               true,
		AutocompleteNoDefaultFlags: true,
		HelpFunc: groupedHelpFunc(
			cli.BasicHelpFunc(appName),
		),
		HelpWriter: os.Stdout,
	}

	exitCode, err := cli.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %s\n", err.Error())
		return 1
	}
	return exitCode
}

func groupedHelpFunc(f cli.HelpFunc) cli.HelpFunc {
	return func(commands map[string]cli.CommandFactory) string {
		var b bytes.Buffer
		tw := tabwriter.NewWriter(&b, 0, 2, 6, ' ', 0)

		fmt.Fprintf(tw, "Usage: jobpool [-version] [-help] [-autocomplete-(un)install] <command> [args]\n\n")
		fmt.Fprintf(tw, "Common commands:\n")
		for _, v := range commonCommands {
			printCommand(tw, v, commands[v])
		}

		// Filter out common commands and aliased commands from the other
		// commands output
		otherCommands := make([]string, 0, len(commands))
		for k := range commands {
			found := false
			for _, v := range commonCommands {
				if k == v {
					found = true
					break
				}
			}
			if !found {
				otherCommands = append(otherCommands, k)
			}
		}
		sort.Strings(otherCommands)

		fmt.Fprintf(tw, "\n")
		fmt.Fprintf(tw, "Other commands:\n")
		for _, v := range otherCommands {
			printCommand(tw, v, commands[v])
		}

		tw.Flush()

		return strings.TrimSpace(b.String())
	}
}

func printCommand(w io.Writer, name string, cmdFn cli.CommandFactory) {
	cmd, err := cmdFn()
	if err != nil {
		panic(fmt.Sprintf("failed to load %q command: %s", name, err))
	}
	fmt.Fprintf(w, "    %s\t%s\n", name, cmd.Synopsis())
}
