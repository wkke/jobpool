package command

import (
	"github.com/mitchellh/cli"
	"os"
	"yunli.com/jobpool/command/agent"
	colorable "github.com/mattn/go-colorable"
	"yunli.com/jobpool/version"
)

type NamedCommand interface {
	Name() string
}

func Commands(metaInfo *Meta, agentUi cli.Ui) map[string]cli.CommandFactory {
	if metaInfo == nil {
		metaInfo = new(Meta)
	}
	meta := *metaInfo
	if meta.Ui == nil {
		meta.Ui = &cli.BasicUi{
			Reader:      os.Stdin,
			Writer:      colorable.NewColorableStdout(),
			ErrorWriter: colorable.NewColorableStderr(),
		}
	}
	all := map[string]cli.CommandFactory{
		"agent": func() (cli.Command, error) {
			return &agent.Command{
				Version:    version.GetVersion(),
				Ui:         agentUi,
				ShutdownCh: make(chan struct{}),
			}, nil
		},
		"server": func() (cli.Command, error) {
			return &ServerCommand{
				Meta: meta,
			}, nil
		},
		"server force-leave": func() (cli.Command, error) {
			return &ServerForceLeaveCommand{
				Meta: meta,
			}, nil
		},
		"server join": func() (cli.Command, error) {
			return &ServerJoinCommand{
				Meta: meta,
			}, nil
		},
		"server members": func() (cli.Command, error) {
			return &ServerMembersCommand{
				Meta: meta,
			}, nil
		},
	}
	return all
}
