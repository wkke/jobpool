package command

import (
	"github.com/mitchellh/cli"
	"github.com/posener/complete"
	"yunli.com/jobpool/command/constant"
)

type NamespaceCommand struct {
	Meta
}

func (f *NamespaceCommand) Run(args []string) int {
	return cli.RunResultHelp
}

func (f *NamespaceCommand) Name() string { return "namespace" }

func NamespacePredictor(factory ApiClientFactory, filter map[string]struct{}) complete.Predictor {
	return complete.PredictFunc(func(a complete.Args) []string {
		client, err := factory()
		if err != nil {
			return nil
		}

		resp, _, err := client.Search().PrefixSearch(a.Last, constant.Namespaces, nil)
		if err != nil {
			return []string{}
		}

		// Filter the returned namespaces. We assign the unfiltered slice to the
		// filtered slice but with no elements. This causes the slices to share
		// the underlying array and makes the filtering allocation free.
		unfiltered := resp.Matches[constant.Namespaces]
		filtered := unfiltered[:0]
		for _, ns := range unfiltered {
			if _, ok := filter[ns]; !ok {
				filtered = append(filtered, ns)
			}
		}

		return filtered
	})
}
