package structs

import "yunli.com/jobpool/core/structs"

type AllocUpdates struct {
	// pulled is the set of allocations that were downloaded from the servers.
	Pulled map[string]*structs.Allocation

	// filtered is the set of allocations that were not pulled because their
	// AllocModifyIndex didn't change.
	Filtered map[string]struct{}

}