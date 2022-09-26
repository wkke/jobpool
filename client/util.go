package client

import (
	"time"
	xstructs "yunli.com/jobpool/client/structs"
	"yunli.com/jobpool/core/structs"
)
// stoppedTimer returns a timer that's stopped and wouldn't fire until
// it's reset
func stoppedTimer() *time.Timer {
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	return timer
}

type diffResult struct {
	added   []*structs.Allocation
	removed []string
	updated []*structs.Allocation
	ignore  []string
}


// diffAllocs is used to diff the existing and updated allocations
// to see what has happened.
func diffAllocs(existing map[string]uint64, allocs *xstructs.AllocUpdates) *diffResult {
	// Scan the existing allocations
	result := &diffResult{}
	for existID, existIndex := range existing {
		// Check if the alloc was updated or filtered because an update wasn't
		// needed.
		alloc, pulled := allocs.Pulled[existID]
		_, filtered := allocs.Filtered[existID]

		// If not updated or filtered, removed
		if !pulled && !filtered {
			result.removed = append(result.removed, existID)
			continue
		}

		// Check for an update
		if pulled && alloc.AllocModifyIndex > existIndex {
			result.updated = append(result.updated, alloc)
			continue
		}

		// Ignore this
		result.ignore = append(result.ignore, existID)
	}

	// Scan the updated allocations for any that are new
	for id, pulled := range allocs.Pulled {
		if _, ok := existing[id]; !ok {
			result.added = append(result.added, pulled)
		}
	}
	return result
}