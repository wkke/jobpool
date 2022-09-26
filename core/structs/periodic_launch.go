package structs

import "time"

// PeriodicLaunch tracks the last launch time of a periodic plan.
type PeriodicLaunch struct {
	ID        string    // ID of the periodic plan.
	Namespace string    // Namespace of the periodic plan
	Launch    time.Time // The last launch time.

	// Raft Indexes
	CreateIndex uint64
	ModifyIndex uint64
}