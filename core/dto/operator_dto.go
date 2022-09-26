package dto

import "yunli.com/jobpool/core/structs"

type RaftConfigurationResponse struct {
	// Servers has the list of servers in the Raft configuration.
	Servers []*structs.RaftServer

	// Index has the Raft index of this configuration.
	Index uint64
}