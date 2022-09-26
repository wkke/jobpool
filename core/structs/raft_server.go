package structs

import "github.com/hashicorp/raft"

type RaftServer struct {
	// ID is the unique ID for the server. These are currently the same
	// as the address, but they will be changed to a real GUID in a future
	// release of jobpool.
	ID raft.ServerID

	// Node is the node name of the server, as known by jobpool, or this
	// will be set to "(unknown)" otherwise.
	Node string

	// Address is the IP:port of the server, used for Raft communications.
	Address raft.ServerAddress

	// Leader is true if this server is the current cluster leader.
	Leader bool

	// Voter is true if this server has a vote in the cluster. This might
	// be false if the server is staging and still coming online, or if
	// it's a non-voting server, which will be added in a future release of
	// jobpool.
	Voter bool

	// RaftProtocol is the version of the Raft protocol spoken by this server.
	RaftProtocol string
}