package constant

import "time"

const (

	RaftState         = "raft/"
	SerfSnapshot      = "serf/snapshot"
	SnapshotsRetained = 2

	// serverRPCCache controls how long we keep an idle connection open to a server
	ServerRPCCache = 2 * time.Minute

	// serverMaxStreams controls how many idle streams we keep open to a server
	ServerMaxStreams = 64

	// raftLogCacheSize is the maximum number of logs to cache in-memory.
	// This is used to reduce disk I/O for the recently committed entries.
	RaftLogCacheSize = 512

	// aclCacheSize is the number of ACL objects to keep cached. ACLs have a parsing and
	// construction cost, so we keep the hot objects cached to reduce the ACL token resolution time.
	aclCacheSize = 512
)