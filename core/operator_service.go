package core

import (
	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"net"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
)

// Operator endpoint is used to perform low-level operator tasks for jobpool.
type Operator struct {
	srv    *Server
	logger log.Logger
}

// RaftGetConfiguration is used to retrieve the current Raft configuration.
func (op *Operator) RaftGetConfiguration(args *dto.GenericRequest, reply *dto.RaftConfigurationResponse) error {
	if done, err := op.srv.forward("Operator.RaftGetConfiguration", args, args, reply); done {
		return err
	}
	// We can't fetch the leader and the configuration atomically with
	// the current Raft API.
	future := op.srv.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}

	// Index the jobpool information about the servers.
	serverMap := make(map[raft.ServerAddress]serf.Member)
	for _, member := range op.srv.serf.Members() {
		valid, parts := isAppServer(member)
		if !valid {
			continue
		}

		addr := (&net.TCPAddr{IP: member.Addr, Port: parts.Port}).String()
		serverMap[raft.ServerAddress(addr)] = member
	}

	// Fill out the reply.
	leader := op.srv.raft.Leader()
	reply.Index = future.Index()
	for _, server := range future.Configuration().Servers {
		node := "(unknown)"
		raftProtocolVersion := "unknown"
		if member, ok := serverMap[server.Address]; ok {
			node = member.Name
			if raftVsn, ok := member.Tags["raft_vsn"]; ok {
				raftProtocolVersion = raftVsn
			}
		}

		entry := &structs.RaftServer{
			ID:           server.ID,
			Node:         node,
			Address:      server.Address,
			Leader:       server.Address == leader,
			Voter:        server.Suffrage == raft.Voter,
			RaftProtocol: raftProtocolVersion,
		}
		reply.Servers = append(reply.Servers, entry)
	}
	return nil
}