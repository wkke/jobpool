package core

import (
	"errors"
	log "github.com/hashicorp/go-hclog"
	"yunli.com/jobpool/core/dto"

	"yunli.com/jobpool/core/structs"
)

// Status service is used to check on server status
type Status struct {
	srv    *Server
	logger log.Logger
}

// Ping is used to just check for connectivity
func (s *Status) Ping(args struct{}, reply *struct{}) error {
	return nil
}

// Leader is used to get the address of the leader
func (s *Status) Leader(args *dto.GenericRequest, reply *string) error {
	if args.Region == "" {
		args.Region = s.srv.config.Region
	}
	if done, err := s.srv.forward("Status.Leader", args, args, reply); done {
		return err
	}

	leader := string(s.srv.raft.Leader())
	if leader != "" {
		*reply = leader
	} else {
		*reply = ""
	}
	return nil
}

// Peers is used to get all the Raft peers
func (s *Status) Peers(args *dto.GenericRequest, reply *[]string) error {
	if args.Region == "" {
		args.Region = s.srv.config.Region
	}
	if done, err := s.srv.forward("Status.Peers", args, args, reply); done {
		return err
	}

	future := s.srv.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}

	for _, server := range future.Configuration().Servers {
		*reply = append(*reply, string(server.Address))
	}
	return nil
}

// Members return the list of servers in a cluster that a particular server is
// aware of
func (s *Status) Members(args *dto.GenericRequest, reply *structs.ServerMembersResponse) error {
	serfMembers := s.srv.Members()
	members := make([]*structs.ServerMember, len(serfMembers))
	for i, mem := range serfMembers {
		members[i] = &structs.ServerMember{
			Name:        mem.Name,
			Addr:        mem.Addr,
			Port:        mem.Port,
			Tags:        mem.Tags,
			Status:      mem.Status.String(),
			ProtocolMin: mem.ProtocolMin,
			ProtocolMax: mem.ProtocolMax,
			ProtocolCur: mem.ProtocolCur,
			DelegateMin: mem.DelegateMin,
			DelegateMax: mem.DelegateMax,
			DelegateCur: mem.DelegateCur,
		}
	}
	*reply = structs.ServerMembersResponse{
		ServerName:   s.srv.config.NodeName,
		ServerRegion: s.srv.config.Region,
		ServerDC:     s.srv.config.Datacenter,
		Members:      members,
	}
	return nil
}

// HasNodeConn returns whether the server has a connection to the requested
// Node.
func (s *Status) HasNodeConn(args *dto.NodeSpecificRequest, reply *dto.NodeConnQueryResponse) error {
	// Validate the args
	if args.NodeID == "" {
		return errors.New("Must provide the NodeID")
	}

	state, ok := s.srv.getNodeConn(args.NodeID)
	if ok {
		reply.Connected = true
		reply.Established = state.Established
	}

	return nil
}
