package agent

import (
	"net/http"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
)

func (s *HTTPServer) NodesRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	args := dto.NodeListRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	args.Fields = &structs.NodeStubFields{}

	var out dto.NodeListResponse
	if err := s.agent.RPC("Node.List", &args, &out); err != nil {
		return nil, err
	}
	if out.Nodes == nil {
		out.Nodes = make([]*structs.NodeListStub, 0)
	}
	return out, nil
}

func (s *HTTPServer) NodesHealth(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	memberArgs := dto.GenericRequest{
		QueryOptions: dto.QueryOptions{PageSize: 1000},
	}
	s.parseRegion(req, &memberArgs.Region)
	var memberOut structs.ServerMembersResponse
	if err := s.agent.RPC("Status.Members", memberArgs, &memberOut); err != nil {
		return nil, err
	}

	leaderArgs := dto.GenericRequest{}
	s.parseRegion(req, &leaderArgs.Region)
	var leader string
	if err := s.agent.RPC("Status.Leader", &leaderArgs, &leader); err != nil {
		return nil, err
	}

	nodeArgs := dto.NodeListRequest{
		QueryOptions: dto.QueryOptions{PageSize: 1000},
	}
	s.parseRegion(req, &nodeArgs.Region)
	var nodeOut dto.NodeListResponse
	if err := s.agent.RPC("Node.List", &nodeArgs, &nodeOut); err != nil {
		return nil, err
	}
	if nodeOut.Nodes == nil {
		nodeOut.Nodes = make([]*structs.NodeListStub, 0)
	}

	var out dto.NodeHealthResponse
	out.Master = len(memberOut.Members)
	for _, node := range nodeOut.Nodes {
		if node.Status == "ready" {
			out.Client += 1
		}
	}
	if leader != "" {
		out.Leader = 1
	} else {
		out.Leader = 0
	}
	return out, nil
}
