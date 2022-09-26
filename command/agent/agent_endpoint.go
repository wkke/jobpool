package agent

import (
	"net/http"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
)

func (s *HTTPServer) AgentMembersRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	args := &dto.GenericRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out structs.ServerMembersResponse
	if err := s.agent.RPC("Status.Members", args, &out); err != nil {
		return nil, err
	}

	return out, nil
}


func (s *HTTPServer) AgentForceLeaveRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "PUT" && req.Method != "POST" {
		return nil, CodedError(405, ErrInvalidMethod)
	}
	srv := s.agent.Server()
	if srv == nil {
		return nil, CodedError(501, ErrInvalidMethod)
	}

	// Get the node to eject
	node := req.URL.Query().Get("node")
	if node == "" {
		return nil, CodedError(400, "missing node to force leave")
	}

	// Attempt remove
	err := srv.RemoveFailedNode(node)
	return nil, err
}


func (s *HTTPServer) AgentJoinRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "PUT" && req.Method != "POST" {
		return nil, CodedError(405, ErrInvalidMethod)
	}
	srv := s.agent.Server()
	if srv == nil {
		return nil, CodedError(501, ErrInvalidMethod)
	}

	// Get the join addresses
	query := req.URL.Query()
	addrs := query["address"]
	if len(addrs) == 0 {
		return nil, CodedError(400, "missing address to join")
	}

	// Attempt the join
	num, err := srv.Join(addrs)
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	return joinResult{num, errStr}, nil
}

type joinResult struct {
	NumJoined int    `json:"num_joined"`
	Error     string `json:"error"`
}