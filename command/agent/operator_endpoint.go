package agent

import (
	"net/http"
	"yunli.com/jobpool/core/dto"
)

func (s *HTTPServer) OperatorRaftConfiguration(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return nil, nil
	}

	var args dto.GenericRequest
	if done := s.parse(resp, req, &args.Region, &args.QueryOptions); done {
		return nil, nil
	}

	var reply dto.RaftConfigurationResponse
	if err := s.agent.RPC("Operator.RaftGetConfiguration", &args, &reply); err != nil {
		return nil, err
	}

	return reply, nil
}