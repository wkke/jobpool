package agent

import (
	"net/http"
	"yunli.com/jobpool/core/dto"
)

func (s *HTTPServer) RegionListRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	var args dto.GenericRequest
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var regions []string
	if err := s.agent.RPC("Region.List", &args, &regions); err != nil {
		return nil, err
	}
	return regions, nil
}
