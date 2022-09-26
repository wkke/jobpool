package agent

import (
	"net/http"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
)


func (s *HTTPServer) EvalsRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	args := dto.EvalListRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	query := req.URL.Query()
	args.FilterEvalStatus = query.Get("status")
	args.FilterJobID = query.Get("job")

	var out dto.EvalListResponse
	if err := s.agent.RPC("Eval.List", &args, &out); err != nil {
		return nil, err
	}

	if out.Evaluations == nil {
		out.Evaluations = make([]*structs.Evaluation, 0)
	}
	return out, nil
}

func (s *HTTPServer) GetEvalStat(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var args dto.EvalStatRequest
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}
	var out dto.EvalStatResponse
	if err := s.agent.RPC("Eval.Stat", &args, &out); err != nil {
		return nil, err
	}
	// setIndex(resp, out.Index)
	return out, nil
}