package agent

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
)

func (s *HTTPServer) JobList(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	args := dto.JobListRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}
	// 接口返回数据需要租户隔离
	s.parseNamespaceFilter(args.Namespace, &args.QueryOptions)
	var out dto.JobListResponse
	if err := s.agent.RPC("Job.List", &args, &out); err != nil {
		return nil, err
	}
	if out.Jobs == nil {
		out.Jobs = make([]*structs.Job, 0)
	}
	return out, nil
}


func (s *HTTPServer) JobDelete(resp http.ResponseWriter, req *http.Request, params gin.Params) (interface{}, error) {
	jobId, exist := params.Get("jobId")
	if !exist {
		return nil, fmt.Errorf("no parameter planId in uri")
	}
	args := dto.JobDeleteRequest{JobID: jobId}

	s.parseWriteRequest(req, &args.WriteRequest)

	var out dto.PlanDeregisterResponse
	if err := s.agent.RPC("Job.DeleteJob", &args, &out); err != nil {
		return nil, err
	}
	return out, nil
}


func (s *HTTPServer) JobMap(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var args dto.JobMapStatRequest
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}
	var out dto.JobMapStatResponse
	if err := s.agent.RPC("Job.Stats", &args, &out); err != nil {
		return nil, err
	}
	// setIndex(resp, out.Index)
	return out, nil
}
