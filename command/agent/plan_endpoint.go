package agent

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"yunli.com/jobpool/core/dto"
)

func (s *HTTPServer) PlanList(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	args := dto.PlanListRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}
	// 接口返回数据需要租户隔离
	s.parseNamespaceFilter(args.Namespace, &args.QueryOptions)

	var out dto.PlanListResponse
	if err := s.agent.RPC("Plan.List", &args, &out); err != nil {
		return nil, err
	}
	if out.Plans == nil {
		out.Plans = make([]*dto.PlanDto, 0)
	}
	return out, nil
}

func (s *HTTPServer) PlanSaveOrUpdate(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var args dto.PlanAddRequest
	if err := decodeBody(req, &args); err != nil {
		return nil, CodedError(400, err.Error())
	}
	s.parseWriteRequest(req, &args.WriteRequest)
	if args.PlanAddDto.ID == "" && args.PlanAddDto.Name == "" {
		return nil, fmt.Errorf("parameter plan name is null")
	}

	var out dto.PlanAddResponse
	if err := s.agent.RPC("Plan.Add", &args, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *HTTPServer) PlanDelete(resp http.ResponseWriter, req *http.Request, params gin.Params) (interface{}, error) {
	planId, exist := params.Get("planId")
	if !exist {
		return nil, fmt.Errorf("no parameter planId in uri")
	}
	args := dto.PlanDeregisterRequest{PlanID: planId}
	s.parseWriteRequest(req, &args.WriteRequest)
	var out dto.PlanDeregisterResponse
	if err := s.agent.RPC("Plan.Deregister", &args, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *HTTPServer) PlanOnline(resp http.ResponseWriter, req *http.Request, params gin.Params) (interface{}, error) {
	planId, exist := params.Get("planId")
	if !exist {
		return nil, fmt.Errorf("no parameter planId in uri")
	}
	args := dto.PlanOnlineOfflineRequest{PlanID: planId}

	s.parseWriteRequest(req, &args.WriteRequest)

	var out dto.PlanOnlineOfflineResponse
	if err := s.agent.RPC("Plan.Online", &args, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *HTTPServer) PlanOffline(resp http.ResponseWriter, req *http.Request, params gin.Params) (interface{}, error) {
	planId, exist := params.Get("planId")
	if !exist {
		return nil, fmt.Errorf("no parameter planId in uri")
	}
	args := dto.PlanOnlineOfflineRequest{PlanID: planId}

	s.parseWriteRequest(req, &args.WriteRequest)

	var out dto.PlanOnlineOfflineResponse
	if err := s.agent.RPC("Plan.Offline", &args, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *HTTPServer) PlanDetail(resp http.ResponseWriter, req *http.Request, params gin.Params) (interface{}, error) {
	planId, exist := params.Get("planId")
	if !exist {
		return nil, fmt.Errorf("no parameter planId in uri")
	}
	args := dto.PlanDetailRequest{PlanID: planId}
	s.parseWriteRequest(req, &args.WriteRequest)
	var out dto.PlanDetailResponse
	if err := s.agent.RPC("Plan.Detail", &args, &out); err != nil {
		return nil, err
	}
	return out, nil
}
