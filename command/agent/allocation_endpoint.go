package agent

import (
	"net/http"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
)

func (s *HTTPServer) AllocationList(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	args := dto.AllocsGetRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}
	// 接口返回数据需要租户隔离
	s.parseNamespaceFilter(args.Namespace, &args.QueryOptions)
	var out dto.AllocsGetResponse
	if err := s.agent.RPC("Alloc.List", &args, &out); err != nil {
		return nil, err
	}
	if out.Allocs == nil {
		out.Allocs = make([]*structs.Allocation, 0)
	}
	return out, nil
}

func (s *HTTPServer) AllocationUpdate(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var args dto.AllocUpdateStatusByJobRequest
	if err := decodeBody(req, &args); err != nil {
		return nil, CodedError(400, err.Error())
	}
	if args.Status == "" {
		return nil, structs.NewErr1001Blank("参数status")
	}
	if !constant.AllocStatusSet[args.Status] {
		var valueSample string
		for key, _ := range constant.AllocStatusSet {
			valueSample += key + ","
		}
		if len(valueSample) > 0 {
			valueSample = valueSample[0 : len(valueSample)-1]
		}
		return nil, structs.NewErr1002LimitValue("参数status值", valueSample)
	}

	// 根据jobid 查询 allocation, 并更新状态
	queryAllocArgs := dto.AllocsGetByJobIdRequest{
		JobId:          args.JobId,
		AnyCreateIndex: false,
	}
	s.parseWriteRequest(req, &queryAllocArgs.WriteRequest)
	queryAllocArgs.WriteRequest.Namespace = args.Namespace
	var queryAllocsResp dto.AllocsGetByJobIdResponse
	if err := s.agent.RPC("Alloc.GetAllocsByJobId", &queryAllocArgs, &queryAllocsResp); err != nil {
		return nil, err
	}
	var needUpdateAllocs []*structs.Allocation
	for _, alloc := range queryAllocsResp.Allocs {
		alloc.ClientStatus = args.Status
		if args.Description != ""{
			alloc.ClientDescription = args.Description
		}
		needUpdateAllocs = append(needUpdateAllocs, alloc)
	}

	// 更新 Alloc 状态
	updateAllocArgs := &dto.AllocUpdateRequest{
		Alloc: needUpdateAllocs,
	}
	s.parseWriteRequest(req, &updateAllocArgs.WriteRequest)
	updateAllocArgs.WriteRequest.Namespace = args.Namespace
	var updateAllocResp dto.GenericResponse
	if err := s.agent.RPC("Alloc.UpdateAlloc", &updateAllocArgs, &updateAllocResp); err != nil {
		return nil, err
	}

	return updateAllocResp, nil
}
