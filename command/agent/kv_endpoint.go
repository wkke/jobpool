package agent

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
)

func (s *HTTPServer) KvsRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	args := dto.KvListRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out dto.KvListResponse
	if err := s.agent.RPC("Kv.ListKvs", &args, &out); err != nil {
		return nil, err
	}
	if out.Kvs == nil {
		out.Kvs = make([]*structs.Kv, 0)
	}
	return out, nil
}

func (s *HTTPServer) KvAdd(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var args dto.KvAddRequest
	if err := decodeBody(req, &args); err != nil {
		return nil, CodedError(400, err.Error())
	}
	s.parseWriteRequest(req, &args.WriteRequest)
	if args.Key == "" {
		return nil, fmt.Errorf("parameter key is null")
	}
	argRequest := dto.KvUpsertRequest{
		Kv: &structs.Kv{
			Key:   args.Key,
			Value: args.Value,
		},
		WriteRequest: args.WriteRequest,
	}
	var out dto.KvAddResponse
	if err := s.agent.RPC("Kv.AddKv", &argRequest, &out); err != nil {
		return nil, err
	}
	// setIndex(resp, out.Index)
	return out, nil
}

func (s *HTTPServer) KvUpdate(resp http.ResponseWriter, req *http.Request, params gin.Params) (interface{}, error) {
	key, exist := params.Get("key")
	if !exist {
		return nil, fmt.Errorf("no parameter key in uri")
	}
	var args dto.KvUpdateRequest
	if err := decodeBody(req, &args); err != nil {
		return nil, CodedError(400, err.Error())
	}
	s.parseWriteRequest(req, &args.WriteRequest)
	args.Key = key
	var out dto.KvAddResponse
	if err := s.agent.RPC("Kv.UpdateKv", &args, &out); err != nil {
		return nil, err
	}
	// setIndex(resp, out.Index)
	return out, nil
}

func (s *HTTPServer) KvDetail(resp http.ResponseWriter, req *http.Request, params gin.Params) (interface{}, error) {
	key, exist := params.Get("key")
	if !exist {
		return nil, fmt.Errorf("no parameter key in uri")
	}
	var args dto.KvDetailRequest
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}
	args.Key = key
	var out dto.KvDetailResponse
	if err := s.agent.RPC("Kv.GetKv", &args, &out); err != nil {
		return nil, err
	}
	// setIndex(resp, out.Index)
	return out, nil
}

func (s *HTTPServer) KvDelete(resp http.ResponseWriter, req *http.Request, params gin.Params) (interface{}, error) {
	key, exist := params.Get("key")
	if !exist {
		return nil, fmt.Errorf("no parameter key in uri")
	}
	var args dto.KvDetailRequest
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}
	args.Key = key
	var out dto.KvDeleteResponse
	if err := s.agent.RPC("Kv.DeleteKv", &args, &out); err != nil {
		return nil, err
	}
	// setIndex(resp, out.Index)
	return out, nil
}
