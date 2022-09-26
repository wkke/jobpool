package agent

import (
	"fmt"
	"net/http"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
)

func (s *HTTPServer) NamespacesRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	args := dto.NamespaceListRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out dto.NamespaceListResponse
	if err := s.agent.RPC("Namespace.ListNamespaces", &args, &out); err != nil {
		return nil, err
	}

	if out.Namespaces == nil {
		out.Namespaces = make([]*structs.Namespace, 0)
	}
	return out.Namespaces, nil
}

func (s *HTTPServer) NamespacesAdd(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "POST" {
		return nil, CodedError(405, ErrInvalidMethod)
	}
	args := dto.NamespaceAddRequest{}
	if err := decodeBody(req, &args); err != nil {
		return nil, CodedError(400, err.Error())
	}
	s.parseWriteRequest(req, &args.WriteRequest)
	if args.Name == "" {
		return nil, fmt.Errorf("parameter namespace name is null")
	}

	out := dto.SingleNamespaceResponse{}
	if err := s.agent.RPC("Namespace.AddNamespace", &args, &out); err != nil {
		return nil, err
	}
	return out, nil
}
