package agent

import "net/http"

func (s *HTTPServer) MetricsRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}
	return s.agent.InmemSink.DisplayMetrics(resp, req)
}