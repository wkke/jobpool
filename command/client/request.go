package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"yunli.com/jobpool/command/cfg"
	"yunli.com/jobpool/command/model"
)

type Request struct {
	config *cfg.RestClientConfig
	method string
	url    *url.URL
	params url.Values
	token  string
	body   io.Reader
	obj    interface{}
	ctx    context.Context
	header http.Header
}


func (r *Request) setQueryOptions(q *model.QueryOptions) {
	if q == nil {
		return
	}
	if q.Region != "" {
		r.params.Set("region", q.Region)
	}
	if q.Namespace != "" {
		r.params.Set("namespace", q.Namespace)
	}
	if q.AuthToken != "" {
		r.token = q.AuthToken
	}
	if q.AllowStale {
		r.params.Set("stale", "")
	}
	if q.WaitIndex != 0 {
		r.params.Set("index", strconv.FormatUint(q.WaitIndex, 10))
	}
	if q.WaitTime != 0 {
		r.params.Set("wait", durToMsec(q.WaitTime))
	}
	if q.Prefix != "" {
		r.params.Set("prefix", q.Prefix)
	}
	if q.Filter != "" {
		r.params.Set("filter", q.Filter)
	}
	if q.PageSize != 0 {
		r.params.Set("pageSize", fmt.Sprint(q.PageSize))
	}
	if q.NextToken != "" {
		r.params.Set("next_token", q.NextToken)
	}
	if q.Reverse {
		r.params.Set("reverse", "true")
	}
	for k, v := range q.Params {
		r.params.Set(k, v)
	}
	r.ctx = q.Context()

	for k, v := range q.Headers {
		r.header.Set(k, v)
	}
}

// setWriteOptions is used to annotate the request with
// additional write options
func (r *Request) setWriteOptions(q *model.WriteOptions) {
	if q == nil {
		return
	}
	if q.Region != "" {
		r.params.Set("region", q.Region)
	}
	if q.Namespace != "" {
		r.params.Set("namespace", q.Namespace)
	}
	r.ctx = q.Context()

	for k, v := range q.Headers {
		r.header.Set(k, v)
	}
}

func durToMsec(dur time.Duration) string {
	return fmt.Sprintf("%dms", dur/time.Millisecond)
}

func requireOK(d time.Duration, resp *http.Response, e error) (time.Duration, *http.Response, error) {
	if e != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return d, nil, e
	}
	if resp.StatusCode != 200 {
		var buf bytes.Buffer
		io.Copy(&buf, resp.Body)
		resp.Body.Close()
		return d, nil, fmt.Errorf("Unexpected response code: %d (%s)", resp.StatusCode, buf.Bytes())
	}
	return d, resp, nil
}

func encodeBody(obj interface{}) (io.Reader, error) {
	if reader, ok := obj.(io.Reader); ok {
		return reader, nil
	}

	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(obj); err != nil {
		return nil, err
	}
	return buf, nil
}

func (r *Request) toHTTP() (*http.Request, error) {
	// Encode the query parameters
	r.url.RawQuery = r.params.Encode()

	// Check if we should encode the body
	if r.body == nil && r.obj != nil {
		if b, err := encodeBody(r.obj); err != nil {
			return nil, err
		} else {
			r.body = b
		}
	}

	ctx := func() context.Context {
		if r.ctx != nil {
			return r.ctx
		}
		return context.Background()
	}()

	// Create the HTTP Request
	req, err := http.NewRequestWithContext(ctx, r.method, r.url.RequestURI(), r.body)
	if err != nil {
		return nil, err
	}

	req.Header = r.header

	// Optionally configure HTTP basic authentication
	if r.url.User != nil {
		username := r.url.User.Username()
		password, _ := r.url.User.Password()
		req.SetBasicAuth(username, password)
	}

	req.Header.Add("Accept-Encoding", "gzip")
	if r.token != "" {
		req.Header.Set("x-token", r.token)
	}

	req.URL.Host = r.url.Host
	req.URL.Scheme = r.url.Scheme
	req.Host = r.url.Host
	return req, nil
}

type multiCloser struct {
	reader       io.Reader
	inorderClose []io.Closer
}

func (m *multiCloser) Close() error {
	for _, c := range m.inorderClose {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (m *multiCloser) Read(p []byte) (int, error) {
	return m.reader.Read(p)
}



func decodeBody(resp *http.Response, out interface{}) error {
	switch resp.ContentLength {
	case 0:
		if out == nil {
			return nil
		}
		return errors.New("Got 0 byte response with non-nil decode object")
	default:
		dec := json.NewDecoder(resp.Body)
		return dec.Decode(out)
	}
}
