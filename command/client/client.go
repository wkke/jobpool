package client

import (
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
	"yunli.com/jobpool/command/cfg"
	"yunli.com/jobpool/command/model"

	cleanhttp "github.com/hashicorp/go-cleanhttp"
)


type Client struct {
	httpClient *http.Client
	config     cfg.RestClientConfig
}

func NewClient(config *cfg.RestClientConfig) (*Client, error) {
	// bootstrap the cfg
	defConfig := cfg.DefaultRestClientConfig()

	if config.Address == "" {
		config.Address = defConfig.Address
	} else if _, err := url.Parse(config.Address); err != nil {
		return nil, fmt.Errorf("invalid address '%s': %v", config.Address, err)
	}

	httpClient := config.HttpClient
	if httpClient == nil {
		httpClient = defaultHttpClient()
	}

	client := &Client{
		config:     *config,
		httpClient: httpClient,
	}
	return client, nil
}


func defaultHttpClient() *http.Client {
	httpClient := cleanhttp.DefaultClient()
	transport := httpClient.Transport.(*http.Transport)
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	transport.ForceAttemptHTTP2 = false
	return httpClient
}


func (c *Client) query(endpoint string, out interface{}, q *model.QueryOptions) (*model.QueryMeta, error) {
	r, err := c.newRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}
	r.setQueryOptions(q)
	rtt, resp, err := requireOK(c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	qm := &model.QueryMeta{}
	model.ParseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	if err := decodeBody(resp, out); err != nil {
		return nil, err
	}
	return qm, nil
}

func (c *Client) putQuery(endpoint string, in, out interface{}, q *model.QueryOptions) (*model.QueryMeta, error) {
	r, err := c.newRequest("PUT", endpoint)
	if err != nil {
		return nil, err
	}
	r.setQueryOptions(q)
	r.obj = in
	rtt, resp, err := requireOK(c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	qm := &model.QueryMeta{}
	model.ParseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	if err := decodeBody(resp, out); err != nil {
		return nil, err
	}
	return qm, nil
}

func (c *Client) write(endpoint string, in, out interface{}, q *model.WriteOptions) (*model.WriteMeta, error) {
	r, err := c.newRequest("PUT", endpoint)
	if err != nil {
		return nil, err
	}
	r.setWriteOptions(q)
	r.obj = in
	rtt, resp, err := requireOK(c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	wm := &model.WriteMeta{RequestTime: rtt}
	model.ParseWriteMeta(resp, wm)

	if out != nil {
		if err := decodeBody(resp, &out); err != nil {
			return nil, err
		}
	}
	return wm, nil
}

func (c *Client) newRequest(method, path string) (*Request, error) {
	base, _ := url.Parse(c.config.Address)
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	r := &Request{
		config: &c.config,
		method: method,
		url: &url.URL{
			Scheme:  base.Scheme,
			User:    base.User,
			Host:    base.Host,
			Path:    u.Path,
			RawPath: u.RawPath,
		},
		header: make(http.Header),
		params: make(map[string][]string),
	}
	if c.config.Region != "" {
		r.params.Set("region", c.config.Region)
	}
	if c.config.Namespace != "" {
		r.params.Set("namespace", c.config.Namespace)
	}
	if c.config.WaitTime != 0 {
		r.params.Set("wait", durToMsec(r.config.WaitTime))
	}

	// Add in the query parameters, if any
	for key, values := range u.Query() {
		for _, value := range values {
			r.params.Add(key, value)
		}
	}

	for key, values := range c.config.Headers {
		r.header[key] = values
	}

	return r, nil
}


func (*Client) autoUnzip(resp *http.Response) error {
	if resp == nil || resp.Header == nil {
		return nil
	}

	if resp.Header.Get("Content-Encoding") == "gzip" {
		zReader, err := gzip.NewReader(resp.Body)
		if err == io.EOF {
			// zero length response, do not wrap
			return nil
		} else if err != nil {
			// some other error (e.g. corrupt)
			return err
		}

		// The gzip reader does not close an underlying reader, so use a
		// multiCloser to make sure response body does get closed.
		resp.Body = &multiCloser{
			reader:       zReader,
			inorderClose: []io.Closer{zReader, resp.Body},
		}
	}

	return nil
}

func (c *Client) doRequest(r *Request) (time.Duration, *http.Response, error) {
	req, err := r.toHTTP()
	if err != nil {
		return 0, nil, err
	}

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	diff := time.Since(start)

	// If the response is compressed, we swap the body's reader.
	if zipErr := c.autoUnzip(resp); zipErr != nil {
		return 0, nil, zipErr
	}

	return diff, resp, err
}