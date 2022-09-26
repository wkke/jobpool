package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/go-multierror"
	"regexp"
	"strconv"
	"yunli.com/jobpool/command/cfg"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/helper/logging"

	"github.com/hashicorp/go-connlimit"
	log "github.com/hashicorp/go-hclog"
	// "github.com/hashicorp/serf/serf"
	// "github.com/mitchellh/copystructure"
	"net"
	"net/http"
	// "strings"
	"time"
)

const (
	// ErrInvalidMethod is used if the HTTP method is not supported
	ErrInvalidMethod = "Invalid method"

	// ErrEntOnly is the error returned if accessing an enterprise only
	// endpoint
	ErrEntOnly = "Jobpool Enterprise only endpoint"

	// ErrServerOnly is the error text returned if accessing a server only
	// endpoint
	ErrServerOnly = "Server only endpoint"

	// ContextKeyReqID is a unique ID for a given request
	ContextKeyReqID = "requestID"

	// MissingRequestID is a placeholder if we cannot retrieve a request
	// UUID from context
	MissingRequestID = "<missing request id>"
)

type handlerFn func(resp http.ResponseWriter, req *http.Request) (interface{}, error)
type handlerByteFn func(resp http.ResponseWriter, req *http.Request) ([]byte, error)

// HTTPServer is used to wrap an Agent and expose it over an HTTP interface
type HTTPServer struct {
	agent      *Agent
	mux        *http.ServeMux
	listener   net.Listener
	listenerCh chan struct{}
	logger     log.Logger
	Addr       string
}

// NewHTTPServers starts an HTTP server for every address.http configured in
// the agent.
func NewHTTPServers(agent *Agent, config *cfg.Config) ([]*HTTPServer, error) {
	var srvs []*HTTPServer
	var serverInitializationErrors error

	// Get connection handshake timeout limit
	handshakeTimeout, err := time.ParseDuration(config.Limits.HTTPSHandshakeTimeout)
	if err != nil {
		return srvs, fmt.Errorf("error parsing https_handshake_timeout: %v", err)
	} else if handshakeTimeout < 0 {
		return srvs, fmt.Errorf("https_handshake_timeout must be >= 0")
	}

	// Get max connection limit
	maxConns := 0
	if mc := config.Limits.HTTPMaxConnsPerClient; mc != nil {
		maxConns = *mc
	}
	if maxConns < 0 {
		return srvs, fmt.Errorf("http_max_conns_per_client must be >= 0")
	}

	// Start the listener
	for _, addr := range config.NormalizedAddrs.HTTP {
		// Create the mux
		mux := http.NewServeMux()

		lnAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			serverInitializationErrors = multierror.Append(serverInitializationErrors, err)
			continue
		}
		ln, err := config.Listener("tcp", lnAddr.IP.String(), lnAddr.Port)
		if err != nil {
			serverInitializationErrors = multierror.Append(serverInitializationErrors, fmt.Errorf("failed to start HTTP listener: %v", err))
			continue
		}

		// Create the server
		srv := &HTTPServer{
			agent:      agent,
			mux:        mux,
			listener:   ln,
			listenerCh: make(chan struct{}),
			logger:     agent.httpLogger,
			Addr:       ln.Addr().String(),
		}
		httpServer := http.Server{
			Addr:         srv.Addr,
			Handler:      srv.router(),
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			ConnState:    makeConnState(false, handshakeTimeout, maxConns),
			ErrorLog:     logging.NewHTTPServerLogger(srv.logger),
		}

		go func() {
			defer close(srv.listenerCh)
			httpServer.Serve(ln)
		}()

		srvs = append(srvs, srv)
	}

	if serverInitializationErrors != nil {
		for _, srv := range srvs {
			srv.Shutdown()
		}
	}

	return srvs, serverInitializationErrors
}

func (s *HTTPServer) deal(c *gin.Context, handler func(resp http.ResponseWriter, req *http.Request) (interface{}, error)) {
	resp, err := handler(c.Writer, c.Request)
	// TODO print the error detail
	if err != nil {
		s.logger.Warn(fmt.Sprintf("fetch request error :%s", err.Error()))
		c.JSON(
			http.StatusInternalServerError,
			&dto.ExcludeDataResponse{
				Status:  500,
				Message: err.Error(),
			})
	} else {
		c.JSON(http.StatusOK,
			resp,
		)
	}
}

func (s *HTTPServer) dealWithParams(c *gin.Context, handler func(resp http.ResponseWriter, req *http.Request, params gin.Params) (interface{}, error)) {
	resp, err := handler(c.Writer, c.Request, c.Params)
	// TODO print the error detail
	if err != nil {
		s.logger.Warn(fmt.Sprintf("fetch request error :%s", err.Error()))
		c.JSON(
			http.StatusInternalServerError,
			&dto.ExcludeDataResponse{
				Status:  500,
				Message: err.Error(),
			})
	} else {
		c.JSON(http.StatusOK,
			resp,
		)
	}
}

func makeConnState(isTLS bool, handshakeTimeout time.Duration, connLimit int) func(conn net.Conn, state http.ConnState) {
	if !isTLS || handshakeTimeout == 0 {
		if connLimit > 0 {
			// Still return the connection limiter
			return connlimit.NewLimiter(connlimit.Config{
				MaxConnsPerClientIP: connLimit,
			}).HTTPConnStateFunc()
		}
		return nil
	}

	if connLimit > 0 {
		// Return conn state callback with connection limiting and a
		// handshake timeout.
		connLimiter := connlimit.NewLimiter(connlimit.Config{
			MaxConnsPerClientIP: connLimit,
		}).HTTPConnStateFunc()

		return func(conn net.Conn, state http.ConnState) {
			switch state {
			case http.StateNew:
				// Set deadline to prevent slow send before TLS handshake or first
				// byte of request.
				conn.SetDeadline(time.Now().Add(handshakeTimeout))
			case http.StateActive:
				// Clear read deadline. We should maybe set read timeouts more
				// generally but that's a bigger task as some HTTP endpoints may
				// stream large requests and responses (e.g. snapshot) so we can't
				// set sensible blanket timeouts here.
				conn.SetDeadline(time.Time{})
			}

			// Call connection limiter
			connLimiter(conn, state)
		}
	}

	// Return conn state callback with just a handshake timeout
	// (connection limiting disabled).
	return func(conn net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			// Set deadline to prevent slow send before TLS handshake or first
			// byte of request.
			conn.SetDeadline(time.Now().Add(handshakeTimeout))
		case http.StateActive:
			// Clear read deadline. We should maybe set read timeouts more
			// generally but that's a bigger task as some HTTP endpoints may
			// stream large requests and responses (e.g. snapshot) so we can't
			// set sensible blanket timeouts here.
			conn.SetDeadline(time.Time{})
		}
	}
}

// HTTPCodedError is used to provide the HTTP error code
type HTTPCodedError interface {
	error
	Code() int
}

func CodedError(c int, s string) HTTPCodedError {
	return &codedError{s, c}
}

type codedError struct {
	s    string
	code int
}

func (e *codedError) Error() string {
	return e.s
}

func (e *codedError) Code() int {
	return e.code
}

// parse is a convenience method for endpoints that need to parse multiple flags
// It sets r to the region and b to the QueryOptions in req
func (s *HTTPServer) parse(resp http.ResponseWriter, req *http.Request, r *string, b *dto.QueryOptions) bool {
	s.parseRegion(req, r)
	parseConsistency(req, b)
	parsePrefix(req, b)
	parseNamespace(req, &b.Namespace)
	parsePagination(req, b)
	parseFilter(req, b)
	parseReverse(req, b)
	return parseWait(resp, req, b)
}

// parseWriteRequest is a convenience method for endpoints that need to parse a
// write request.
func (s *HTTPServer) parseWriteRequest(req *http.Request, w *dto.WriteRequest) {
	parseNamespace(req, &w.Namespace)
	s.parseRegion(req, &w.Region)
}

// parsePagination parses the pagination fields for QueryOptions
func parsePagination(req *http.Request, b *dto.QueryOptions) {
	query := req.URL.Query()
	rawPerPage := query.Get("pageSize")
	if rawPerPage != "" {
		pageSize, err := strconv.ParseInt(rawPerPage, 10, 32)
		if err == nil {
			b.PageSize = int32(pageSize)
		}
	}
	pageNumber := query.Get("pageNumber")
	if pageNumber != "" {
		pageNumberIndex, err := strconv.ParseInt(pageNumber, 10, 32)
		if err == nil {
			b.PageNumber = int32(pageNumberIndex)
		}
	}

	b.NextToken = query.Get("next_token")
}

// parseFilter parses the filter query parameter for QueryOptions
func parseFilter(req *http.Request, b *dto.QueryOptions) {
	query := req.URL.Query()
	if filter := query.Get("filter"); filter != "" {
		b.Filter = filter
	}
}

// parsePrefix is used to parse the ?prefix query param
func parsePrefix(req *http.Request, b *dto.QueryOptions) {
	query := req.URL.Query()
	if prefix := query.Get("prefix"); prefix != "" {
		b.Prefix = prefix
	}
}

// parseReverse parses the reverse query parameter for QueryOptions
func parseReverse(req *http.Request, b *dto.QueryOptions) {
	query := req.URL.Query()
	b.Reverse = query.Get("reverse") == "true"
}

// parseRegion is used to parse the ?region query param
func (s *HTTPServer) parseRegion(req *http.Request, r *string) {
	if other := req.URL.Query().Get("region"); other != "" {
		*r = other
	} else if *r == "" {
		*r = s.agent.config.Region
	}
}

// parseConsistency is used to parse the ?stale query params.
func parseConsistency(req *http.Request, b *dto.QueryOptions) {
	query := req.URL.Query()
	if _, ok := query["stale"]; ok {
		b.AllowStale = true
	}
}

// parseNamespace is used to parse the ?namespace parameter
func parseNamespace(req *http.Request, n *string) {
	if other := req.URL.Query().Get("namespace"); other != "" {
		*n = other
	} else if *n == "" {
		*n = constant.DefaultNamespace
	}
}

// parseWait is used to parse the ?wait and ?index query params
// Returns true on error
func parseWait(resp http.ResponseWriter, req *http.Request, b *dto.QueryOptions) bool {
	query := req.URL.Query()
	if wait := query.Get("wait"); wait != "" {
		dur, err := time.ParseDuration(wait)
		if err != nil {
			resp.WriteHeader(400)
			resp.Write([]byte("Invalid wait time"))
			return true
		}
		b.MaxQueryTime = dur
	}
	if idx := query.Get("index"); idx != "" {
		index, err := strconv.ParseUint(idx, 10, 64)
		if err != nil {
			resp.WriteHeader(400)
			resp.Write([]byte("Invalid index"))
			return true
		}
		b.MinQueryIndex = index
	}
	return false
}

// rpcHandlerForNode is a helper that given a node ID returns whether to
// use the local clients RPC, the local clients remote RPC or the server on the
// agent. If there is a local node and no node id is given, it is assumed the
// local node is being targed.
func (s *HTTPServer) rpcHandlerForNode(nodeID string) (localClient, remoteClient, server bool) {
	c := s.agent.Client()
	srv := s.agent.Server()

	// See if the local client can handle the request.
	localClient = c != nil && // Must have a client
		(nodeID == "" || // If no node ID is given
			nodeID == c.NodeID()) // Requested node is the local node.

	// Only use the client RPC to server if we don't have a server and the local
	// client can't handle the call.
	useClientRPC := c != nil && !localClient && srv == nil

	// Use the server as a last case.
	useServerRPC := !localClient && !useClientRPC && srv != nil && nodeID != ""

	return localClient, useClientRPC, useServerRPC
}

// decodeBody is used to decode a JSON request body
func decodeBody(req *http.Request, out interface{}) error {

	if req.Body == http.NoBody {
		return errors.New("Request body is empty")
	}

	dec := json.NewDecoder(req.Body)
	return dec.Decode(&out)
}

// Shutdown is used to shutdown the HTTP server
func (s *HTTPServer) Shutdown() {
	if s != nil {
		s.logger.Debug("shutting down http server")
		s.listener.Close()
		<-s.listenerCh // block until http.Serve has returned.
	}
}

func (s *HTTPServer) parseNamespaceFilter(namespace string, b *dto.QueryOptions) {
	var hasNamespaceFilter = regexp.MustCompile(`Namespace[\s]+==`)
	if b.Filter == "" {
		b.Filter = fmt.Sprintf(`Namespace == "%s"`, namespace)
	}else if !hasNamespaceFilter.Match([]byte(b.Filter)) {
		b.Filter += fmt.Sprintf(` and Namespace == "%s"`, namespace)
	}
}