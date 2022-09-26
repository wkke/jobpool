package model

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

type QueryOptions struct {
	// Providing a datacenter overwrites the region provided
	// by the Config
	Region string

	// Namespace is the target namespace for the query.
	Namespace string

	// AllowStale allows any Jobpool server (non-leader) to service
	// a read. This allows for lower latency and higher throughput
	AllowStale bool

	// WaitIndex is used to enable a blocking query. Waits
	// until the timeout or the next index is reached
	WaitIndex uint64

	// WaitTime is used to bound the duration of a wait.
	// Defaults to that of the Config, but can be overridden.
	WaitTime time.Duration

	// If set, used as prefix for resource list searches
	Prefix string

	// Set HTTP parameters on the query.
	Params map[string]string

	// Set HTTP headers on the query.
	Headers map[string]string

	// AuthToken is the secret ID of an ACL token
	AuthToken string

	// Filter specifies the go-bexpr filter expression to be used for
	// filtering the data prior to returning a response
	Filter string

	// PageSize is the number of entries to be returned in queries that support
	// paginated lists.
	PageSize int32

	// NextToken is the token used to indicate where to start paging
	// for queries that support paginated lists. This token should be
	// the ID of the next object after the last one seen in the
	// previous response.
	NextToken string

	// Reverse is used to reverse the default order of list results.
	//
	// Currently only supported by specific endpoints.
	Reverse bool

	// ctx is an optional context pass through to the underlying HTTP
	// Request layer. Use Context() and WithContext() to manage this.
	ctx context.Context
}

type QueryMeta struct {
	// LastIndex. This can be used as a WaitIndex to perform
	// a blocking query
	LastIndex uint64

	// Time of last contact from the leader for the
	// server servicing the Request
	LastContact time.Duration

	// Is there a known leader
	KnownLeader bool

	// How long did the Request take
	RequestTime time.Duration

	// NextToken is the token used to indicate where to start paging
	// for queries that support paginated lists. To resume paging from
	// this point, pass this token in the next Request's QueryOptions
	NextToken string
}


func (o *QueryOptions) Context() context.Context {
	if o != nil && o.ctx != nil {
		return o.ctx
	}
	return context.Background()
}


// parseQueryMeta is used to help parse query meta-data
func ParseQueryMeta(resp *http.Response, q *QueryMeta) error {
	header := resp.Header

	index, err := strconv.ParseUint(header.Get("X-Index"), 10, 64)
	if err != nil {
		return fmt.Errorf("Failed to parse X-Index: %v", err)
	}
	q.LastIndex = index

	last, err := strconv.ParseUint(header.Get("X-LastContact"), 10, 64)
	if err != nil {
		return fmt.Errorf("Failed to parse X-LastContact: %v", err)
	}
	q.LastContact = time.Duration(last) * time.Millisecond
	q.NextToken = header.Get("X-NextToken")

	switch header.Get("X-KnownLeader") {
	case "true":
		q.KnownLeader = true
	default:
		q.KnownLeader = false
	}
	return nil
}