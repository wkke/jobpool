package model

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// WriteOptions are used to parametrize a write
type WriteOptions struct {
	// Providing a datacenter overwrites the region provided
	// by the Config
	Region string

	// Namespace is the target namespace for the write.
	Namespace string

	// AuthToken is the secret ID of an ACL token
	AuthToken string

	// Set HTTP headers on the query.
	Headers map[string]string

	// ctx is an optional context pass through to the underlying HTTP
	// request layer. Use Context() and WithContext() to manage this.
	ctx context.Context

	// IdempotencyToken can be used to ensure the write is idempotent.
	IdempotencyToken string
}


// WriteMeta is used to return meta data about a write
type WriteMeta struct {
	// LastIndex. This can be used as a WaitIndex to perform
	// a blocking query
	LastIndex uint64

	// How long did the request take
	RequestTime time.Duration
}


// Context returns the context used for canceling HTTP requests related to this write
func (o *WriteOptions) Context() context.Context {
	if o != nil && o.ctx != nil {
		return o.ctx
	}
	return context.Background()
}

func ParseWriteMeta(resp *http.Response, q *WriteMeta) error {
	header := resp.Header

	// Parse the X-Jobpool-Index
	index, err := strconv.ParseUint(header.Get("X-Jobpool-Index"), 10, 64)
	if err != nil {
		return fmt.Errorf("Failed to parse X-Jobpool-Index: %v", err)
	}
	q.LastIndex = index
	return nil
}