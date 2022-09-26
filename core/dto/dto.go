package dto

import (
	"fmt"
	"net/http"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
)

type WriteRequest struct {
	// The target region for this write
	Region string

	// Namespace is the target namespace for the write.
	//
	// Since RPC handlers do not have a default value set they should
	// access the Namespace via the RequestNamespace method.
	//
	// Requests accessing specific namespaced objects must check ACLs
	// against the namespace of the object, not the namespace in the
	// request.
	Namespace string

	// AuthToken is secret portion of the ACL token used for the request
	AuthToken string

	// IdempotencyToken can be used to ensure the write is idempotent.
	IdempotencyToken string

	structs.InternalRpcInfo
}

func (w WriteRequest) TimeToBlock() time.Duration {
	return 0
}

func (w WriteRequest) SetTimeToBlock(_ time.Duration) {
}

func (w WriteRequest) RequestRegion() string {
	// The target region for this request
	return w.Region
}

func (w WriteRequest) RequestNamespace() string {
	if w.Namespace == "" {
		return constant.DefaultNamespace
	}
	return w.Namespace
}

// IsRead only applies to writes, always false.
func (w WriteRequest) IsRead() bool {
	return false
}

func (w WriteRequest) AllowStaleRead() bool {
	return false
}

type QueryMeta struct {
	// This is the index associated with the read
	Index uint64 `json:"-"`

	// If AllowStale is used, this is time elapsed since
	// last contact between the follower and leader. This
	// can be used to gauge staleness.
	LastContact time.Duration `json:"-"`

	// Used to indicate if there is a known leader node
	KnownLeader bool `json:"-"`

	// NextToken is the token returned with queries that support
	// paginated lists. To resume paging from this point, pass
	// this token in the next request's QueryOptions.
	NextToken string `json:"-"`
}

type WriteMeta struct {
	// This is the index associated with the write
	Index uint64
}

// QueryOptions is used to specify various flags for read queries
type QueryOptions struct {
	// The target region for this query
	Region string

	// Namespace is the target namespace for the query.
	//
	// Since handlers do not have a default value set they should access
	// the Namespace via the RequestNamespace method.
	//
	// Requests accessing specific namespaced objects must check ACLs
	// against the namespace of the object, not the namespace in the
	// request.
	Namespace string

	// If set, wait until query exceeds given index. Must be provided
	// with MaxQueryTime.
	MinQueryIndex uint64

	// Provided with MinQueryIndex to wait for change.
	MaxQueryTime time.Duration

	// If set, any follower can service the request. Results
	// may be arbitrarily stale.
	AllowStale bool

	// If set, used as prefix for resource list searches
	Prefix string

	// AuthToken is secret portion of the ACL token used for the request
	AuthToken string

	// Filter specifies the go-bexpr filter expression to be used for
	// filtering the data prior to returning a response
	Filter string

	// PageSize is the number of entries to be returned in queries that support
	// paginated lists.
	PageSize int32

	PageNumber int32

	// NextToken is the token used to indicate where to start paging
	// for queries that support paginated lists. This token should be
	// the ID of the next object after the last one seen in the
	// previous response.
	NextToken string

	// Reverse is used to reverse the default order of list results.
	Reverse bool

	structs.InternalRpcInfo
}

func (q QueryOptions) RequestNamespace() string {
	if q.Namespace == "" {
		return "default"
	}
	return q.Namespace
}

// GenericRequest is used to request where no
// specific information is needed.
type GenericRequest struct {
	QueryOptions
}

// NodeSpecificRequest is used when we just need to specify a target node
type NodeSpecificRequest struct {
	NodeID   string
	SecretID string
	QueryOptions
}

// TimeToBlock returns MaxQueryTime adjusted for maximums and defaults
// it will return 0 if this is not a blocking query
func (q QueryOptions) TimeToBlock() time.Duration {
	if q.MinQueryIndex == 0 {
		return 0
	}
	if q.MaxQueryTime > constant.MaxBlockingRPCQueryTime {
		return constant.MaxBlockingRPCQueryTime
	} else if q.MaxQueryTime <= 0 {
		return constant.DefaultBlockingRPCQueryTime
	}
	return q.MaxQueryTime
}

func (q *QueryOptions) SetTimeToBlock(t time.Duration) {
	q.MaxQueryTime = t
}

func (q QueryOptions) RequestRegion() string {
	return q.Region
}

// IsRead only applies to reads, so always true.
func (q QueryOptions) IsRead() bool {
	return true
}

func (q QueryOptions) AllowStaleRead() bool {
	return q.AllowStale
}

// NodeConnQueryResponse is used to respond to a query of whether a server has
// a connection to a specific Node
type NodeConnQueryResponse struct {
	// Connected indicates whether a connection to the Client exists
	Connected bool

	// Established marks the time at which the connection was established
	Established time.Time

	QueryMeta
}

// ExcludeDataResponse no data field response.
type ExcludePageResponse struct {
	Status        int    `json:"status"`
	Message       string `json:"message"`
	TotalElements int32  `json:"totalElements"`
	TotalPages    int32  `json:"totalPages"`
}

// ExcludeDataResponse no data field response.
type ExcludeDataResponse struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

// CheckStatus check status.
func (e *ExcludeDataResponse) CheckStatus() (err error) {
	if e.Status >= http.StatusMultipleChoices || e.Status < http.StatusOK {
		// log.Error("The status(%d) of paas may represent a request error(%s)", e.Status, e.Message)
		err = fmt.Errorf("The status(%d) of paas may represent a request error(%s)", e.Status, e.Message)
	}
	return
}

type GenericResponse struct {
	WriteMeta
}
