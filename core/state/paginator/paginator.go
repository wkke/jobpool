package paginator

import (
	"fmt"
	"github.com/hashicorp/go-bexpr"
	"yunli.com/jobpool/core/dto"
)

// Iterator is the interface that must be implemented to supply data to the
// Paginator.
type Iterator interface {
	// Next returns the next element to be considered for pagination.
	// The page will end if nil is returned.
	Next() interface{}
}

// Paginator wraps an iterator and returns only the expected number of pages.
type Paginator struct {
	iter           Iterator
	filters        []Filter
	pageSize       int32
	pageNumber     int32
	itemCount      int32
	totalElements  int32
	totalPages     int32
	seekingToken   string
	nextToken      string
	reverse        bool
	nextTokenFound bool
	pageErr        error

	// appendFunc is the function the caller should use to append raw
	// entries to the results set. The object is guaranteed to be
	// non-nil.
	appendFunc func(interface{}) error
}

// NamespaceGetter is the interface that must be implemented by structs that
// need to have their Namespace as part of the pagination token.
type NamespaceGetter interface {
	GetNamespace() string
}

// NewPaginator returns a new Paginator. Any error creating the paginator is
// due to bad user filter input, RPC functions should therefore return a 400
// error code along with an appropriate message.
func NewPaginator(iter Iterator, filters []Filter,
	opts dto.QueryOptions, appendFunc func(interface{}) error) (*Paginator, error) {

	var evaluator *bexpr.Evaluator
	var err error

	if opts.Filter != "" {
		evaluator, err = bexpr.CreateEvaluator(opts.Filter)
		if err != nil {
			return nil, fmt.Errorf("failed to read filter expression: %v", err)
		}
		filters = append(filters, evaluator)
	}
	paginator := Paginator{
		iter:           iter,
		filters:        filters,
		pageSize:       opts.PageSize,
		pageNumber:     opts.PageNumber,
		seekingToken:   opts.NextToken,
		reverse:        opts.Reverse,
		nextTokenFound: opts.NextToken == "",
		appendFunc:     appendFunc,
	}
	if paginator.pageSize == 0 {
		paginator.pageSize = 10
	}
	if paginator.pageNumber < 1 {
		paginator.pageNumber = 1
	}
	return &paginator, nil
}

// Page populates a page by running the append function
// over all results. Returns the next token.
func (p *Paginator) Page() (string, int32, int32, error) {
DONE:
	for {
		raw, andThen := p.next()
		switch andThen {
		case paginatorInclude:
			err := p.appendFunc(raw)
			if err != nil {
				p.pageErr = err
				break DONE
			}
		case paginatorSkip:
			continue
		case paginatorComplete:
			break DONE
		}
	}
	if p.pageSize > 0 {
		var hasExist int32
		if p.totalElements%p.pageSize > 0 {
			hasExist = 1
		}
		p.totalPages = p.totalElements/p.pageSize + hasExist
	}
	return p.nextToken, p.totalElements, p.totalPages, p.pageErr
}

func (p *Paginator) next() (interface{}, paginatorState) {
	raw := p.iter.Next()
	if raw == nil {
		return nil, paginatorComplete
	}
	// apply filters if defined
	for _, f := range p.filters {
		allow, err := f.Evaluate(raw)
		if err != nil {
			p.pageErr = err
			return nil, paginatorComplete
		}
		if !allow {
			return nil, paginatorSkip
		}
	}

	p.nextTokenFound = true

	// have we produced enough results for this page?
	p.itemCount++

	var indexStart int32
	var indexEnd int32

	indexStart = (p.pageNumber-1)*p.pageSize + 1
	indexEnd = indexStart + p.pageSize - 1

	p.totalElements = p.itemCount

	if p.itemCount < indexStart {
		return nil, paginatorSkip
	}
	// 需要总条数，否则后续的数据获取可以不遍历
	if p.itemCount > indexEnd {
		return raw, paginatorSkip
	}
	return raw, paginatorInclude
}

type paginatorState int

const (
	paginatorInclude paginatorState = iota
	paginatorSkip
	paginatorComplete
)
