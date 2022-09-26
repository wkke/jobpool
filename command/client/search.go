package client

import (
	"yunli.com/jobpool/command/constant"
	"yunli.com/jobpool/command/model"
)

type Search struct {
	client *Client
}

// Search returns a handle on the Search endpoints
func (c *Client) Search() *Search {
	return &Search{client: c}
}


// PrefixSearch returns a set of matches for a particular context and prefix.
func (s *Search) PrefixSearch(prefix string, context constant.Context, q *model.QueryOptions) (*SearchResponse, *model.QueryMeta, error) {
	var resp SearchResponse
	req := &SearchRequest{Prefix: prefix, Context: context}

	qm, err := s.client.putQuery("/v1/search", req, &resp, q)
	if err != nil {
		return nil, nil, err
	}

	return &resp, qm, nil
}


type SearchResponse struct {
	Matches     map[constant.Context][]string
	Truncations map[constant.Context]bool
	model.QueryMeta
}

type SearchRequest struct {
	Prefix  string
	Context constant.Context
	model.QueryOptions
}