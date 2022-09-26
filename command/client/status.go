package client

import "yunli.com/jobpool/command/model"

type Status struct {
	client *Client
}

func (c *Client) Status() *Status {
	return &Status{client: c}
}


// RegionLeader is used to query for the leader in the passed region.
func (s *Status) RegionLeader(region string) (string, error) {
	var resp string
	q := model.QueryOptions{Region: region}
	_, err := s.client.query("/v1/status/leader", &resp, &q)
	if err != nil {
		return "", err
	}
	return resp, nil
}