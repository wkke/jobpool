package client

import (
	"fmt"
	"net/url"
)

type Agent struct {
	client *Client

	// Cache static agent info
	nodeName   string
	datacenter string
	region     string
}

func (c *Client) Agent() *Agent {
	return &Agent{client: c}
}


type AgentMember struct {
	Name        string
	Addr        string
	Port        uint16
	Tags        map[string]string
	Status      string
	ProtocolMin uint8
	ProtocolMax uint8
	ProtocolCur uint8
	DelegateMin uint8
	DelegateMax uint8
	DelegateCur uint8
}

type ServerMembers struct {
	ServerName   string
	ServerRegion string
	ServerDC     string
	Members      []*AgentMember
}

// sending a member join request.
type joinResponse struct {
	NumJoined int    `json:"num_joined"`
	Error     string `json:"error"`
}

// Members is used to query all of the known server members
func (a *Agent) Members() (*ServerMembers, error) {
	var resp *ServerMembers

	// Query the known members
	_, err := a.client.query("/v1/agent/members", &resp, nil)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *Agent) ForceLeave(node string) error {
	_, err := a.client.write("/v1/agent/force-leave?node="+node, nil, nil, nil)
	return err
}

func (a *Agent) Join(addrs ...string) (int, error) {
	// Accumulate the addresses
	v := url.Values{}
	for _, addr := range addrs {
		v.Add("address", addr)
	}

	// Send the join request
	var resp joinResponse
	_, err := a.client.write("/v1/agent/join?"+v.Encode(), nil, &resp, nil)
	if err != nil {
		return 0, fmt.Errorf("failed joining: %s", err)
	}
	if resp.Error != "" {
		return 0, fmt.Errorf("failed joining: %s", resp.Error)
	}
	return resp.NumJoined, nil
}