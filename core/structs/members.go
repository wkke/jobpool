package structs

import "net"

// ServerMembersResponse has the list of servers in a cluster
type ServerMembersResponse struct {
	ServerName   string
	ServerRegion string
	ServerDC     string
	Members      []*ServerMember
}

// ServerMember holds information about a Jobpool server agent in a cluster
type ServerMember struct {
	Name        string
	Addr        net.IP
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