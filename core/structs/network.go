package structs


type ClientHostNetworkConfig struct {
	Name          string `hcl:",key"`
	CIDR          string `hcl:"cidr"`
	Interface     string `hcl:"interface"`
	ReservedPorts string `hcl:"reserved_ports"`
}


func (p *ClientHostNetworkConfig) Copy() *ClientHostNetworkConfig {
	if p == nil {
		return nil
	}

	c := new(ClientHostNetworkConfig)
	*c = *p
	return c
}