package structs


// InternalRpcInfo allows adding internal RPC metadata to an RPC. This struct
// should NOT be replicated in the API package as it is internal only.
type InternalRpcInfo struct {
	// Forwarded marks whether the RPC has been forwarded.
	Forwarded bool
}

// IsForwarded returns whether the RPC is forwarded from another server.
func (i *InternalRpcInfo) IsForwarded() bool {
	return i.Forwarded
}

// SetForwarded marks that the RPC is being forwarded from another server.
func (i *InternalRpcInfo) SetForwarded() {
	i.Forwarded = true
}