package constant

type Topic string

const (
	TopicAll               Topic = "*"
	TopicNode              Topic = "Node"
	TypeNodeRegistration         = "NodeRegistration"
	TypeNodeDeregistration       = "NodeDeregistration"
	TypeNodeDrain                = "NodeDrain"
	TypeNodeEvent                = "NodeStreamEvent"
)
