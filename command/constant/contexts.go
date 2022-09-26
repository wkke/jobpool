package constant

// Context defines the scope in which a search for Jobpool object operates.
type Context string

const (
	Nodes           Context = "nodes"
	Namespaces      Context = "namespaces"

	// These Context types are used to associate a search result from a lower
	// level Jobpool object with one of the higher level Context types above.
	Groups   Context = "groups"
	Services Context = "services"
	Tasks    Context = "tasks"

	// Context used to represent the set of all the higher level Context types.
	All Context = "all"
)
