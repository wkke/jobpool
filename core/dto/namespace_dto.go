package dto

import "yunli.com/jobpool/core/structs"

// NamespaceListRequest is used to request a list of namespaces
type NamespaceListRequest struct {
	QueryOptions
}

// NamespaceListResponse is used for a list request
type NamespaceListResponse struct {
	Namespaces []*structs.Namespace
	QueryMeta
}

// NamespaceSpecificRequest is used to query a specific namespace
type NamespaceSpecificRequest struct {
	Name string
	QueryOptions
}

// SingleNamespaceResponse is used to return a single namespace
type SingleNamespaceResponse struct {
	Namespace *structs.Namespace
	QueryMeta
}

// NamespaceSetRequest is used to query a set of namespaces
type NamespaceSetRequest struct {
	Namespaces []string
	QueryOptions
}

// NamespaceSetResponse is used to return a set of namespaces
type NamespaceSetResponse struct {
	Namespaces map[string]*structs.Namespace // Keyed by namespace Name
	QueryMeta
}

// NamespaceDeleteRequest is used to delete a set of namespaces
type NamespaceDeleteRequest struct {
	Namespaces []string
	WriteRequest
}

// NamespaceUpsertRequest is used to upsert a set of namespaces
type NamespaceUpsertRequest struct {
	Namespaces []*structs.Namespace
	WriteRequest
}

// NamespaceAddRequest is used to delete a set of namespaces
type NamespaceAddRequest struct {
	*structs.Namespace
	WriteRequest
}
