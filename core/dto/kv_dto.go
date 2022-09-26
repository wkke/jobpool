package dto

import "yunli.com/jobpool/core/structs"

type KvListRequest struct {
	QueryOptions
}

type KvListResponse struct {
	Kvs       []*structs.Kv `json:"data"`
	QueryMeta
	ExcludePageResponse
}

type KvUpsertRequest struct {
	Kv *structs.Kv
	WriteRequest
}

type KvDto struct {
	Key   string
	Value string
}

type KvAddRequest struct {
	KvDto
	WriteRequest
}

type KvAddResponse struct {
	KvDto
	QueryMeta
	ExcludeDataResponse
}

type KvUpdateRequest struct {
	KvDto
	WriteRequest
}

type KvDetailRequest struct {
	Key string
	QueryOptions
}

type KvDetailResponse struct {
	Kv *structs.Kv
	QueryMeta
	ExcludeDataResponse
}

type KvDeleteResponse struct {
	ExcludeDataResponse
}