package structs

import (
	"yunli.com/jobpool/helper/time"
)

type Kv struct {
	Key         string
	Value       string
	CreateTime  time.FormatTime `json:"createTime"`
	UpdateTime  time.FormatTime `json:"updateTime"`
	CreateIndex uint64
	ModifyIndex uint64
}

