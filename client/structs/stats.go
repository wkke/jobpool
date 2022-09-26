package structs

import (
	"yunli.com/jobpool/client/stats"
	"yunli.com/jobpool/core/dto"
)

// ClientStatsResponse is used to return statistics about a node.
type ClientStatsResponse struct {
	HostStats *stats.HostStats
	dto.QueryMeta
}

