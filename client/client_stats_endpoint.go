package client

import (
	"time"
	"yunli.com/jobpool/core/dto"

	metrics "github.com/armon/go-metrics"
	"yunli.com/jobpool/client/structs"
)

// ClientStats endpoint is used for retrieving stats about a client
type ClientStats struct {
	c *Client
}

// Stats is used to retrieve the Clients stats.
func (s *ClientStats) Stats(args *dto.NodeSpecificRequest, reply *structs.ClientStatsResponse) error {
	defer metrics.MeasureSince([]string{"client", "client_stats", "stats"}, time.Now())
	reply.HostStats = s.c.hostStatsCollector.Stats()
	return nil
}
