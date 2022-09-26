package client

import (
	"fmt"
	"strings"
	"time"
	"yunli.com/jobpool/client/servers"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper"
)

// registerAndHeartbeat is a long lived goroutine used to register the client
// and then start heartbeating to the server.
func (c *Client) registerAndHeartbeat() {

	// Register the node
	c.retryRegisterNode()

	// Start watching changes for node changes
	go c.watchNodeUpdates()

	// Start watching for emitting node events
	go c.watchNodeEvents()

	// Setup the heartbeat timer, for the initial registration
	// we want to do this quickly. We want to do it extra quickly
	// in development mode.
	var heartbeat <-chan time.Time
	if c.config.DevMode {
		heartbeat = time.After(0)
	} else {
		heartbeat = time.After(helper.RandomStagger(initialHeartbeatStagger))
	}

	for {
		select {
		case <-c.rpcRetryWatcher():
		case <-heartbeat:
		case <-c.shutdownCh:
			return
		}
		if err := c.updateNodeStatus(); err != nil {
			// The servers have changed such that this node has not been
			// registered before
			if strings.Contains(err.Error(), "node not found") {
				// Re-register the node
				c.logger.Info("re-registering node")
				c.retryRegisterNode()
				heartbeat = time.After(helper.RandomStagger(initialHeartbeatStagger))
			} else {
				intv := c.getHeartbeatRetryIntv(err)
				c.logger.Error("error heartbeating. retrying", "error", err, "period", intv)
				heartbeat = time.After(intv)

				// c.triggerDiscovery()
			}
		} else {
			c.heartbeatLock.Lock()
			heartbeat = time.After(c.heartbeatTTL)
			c.heartbeatLock.Unlock()
		}
	}
}

func (c *Client) lastHeartbeat() time.Time {
	return c.heartbeatStop.getLastOk()
}

// watchNodeUpdates blocks until it is edge triggered. Once triggered,
// it will update the client node copy and re-register the node.
func (c *Client) watchNodeUpdates() {
	var hasChanged bool

	timer := stoppedTimer()
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			c.logger.Info("state changed, updating node and re-registering")
			c.retryRegisterNode()
			hasChanged = false
		case <-c.triggerNodeUpdate:
			if hasChanged {
				continue
			}
			hasChanged = true
			timer.Reset(c.retryIntv(nodeUpdateRetryIntv))
		case <-c.shutdownCh:
			return
		}
	}
}


// watchNodeEvents is a handler which receives node events and on a interval
// and submits them in batch format to the server
func (c *Client) watchNodeEvents() {
	// batchEvents stores events that have yet to be published
	var batchEvents []*structs.NodeEvent

	timer := stoppedTimer()
	defer timer.Stop()

	for {
		select {
		case event := <-c.triggerEmitNodeEvent:
			if l := len(batchEvents); l <= constant.MaxRetainedNodeEvents {
				batchEvents = append(batchEvents, event)
			} else {
				// Drop the oldest event
				c.logger.Warn("dropping node event", "node_event", batchEvents[0])
				batchEvents = append(batchEvents[1:], event)
			}
			timer.Reset(c.retryIntv(nodeUpdateRetryIntv))
		case <-timer.C:
			if err := c.submitNodeEvents(batchEvents); err != nil {
				c.logger.Error("error submitting node events", "error", err)
				timer.Reset(c.retryIntv(nodeUpdateRetryIntv))
			} else {
				// Reset the events since we successfully sent them.
				batchEvents = []*structs.NodeEvent{}
			}
		case <-c.shutdownCh:
			return
		}
	}
}


// submitNodeEvents is used to submit a client-side node event. Examples of
// these kinds of events include when a driver moves from healthy to unhealthy
// (and vice versa)
func (c *Client) submitNodeEvents(events []*structs.NodeEvent) error {
	nodeID := c.NodeID()
	nodeEvents := map[string][]*structs.NodeEvent{
		nodeID: events,
	}
	req := dto.EmitNodeEventsRequest{
		NodeEvents:   nodeEvents,
		WriteRequest: dto.WriteRequest{Region: c.Region()},
	}
	var resp dto.EmitNodeEventsResponse
	if err := c.RPC("Node.EmitEvents", &req, &resp); err != nil {
		return fmt.Errorf("Emitting node events failed: %v", err)
	}
	return nil
}


// updateNodeStatus is used to heartbeat and update the status of the node
func (c *Client) updateNodeStatus() error {
	start := time.Now()
	req := dto.NodeUpdateStatusRequest{
		NodeID:       c.NodeID(),
		Status:       constant.NodeStatusReady,
		WriteRequest: dto.WriteRequest{Region: c.Region()},
	}
	var resp dto.NodeUpdateResponse
	if err := c.RPC("Node.UpdateStatus", &req, &resp); err != nil {
		// c.triggerDiscovery()
		return fmt.Errorf("failed to update status: %v", err)
	}
	end := time.Now()

	if len(resp.EvalIDs) != 0 {
		c.logger.Debug("evaluations triggered by node update", "num_evals", len(resp.EvalIDs))
	}

	// Update the last heartbeat and the new TTL, capturing the old values
	c.heartbeatLock.Lock()
	last := c.lastHeartbeat()
	oldTTL := c.heartbeatTTL
	haveHeartbeated := c.haveHeartbeated
	c.heartbeatStop.setLastOk(time.Now())
	c.heartbeatTTL = resp.HeartbeatTTL
	c.haveHeartbeated = true
	c.heartbeatLock.Unlock()
	c.logger.Trace("next heartbeat", "period", resp.HeartbeatTTL)

	if resp.Index != 0 {
		c.logger.Debug("state updated", "node_status", req.Status)

		// We have potentially missed our TTL log how delayed we were
		if haveHeartbeated {
			c.logger.Warn("missed heartbeat",
				"req_latency", end.Sub(start), "heartbeat_ttl", oldTTL, "since_last_heartbeat", time.Since(last))
		}
	}

	// Update the number of nodes in the cluster so we can adjust our server
	// rebalance rate.
	c.servers.SetNumNodes(resp.NumNodes)

	// Convert []*NodeServerInfo to []*servers.Server
	jobpoolServers := make([]*servers.Server, 0, len(resp.Servers))
	for _, s := range resp.Servers {
		addr, err := resolveServer(s.RPCAdvertiseAddr)
		if err != nil {
			c.logger.Warn("ignoring invalid server", "error", err, "server", s.RPCAdvertiseAddr)
			continue
		}
		e := &servers.Server{Addr: addr}
		jobpoolServers = append(jobpoolServers, e)
	}
	if len(jobpoolServers) == 0 {
		return fmt.Errorf("heartbeat response returned no valid servers")
	}
	c.servers.SetServers(jobpoolServers)

	if resp.LeaderRPCAddr == "" {
		// c.triggerDiscovery()
	}
	return nil
}


// getHeartbeatRetryIntv is used to retrieve the time to wait before attempting
// another heartbeat.
func (c *Client) getHeartbeatRetryIntv(err error) time.Duration {
	if c.config.DevMode {
		return devModeRetryIntv
	}

	// Collect the useful heartbeat info
	c.heartbeatLock.Lock()
	haveHeartbeated := c.haveHeartbeated
	last := c.lastHeartbeat()
	ttl := c.heartbeatTTL
	c.heartbeatLock.Unlock()

	// If we haven't even successfully heartbeated once or there is no leader
	// treat it as a registration. In the case that there is a leadership loss,
	// we will have our heartbeat timer reset to a much larger threshold, so
	// do not put unnecessary pressure on the new leader.
	if !haveHeartbeated || err == structs.ErrNoLeader {
		return c.retryIntv(registerRetryIntv)
	}

	// Determine how much time we have left to heartbeat
	left := time.Until(last.Add(ttl))

	// Logic for retrying is:
	// * Do not retry faster than once a second
	// * Do not retry less that once every 30 seconds
	// * If we have missed the heartbeat by more than 30 seconds, start to use
	// the absolute time since we do not want to retry indefinitely
	switch {
	case left < -30*time.Second:
		// Make left the absolute value so we delay and jitter properly.
		left *= -1
	case left < 0:
		return time.Second + helper.RandomStagger(time.Second)
	default:
	}

	stagger := helper.RandomStagger(left)
	switch {
	case stagger < time.Second:
		return time.Second + helper.RandomStagger(time.Second)
	case stagger > 30*time.Second:
		return 25*time.Second + helper.RandomStagger(5*time.Second)
	default:
		return stagger
	}
}