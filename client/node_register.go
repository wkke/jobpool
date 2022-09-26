package client

import (
	"fmt"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper"
)

// updateNode updates the Node copy and triggers the client to send the updated
// Node to the server. This should be done while the caller holds the
// configLock lock.
func (c *Client) updateNodeLocked() {
	// Update the config copy.
	node := c.config.Node.Copy()
	c.configCopy.Node = node

	select {
	case c.triggerNodeUpdate <- struct{}{}:
		// Node update goroutine was released to execute
	default:
		// Node update goroutine was already running
	}
}

// retryRegisterNode is used to register the node or update the registration and
// retry in case of failure.
func (c *Client) retryRegisterNode() {
	for {
		err := c.registerNode()
		if err == nil {
			// Registered!
			return
		} else {
			c.logger.Warn(fmt.Sprintf("register node failed, %s", err.Error()))
		}

		retryIntv := registerRetryIntv
		if err == noServersErr {
			c.logger.Debug("registration waiting on servers")
			retryIntv = noServerRetryIntv
		} else {
			c.logger.Error("error registering", "error", err)
		}
		select {
		case <-c.rpcRetryWatcher():
		case <-time.After(c.retryIntv(retryIntv)):
		case <-c.shutdownCh:
			return
		}
	}
}

// registerNode is used to register the node or update the registration
func (c *Client) registerNode() error {
	node := c.Node()
	req := dto.NodeRegisterRequest{
		Node:         node,
		WriteRequest: dto.WriteRequest{Region: c.Region()},
	}
	var resp dto.NodeUpdateResponse
	if err := c.RPC("Node.Register", &req, &resp); err != nil {
		c.logger.Warn("Node.Register error", "error", err)
		return err
	}

	// Update the node status to ready after we register.
	c.configLock.Lock()
	node.Status = constant.NodeStatusReady
	c.config.Node.Status = constant.NodeStatusReady
	c.configLock.Unlock()

	c.logger.Info("node registration complete")
	if len(resp.EvalIDs) != 0 {
		c.logger.Debug("evaluations triggered by node registration", "num_evals", len(resp.EvalIDs))
	}

	c.heartbeatLock.Lock()
	defer c.heartbeatLock.Unlock()
	c.heartbeatStop.setLastOk(time.Now())
	c.heartbeatTTL = resp.HeartbeatTTL
	return nil
}

// Node returns the locally registered node
func (c *Client) Node() *structs.Node {
	c.configLock.RLock()
	defer c.configLock.RUnlock()
	return c.configCopy.Node
}

// retryIntv calculates a retry interval value given the base
func (c *Client) retryIntv(base time.Duration) time.Duration {
	if c.config.DevMode {
		return devModeRetryIntv
	}
	return base + helper.RandomStagger(base)
}
