package client

import (
	"fmt"
	"time"
	allocrunner "yunli.com/jobpool/client/service/runner"
	xstructs "yunli.com/jobpool/client/structs"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
)

func (c *Client) watchAllocations(updates chan *xstructs.AllocUpdates) {
	req := dto.NodeSpecificRequest{
		NodeID:   c.NodeID(),
		SecretID: c.config.Node.SecretID,
		QueryOptions: dto.QueryOptions{
			Region:     c.Region(),
			AllowStale: false,
		},
	}
	var resp dto.NodeClientAllocsResponse
	// The request and response for pulling down the set of allocations that are
	// new, or updated server side.
	allocsReq := dto.AllocsGetRequest{
		QueryOptions: dto.QueryOptions{
			Region:     c.Region(),
			AllowStale: true,
		},
	}
	var allocsResp dto.AllocsGetResponse

OUTER:
	for {
		// Get the allocation modify index map, blocking for updates. We will
		// use this to determine exactly what allocations need to be downloaded
		// in full.
		resp = dto.NodeClientAllocsResponse{}
		err := c.RPC("Node.GetClientAllocs", &req, &resp)
		if err != nil {
			// Shutdown often causes EOF errors, so check for shutdown first
			select {
			case <-c.shutdownCh:
				return
			default:
			}
			if err != noServersErr {
				c.logger.Error("error querying node allocations", "error", err)
			}
			retry := c.retryIntv(getAllocRetryIntv)
			select {
			case <-c.rpcRetryWatcher():
				continue
			case <-time.After(retry):
				continue
			case <-c.shutdownCh:
				return
			}
		}

		// Check for shutdown
		select {
		case <-c.shutdownCh:
			return
		default:
		}

		// Filter all allocations whose AllocModifyIndex was not incremented.
		// These are the allocations who have either not been updated, or whose
		// updates are a result of the client sending an update for the alloc.
		// This lets us reduce the network traffic to the server as we don't
		// need to pull all the allocations.
		var pull []string
		filtered := make(map[string]struct{})
		var pullIndex uint64
		for allocID, modifyIndex := range resp.Allocs {
			// Pull the allocation if we don't have an alloc runner for the
			// allocation or if the alloc runner requires an updated allocation.
			//XXX Part of Client alloc index tracking exp
			c.allocLock.RLock()
			currentAR, ok := c.allocs[allocID]
			c.allocLock.RUnlock()

			// Ignore alloc updates for allocs that are invalid because of initialization errors
			c.invalidAllocsLock.Lock()
			_, isInvalid := c.invalidAllocs[allocID]
			c.invalidAllocsLock.Unlock()

			if (!ok || modifyIndex > currentAR.Alloc().AllocModifyIndex) && !isInvalid {
				// Only pull allocs that are required. Filtered
				// allocs might be at a higher index, so ignore
				// it.
				if modifyIndex > pullIndex {
					pullIndex = modifyIndex
				}
				pull = append(pull, allocID)
			} else {
				filtered[allocID] = struct{}{}
			}
		}

		// Pull the allocations that passed filtering.
		allocsResp.Allocs = nil
		var pulledAllocs map[string]*structs.Allocation
		if len(pull) != 0 {
			// Pull the allocations that need to be updated.
			allocsReq.AllocIDs = pull
			allocsReq.MinQueryIndex = pullIndex - 1
			allocsResp = dto.AllocsGetResponse{}
			if err := c.RPC("Alloc.GetAllocs", &allocsReq, &allocsResp); err != nil {
				c.logger.Error("error querying updated allocations", "error", err)
				retry := c.retryIntv(getAllocRetryIntv)
				select {
				case <-c.rpcRetryWatcher():
					continue
				case <-time.After(retry):
					continue
				case <-c.shutdownCh:
					return
				}
			}

			// Ensure that we received all the allocations we wanted
			pulledAllocs = make(map[string]*structs.Allocation, len(allocsResp.Allocs))
			for _, alloc := range allocsResp.Allocs {
				//if alloc.ClientStatus != constant.AllocClientStatusPending {
				//	continue
				//}
				// update job status to running
				//err := c.updateJobStatusByClientAlloc(alloc)
				//if err != nil {
				//	// TODO 完善这里的状态更新
				//	// c.logger.Error("error update job status by alloc", "error", err)
				//}
				pulledAllocs[alloc.ID] = alloc
			}

			for _, desiredID := range pull {
				if _, ok := pulledAllocs[desiredID]; !ok {
					// We didn't get everything we wanted. Do not update the
					// MinQueryIndex, sleep and then retry.
					wait := c.retryIntv(5 * time.Second)
					select {
					case <-time.After(wait):
						// Wait for the server we contact to receive the
						// allocations
						continue OUTER
					case <-c.shutdownCh:
						return
					}
				}
			}

			// Check for shutdown
			select {
			case <-c.shutdownCh:
				return
			default:
			}
		}

		c.logger.Debug("updated allocations", "index", resp.Index,
			"total", len(resp.Allocs), "pulled", len(allocsResp.Allocs), "filtered", len(filtered))

		// After the first request, only require monotonically increasing state.
		req.AllowStale = true
		if resp.Index > req.MinQueryIndex {
			req.MinQueryIndex = resp.Index
		}

		// Push the updates.
		update := &xstructs.AllocUpdates{
			Filtered: filtered,
			Pulled:   pulledAllocs,
		}

		select {
		case updates <- update:
		case <-c.shutdownCh:
			return
		}
	}
}

func (c *Client) updateJobStatusByClientAlloc(allocation *structs.Allocation) error {
	req := dto.JobStatusUpdateRequest{
		JobID:        allocation.JobId,
		Status:       constant.JobStatusRunning,
		WriteRequest: dto.WriteRequest{Region: c.Region(), Namespace: allocation.Namespace},
	}
	var resp dto.JobStatusUpdateResponse
	err := c.RPC("Job.UpdateStatus", &req, &resp)
	return err
}

// runAllocs is invoked when we get an updated set of allocations
func (c *Client) runAllocs(update *xstructs.AllocUpdates) {
	// Get the existing allocs
	c.allocLock.RLock()
	existing := make(map[string]uint64, len(c.allocs))
	for id, ar := range c.allocs {
		existing[id] = ar.Alloc().AllocModifyIndex
	}
	c.allocLock.RUnlock()

	diff := diffAllocs(existing, update)
	c.logger.Debug("allocation updates", "added", len(diff.added), "removed", len(diff.removed),
		"updated", len(diff.updated), "ignored", len(diff.ignore))
	// Remove the old allocations
	//for _, remove := range diff.removed {
	//	c.removeAlloc(remove)
	//}

	// Update the existing allocations
	for _, update := range diff.updated {
		c.updateAlloc(update)
	}

	c.logger.Debug("in run allocs method---------")

	// 比较差异，增删改查 TODO 只实现新增逻辑
	var allocAddList []*structs.Allocation
	for _, pulled := range update.Pulled {
		allocAddList = append(allocAddList, pulled)
	}

	errs := 0
	// Start the new allocations
	for _, add := range allocAddList {
		if err := c.addAlloc(add); err != nil {
			c.logger.Error("error adding alloc", "error", err, "alloc_id", add.ID)
			errs++
			// We mark the alloc as failed and send an update to the server
			// We track the fact that creating an allocrunner failed so that we don't send updates again
			if add.ClientStatus != constant.AllocClientStatusFailed {
				c.handleInvalidAllocs(add, err)
			}
		}
	}

	// Mark servers as having been contacted so blocked tasks that failed
	// to restore can now restart.
	c.serversContactedOnce.Do(func() {
		close(c.serversContactedCh)
	})

	c.logger.Debug("allocation updates applied", "added", len(allocAddList), "errors", errs)
}

func (c *Client) handleInvalidAllocs(alloc *structs.Allocation, err error) {
	c.invalidAllocsLock.Lock()
	c.invalidAllocs[alloc.ID] = struct{}{}
	c.invalidAllocsLock.Unlock()

	// Mark alloc as failed so server can handle this
	failed := makeFailedAlloc(alloc, err)
	select {
	case c.allocUpdates <- failed:
	case <-c.shutdownCh:
	}
}

func makeFailedAlloc(add *structs.Allocation, err error) *structs.Allocation {
	stripped := new(structs.Allocation)
	stripped.ID = add.ID
	stripped.ClientStatus = constant.AllocClientStatusFailed
	stripped.ClientDescription = fmt.Sprintf("Unable to add allocation due to error: %v", err)
	return stripped
}

// addAlloc is invoked when we should add an allocation
func (c *Client) addAlloc(alloc *structs.Allocation) error {
	c.allocLock.Lock()
	defer c.allocLock.Unlock()

	// Check if we already have an alloc runner
	if _, ok := c.allocs[alloc.ID]; ok {
		c.logger.Debug("dropping duplicate add allocation request", "alloc_id", alloc.ID)
		return nil
	}

	// Initialize local copy of alloc before creating the alloc runner so
	// we can't end up with an alloc runner that does not have an alloc.
	if err := c.stateDB.PutAllocation(alloc); err != nil {
		return err
	}

	// Copy the config since the node can be swapped out as it is being updated.
	// The long term fix is to pass in the config and node separately and then
	// we don't have to do a copy.
	c.configLock.RLock()
	arConf := &allocrunner.Config{
		Alloc:         alloc,
		Logger:        c.logger,
		ClientConfig:  c.configCopy,
		StateDB:       c.stateDB,
		StateUpdater:  c,
		RPCClient:     c,
		Region:        c.Region(),
	}
	c.configLock.RUnlock()

	ar, err := allocrunner.NewAllocRunner(arConf)
	if err != nil {
		return err
	}

	// Store the alloc runner.
	c.allocs[alloc.ID] = ar

	go ar.Run()
	return nil
}
