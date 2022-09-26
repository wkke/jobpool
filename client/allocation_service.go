package client

import (
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
)

// AllocStateUpdated asynchronously updates the server with the current state
// of an allocations and its tasks.
func (c *Client) AllocStateUpdated(alloc *structs.Allocation) {
	if alloc.Terminated() {
		// Terminated, mark for GC if we're still tracking this alloc
		// runner. If it's not being tracked that means the server has
		// already GC'd it (see removeAlloc).
		// TODO do somting like gc
	}

	// Strip all the information that can be reconstructed at the server.  Only
	// send the fields that are updatable by the client.
	stripped := new(structs.Allocation)
	stripped.ID = alloc.ID
	stripped.JobId = alloc.JobId
	stripped.Namespace = alloc.Namespace
	stripped.NodeID = c.NodeID()
	stripped.ClientStatus = alloc.ClientStatus
	stripped.ClientDescription = alloc.ClientDescription

	select {
	case c.allocUpdates <- stripped:
	case <-c.shutdownCh:
	}
}

// PutAllocation stores an allocation or returns an error if it could not be stored.
func (c *Client) PutAllocation(alloc *structs.Allocation) error {
	return c.stateDB.PutAllocation(alloc)
}

func (c *Client) GetAllocJobStatus(alloc *structs.Allocation) string {
	args := dto.JobDetailRequest{
		JobID: alloc.JobId,
		QueryOptions: dto.QueryOptions{
			Region:     c.Region(),
			AllowStale: false,
		},
	}
	args.Namespace = alloc.Namespace
	var response dto.JobDetailResponse
	err := c.RPC("Job.Detail", &args, &response)
	if err != nil {
		return ""
	}
	if response.Job == nil {
		return ""
	}
	return response.Job.Status
}

func (c *Client) allocSync() {
	syncTicker := time.NewTicker(allocSyncIntv)
	updates := make(map[string]*structs.Allocation)
	for {
		select {
		case <-c.shutdownCh:
			syncTicker.Stop()
			return
		case alloc := <-c.allocUpdates:
			// Batch the allocation updates until the timer triggers.
			updates[alloc.ID] = alloc
		case <-syncTicker.C:
			// Fast path if there are no updates
			if len(updates) == 0 {
				continue
			}

			sync := make([]*structs.Allocation, 0, len(updates))
			for _, alloc := range updates {
				sync = append(sync, alloc)
			}

			// Send to server.
			args := dto.AllocUpdateRequest{
				Alloc:        sync,
				WriteRequest: dto.WriteRequest{Region: c.Region()},
			}

			var resp dto.GenericResponse
			c.logger.Trace("-----start run update alloc stauts ----", "alloc", len(sync))
			for _, item := range sync {
				c.logger.Debug("the state is :", "allocId", item.ID, "state", item.ClientStatus)
			}
			err := c.RPC("Alloc.UpdateAlloc", &args, &resp)
			if err != nil {
				// Error updating allocations, do *not* clear
				// updates and retry after backoff
				c.logger.Error("error updating allocations", "error", err)
				syncTicker.Stop()
				syncTicker = time.NewTicker(c.retryIntv(allocSyncRetryIntv))
				continue
			}

			// Successfully updated allocs, reset map and ticker.
			// Always reset ticker to give loop time to receive
			// alloc updates. If the RPC took the ticker interval
			// we may call it in a tight loop before draining
			// buffered updates.
			updates = make(map[string]*structs.Allocation, len(updates))
			syncTicker.Stop()
			syncTicker = time.NewTicker(allocSyncIntv)
		}
	}
}

// updateAlloc is invoked when we should update an allocation
func (c *Client) updateAlloc(update *structs.Allocation) {
	c.logger.Info("-----更新alloc的状态开始啦------")
	ar, err := c.getAllocRunner(update.ID)
	if err != nil {
		c.logger.Warn("cannot update nonexistent alloc", "alloc_id", update.ID)
		return
	}

	// Reconnect unknown allocations
	if update.ClientStatus == constant.AllocClientStatusUnknown && update.AllocModifyIndex > ar.Alloc().AllocModifyIndex {
		err = ar.Reconnect(update)
		if err != nil {
			c.logger.Error("error reconnecting alloc", "alloc_id", update.ID, "alloc_modify_index", update.AllocModifyIndex, "err", err)
		}
		return
	}

	// Update local copy of alloc
	if err := c.stateDB.PutAllocation(update); err != nil {
		c.logger.Error("error persisting updated alloc locally", "error", err, "alloc_id", update.ID)
	}

	// Update alloc runner
	ar.Update(update)
}

func (c *Client) getAllocRunner(allocID string) (AllocRunner, error) {
	c.allocLock.RLock()
	defer c.allocLock.RUnlock()

	ar, ok := c.allocs[allocID]
	if !ok {
		return nil, structs.NewErrUnknownAllocation(allocID)
	}

	return ar, nil
}
