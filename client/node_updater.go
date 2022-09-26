package client

import (
	"context"
	"time"
)

var (
	// batchFirstFingerprintsTimeout is the maximum amount of time to wait for
	// initial fingerprinting to complete before sending a batched Node update
	batchFirstFingerprintsTimeout = 50 * time.Second
)


// batchFirstFingerprints waits for the first fingerprint response from all
// plugin managers and sends a single Node update for all fingerprints. It
// should only ever be called once
func (c *Client) batchFirstFingerprints() {
	_, cancel := context.WithTimeout(context.Background(), batchFirstFingerprintsTimeout)
	defer cancel()
	c.configLock.Lock()
	defer c.configLock.Unlock()

	// TODO only update the node if changes occurred
	c.updateNodeLocked()
	close(c.fpInitialized)
}