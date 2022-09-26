package state

import (
	"bytes"
	"fmt"
	"os"

	hclog "github.com/hashicorp/go-hclog"
	"go.etcd.io/bbolt"
	"yunli.com/jobpool/helper/boltdd"
)

// NeedsUpgrade returns true if the BoltDB needs upgrading or false if it is
// already up to date.
func NeedsUpgrade(bdb *bbolt.DB) (upgradeTo09, upgradeTo13 bool, err error) {
	upgradeTo09 = true
	upgradeTo13 = true
	err = bdb.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(metaBucketName)
		if b == nil {
			// No meta bucket; upgrade
			return nil
		}

		v := b.Get(metaVersionKey)
		if len(v) == 0 {
			// No version; upgrade
			return nil
		}

		if bytes.Equal(v, []byte{'2'}) {
			upgradeTo09 = false
			return nil
		}
		if bytes.Equal(v, metaVersion) {
			upgradeTo09 = false
			upgradeTo13 = false
			return nil
		}

		// Version exists but does not match. Abort.
		return fmt.Errorf("incompatible state version. expected %q but found %q",
			metaVersion, v)

	})

	return
}

// addMeta adds version metadata to BoltDB to mark it as upgraded and
// should be run at the end of the upgrade transaction.
func addMeta(tx *bbolt.Tx) error {
	// Create the meta bucket if it doesn't exist
	bkt, err := tx.CreateBucketIfNotExists(metaBucketName)
	if err != nil {
		return err
	}
	return bkt.Put(metaVersionKey, metaVersion)
}

// backupDB backs up the existing state database prior to upgrade overwriting
// previous backups.
func backupDB(bdb *bbolt.DB, dst string) error {
	fd, err := os.Create(dst)
	if err != nil {
		return err
	}

	return bdb.View(func(tx *bbolt.Tx) error {
		if _, err := tx.WriteTo(fd); err != nil {
			fd.Close()
			return err
		}

		return fd.Close()
	})
}

// UpgradeAllocs upgrades the boltdb schema. Example 0.8 schema:
//
//	* allocations
//	  * 15d83e8a-74a2-b4da-3f17-ed5c12895ea8
//	    * echo
//	       - simple-all (342 bytes)
//	     - alloc (2827 bytes)
//	     - alloc-dir (166 bytes)
//	     - immutable (15 bytes)
//	     - mutable (1294 bytes)
//
func UpgradeAllocs(logger hclog.Logger, tx *boltdd.Tx) error {
	btx := tx.BoltTx()
	allocationsBucket := btx.Bucket(allocationsBucketName)
	if allocationsBucket == nil {
		// No state!
		return nil
	}

	// Gather alloc buckets and remove unexpected key/value pairs
	allocBuckets := [][]byte{}
	cur := allocationsBucket.Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		if v != nil {
			logger.Warn("deleting unexpected key in state db",
				"key", string(k), "value_bytes", len(v),
			)

			if err := cur.Delete(); err != nil {
				return fmt.Errorf("error deleting unexpected key %q: %v", string(k), err)
			}
			continue
		}

		allocBuckets = append(allocBuckets, k)
	}

	for _, allocBucket := range allocBuckets {
		allocID := string(allocBucket)

		bkt := allocationsBucket.Bucket(allocBucket)
		if bkt == nil {
			// This should never happen as we just read the bucket.
			return fmt.Errorf("unexpected bucket missing %q", allocID)
		}

		allocLogger := logger.With("alloc_id", allocID)
		if err := upgradeAllocBucket(allocLogger, tx, bkt, allocID); err != nil {
			// Log and drop invalid allocs
			allocLogger.Error("dropping invalid allocation due to error while upgrading state",
				"error", err,
			)

			// If we can't delete the bucket something is seriously
			// wrong, fail hard.
			if err := allocationsBucket.DeleteBucket(allocBucket); err != nil {
				return fmt.Errorf("error deleting invalid allocation state: %v", err)
			}
		}
	}

	return nil
}

// upgradeAllocBucket upgrades an alloc bucket.
func upgradeAllocBucket(logger hclog.Logger, tx *boltdd.Tx, bkt *bbolt.Bucket, allocID string) error {
	allocFound := false
	taskBuckets := [][]byte{}
	cur := bkt.Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		switch string(k) {
		case "alloc":
			// Alloc has not changed; leave it be
			allocFound = true
		case "alloc-dir":
			// Drop alloc-dir entries as they're no longer needed.
			cur.Delete()
		case "immutable":
			// Drop immutable state. Nothing from it needs to be
			// upgraded.
			cur.Delete()
		default:
			if v != nil {
				logger.Warn("deleting unexpected state entry for allocation",
					"key", string(k), "value_bytes", len(v),
				)

				if err := cur.Delete(); err != nil {
					return err
				}

				continue
			}

			// Nested buckets are tasks
			taskBuckets = append(taskBuckets, k)
		}
	}

	// If the alloc entry was not found, abandon this allocation as the
	// state has been corrupted.
	if !allocFound {
		return fmt.Errorf("alloc entry not found")
	}

	// Upgrade tasks
	for _, taskBucket := range taskBuckets {
		taskName := string(taskBucket)
		taskBkt := bkt.Bucket(taskBucket)
		if taskBkt == nil {
			// This should never happen as we just read the bucket.
			return fmt.Errorf("unexpected bucket missing %q", taskName)
		}

		// Delete the old task bucket
		if err := bkt.DeleteBucket(taskBucket); err != nil {
			return err
		}
	}
	return nil
}
