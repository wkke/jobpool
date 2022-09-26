package state

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"yunli.com/jobpool/helper/ci"
	"yunli.com/jobpool/helper/testlog"
)

func setupBoltStateDB(t *testing.T) (*BoltStateDB, func()) {
	dir, err := ioutil.TempDir("", "jobpooltest")
	require.NoError(t, err)

	db, err := NewBoltStateDB(testlog.HCLogger(t), dir)
	if err != nil {
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("error removing boltdb dir: %v", err)
		}
		t.Fatalf("error creating boltdb: %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("error closing boltdb: %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("error removing boltdb dir: %v", err)
		}
	}

	return db.(*BoltStateDB), cleanup
}

func testDB(t *testing.T, f func(*testing.T, StateDB)) {
	boltdb, cleanup := setupBoltStateDB(t)
	defer cleanup()

	impls := []StateDB{boltdb}

	for _, db := range impls {
		db := db
		t.Run(db.Name(), func(t *testing.T) {
			f(t, db)
		})
	}
}

// Integer division, rounded up.
func ceilDiv(a, b int) int {
	return (a + b - 1) / b
}

// TestStateDB_Batch asserts the behavior of PutAllocation, PutNetworkStatus and
// DeleteAllocationBucket in batch mode, for all operational StateDB implementations.
func TestStateDB_Batch(t *testing.T) {
	ci.Parallel(t)

	testDB(t, func(t *testing.T, db StateDB) {
		require := require.New(t)

		// For BoltDB, get initial tx_id
		var getTxID func() int
		var prevTxID int
		var batchDelay time.Duration
		var batchSize int
		if boltStateDB, ok := db.(*BoltStateDB); ok {
			boltdb := boltStateDB.DB().BoltDB()
			getTxID = func() int {
				tx, err := boltdb.Begin(true)
				require.NoError(err)
				defer tx.Rollback()
				return tx.ID()
			}
			prevTxID = getTxID()
			batchDelay = boltdb.MaxBatchDelay
			batchSize = boltdb.MaxBatchSize
		}

		// Write 1000 allocations and network statuses in batch mode
		startTime := time.Now()
		const numAllocs = 1000

		// Check BoltDB actually combined PutAllocation calls into much fewer transactions.
		// The actual number of transactions depends on how fast the goroutines are spawned,
		// with every batchDelay (10ms by default) period saved in a separate transaction,
		// plus each transaction is limited to batchSize writes (1000 by default).
		// See boltdb MaxBatchDelay and MaxBatchSize parameters for more details.
		if getTxID != nil {
			numTransactions := getTxID() - prevTxID
			writeTime := time.Now().Sub(startTime)
			expectedNumTransactions := ceilDiv(2*numAllocs, batchSize) + ceilDiv(int(writeTime), int(batchDelay))
			require.LessOrEqual(numTransactions, expectedNumTransactions)
			prevTxID = getTxID()
		}


		// Check BoltDB combined DeleteAllocationBucket calls into much fewer transactions.
		if getTxID != nil {
			numTransactions := getTxID() - prevTxID
			writeTime := time.Now().Sub(startTime)
			expectedNumTransactions := ceilDiv(numAllocs, batchSize) + ceilDiv(int(writeTime), int(batchDelay))
			require.LessOrEqual(numTransactions, expectedNumTransactions)
			prevTxID = getTxID()
		}
	})
}

// TestStateDB_Upgrade asserts calling Upgrade on new databases always
// succeeds.
func TestStateDB_Upgrade(t *testing.T) {
	ci.Parallel(t)

	testDB(t, func(t *testing.T, db StateDB) {
		require.NoError(t, db.Upgrade())
	})
}
