package state

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper/boltdd"

	hclog "github.com/hashicorp/go-hclog"
	"go.etcd.io/bbolt"
)

/*
The client has a boltDB backed state store. The schema as of 0.9 looks as follows:

meta/
|--> version -> '2' (not msgpack encoded)
|--> upgraded -> time.Now().Format(timeRFC3339)
allocations/
|--> <alloc-id>/
   |--> alloc          -> allocEntry{*structs.Allocation}
	 |--> deploy_status  -> deployStatusEntry{*structs.AllocDeploymentStatus}
	 |--> network_status -> networkStatusEntry{*structs.AllocNetworkStatus}
   |--> task-<name>/
      |--> local_state -> *trstate.LocalState # Local-only state
      |--> task_state  -> *structs.TaskState  # Sync'd to servers

devicemanager/
|--> plugin_state -> *dmstate.PluginState

drivermanager/
|--> plugin_state -> *driverstate.PluginState

dynamicplugins/
|--> registry_state -> *dynamicplugins.RegistryState
*/

var (
	// metaBucketName is the name of the metadata bucket
	metaBucketName = []byte("meta")

	// metaVersionKey is the key the state schema version is stored under.
	metaVersionKey = []byte("version")

	// metaVersion is the value of the state schema version to detect when
	// an upgrade is needed. It skips the usual boltdd/msgpack backend to
	// be as portable and futureproof as possible.
	metaVersion = []byte{'3'}

	// metaUpgradedKey is the key that stores the timestamp of the last
	// time the schema was upgraded.
	metaUpgradedKey = []byte("upgraded")

	// allocationsBucketName is the bucket name containing all allocation related
	// data
	allocationsBucketName = []byte("allocations")

	// allocKey is the key Allocations are stored under encapsulated in
	// allocEntry structs.
	allocKey = []byte("alloc")

	// allocDeployStatusKey is the key *structs.AllocDeploymentStatus is
	// stored under.
	allocDeployStatusKey = []byte("deploy_status")

	// allocNetworkStatusKey is the key *structs.AllocNetworkStatus is
	// stored under
	allocNetworkStatusKey = []byte("network_status")

	// allocations -> $allocid -> task-$taskname -> the keys below
	taskLocalStateKey = []byte("local_state")
	taskStateKey      = []byte("task_state")

	// devManagerBucket is the bucket name containing all device manager related
	// data
	devManagerBucket = []byte("devicemanager")

	// driverManagerBucket is the bucket name containing all driver manager
	// related data
	driverManagerBucket = []byte("drivermanager")

	// managerPluginStateKey is the key by which plugin manager plugin state is
	// stored at
	managerPluginStateKey = []byte("plugin_state")

	// dynamicPluginBucketName is the bucket name containing all dynamic plugin
	// registry data. each dynamic plugin registry will have its own subbucket.
	dynamicPluginBucketName = []byte("dynamicplugins")

	// registryStateKey is the key at which dynamic plugin registry state is stored
	registryStateKey = []byte("registry_state")
)

// taskBucketName returns the bucket name for the given task name.
func taskBucketName(taskName string) []byte {
	return []byte("task-" + taskName)
}

// NewStateDBFunc creates a StateDB given a state directory.
type NewStateDBFunc func(logger hclog.Logger, stateDir string) (StateDB, error)

// GetStateDBFactory returns a func for creating a StateDB
func GetStateDBFactory(devMode bool) NewStateDBFunc {
	// Return a noop state db implementation when in debug mode
	if devMode {
		return func(hclog.Logger, string) (StateDB, error) {
			return NoopDB{}, nil
		}
	}

	return NewBoltStateDB
}

// BoltStateDB persists and restores Jobpool client state in a boltdb. All
// methods are safe for concurrent access.
type BoltStateDB struct {
	stateDir string
	db       *boltdd.DB
	logger   hclog.Logger
}

// NewBoltStateDB creates or opens an existing boltdb state file or returns an
// error.
func NewBoltStateDB(logger hclog.Logger, stateDir string) (StateDB, error) {
	fn := filepath.Join(stateDir, "state.db")

	// Check to see if the DB already exists
	fi, err := os.Stat(fn)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	firstRun := fi == nil

	// Timeout to force failure when accessing a data dir that is already in use
	timeout := &bbolt.Options{Timeout: 5 * time.Second}

	// Create or open the boltdb state database
	db, err := boltdd.Open(fn, 0600, timeout)
	if err == bbolt.ErrTimeout {
		return nil, fmt.Errorf("timed out while opening database, is another Jobpool process accessing data_dir %s?", stateDir)
	} else if err != nil {
		return nil, fmt.Errorf("failed to create state database: %v", err)
	}

	sdb := &BoltStateDB{
		stateDir: stateDir,
		db:       db,
		logger:   logger,
	}

	// If db did not already exist, initialize metadata fields
	if firstRun {
		if err := sdb.init(); err != nil {
			return nil, err
		}
	}

	return sdb, nil
}

func (s *BoltStateDB) Name() string {
	return "boltdb"
}

// Close releases all database resources and unlocks the database file on disk.
// All transactions must be closed before closing the database.
func (s *BoltStateDB) Close() error {
	return s.db.Close()
}

// init initializes metadata entries in a newly created state database.
func (s *BoltStateDB) init() error {
	return s.db.Update(func(tx *boltdd.Tx) error {
		return addMeta(tx.BoltTx())
	})
}

// updateWithOptions enables adjustments to db.Update operation, including Batch mode.
func (s *BoltStateDB) updateWithOptions(opts []WriteOption, updateFn func(tx *boltdd.Tx) error) error {
	writeOpts := mergeWriteOptions(opts)

	if writeOpts.BatchMode {
		// In Batch mode, BoltDB opportunistically combines multiple concurrent writes into one or
		// several transactions. See boltdb.Batch() documentation for details.
		return s.db.Batch(updateFn)
	} else {
		return s.db.Update(updateFn)
	}
}

// Upgrade bolt state db from 0.8 schema to 0.9 schema. Noop if already using
// 0.9 schema. Creates a backup before upgrading.
func (s *BoltStateDB) Upgrade() error {
	// Check to see if the underlying DB needs upgrading.
	upgrade09, upgrade13, err := NeedsUpgrade(s.db.BoltDB())
	if err != nil {
		return err
	}
	if !upgrade09 && !upgrade13 {
		// No upgrade needed!
		return nil
	}

	// Upgraded needed. Backup the boltdb first.
	backupFileName := filepath.Join(s.stateDir, "state.db.backup")
	if err := backupDB(s.db.BoltDB(), backupFileName); err != nil {
		return fmt.Errorf("error backing up state db: %v", err)
	}

	// Perform the upgrade
	if err := s.db.Update(func(tx *boltdd.Tx) error {

		if upgrade09 {
			if err := UpgradeAllocs(s.logger, tx); err != nil {
				return err
			}
		}

		// Add standard metadata
		if err := addMeta(tx.BoltTx()); err != nil {
			return err
		}

		// Write the time the upgrade was done
		bkt, err := tx.CreateBucketIfNotExists(metaBucketName)
		if err != nil {
			return err
		}

		return bkt.Put(metaUpgradedKey, time.Now().Format(time.RFC3339))
	}); err != nil {
		return err
	}

	s.logger.Info("successfully upgraded state")
	return nil
}

// DB allows access to the underlying BoltDB for testing purposes.
func (s *BoltStateDB) DB() *boltdd.DB {
	return s.db
}

// ---- start business

type allocEntry struct {
	Alloc *structs.Allocation
}

func (s *BoltStateDB) PutAllocation(alloc *structs.Allocation, opts ...WriteOption) error {
	return s.updateWithOptions(opts, func(tx *boltdd.Tx) error {
		// Retrieve the root allocations bucket
		allocsBkt, err := tx.CreateBucketIfNotExists(allocationsBucketName)
		if err != nil {
			return err
		}

		// Retrieve the specific allocations bucket
		key := []byte(alloc.ID)
		allocBkt, err := allocsBkt.CreateBucketIfNotExists(key)
		if err != nil {
			return err
		}

		allocState := allocEntry{
			Alloc: alloc,
		}
		return allocBkt.Put(allocKey, &allocState)
	})
}

