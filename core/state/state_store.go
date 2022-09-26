package state

import (
	"context"
	"fmt"
	"time"
	"yunli.com/jobpool/core/constant"

	log "github.com/hashicorp/go-hclog"
	memdb "github.com/hashicorp/go-memdb"
	"yunli.com/jobpool/core/stream"
	"yunli.com/jobpool/core/structs"
)

// Txn is a transaction against a state store.
// This can be a read or write transaction.
type Txn = *txn

// SortOption represents how results can be sorted.
type SortOption bool

const (
	// SortDefault indicates that the result should be returned using the
	// default go-memdb ResultIterator order.
	SortDefault SortOption = false

	// SortReverse indicates that the result should be returned using the
	// reversed go-memdb ResultIterator order.
	SortReverse SortOption = true
)

const (
	// NodeRegisterEventReregistered is the message used when the node becomes
	// reregistered.
	NodeRegisterEventRegistered = "Node registered"

	// NodeRegisterEventReregistered is the message used when the node becomes
	// reregistered.
	NodeRegisterEventReregistered = "Node re-registered"

)

// terminate appends the go-memdb terminator character to s.
//
// We can then use the result for exact matches during prefix
// scans over compound indexes that start with s.
func terminate(s string) string {
	return s + "\x00"
}

// IndexEntry is used with the "index" table
// for managing the latest Raft index affecting a table.
type IndexEntry struct {
	Key   string
	Value uint64
}

// StateStoreConfig is used to configure a new state store
type StateStoreConfig struct {
	// Logger is used to output the state store's logs
	Logger log.Logger

	// Region is the region of the server embedding the state store.
	Region string

	// EnablePublisher is used to enable or disable the event publisher
	EnablePublisher bool

	// EventBufferSize configures the amount of events to hold in memory
	EventBufferSize int64
}

// The StateStore is responsible for maintaining all the Jobpool
// state. It is manipulated by the FSM which maintains consistency
// through the use of Raft. The goals of the StateStore are to provide
// high concurrency for read operations without blocking writes, and
// to provide write availability in the face of reads. EVERY object
// returned as a result of a read against the state store should be
// considered a constant and NEVER modified in place.
type StateStore struct {
	logger log.Logger
	db     *changeTrackerDB

	// cfg is the passed in configuration
	config *StateStoreConfig

	// abandonCh is used to signal watchers that this state store has been
	// abandoned (usually during a restore). This is only ever closed.
	abandonCh chan struct{}

	// TODO: refactor abandonCh to use a context so that both can use the same
	// cancel mechanism.
	stopEventBroker func()
}

type streamACLDelegate struct {
	s *StateStore
}

// NewStateStore is used to create a new state store
func NewStateStore(config *StateStoreConfig) (*StateStore, error) {
	// Create the MemDB
	db, err := memdb.NewMemDB(stateStoreSchema())
	if err != nil {
		return nil, fmt.Errorf("state store setup failed: %v", err)
	}

	// Create the state store
	ctx, cancel := context.WithCancel(context.TODO())
	s := &StateStore{
		logger:          config.Logger.Named("state_store"),
		config:          config,
		abandonCh:       make(chan struct{}),
		stopEventBroker: cancel,
	}

	if config.EnablePublisher {
		// Create new event publisher using provided cfg
		broker, err := stream.NewEventBroker(ctx, stream.EventBrokerCfg{
			EventBufferSize: config.EventBufferSize,
			Logger:          config.Logger,
		})
		if err != nil {
			return nil, fmt.Errorf("creating state store event broker %w", err)
		}
		s.db = NewChangeTrackerDB(db, broker, eventsFromChanges)
	} else {
		s.db = NewChangeTrackerDB(db, nil, noOpProcessChanges)
	}

	// Initialize the state store with the default namespace.
	if err := s.namespaceInit(); err != nil {
		return nil, fmt.Errorf("enterprise state store initialization failed: %v", err)
	}

	return s, nil
}

// NewWatchSet returns a new memdb.WatchSet that adds the state stores abandonCh
// as a watcher. This is important in that it will notify when this specific
// state store is no longer valid, usually due to a new snapshot being loaded
func (s *StateStore) NewWatchSet() memdb.WatchSet {
	ws := memdb.NewWatchSet()
	ws.Add(s.AbandonCh())
	return ws
}

func (s *StateStore) EventBroker() (*stream.EventBroker, error) {
	if s.db.publisher == nil {
		return nil, fmt.Errorf("EventBroker not configured")
	}
	return s.db.publisher, nil
}

// namespaceInit ensures the default namespace exists.
func (s *StateStore) namespaceInit() error {
	// Create the default namespace. This is safe to do every time we create the
	// state store. There are two main cases, a brand new cluster in which case
	// each server will have the same default namespace object, or a new cluster
	// in which case if the default namespace has been modified, it will be
	// overridden by the restore code path.
	defaultNs := &structs.Namespace{
		Name:        constant.DefaultNamespace,
		Description: constant.DefaultNamespaceDescription,
	}

	if err := s.UpsertNamespaces(1, []*structs.Namespace{defaultNs}); err != nil {
		return fmt.Errorf("inserting default namespace failed: %v", err)
	}

	return nil
}

// Config returns the state store configuration.
func (s *StateStore) Config() *StateStoreConfig {
	return s.config
}

// Snapshot is used to create a point in time snapshot. Because
// we use MemDB, we just need to snapshot the state of the underlying
// database.
func (s *StateStore) Snapshot() (*StateSnapshot, error) {
	memDBSnap := s.db.memdb.Snapshot()

	store := StateStore{
		logger: s.logger,
		config: s.config,
	}

	// Create a new change tracker DB that does not publish or track changes
	store.db = NewChangeTrackerDB(memDBSnap, nil, noOpProcessChanges)

	snap := &StateSnapshot{
		StateStore: store,
	}
	return snap, nil
}

// SnapshotMinIndex is used to create a state snapshot where the index is
// guaranteed to be greater than or equal to the index parameter.
//
// Some server operations (such as scheduling) exchange objects via RPC
// concurrent with Raft log application, so they must ensure the state store
// snapshot they are operating on is at or after the index the objects
// retrieved via RPC were applied to the Raft log at.
//
// Callers should maintain their own timer metric as the time this method
// blocks indicates Raft log application latency relative to scheduling.
func (s *StateStore) SnapshotMinIndex(ctx context.Context, index uint64) (*StateSnapshot, error) {
	// Ported from work.go:waitForIndex prior to 0.9

	const backoffBase = 20 * time.Millisecond
	const backoffLimit = 1 * time.Second
	var retries uint
	var retryTimer *time.Timer

	// XXX: Potential optimization is to set up a watch on the state
	// store's index table and only unblock via a trigger rather than
	// polling.
	for {
		// Get the states current index
		snapshotIndex, err := s.LatestIndex()
		if err != nil {
			return nil, fmt.Errorf("failed to determine state store's index: %v", err)
		}

		// We only need the FSM state to be as recent as the given index
		if snapshotIndex >= index {
			return s.Snapshot()
		}

		// Exponential back off
		retries++
		if retryTimer == nil {
			// First retry, start at baseline
			retryTimer = time.NewTimer(backoffBase)
		} else {
			// Subsequent retry, reset timer
			deadline := 1 << (2 * retries) * backoffBase
			if deadline > backoffLimit {
				deadline = backoffLimit
			}
			retryTimer.Reset(deadline)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-retryTimer.C:
		}
	}
}

// Restore is used to optimize the efficiency of rebuilding
// state by minimizing the number of transactions and checking
// overhead.
func (s *StateStore) Restore() (*StateRestore, error) {
	txn := s.db.WriteTxnRestore()
	r := &StateRestore{
		txn: txn,
	}
	return r, nil
}

// AbandonCh returns a channel you can wait on to know if the state store was
// abandoned.
func (s *StateStore) AbandonCh() <-chan struct{} {
	return s.abandonCh
}

// Abandon is used to signal that the given state store has been abandoned.
// Calling this more than one time will panic.
func (s *StateStore) Abandon() {
	s.StopEventBroker()
	close(s.abandonCh)
}

// StopEventBroker calls the cancel func for the state stores event
// publisher. It should be called during server shutdown.
func (s *StateStore) StopEventBroker() {
	s.stopEventBroker()
}

// QueryFn is the definition of a function that can be used to implement a basic
// blocking query against the state store.
type QueryFn func(memdb.WatchSet, *StateStore) (resp interface{}, index uint64, err error)

// BlockingQuery takes a query function and runs the function until the minimum
// query index is met or until the passed context is cancelled.
func (s *StateStore) BlockingQuery(query QueryFn, minIndex uint64, ctx context.Context) (
	resp interface{}, index uint64, err error) {

RUN_QUERY:
	// We capture the state store and its abandon channel but pass a snapshot to
	// the blocking query function. We operate on the snapshot to allow separate
	// calls to the state store not all wrapped within the same transaction.
	abandonCh := s.AbandonCh()
	snap, _ := s.Snapshot()
	stateSnap := &snap.StateStore

	// We can skip all watch tracking if this isn't a blocking query.
	var ws memdb.WatchSet
	if minIndex > 0 {
		ws = memdb.NewWatchSet()

		// This channel will be closed if a snapshot is restored and the
		// whole state store is abandoned.
		ws.Add(abandonCh)
	}

	resp, index, err = query(ws, stateSnap)
	if err != nil {
		return nil, index, err
	}

	// We haven't reached the min-index yet.
	if minIndex > 0 && index <= minIndex {
		if err := ws.WatchCtx(ctx); err != nil {
			return nil, index, err
		}

		goto RUN_QUERY
	}

	return resp, index, nil
}

func indexEntry(table string, index uint64) *IndexEntry {
	return &IndexEntry{
		Key:   table,
		Value: index,
	}
}

// LatestIndex returns the greatest index value for all indexes.
func (s *StateStore) LatestIndex() (uint64, error) {
	indexes, err := s.Indexes()
	if err != nil {
		return 0, err
	}

	var max uint64 = 0
	for {
		raw := indexes.Next()
		if raw == nil {
			break
		}

		// Prepare the request struct
		idx := raw.(*IndexEntry)

		// Determine the max
		if idx.Value > max {
			max = idx.Value
		}
	}

	return max, nil
}

// Index finds the matching index value
func (s *StateStore) Index(name string) (uint64, error) {
	txn := s.db.ReadTxn()

	// Lookup the first matching index
	out, err := txn.First("index", "id", name)
	if err != nil {
		return 0, err
	}
	if out == nil {
		return 0, nil
	}
	return out.(*IndexEntry).Value, nil
}

// Indexes returns an iterator over all the indexes
func (s *StateStore) Indexes() (memdb.ResultIterator, error) {
	txn := s.db.ReadTxn()

	// Walk the entire nodes table
	iter, err := txn.Get("index", "id")
	if err != nil {
		return nil, err
	}
	return iter, nil
}

// WithWriteTransaction executes the passed function within a write transaction,
// and returns its result.  If the invocation returns no error, the transaction
// is committed; otherwise, it's aborted.
func (s *StateStore) WithWriteTransaction(msgType constant.MessageType, index uint64, fn func(Txn) error) error {
	tx := s.db.WriteTxnMsgT(msgType, index)
	defer tx.Abort()

	err := fn(tx)
	if err == nil {
		return tx.Commit()
	}
	return err
}


// StateSnapshot is used to provide a point-in-time snapshot
type StateSnapshot struct {
	StateStore
}
func getPreemptedAllocDesiredDescription(preemptedByAllocID string) string {
	return fmt.Sprintf("Preempted by alloc ID %v", preemptedByAllocID)
}
