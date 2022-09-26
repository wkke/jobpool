package state

import "yunli.com/jobpool/core/structs"

// StateDB implementations store and load Jobpool client state.
type StateDB interface {
	// Name of implementation.
	Name() string

	// Upgrade ensures the layout of the database is at the latest version
	// or returns an error. Corrupt data will be dropped when possible.
	// Errors should be considered critical and unrecoverable.
	Upgrade() error
	// Close the database. Unsafe for further use after calling regardless
	// of return value.
	Close() error

	// PutAllocation stores an allocation or returns an error if it could
	// not be stored.
	PutAllocation(*structs.Allocation, ...WriteOption) error

}

// WriteOptions adjusts the way the data is persisted by the StateDB above. Default is
// zero/false values for all fields. To provide different values, use With* functions
// below, like this: statedb.PutAllocation(alloc, WithBatchMode())
type WriteOptions struct {
	// In Batch mode, concurrent writes (Put* and Delete* operations above) are
	// coalesced into a single transaction, increasing write performance. To benefit
	// from this mode, writes must happen concurrently in goroutines, as every write
	// request still waits for the shared transaction to commit before returning.
	// See https://github.com/boltdb/bolt#batch-read-write-transactions for details.
	// This mode is only supported for BoltDB state backend and is ignored in other backends.
	BatchMode bool
}

// WriteOption is a function that modifies WriteOptions struct above.
type WriteOption func(*WriteOptions)

// mergeWriteOptions creates a final WriteOptions struct to be used by the write methods above
// from a list of WriteOption-s provided as variadic arguments.
func mergeWriteOptions(opts []WriteOption) WriteOptions {
	writeOptions := WriteOptions{} // Default WriteOptions is zero value.
	for _, opt := range opts {
		opt(&writeOptions)
	}
	return writeOptions
}

// WithBatchMode enables Batch mode for write requests (Put* and Delete*
// operations above).
func WithBatchMode() WriteOption {
	return func(s *WriteOptions) {
		s.BatchMode = true
	}
}
