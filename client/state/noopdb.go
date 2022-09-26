package state

import "yunli.com/jobpool/core/structs"

// NoopDB implements a StateDB that does not persist any data.
type NoopDB struct{}

func (n NoopDB) Name() string {
	return "noopdb"
}

func (n NoopDB) Upgrade() error {
	return nil
}

func (n NoopDB) Close() error {
	return nil
}

func (n NoopDB) PutAllocation(*structs.Allocation, ...WriteOption) error {
	return nil
}
