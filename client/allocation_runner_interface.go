package client

import (
	cstructs "yunli.com/jobpool/client/structs"
	"yunli.com/jobpool/core/structs"
	arstate "yunli.com/jobpool/client/service/runner/state"
)

type AllocRunner interface {
	Listener() *cstructs.AllocListener
	Alloc() *structs.Allocation
	AllocState() *arstate.State
	Shutdown()
	Run()
	WaitCh() <-chan struct{}
	DestroyCh() <-chan struct{}
	ShutdownCh() <-chan struct{}
	Reconnect(update *structs.Allocation) error
	Update(*structs.Allocation)
}