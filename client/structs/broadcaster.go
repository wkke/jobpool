package structs

import (
	"github.com/hashicorp/go-hclog"
	"sync"
	"yunli.com/jobpool/core/structs"
)

type AllocBroadcaster struct {
	mu sync.Mutex

	// listeners is a map of unique ids to listener chans. lazily
	// initialized on first listen
	listeners map[int]chan *structs.Allocation

	// nextId is the next id to assign in listener map
	nextId int

	// closed is true if broadcsater is closed
	closed bool

	// last alloc sent to prime new listeners
	last *structs.Allocation

	logger hclog.Logger
}


// NewAllocBroadcaster returns a new AllocBroadcaster.
func NewAllocBroadcaster(l hclog.Logger) *AllocBroadcaster {
	return &AllocBroadcaster{
		logger: l,
	}
}



// stop an individual listener
func (b *AllocBroadcaster) stop(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// If broadcaster has been closed there's nothing more to do.
	if b.closed {
		return
	}

	l, ok := b.listeners[id]
	if !ok {
		// If this listener has been stopped already there's nothing
		// more to do.
		return
	}

	close(l)
	delete(b.listeners, id)
}

// AllocListener implements a listening endpoint for an allocation broadcast
// channel.
type AllocListener struct {
	// ch receives the broadcast messages.
	ch <-chan *structs.Allocation
	b  *AllocBroadcaster
	id int
}

func (l *AllocListener) Ch() <-chan *structs.Allocation {
	return l.ch
}

// Close closes the Listener, disabling the receival of further messages. Safe
// to call more than once and concurrently with receiving on Ch.
func (l *AllocListener) Close() {
	l.b.stop(l.id)
}


// Listen returns a Listener for the broadcast channel. New listeners receive
// the last sent alloc update.
func (b *AllocBroadcaster) Listen() *AllocListener {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listeners == nil {
		b.listeners = make(map[int]chan *structs.Allocation)
	}

	for b.listeners[b.nextId] != nil {
		b.nextId++
	}

	ch := make(chan *structs.Allocation, 1)

	// Send last update if there was one
	if b.last != nil {
		ch <- b.last
	}

	// Broadcaster is already closed, close this listener. Must be done
	// after the last update was sent.
	if b.closed {
		close(ch)
	}

	b.listeners[b.nextId] = ch

	return &AllocListener{ch, b, b.nextId}
}

func (b *AllocBroadcaster) Send(v *structs.Allocation) error {
	b.logger.Trace("---- send allocation broadcast---", "alloc", v)
	return nil
}