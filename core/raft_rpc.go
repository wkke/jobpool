package core

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"yunli.com/jobpool/helper/pool"
)

// RaftLayer implements the raft.StreamLayer interface,
// so that we can use a single RPC layer for Raft and Jobpool
type RaftLayer struct {
	// Addr is the listener address to return
	addr net.Addr

	// connCh is used to accept connections
	connCh chan net.Conn

	// Tracks if we are closed
	closed    bool
	closeCh   chan struct{}
	closeLock sync.Mutex
}

// NewRaftLayer is used to initialize a new RaftLayer which can
// be used as a StreamLayer for Raft. If a tlsConfig is provided,
// then the connection will use TLS.
func NewRaftLayer(addr net.Addr) *RaftLayer {
	layer := &RaftLayer{
		addr:    addr,
		connCh:  make(chan net.Conn),
		closeCh: make(chan struct{}),
	}
	return layer
}

// Handoff is used to hand off a connection to the
// RaftLayer. This allows it to be Accept()'ed
func (l *RaftLayer) Handoff(ctx context.Context, c net.Conn) error {
	select {
	case l.connCh <- c:
		return nil
	case <-l.closeCh:
		return fmt.Errorf("Raft RPC layer closed")
	case <-ctx.Done():
		return nil
	}
}

// Accept is used to return connection which are
// dialed to be used with the Raft layer
func (l *RaftLayer) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connCh:
		return conn, nil
	case <-l.closeCh:
		return nil, fmt.Errorf("Raft RPC layer closed")
	}
}

// Close is used to stop listening for Raft connections
func (l *RaftLayer) Close() error {
	l.closeLock.Lock()
	defer l.closeLock.Unlock()

	if !l.closed {
		l.closed = true
		close(l.closeCh)
	}
	return nil
}

// Addr is used to return the address of the listener
func (l *RaftLayer) Addr() net.Addr {
	return l.addr
}

// Dial is used to create a new outgoing connection
func (l *RaftLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		return nil, err
	}

	// Write the Raft byte to set the mode
	_, err = conn.Write([]byte{byte(pool.RpcRaft)})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, err
}
