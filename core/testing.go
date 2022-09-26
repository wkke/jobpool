package core

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"net"
	"sync/atomic"
	"testing"
	"time"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper/ports"
	"yunli.com/jobpool/helper/testlog"
	"yunli.com/jobpool/version"
)

var (
	nodeNumber int32 = 0
)
func TestServer(t *testing.T, cb func(*Config)) (*Server, func()) {
	s, c, err := TestServerErr(t, cb)
	require.NoError(t, err, "failed to start test server")
	return s, c
}

func TestServerErr(t *testing.T, cb func(*Config)) (*Server, func(), error) {
	// Setup the default settings
	config := DefaultConfig()


	config.Build = version.Version + "+unittest"
	config.DevMode = true
	config.EnableEventBroker = true
	config.BootstrapExpect = 1
	nodeNum := atomic.AddInt32(&nodeNumber, 1)
	config.NodeName = fmt.Sprintf("jobpool-%03d", nodeNum)

	// configure logger
	config.Logger, config.LogOutput = testlog.HCLoggerNode(t, nodeNum)

	// Tighten the Serf timing
	config.SerfConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	config.SerfConfig.MemberlistConfig.SuspicionMult = 2
	config.SerfConfig.MemberlistConfig.RetransmitMult = 2
	config.SerfConfig.MemberlistConfig.ProbeTimeout = 50 * time.Millisecond
	config.SerfConfig.MemberlistConfig.ProbeInterval = 100 * time.Millisecond
	config.SerfConfig.MemberlistConfig.GossipInterval = 100 * time.Millisecond

	// Tighten the Raft timing
	config.RaftConfig.LeaderLeaseTimeout = 50 * time.Millisecond
	config.RaftConfig.HeartbeatTimeout = 50 * time.Millisecond
	config.RaftConfig.ElectionTimeout = 50 * time.Millisecond
	config.RaftTimeout = 500 * time.Millisecond


	// Tighten the autopilot timing
	config.ServerHealthInterval = 50 * time.Millisecond


	// Enable fuzzy search API
	config.SearchConfig = &structs.SearchConfig{
		FuzzyEnabled:  true,
		LimitQuery:    20,
		LimitResults:  100,
		MinTermLength: 2,
	}

	// Invoke the callback if any
	if cb != nil {
		cb(config)
	}


	for i := 10; i >= 0; i-- {
		// Get random ports, need to cleanup later
		ports, err := ports.GetFreePorts(2)
		if err != nil {
			t.Fatal("get free ports in server", "err", err)
		}

		config.RPCAddr = &net.TCPAddr{
			IP:   []byte{127, 0, 0, 1},
			Port: ports[0],
		}
		config.SerfConfig.MemberlistConfig.BindPort = ports[1]

		// Create server
		server, err := NewServer(config)
		if err == nil {
			return server, func() {
				ch := make(chan error)
				go func() {
					defer close(ch)

					// Shutdown server
					err := server.Shutdown()
					if err != nil {
						ch <- fmt.Errorf("failed to shutdown server: %w", err)
					}
				}()

				select {
				case e := <-ch:
					if e != nil {
						t.Fatal(e.Error())
					}
				case <-time.After(1 * time.Minute):
					t.Fatal("timed out while shutting down server")
				}
			}, nil
		} else if i == 0 {
			return nil, nil, err
		} else {
			if server != nil {
				_ = server.Shutdown()
			}
			wait := time.Duration(rand.Int31n(2000)) * time.Millisecond
			time.Sleep(wait)
		}
	}

	return nil, nil, nil
}


func TestJoin(t *testing.T, servers ...*Server) {
	for i := 0; i < len(servers)-1; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d",
			servers[i].config.SerfConfig.MemberlistConfig.BindPort)

		for j := i + 1; j < len(servers); j++ {
			num, err := servers[j].Join([]string{addr})
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if num != 1 {
				t.Fatalf("bad: %d", num)
			}
		}
	}
}


type rpcFn func(string, interface{}, interface{}) error

// WaitForLeader blocks until a leader is elected.
func WaitForLeader(t testing.TB, rpc rpcFn) {
	t.Helper()
	WaitForResult(func() (bool, error) {
		args := &dto.GenericRequest{}
		var leader string
		err := rpc("Status.Leader", args, &leader)
		return leader != "", err
	}, func(err error) {
		t.Fatalf("failed to find leader: %v", err)
	})
}


type testFn func() (bool, error)
type errorFn func(error)

func WaitForResult(test testFn, error errorFn) {
	WaitForResultRetries(500*TestMultiplier(), test, error)
}


// TestMultiplier returns a multiplier for retries and waits given environment
// the tests are being run under.
func TestMultiplier() int64 {
	return 1
}

func WaitForResultRetries(retries int64, test testFn, error errorFn) {
	for retries > 0 {
		time.Sleep(10 * time.Millisecond)
		retries--

		success, err := test()
		if success {
			return
		}

		if retries == 0 {
			error(err)
		}
	}
}