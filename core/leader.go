package core

import (
	"fmt"
	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-version"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"golang.org/x/time/rate"
	"math/rand"
	"net"
	"sync"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/structs"
	xtime "yunli.com/jobpool/helper/time"
	"yunli.com/jobpool/helper/uuid"
)

const (
	// failedEvalUnblockInterval is the interval at which failed evaluations are
	// unblocked to re-enter the scheduler. A failed evaluation occurs under
	// high contention when the schedulers plan does not make progress.
	failedEvalUnblockInterval = 1 * time.Minute

	// replicationRateLimit is used to rate limit how often data is replicated
	// between the authoritative region and the local region
	replicationRateLimit rate.Limit = 10.0

	// barrierWriteTimeout is used to give Raft a chance to process a
	// possible loss of leadership event if we are unable to get a barrier
	// while leader.
	barrierWriteTimeout = 2 * time.Minute
)

var minClusterIDVersion = version.Must(version.NewVersion("0.10.4"))

// monitorLeadership is used to monitor if we acquire or lose our role
// as the leader in the Raft cluster. There is some work the leader is
// expected to do, so we must react to changes
func (s *Server) monitorLeadership() {
	var weAreLeaderCh chan struct{}
	var leaderLoop sync.WaitGroup

	leaderCh := s.raft.LeaderCh()

	leaderStep := func(isLeader bool) {
		if isLeader {
			if weAreLeaderCh != nil {
				s.logger.Error("attempted to start the leader loop while running")
				return
			}

			weAreLeaderCh = make(chan struct{})
			leaderLoop.Add(1)
			go func(ch chan struct{}) {
				defer leaderLoop.Done()
				s.leaderLoop(ch)
			}(weAreLeaderCh)
			s.logger.Info("cluster leadership acquired")
			return
		}

		if weAreLeaderCh == nil {
			s.logger.Error("attempted to stop the leader loop while not running")
			return
		}

		s.logger.Debug("shutting down leader loop")
		close(weAreLeaderCh)
		leaderLoop.Wait()
		weAreLeaderCh = nil
		s.logger.Info("cluster leadership lost")
	}

	wasLeader := false
	for {
		select {
		case isLeader := <-leaderCh:
			if wasLeader != isLeader {
				wasLeader = isLeader
				// normal case where we went through a transition
				leaderStep(isLeader)
			} else if wasLeader && isLeader {
				// Server lost but then gained leadership immediately.
				// During this time, this server may have received
				// Raft transitions that haven't been applied to the FSM
				// yet.
				// Ensure that that FSM caught up and eval queues are refreshed
				s.logger.Warn("cluster leadership lost and gained leadership immediately.  Could indicate network issues, memory paging, or high CPU load.")

				leaderStep(false)
				leaderStep(true)
			} else {
				// Server gained but lost leadership immediately
				// before it reacted; nothing to do, move on
				s.logger.Warn("cluster leadership gained and lost leadership immediately.  Could indicate network issues, memory paging, or high CPU load.")
			}
		case <-s.shutdownCh:
			if weAreLeaderCh != nil {
				leaderStep(false)
			}
			return
		}
	}
}

// leaderLoop runs as long as we are the leader to run various
// maintenance activities
func (s *Server) leaderLoop(stopCh chan struct{}) {
	var reconcileCh chan serf.Member
	establishedLeader := false

RECONCILE:
	// Setup a reconciliation timer
	reconcileCh = nil
	interval := time.After(s.config.ReconcileInterval)

	// Apply a raft barrier to ensure our FSM is caught up
	start := time.Now()
	barrier := s.raft.Barrier(barrierWriteTimeout)
	if err := barrier.Error(); err != nil {
		s.logger.Error("failed to wait for barrier", "error", err)
		goto WAIT
	}
	metrics.MeasureSince([]string{constant.JobPoolName, "leader", "barrier"}, start)

	// Check if we need to handle initial leadership actions
	if !establishedLeader {
		if err := s.establishLeadership(stopCh); err != nil {
			s.logger.Error("failed to establish leadership", "error", err)

			// Immediately revoke leadership since we didn't successfully
			// establish leadership.
			if err := s.revokeLeadership(); err != nil {
				s.logger.Error("failed to revoke leadership", "error", err)
			}

			// Attempt to transfer leadership. If successful, leave the
			// leaderLoop since this node is no longer the leader. Otherwise
			// try to establish leadership again after 5 seconds.
			if err := s.leadershipTransfer(); err != nil {
				s.logger.Error("failed to transfer leadership", "error", err)
				interval = time.After(5 * time.Second)
				goto WAIT
			}
			return
		}

		establishedLeader = true
		defer func() {
			if err := s.revokeLeadership(); err != nil {
				s.logger.Error("failed to revoke leadership", "error", err)
			}
		}()
	}

	// Reconcile any missing data
	if err := s.reconcile(); err != nil {
		s.logger.Error("failed to reconcile", "error", err)
		goto WAIT
	}

	// Initial reconcile worked, now we can process the channel
	// updates
	reconcileCh = s.reconcileCh

	// Poll the stop channel to give it priority so we don't waste time
	// trying to perform the other operations if we have been asked to shut
	// down.
	select {
	case <-stopCh:
		return
	default:
	}

WAIT:
	// Wait until leadership is lost or periodically reconcile as long as we
	// are the leader, or when Serf events arrive.
	for {
		select {
		case <-stopCh:
			// Lost leadership.
			return
		case <-s.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case member := <-reconcileCh:
			s.reconcileMember(member)
		case errCh := <-s.reassertLeaderCh:
			// Recompute leader state, by asserting leadership and
			// repopulating leader states.

			// Check first if we are indeed the leaders first. We
			// can get into this state when the initial
			// establishLeadership has failed.
			// Afterwards we will be waiting for the interval to
			// trigger a reconciliation and can potentially end up
			// here. There is no point to reassert because this
			// agent was never leader in the first place.
			if !establishedLeader {
				errCh <- fmt.Errorf("leadership has not been established")
				continue
			}

			// refresh leadership state
			s.revokeLeadership()
			err := s.establishLeadership(stopCh)
			errCh <- err

			// In case establishLeadership fails, try to transfer leadership.
			// At this point Raft thinks we are the leader, but Jobpool did not
			// complete the required steps to act as the leader.
			if err != nil {
				if err := s.leadershipTransfer(); err != nil {
					// establishedLeader was true before, but it no longer is
					// since we revoked leadership and leadershipTransfer also
					// failed.
					// Stay in the leaderLoop with establishedLeader set to
					// false so we try to establish leadership again in the
					// next loop.
					establishedLeader = false
					interval = time.After(5 * time.Second)
					goto WAIT
				}

				// leadershipTransfer was successful and it is
				// time to leave the leaderLoop.
				return
			}
		}
	}
}

// reconcile is used to reconcile the differences between Serf
// membership and what is reflected in our strongly consistent store.
func (s *Server) reconcile() error {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "leader", "reconcile"}, time.Now())
	members := s.serf.Members()
	for _, member := range members {
		if err := s.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}

// reconcileMember is used to do an async reconcile of a single serf member
func (s *Server) reconcileMember(member serf.Member) error {
	// Check if this is a member we should handle
	valid, parts := isAppServer(member)
	if !valid || parts.Region != s.config.Region {
		return nil
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "leader", "reconcileMember"}, time.Now())

	var err error
	switch member.Status {
	case serf.StatusAlive:
		err = s.addRaftPeer(member, parts)
	case serf.StatusLeft, StatusReap:
		err = s.removeRaftPeer(member, parts)
	}
	if err != nil {
		s.logger.Error("failed to reconcile member", "member", member, "error", err)
		return err
	}
	return nil
}

// addRaftPeer is used to add a new Raft peer when a Jobpool server joins
func (s *Server) addRaftPeer(m serf.Member, parts *serverParts) error {
	// Check for possibility of multiple bootstrap nodes
	members := s.serf.Members()
	if parts.Bootstrap {
		for _, member := range members {
			valid, p := isAppServer(member)
			if valid && member.Name != m.Name && p.Bootstrap {
				s.logger.Error("skipping adding Raft peer because an existing peer is in bootstrap mode and only one server should be in bootstrap mode",
					"existing_peer", member.Name, "joining_peer", m.Name)
				return nil
			}
		}
	}

	// Processing ourselves could result in trying to remove ourselves to
	// fix up our address, which would make us step down. This is only
	// safe to attempt if there are multiple servers available.
	addr := (&net.TCPAddr{IP: m.Addr, Port: parts.Port}).String()
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Error("failed to get raft configuration", "error", err)
		return err
	}

	if m.Name == s.config.NodeName {
		if l := len(configFuture.Configuration().Servers); l < 3 {
			s.logger.Debug("skipping self join check for peer since the cluster is too small", "peer", m.Name)
			return nil
		}
	}

	for _, server := range configFuture.Configuration().Servers {
		// No-op if the raft version is too low
		if server.Address == raft.ServerAddress(addr) && (parts.RaftVersion < 3) {
			return nil
		}
		// If the address or ID matches an existing server, see if we need to remove the old one first
		if server.Address == raft.ServerAddress(addr) || server.ID == raft.ServerID(parts.ID) {
			// Exit with no-op if this is being called on an existing server and both the ID and address match
			if server.Address == raft.ServerAddress(addr) && server.ID == raft.ServerID(parts.ID) {
				return nil
			}
			future := s.raft.RemoveServer(server.ID, 0, 0)
			if server.Address == raft.ServerAddress(addr) {
				if err := future.Error(); err != nil {
					return fmt.Errorf("error removing server with duplicate address %q: %s", server.Address, err)
				}
				s.logger.Info("removed server with duplicate address", "address", server.Address)
			} else {
				if err := future.Error(); err != nil {
					// validated
					s.logger.Warn("error removing server with duplicate ID， try to use update method later. %q: %s", server.ID, err)
					addFuture := s.raft.AddVoter(raft.ServerID(parts.ID), raft.ServerAddress(addr), 0, 0)
					if err := addFuture.Error(); err != nil {
						s.logger.Error("failed to add raft peer", "error", err)
						return err
					}
					s.logger.Info("finish add voter to change the address for server ")
					return nil
				}
				s.logger.Info("removed server with duplicate ID", "id", server.ID)
			}
		}
	}

	addFuture := s.raft.AddNonvoter(raft.ServerID(parts.ID), raft.ServerAddress(addr), 0, 0)
	if err := addFuture.Error(); err != nil {
		s.logger.Error("failed to add raft peer", "error", err)
		return err
	}

	return nil
}

// removeRaftPeer is used to remove a Raft peer when a Jobpool server leaves
// or is reaped
func (s *Server) removeRaftPeer(m serf.Member, parts *serverParts) error {
	addr := (&net.TCPAddr{IP: m.Addr, Port: parts.Port}).String()

	// See if it's already in the configuration. It's harmless to re-remove it
	// but we want to avoid doing that if possible to prevent useless Raft
	// log entries.
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Error("failed to get raft configuration", "error", err)
		return err
	}
	// Pick which remove API to use based on how the server was added.
	for _, server := range configFuture.Configuration().Servers {
		// Check if this is the server to remove based on how it was registered.
		// Raft v2 servers are registered by address.
		// Raft v3 servers are registered by ID.
		if server.ID == raft.ServerID(parts.ID) || server.Address == raft.ServerAddress(addr) {
			// Use the new add/remove APIs if we understand them.

			// If not, use the old remove API
			s.logger.Info("removing server by address", "address", server.Address)
			future := s.raft.RemovePeer(raft.ServerAddress(addr))
			if err := future.Error(); err != nil {
				s.logger.Error("failed to remove raft peer", "address", addr, "error", err)
				return err
			}
			break
		}
	}

	return nil
}

func (s *Server) leadershipTransfer() error {
	retryCount := 3
	for i := 0; i < retryCount; i++ {
		err := s.raft.LeadershipTransfer().Error()
		if err == nil {
			s.logger.Info("successfully transferred leadership")
			return nil
		}

		// Don't retry if the Raft version doesn't support leadership transfer
		// since this will never succeed.
		if err == raft.ErrUnsupportedProtocol {
			return fmt.Errorf("leadership transfer not supported with Raft version lower than 3")
		}

		s.logger.Error("failed to transfer leadership attempt, will retry",
			"attempt", i,
			"retry_limit", retryCount,
			"error", err,
		)
	}
	return fmt.Errorf("failed to transfer leadership in %d attempts", retryCount)
}

// revokeLeadership is invoked once we step down as leader.
// This is used to cleanup any state that may be specific to a leader.
func (s *Server) revokeLeadership() error {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "leader", "revoke_leadership"}, time.Now())

	s.resetConsistentReadReady()

	// Disable the node drainer
	s.nodeDrainer.SetEnabled(false, nil)

	// Clear the heartbeat timers on either shutdown or step down,
	// since we are no longer responsible for TTL expirations.
	if err := s.clearAllHeartbeatTimers(); err != nil {
		s.logger.Error("clearing heartbeat timers failed", "error", err)
		return err
	}

	return nil
}

// establishLeadership is invoked once we become leader and are able
// to invoke an initial barrier. The barrier is used to ensure any
// previously inflight transactions have been committed and that our
// state is up-to-date.
func (s *Server) establishLeadership(stopCh chan struct{}) error {
	defer metrics.MeasureSince([]string{constant.JobPoolName, "leader", "establish_leadership"}, time.Now())

	// Initialize the ClusterID
	_, _ = s.ClusterID()
	// todo: use cluster ID for stuff, later!
	// Enable the NodeDrainer
	s.nodeDrainer.SetEnabled(true, s.State())

	// Activate the vault client
	// s.vault.SetActive(true)

	// Enable the periodic dispatcher, since we are now the leader.
	// s.periodicDispatcher.SetEnabled(true)

	// Activate RPC now that local FSM caught up with Raft (as evident by Barrier call success)
	// and all leader related components (e.g. broker queue) are enabled.
	// Auxiliary processes (e.g. background, bookkeeping, and cleanup tasks can start after)
	s.setConsistentReadReady()

	// Further clean ups and follow up that don't block RPC consistency

	// Setup the heartbeat timers. This is done both when starting up or when
	// a leader fail over happens. Since the timers are maintained by the leader
	// node, effectively this means all the timers are renewed at the time of failover.
	// The TTL contract is that the session will not be expired before the TTL,
	// so expiring it later is allowable.
	//
	// This MUST be done after the initial barrier to ensure the latest Nodes
	// are available to be initialized. Otherwise initialization may use stale
	// data.
	if err := s.initializeHeartbeatTimers(); err != nil {
		s.logger.Error("heartbeat timer setup failed", "error", err)
		return err
	}

	// Cleanup orphaned Vault token accessors
	//if err := s.revokeVaultAccessorsOnRestore(); err != nil {
	//	return err
	//}
	// leader has queue for plan apply
	s.planQueue.SetEnabled(true)

	go s.planApply()

	// Enable the eval broker, since we are now the leader
	s.evalBroker.SetEnabled(true)
	// block eval enabled
	s.blockedEvals.SetEnabled(true)
	s.blockedEvals.SetTimetable(s.fsm.TimeTable())
	// enable the task road map
	s.jobRoadMap.SetEnabled(true)

	// schedule will be enabled
	s.periodicDispatcher.SetEnabled(true)

	// Restore the eval broker state
	if err := s.restoreEvals(); err != nil {
		return err
	}

	// Restore the periodic dispatcher state
	if err := s.restorePeriodicDispatcher(); err != nil {
		return err
	}

	go s.schedulePeriodic(stopCh)

	// Reap any failed evaluations
	go s.reapFailedEvaluations(stopCh)

	// Reap any duplicate blocked evaluations
	go s.reapDupBlockedEvaluations(stopCh)

	// Periodically unblock failed allocations
	go s.periodicUnblockFailedEvals(stopCh)

	// 同步任务
	//go s.syncJobs(stopCh)
	//go s.syncTasks(stopCh)

	return nil
}

func (s *Server) generateClusterID() (string, error) {
	if !ServersMeetMinimumVersion(s.Members(), minClusterIDVersion, false) {
		s.logger.Named("core").Warn("cannot initialize cluster ID until all servers are above minimum version", "min_version", minClusterIDVersion)
		return "", fmt.Errorf("cluster ID cannot be created until all servers are above minimum version %s", minClusterIDVersion)
	}

	newMeta := structs.ClusterMetadata{ClusterID: uuid.Generate(), CreateTime: time.Now().UnixNano()}
	if _, _, err := s.raftApply(constant.ClusterMetadataRequestType, newMeta); err != nil {
		s.logger.Named("core").Error("failed to create cluster ID", "error", err)
		return "", fmt.Errorf("failed to create cluster ID: %w", err)
	}

	s.logger.Named("core").Info("established cluster id", "cluster_id", newMeta.ClusterID, "create_time", newMeta.CreateTime)
	return newMeta.ClusterID, nil
}

// reapFailedEvaluations is used to reap evaluations that
// have reached their delivery limit and should be failed
func (s *Server) reapFailedEvaluations(stopCh chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
			// Scan for a failed evaluation
			eval, token, err := s.evalBroker.Dequeue([]string{constant.QueueNameFailed}, time.Second)
			if err != nil {
				return
			}
			if eval == nil {
				continue
			}

			// Update the status to failed
			updateEval := eval.Copy()
			updateEval.Status = constant.EvalStatusFailed
			updateEval.StatusDescription = fmt.Sprintf("evaluation reached delivery limit (%d)", s.config.EvalDeliveryLimit)
			s.logger.Warn("eval reached delivery limit, marking as failed",
				"eval", hclog.Fmt("%#v", updateEval))

			// Core job evals that fail or span leader elections will never
			// succeed because the follow-up doesn't have the leader ACL. We
			// rely on the leader to schedule new core plans periodically
			// instead.
			if eval.Type != constant.PlanTypeCore {

				updateEval.UpdateTime = xtime.NewFormatTime(time.Now())
				shouldCreate := s.shouldCreateFllowupEval(eval)
				var req dto.EvalUpdateRequest
				if shouldCreate {
					// Create a follow-up evaluation that will be used to retry the
					// scheduling for the job after the cluster is hopefully more stable
					// due to the fairly large backoff.
					followupEvalWait := s.config.EvalFailedFollowupBaselineDelay +
						time.Duration(rand.Int63n(int64(s.config.EvalFailedFollowupDelayRange)))

					followupEval := eval.CreateFailedFollowUpEval(followupEvalWait)
					updateEval.NextEval = followupEval.ID
					// Update via Raft
					req = dto.EvalUpdateRequest{
						Evals: []*structs.Evaluation{updateEval, followupEval},
					}
				}else {
					// Update via Raft
					req = dto.EvalUpdateRequest{
						Evals: []*structs.Evaluation{updateEval},
					}
				}
				if _, _, err := s.raftApply(constant.EvalUpdateRequestType, &req); err != nil {
					s.logger.Error("failed to update failed eval and create a follow-up",
						"eval", hclog.Fmt("%#v", updateEval), "error", err)
					continue
				}
			}
			// Ack completion
			s.evalBroker.Ack(eval.ID, token)
		}
	}
}


func (s *Server) shouldCreateFllowupEval(eval *structs.Evaluation) bool {
	// 尝试N次之后如果还是不行，放弃
	ws := memdb.NewWatchSet()
	evals, err := s.fsm.state.EvalsFailedByPlanAndJob(ws, eval.Namespace, eval.PlanID, eval.JobID)
	if err != nil {
		s.logger.Error("failed to find exist eval and then create a follow-up",
			"eval", hclog.Fmt("%#v", eval), "error", err)
	}
	if evals != nil {
		if len(evals) > 10 {
			s.logger.Warn("该job已分配失败重试10次，达到最大重试次数，放弃该job的运行", "jobId", eval.JobID, "planId", eval.PlanID)
			// Ack completion
			return  false
		} else {
			s.logger.Debug("job分配失败，创建重试任务", "jobId", eval.JobID, "times", len(evals))
		}
	}
	return true
}


// reapDupBlockedEvaluations is used to reap duplicate blocked evaluations and
// should be cancelled.
func (s *Server) reapDupBlockedEvaluations(stopCh chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
			// Scan for duplicate blocked evals.
			dups := s.blockedEvals.GetDuplicates(time.Second)
			if dups == nil {
				continue
			}

			cancel := make([]*structs.Evaluation, len(dups))
			for i, dup := range dups {
				// Update the status to cancelled
				newEval := dup.Copy()
				newEval.Status = constant.EvalStatusCancelled
				newEval.StatusDescription = fmt.Sprintf("existing blocked evaluation exists for job %q", newEval.JobID)
				newEval.UpdateTime = xtime.NewFormatTime(time.Now())
				cancel[i] = newEval
			}

			// Update via Raft
			req := dto.EvalUpdateRequest{
				Evals: cancel,
			}
			if _, _, err := s.raftApply(constant.EvalUpdateRequestType, &req); err != nil {
				s.logger.Error("failed to update duplicate evals", "evals", hclog.Fmt("%#v", cancel), "error", err)
				continue
			}
		}
	}
}

// periodicUnblockFailedEvals periodically unblocks failed, blocked evaluations.
func (s *Server) periodicUnblockFailedEvals(stopCh chan struct{}) {
	ticker := time.NewTicker(failedEvalUnblockInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			// Unblock the failed allocations
			s.blockedEvals.UnblockFailed()
		}
	}
}

// 上次未运行完成的分配重新入队列
func (s *Server) restoreEvals() error {
	ws := memdb.NewWatchSet()
	iter, err := s.fsm.State().Evals(ws, false)
	if err != nil {
		return fmt.Errorf("failed to get evaluations: %v", err)
	}

	for {
		raw := iter.Next()
		if raw == nil {
			break
		}
		eval := raw.(*structs.Evaluation)

		if eval.ShouldEnqueue() {
			s.evalBroker.Enqueue(eval)
		} else if eval.ShouldBlock() {
			s.blockedEvals.Block(eval)
		}
	}
	return nil
}
