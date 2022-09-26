package core

import (
	"fmt"
	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"net/http"
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"
	"yunli.com/jobpool/core/state"
	"yunli.com/jobpool/core/state/paginator"
	"yunli.com/jobpool/core/structs"
)

type Job struct {
	srv    *Server
	logger hclog.Logger
}

func NewJobEndpoints(s *Server) *Job {
	return &Job{
		srv:    s,
		logger: s.logger.Named("job"),
	}
}

// 查询job
func (j *Job) List(args *dto.JobListRequest, reply *dto.JobListResponse) error {
	if done, err := j.srv.forward("Job.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "job", "list"}, time.Now())
	// Setup the blocking query
	sort := state.SortOption(args.Reverse)
	opts := blockingOptions{
		queryOpts: &args.QueryOptions,
		queryMeta: &reply.QueryMeta,
		run: func(ws memdb.WatchSet, state *state.StateStore) error {
			// Capture all the plans
			var err error
			var iter memdb.ResultIterator
			if prefix := args.QueryOptions.Prefix; prefix != "" {
				iter, err = state.JobsByIDPrefix(ws, args.Namespace, prefix)
			} else {
				iter, err = state.Jobs(ws, sort)
			}
			if err != nil {
				return err
			}

			var jobs []*structs.Job
			paginator, err := paginator.NewPaginator(iter, nil, args.QueryOptions,
				func(raw interface{}) error {
					job := raw.(*structs.Job)
					if job.Parameters!=nil {
						job.ParameDetail = string(job.Parameters)
					}
					jobs = append(jobs, job)
					return nil
				})
			if err != nil {
				return structs.NewErrRPCCodedf(
					http.StatusBadRequest, "failed to create result paginator: %v", err)
			}

			nextToken, totalElements, totalPages, err := paginator.Page()
			if err != nil {
				return structs.NewErrRPCCodedf(
					http.StatusBadRequest, "failed to read result page: %v", err)
			}

			reply.QueryMeta.NextToken = nextToken
			reply.Jobs = jobs
			reply.TotalPages = totalPages
			reply.TotalElements = totalElements

			// Use the last index that affected the plans table or summary
			jindex, err := state.Index("plans")
			if err != nil {
				return err
			}
			reply.Index = jindex

			// Set the query response
			j.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return j.srv.blockingRPC(&opts)
}

func (n *Job) DeleteJob(args *dto.JobDeleteRequest, reply *dto.JobDeleteResponse) error {
	if done, err := n.srv.forward("Job.DeleteJob", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "job", "delete"}, time.Now())

	if args.JobID == "" {
		return fmt.Errorf("missing key for detail")
	}
	// if it is exist
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	out, err := snap.JobByID(ws, args.RequestNamespace(), args.JobID)
	if err != nil {
		return err
	}
	if out == nil {
		return fmt.Errorf("the job id [%s] not exists", args.JobID)
	}
	_, _, err = n.srv.raftApply(constant.JobDeleteRequestType, args)
	if err != nil {
		n.srv.logger.Error("delete job failed", "error", err)
		return err
	}
	return nil
}

func (n *Job) UpdateStatus(args *dto.JobStatusUpdateRequest, reply *dto.JobStatusUpdateResponse) error {
	if done, err := n.srv.forward("Job.UpdateStatus", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "job", "update"}, time.Now())

	if args.JobID == "" {
		return fmt.Errorf("missing jobId for update")
	}
	if args.Status == "" {
		return fmt.Errorf("missing Status for update")
	}
	// if it is exist
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	out, err := snap.JobByID(ws, args.RequestNamespace(), args.JobID)
	if err != nil {
		return err
	}
	if out == nil {
		return fmt.Errorf("the jobId [%s] not exists", args.JobID)
	}
	if out.Status == args.Status {
		return fmt.Errorf("the job id [%s] and status [%s] not changed", args.JobID, args.Status)
	}

	upsertArgs := &dto.JobStatusUpdateRequest{
		JobID:        args.JobID,
		Status:       args.Status,
		WriteRequest: args.WriteRequest,
	}

	_, index, err := n.srv.raftApply(constant.JobUpdateStatusRequestType, upsertArgs)
	if err != nil {
		n.srv.logger.Error("update job status failed", "error", err)
		return err
	}
	reply.Index = index
	reply.JobID = args.JobID
	reply.Status = args.Status
	return nil
}

func (n *Job) Detail(args *dto.JobDetailRequest, reply *dto.JobDetailResponse) error {
	if done, err := n.srv.forward("Job.Detail", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "job", "detail"}, time.Now())
	snap, err := n.srv.fsm.State().Snapshot()
	if err != nil {
		return err
	}
	ws := memdb.NewWatchSet()
	out, err := snap.JobByID(ws, args.RequestNamespace(), args.JobID)
	if err != nil {
		return err
	}
	if out == nil {
		return fmt.Errorf("the jobId [%s] not exists", args.JobID)
	}
	reply.Job = out
	return nil
}

func (n *Job) Stats(args *dto.JobMapStatRequest, reply *dto.JobMapStatResponse) error {
	if done, err := n.srv.forward("Job.Stats", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{constant.JobPoolName, "job", "stats"}, time.Now())

	stat := n.srv.jobRoadMap.Stats(args.Namespace)
	if stat != nil {
		dto := new(dto.JobMapDto)
		dto.Running = stat.TotalRunning
		dto.Retry = stat.TotalRetry
		dto.Pending = stat.TotalPending
		dto.UnUsed = stat.TotalUnUsed
		dto.PendingJobs = stat.PendingJobs
		dto.RunningJobs = stat.RunningJobs
		reply.TaskRoadmapDto = dto
	}
	return nil

}