package core

import (
	"time"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/dto"

	"yunli.com/jobpool/core/structs"
)

// drainerShim implements the drainer.RaftApplier interface required by the
// NodeDrainer.
type drainerShim struct {
	s *Server
}

func (d drainerShim) AllocUpdateDesiredTransition(allocs map[string]*structs.DesiredTransition, evals []*structs.Evaluation) (uint64, error) {
	args := &dto.AllocUpdateDesiredTransitionRequest{
		Allocs:       allocs,
		Evals:        evals,
		WriteRequest: dto.WriteRequest{Region: d.s.config.Region},
	}
	resp, index, err := d.s.raftApply(constant.AllocUpdateDesiredTransitionRequestType, args)
	return d.convertApplyErrors(resp, index, err)
}

func (d drainerShim) NodesDrainComplete(nodes []string, event *structs.NodeEvent) (uint64, error) {
	d.s.logger.Debug("in the method NodesDrainComplete is not implement")
	args := &dto.BatchNodeUpdateDrainRequest{
		Updates:      make(map[string]*structs.DrainUpdate, len(nodes)),
		NodeEvents:   make(map[string]*structs.NodeEvent, len(nodes)),
		WriteRequest: dto.WriteRequest{Region: d.s.config.Region},
		UpdatedAt:    time.Now().Unix(),
	}

	update := &structs.DrainUpdate{}
	for _, node := range nodes {
		args.Updates[node] = update
		if event != nil {
			args.NodeEvents[node] = event
		}
	}

	resp, index, err := d.s.raftApply(constant.BatchNodeUpdateDrainRequestType, args)
	return d.convertApplyErrors(resp, index, err)
}

// convertApplyErrors parses the results of a raftApply and returns the index at
// which it was applied and any error that occurred. Raft Apply returns two
// separate errors, Raft library errors and user returned errors from the FSM.
// This helper, joins the errors by inspecting the applyResponse for an error.
func (d drainerShim) convertApplyErrors(applyResp interface{}, index uint64, err error) (uint64, error) {
	if applyResp != nil {
		if fsmErr, ok := applyResp.(error); ok && fsmErr != nil {
			return index, fsmErr
		}
	}
	return index, err
}

func (d drainerShim) AllocUpdateInDrainNode(allocs []*structs.Allocation) (uint64, error) {
	batch := &dto.AllocUpdateRequest{
		Alloc:        allocs,
		Evals:        make([]*structs.Evaluation, 0),
		WriteRequest: dto.WriteRequest{Region: d.s.config.Region},
	}
	resp, index, err := d.s.raftApply(constant.AllocClientUpdateRequestType, batch)
	return d.convertApplyErrors(resp, index, err)
}
