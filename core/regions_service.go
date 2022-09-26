package core

import (
	log "github.com/hashicorp/go-hclog"
	"yunli.com/jobpool/core/dto"
)

type Region struct {
	srv    *Server
	logger log.Logger
}

func (r *Region) List(args *dto.GenericRequest, reply *[]string) error {
	*reply = r.srv.Regions()
	return nil
}
