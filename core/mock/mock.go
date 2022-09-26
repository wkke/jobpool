package mock

import (
	"fmt"
	"yunli.com/jobpool/core/constant"
	"yunli.com/jobpool/core/structs"
	"yunli.com/jobpool/helper/uuid"
)

func Node() *structs.Node {
	node := &structs.Node{
		ID:         uuid.Generate(),
		SecretID:   uuid.Generate(),
		Datacenter: "dc1",
		Name:       "foobar",
		Attributes: map[string]string{
			"kernel.name":        "linux",
			"arch":               "x86",
			"jobpool.version":      "0.5.0",
			"driver.exec":        "1",
			"driver.mock_driver": "1",
		},
		Meta: map[string]string{
			"pci-dss":  "true",
			"database": "mysql",
			"version":  "5.6",
		},
		NodeClass:             "linux-medium-pci",
		Status:                constant.NodeStatusReady,
		SchedulingEligibility: constant.NodeSchedulingEligible,
	}
	node.ComputeClass()
	return node
}

func Namespace() *structs.Namespace {
	uuid := uuid.Generate()
	ns := &structs.Namespace{
		Name:        fmt.Sprintf("team-%s", uuid),
		Meta:        map[string]string{"team": uuid},
		Description: "test namespace",
		CreateIndex: 100,
		ModifyIndex: 200,
	}
	ns.SetHash()
	return ns
}
