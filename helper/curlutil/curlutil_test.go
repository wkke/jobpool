package curlutil

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestConvertJson(t *testing.T) {
	userArgs := `
	{
	"curl": ["-v", "-X", "POST", "http://kernel-coordinator-service:8080/kernels/maintenances/clean-kubernetes-jobs", "-H", "accept: application/json"]
	}
`
	var curlModel *curlDto
	err := json.Unmarshal([]byte(userArgs), &curlModel)
	if err != nil {
		fmt.Println("can not unmarshal")
		return
	}
	json := "eyJjdXJsIjogWyItdiIsICItWCIsICJQT1NUIiwgImh0dHA6Ly9rZXJuZWwtY29vcmRpbmF0b3Itc2VydmljZTo4MDgwL2tlcm5lbHMvbWFpbnRlbmFuY2VzL2NsZWFuLWt1YmVybmV0ZXMtam9icyIsICItSCIsICJhY2NlcHQ6IGFwcGxpY2F0aW9uL2pzb24iXX0="
	array := ConvertUserArgsToArgs(json)
	if array != nil {
		for _, item := range array {
			fmt.Println(item)
		}
	}

}
