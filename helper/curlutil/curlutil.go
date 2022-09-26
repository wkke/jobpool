package curlutil

import (
	"encoding/base64"
	"encoding/json"
	"regexp"
)

func ConvertUserArgsToArgs(userArgs string) []string {
	var args []string
	if userArgs == "" {
		return args
	}
	var desc []byte
	desc, err := base64.StdEncoding.DecodeString(userArgs)
	if err != nil {
		return args
	}
	hasCurl := regexp.MustCompile(`.*"curl".*`)
	if hasCurl.Match(desc) {
		var curlModel *curlDto
		err := json.Unmarshal(desc, &curlModel)
		if err != nil {
			return args
		}
		return curlModel.Curl
	} else {
		return args
	}
}

type curlDto struct {
	Curl []string `json:"curl"`
}
