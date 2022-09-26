package time

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
	"yunli.com/jobpool/helper/ci"
)

type TestTimeConfig struct {
	StartTime FormatTime `json:"startTime"`
	EndTime FormatTime `json:"endTime"`
}

func TestEnqueueDequeue(t *testing.T) {
	ci.Parallel(t)
	var timeConfig TestTimeConfig
	strJson := `{"startTime": "2022-08-24 12:20:11", "endTime": "2021-01-52"}`
	json.Unmarshal([]byte(strJson), &timeConfig)
	fmt.Println(timeConfig.StartTime.TimeValue())
	fmt.Println(timeConfig.EndTime)
	fmt.Println(timeConfig.StartTime.TimeValue().IsZero())
	timeZero := new(time.Time)
	if timeZero.Equal(timeConfig.StartTime.TimeValue()) {
		fmt.Println("unsupport time format")
	}
	fmt.Println("-----")
	fmt.Println(time.Now())

	if time.Now().Before(timeConfig.StartTime.TimeValue()) {
		fmt.Println("not yet")
	}else {
		fmt.Println("already passed start")
	}

}