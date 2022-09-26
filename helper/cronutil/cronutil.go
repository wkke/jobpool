package cronutil

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"time"
)

const defaultTimeZone = "Asia/Shanghai"
// Next returns the closest time instant matching the spec that is after the
// passed time. If no matching instance exists, the zero value of time.Time is
// returned. The `time.Location` of the returned value matches that of the
// passed time.
func Next(spec string, fromTime time.Time) (time.Time, error) {
	s, err := cron.ParseStandard(spec)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed parsing cron expression: %q: %v", spec, err)
	}
	fetchTime := s.Next(fromTime)
	return fetchTime, nil
}

func NextWithZone(spec string, tz string, fromTime time.Time) (time.Time, error) {
	if tz == "" {
		tz = defaultTimeZone
	}
	tzSpec := fmt.Sprintf("TZ=%s %s", tz, spec)
	s, err := cron.ParseStandard(tzSpec)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed parsing cron expression: %q: %v", spec, err)
	}
	fetchTime := s.Next(fromTime)
	return fetchTime, nil
}
