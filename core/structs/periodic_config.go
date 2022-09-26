package structs

import (
	"time"
	"yunli.com/jobpool/helper/cronutil"
	xtime "yunli.com/jobpool/helper/time"
)

const (
	// PeriodicSpecCron is used for a cron spec.
	PeriodicSpecCron = "cron"
)

// Periodic defines the interval a plan should be run at.
type PeriodicConfig struct {
	// Enabled determines if the plan should be run periodically.
	Enabled bool `json:"enabled"`

	// Spec specifies the interval the plan should be run as. It is parsed based
	// on the SpecType.
	Spec string `json:"spec"`

	// SpecType defines the format of the spec.
	SpecType string `json:"specType"`

	// ProhibitOverlap enforces that spawned jobs do not run in parallel.
	ProhibitOverlap bool `json:"prohibitOverlap"`

	// by default is "Asia/Shanghai"
	TimeZone string `json:"tz"`

	// location is the time zone to evaluate the launch time against
	location *time.Location

	// scheduler after this time
	StartTime xtime.FormatTime `json:"startTime"`

	// scheduler expire after this time
	EndTime xtime.FormatTime `json:"endTime"`
}

// Next returns the closest time instant matching the spec that is after the
// passed time. If no matching instance exists, the zero value of time.Time is
// returned. The `time.Location` of the returned value matches that of the
// passed time.
func (p *PeriodicConfig) Next(fromTime time.Time) (time.Time, error) {
	return cronutil.NextWithZone(p.Spec, p.TimeZone, fromTime)
}

// GetLocation returns the location to use for determining the time zone to run
// the periodic plan against.
func (p *PeriodicConfig) GetLocation() *time.Location {
	// Plans pre 0.5.5 will not have this
	if p.location != nil {
		return p.location
	}
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return time.UTC
	}
	return location
}

func (p *PeriodicConfig) Canonicalize() {
	p.location = p.GetLocation()
}

func (p *PeriodicConfig) Merge(b *PeriodicConfig) *PeriodicConfig {
	var result *PeriodicConfig
	if p == nil {
		result = &PeriodicConfig{}
	} else {
		newP := new(PeriodicConfig)
		*newP = *p
		result = newP
	}
	if b != nil {
		if &b.Enabled != nil {
			result.Enabled = b.Enabled
		}
		if b.Spec != "" {
			result.Spec = b.Spec
		}
		if b.SpecType != "" {
			result.SpecType = b.SpecType
		}
		if &b.ProhibitOverlap != nil {
			result.ProhibitOverlap = b.ProhibitOverlap
		}
		if b.TimeZone != "" {
			result.TimeZone = b.TimeZone
		}
		if b.location != nil {
			result.location = b.location
		}
		if b.StartTime != "" {
			result.StartTime = b.StartTime
		}
		if b.EndTime != "" {
			result.EndTime = b.EndTime
		}
	}
	return result
}
