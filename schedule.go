package jobworker

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

type scheduled interface {
	isItTime() bool
	queueItForLater()
}

type scheduledEvery struct {
	now      func() time.Time
	every    time.Duration
	nextTime time.Time
}

func (j *scheduledEvery) isItTime() bool {
	return !j.nextTime.After(j.now())
}

func (j *scheduledEvery) queueItForLater() {
	j.nextTime = j.now().Add(j.every)
}

type scheduledCron struct {
	now      func() time.Time
	schedule cron.Schedule
	nextTime time.Time
}

func (j *scheduledCron) isItTime() bool {
	return !j.nextTime.After(j.now())
}

func (j *scheduledCron) queueItForLater() {
	j.nextTime = j.schedule.Next(j.now())
}

// ScheduledJob is a job that is scheduled to run at a specific time or interval.
type ScheduledJob struct {
	scheduled
	*Job
}

// NewScheduledJob creates a new job that runs at a specified interval.
func NewScheduledJob(b Business, every time.Duration, opts ...JobOption) *ScheduledJob {
	return &ScheduledJob{
		&scheduledEvery{
			now:      time.Now,
			every:    every,
			nextTime: time.Now().Add(every),
		},
		NewJob(b, opts...),
	}
}

// NewScheduledCronJob creates a new job that runs based on a cron expression.
func NewScheduledCronJob(b Business, pattern string, opts ...JobOption) (*ScheduledJob, error) {
	schedule, err := cronParser.Parse(pattern)
	if err != nil || schedule == nil {
		if err == nil {
			err = fmt.Errorf("cron parser returned nil schedule")
		}
		return nil, fmt.Errorf("invalid cron expression %q: %w", pattern, err)
	}

	return &ScheduledJob{
		&scheduledCron{
			now:      time.Now,
			schedule: schedule,
			nextTime: schedule.Next(time.Now()),
		},
		NewJob(b, opts...),
	}, nil
}
