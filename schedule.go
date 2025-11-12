package jobworker

import (
	"time"

	"github.com/Cery-Tech/log"
	"github.com/robfig/cron/v3"
)

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
	return j.nextTime.Before(j.now())
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
	return j.nextTime.Before(j.now())
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
func NewScheduledJob(b business, every time.Duration, opts ...JobOption) *ScheduledJob {
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
func NewScheduledCronJob(b business, pattern string, opts ...JobOption) *ScheduledJob {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(pattern)
	if err != nil || schedule == nil {
		log.Panicf(`cron pattern "%s" is not a valid cron expression: %v`, pattern, err)
		return nil
	}

	return &ScheduledJob{
		&scheduledCron{
			now:      time.Now,
			schedule: schedule,
			nextTime: schedule.Next(time.Now()),
		},
		NewJob(b, opts...),
	}
}
