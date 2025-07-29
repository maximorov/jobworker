package jobworker

import (
	"time"

	"github.com/Cery-Tech/equipment-backend/pkg/apptime"
)

type scheduled struct {
	every    time.Duration
	nextTime time.Time
}
type ScheduledJob struct {
	*Job
	schedule scheduled
}

func NewScheduledJob(b business, every time.Duration) *ScheduledJob {
	return &ScheduledJob{
		Job: NewJob(b),
		schedule: scheduled{
			every:    every,
			nextTime: apptime.Now().Add(every),
		},
	}
}

func (j *ScheduledJob) isItTime() bool {
	return j.schedule.nextTime.Before(apptime.Now())
}

func (j *ScheduledJob) couldBeProcessed() bool {
	return j.Job.state == StateNew || j.Job.state == StateProcessed
}

func (j *ScheduledJob) queueItForLater() {
	j.schedule.nextTime = apptime.Now().Add(j.schedule.every)
}
