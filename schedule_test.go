package jobworker

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
)

func TestScheduledEvery_IsItTime(t *testing.T) {
	fixedNow := time.Date(2025, 11, 5, 10, 0, 0, 0, time.UTC)

	job := &scheduledEvery{
		now:      func() time.Time { return fixedNow },
		every:    time.Minute,
		nextTime: fixedNow.Add(-time.Second), // already passed
	}

	assert.True(t, job.isItTime(), "should be time to run since nextTime is in the past")

	job.nextTime = fixedNow.Add(time.Second)
	assert.False(t, job.isItTime(), "should not be time to run yet")
}

func TestScheduledEvery_QueueItForLater(t *testing.T) {
	fixedNow := time.Date(2025, 11, 5, 10, 0, 0, 0, time.UTC)

	job := &scheduledEvery{
		now:      func() time.Time { return fixedNow },
		every:    5 * time.Minute,
		nextTime: fixedNow,
	}

	job.queueItForLater()
	assert.Equal(t, fixedNow.Add(5*time.Minute), job.nextTime, "nextTime should be moved 5 minutes ahead")
}

func TestScheduledCron_IsItTime(t *testing.T) {
	fixedNow := time.Date(2025, 11, 5, 10, 0, 0, 0, time.UTC)

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse("*/5 * * * *") // every 5 minutes
	assert.NoError(t, err)

	job := &scheduledCron{
		now:      func() time.Time { return fixedNow },
		schedule: schedule,
		nextTime: fixedNow.Add(-time.Second),
	}

	assert.True(t, job.isItTime(), "should be time to run")

	job.nextTime = fixedNow.Add(time.Second)
	assert.False(t, job.isItTime(), "should not be time to run yet")
}

func TestScheduledCron_QueueItForLater(t *testing.T) {
	fixedNow := time.Date(2025, 11, 5, 10, 0, 0, 0, time.UTC)
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, _ := parser.Parse("*/5 * * * *") // every 5 minutes

	job := &scheduledCron{
		now:      func() time.Time { return fixedNow },
		schedule: schedule,
		nextTime: fixedNow,
	}

	job.queueItForLater()

	expected := schedule.Next(fixedNow)
	assert.Equal(t, expected, job.nextTime, "nextTime should match cron's next scheduled run")
}

func TestNewScheduledJob(t *testing.T) {
	job := NewScheduledJob(nil, time.Minute)
	assert.NotNil(t, job)
	assert.Implements(t, (*scheduled)(nil), job.scheduled)
}

func TestNewScheduledCronJob(t *testing.T) {
	job := NewScheduledCronJob(nil, "*/5 * * * *")
	assert.NotNil(t, job)
	assert.Implements(t, (*scheduled)(nil), job.scheduled)
}
