package jobworker

import (
	"time"
)

var pool *Pool

// QueueJob adds a job to the global worker pool.
func QueueJob(j *Job, opts ...JobOption) {
	for _, opt := range opts {
		opt(j)
	}

	pool.QueueJob(j)
}

// QueueJobIf conditionally adds a job to the global worker pool.
// The job is only queued if a true value is received on the ifCh channel.
func QueueJobIf(j *Job, ifCh chan bool, opts ...JobOption) {
	go func() {
		ok := <-ifCh
		if ok {
			QueueJob(j, opts...)
		}
	}()
}

// JobOption is a function that configures a Job.
type JobOption func(*Job)

// JobWithTimeout returns a JobOption that sets the timeout for a job.
func JobWithTimeout(timeout time.Duration) JobOption {
	return func(j *Job) {
		j.timeout = timeout
	}
}

// JobWithDelay returns a JobOption that delays queueing the job.
func JobWithDelay(delay time.Duration) JobOption {
	return func(j *Job) {
		j.delay = delay
	}
}

// JobWithName returns a JobOption that sets the name for a job.
func JobWithName(name string) JobOption {
	return func(j *Job) {
		j.name = name
	}
}
