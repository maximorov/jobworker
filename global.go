package jobworker

import (
	"time"
)

var pool *Pool

func QueueJob(j *Job, opts ...JobOption) {
	for _, opt := range opts {
		opt(j)
	}

	pool.QueueJob(j)
}

func QueueJobIf(j *Job, ifCh chan bool, opts ...JobOption) {
	go func() {
		ok := <-ifCh
		if ok {
			QueueJob(j, opts...)
		}
	}()
}

type JobOption func(*Job)

func JobWithTimeout(timeout time.Duration) JobOption {
	return func(j *Job) {
		j.timeout = timeout
	}
}
