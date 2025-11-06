package jobworker

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const defaultJobTimeout = time.Second * 10

type jobState string

const (
	StateNew        jobState = "new"
	StateWaiting    jobState = "waiting"
	StateProcessing jobState = `processing`
	StateProcessed  jobState = `processed`
)

type Job struct {
	state    jobState
	business business
	timeout  time.Duration
	name     string
}

type business func(context.Context) (string, error)

func NewJob(b business, opts ...JobOption) *Job {
	j := &Job{
		state:    StateNew,
		business: b,
	}

	for _, opt := range opts {
		opt(j)
	}

	if j.timeout == 0 {
		j.timeout = defaultJobTimeout
	}
	if j.name == `` {
		j.name = uuid.NewString()
	}

	return j
}

func (j *Job) couldBeProcessed() bool {
	return j.state == StateNew || j.state == StateProcessed
}
