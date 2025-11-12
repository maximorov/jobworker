package jobworker

import (
	"context"
	"fmt"
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

// Job represents a unit of work to be executed by a worker.
type Job struct {
	state    jobState
	business business
	timeout  time.Duration
	name     string
}

type business func(context.Context) (fmt.Stringer, error)

// NewJob creates a new Job with the given business logic and options.
// By default, it has a timeout of 10 seconds and a random name.
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
