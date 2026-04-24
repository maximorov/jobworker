package jobworker

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/atomic"
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
	// state is accessed concurrently by the scheduling goroutine
	// (Pool.queueScheduledJobs -> couldBeProcessed) and worker goroutines
	// (Worker.performJob), so it must be read/written atomically.
	state    atomic.String
	business Business
	timeout  time.Duration
	delay    time.Duration
	name     string
}

type Business func(context.Context) (fmt.Stringer, error)

// NewJob creates a new Job with the given Business logic and options.
// By default, it has a timeout of 10 seconds and a random name.
func NewJob(b Business, opts ...JobOption) *Job {
	j := &Job{
		business: b,
	}
	j.setState(StateNew)

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

func (j *Job) setState(s jobState) { j.state.Store(string(s)) }

func (j *Job) getState() jobState { return jobState(j.state.Load()) }

func (j *Job) couldBeProcessed() bool {
	s := j.getState()
	return s == StateNew || s == StateProcessed
}
