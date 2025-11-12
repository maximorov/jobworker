package jobworker

import (
	"context"
	"fmt"

	"github.com/Cery-Tech/log"
)

// InternalErrorHolder is an interface for errors that contain an internal error.
type InternalErrorHolder interface {
	GetInternal() error
}

// Worker is responsible for executing jobs.
type Worker struct {
	id       int
	jobs     chan *Job
	finished chan struct{}
}

// NewWorker creates a new worker.
func NewWorker(id int, jobs chan *Job, finished chan struct{}) Worker {
	return Worker{
		id:       id,
		jobs:     jobs,
		finished: finished,
	}
}

// Listen starts the worker, which listens for jobs on the jobs channel.
// It will stop when the context is canceled.
func (w *Worker) Listen(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			w.finished <- struct{}{}
			return
		case job := <-w.jobs:
			if res, err := w.performJob(ctx, job); err != nil {
				if iErr, ok := err.(InternalErrorHolder); ok {
					log.Errorf(`JOB <%s> failed: %s: %s`, job.name, err, iErr.GetInternal())
				} else {
					log.Errorf(`JOB <%s> failed: %s`, job.name, err)
				}
			} else {
				if res != nil {
					log.Debugf(`JOB <%s> completed successfully: %s`, job.name, res)
				} else {
					log.Debugf(`JOB <%s> completed successfully`, job.name)
				}
			}
		}
	}
}

func (w *Worker) performJob(ctx context.Context, j *Job) (fmt.Stringer, error) {
	j.state = StateProcessing
	defer func() { j.state = StateProcessed }()

	jCtx, cancel := context.WithTimeout(ctx, j.timeout)
	defer func() { cancel() }()

	log.Debugf(`JOB <%s> is performing`, j.name)

	return j.business(jCtx)
}
