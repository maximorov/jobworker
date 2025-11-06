package jobworker

import (
	"context"

	"github.com/Cery-Tech/log"
)

type InternalErrorHolder interface {
	GetInternal() error
}

type Worker struct {
	id       int
	jobs     chan *Job
	finished chan struct{}
}

func NewWorker(id int, jobs chan *Job, finished chan struct{}) Worker {
	return Worker{
		id:       id,
		jobs:     jobs,
		finished: finished,
	}
}

func (w *Worker) Listen(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			w.finished <- struct{}{}
			return
		case job := <-w.jobs:
			if msg, err := w.performJob(ctx, job); err != nil {
				if iErr, ok := err.(InternalErrorHolder); ok {
					log.Errorf(`JOB <%s> failed: %s: %s`, err, iErr.GetInternal())
				} else {
					log.Errorf(`JOB <%s> failed: %s`, err)
				}
			} else {
				if msg != `` {
					log.Debugf(`JOB <%s> completed successfully: %s`, msg)
				} else {
					log.Debugf(`JOB <%s> completed successfully`)
				}
			}
		}
	}
}

func (w *Worker) performJob(ctx context.Context, j *Job) (string, error) {
	j.state = StateProcessing
	defer func() { j.state = StateProcessed }()

	jCtx, cancel := context.WithTimeout(ctx, j.timeout)
	defer func() { cancel() }()

	log.Debugf(`JOB <%s> is performing`, j.name)

	return j.business(jCtx)
}
