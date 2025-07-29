package jobworker

import (
	"context"

	"github.com/Cery-Tech/log"
)

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
			err := w.performJob(ctx, job)
			if err != nil {
				log.Errorf("Failed to perform job: %s", err)
			}
		}
	}
}

func (w *Worker) performJob(ctx context.Context, j *Job) error {
	j.state = StateProcessing
	defer func() { j.state = StateProcessed }()

	jCtx, cancel := context.WithTimeout(ctx, j.timeout)
	defer func() { cancel() }()

	return j.business(jCtx)
}
