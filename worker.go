package jobworker

import (
	"context"
	"fmt"
)

// InternalErrorHolder is an interface for errors that contain an internal error.
type InternalErrorHolder interface {
	GetInternal() error
}

// Worker is responsible for executing jobs.
type Worker struct {
	id   int
	pool *Pool
}

// NewWorker creates a new worker.
func NewWorker(id int, pool *Pool) Worker {
	return Worker{
		id:   id,
		pool: pool,
	}
}

// Listen starts the worker, which listens for jobs on the jobs channel.
// It will stop when the context is canceled.
func (w *Worker) Listen(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			w.pool.workerFinished <- struct{}{}
			return
		case job, ok := <-w.pool.waitingJobs:
			if !ok {
				w.pool.workerFinished <- struct{}{}
				return
			}
			if job == nil {
				continue
			}
			w.safePerformJob(ctx, job)
		}
	}
}

func (w *Worker) safePerformJob(ctx context.Context, job *Job) {
	defer func() {
		if r := recover(); r != nil {
			var err error
			switch t := r.(type) {
			case error:
				err = t
			default:
				err = fmt.Errorf("%v", t)
			}
			w.pool.log.Error("job panicked", "worker_id", w.id, "job", job.name, "error", err.Error())
		}
	}()

	res, err := w.performJob(ctx, job)
	if err != nil {
		if iErr, ok := err.(InternalErrorHolder); ok && iErr.GetInternal() != nil {
			w.pool.log.Error("job failed", "job", job.name, "error", err.Error(), "internal", iErr.GetInternal().Error())
		} else {
			w.pool.log.Error("job failed", "job", job.name, "error", err.Error())
		}
		return
	}
	if res != nil {
		w.pool.log.Info("job completed successfully", "job", job.name, "result", res.String())
	} else {
		w.pool.log.Info("job completed successfully", "job", job.name)
	}
}

func (w *Worker) performJob(ctx context.Context, j *Job) (fmt.Stringer, error) {
	j.setState(StateProcessing)
	defer j.setState(StateProcessed)

	jCtx, cancel := context.WithTimeout(ctx, j.timeout)
	defer func() { cancel() }()

	w.pool.log.Debug("job is performing", "job", j.name)

	return j.business(jCtx)
}
