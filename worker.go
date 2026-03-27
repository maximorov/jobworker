package jobworker

import (
	"context"
	"fmt"

	"github.com/Cery-Tech/log/v2"
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
	//res.waitingJobs, res.workerFinished
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
			w.pool.log.Error("job panicked", err, log.Int("worker_id", w.id), log.String("job", job.name), log.Any("panic", r))
		}
	}()

	if res, err := w.performJob(ctx, job); err != nil {
		if iErr, ok := err.(InternalErrorHolder); ok && iErr.GetInternal() != nil {
			w.pool.log.Error("job failed", err, log.String("job", job.name), log.String("internal", iErr.GetInternal().Error()))
		} else {
			w.pool.log.Error("job failed", err, log.String("job", job.name))
		}
	} else {
		if res != nil {
			w.pool.log.Info("job completed successfully", log.String("job", job.name), log.Stringer("result", res))
		} else {
			w.pool.log.Info("job completed successfully", log.String("job", job.name))
		}
	}
}

func (w *Worker) performJob(ctx context.Context, j *Job) (fmt.Stringer, error) {
	j.state = StateProcessing
	defer func() { j.state = StateProcessed }()

	jCtx, cancel := context.WithTimeout(ctx, j.timeout)
	defer func() { cancel() }()

	w.pool.log.Debug("job is performing", log.String("job", j.name))

	return j.business(jCtx)
}
