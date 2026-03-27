package jobworker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Cery-Tech/log/v2"
	"go.uber.org/atomic"
)

const queueSize = 10
const checkScheduledJobsInterval = 1 * time.Second

// Pool manages a collection of workers and a queue of jobs to be processed.
type Pool struct {
	ctx            context.Context
	workers        []Worker
	waitingJobs    chan *Job
	workerFinished chan struct{}
	scheduledJobs  []*ScheduledJob
	scheduledMu    sync.RWMutex
	shutdownOnce   sync.Once
	shutdownCh     chan struct{}

	log *log.Logger

	stopped *atomic.Bool
}

// newPool creates a new worker pool with the specified number of workers.
func newPool(ctx context.Context, workersNum int, logger *log.Logger) *Pool {
	res := &Pool{
		ctx:            ctx,
		workers:        make([]Worker, workersNum),
		waitingJobs:    make(chan *Job, queueSize),
		workerFinished: make(chan struct{}),
		shutdownCh:     make(chan struct{}),
		log:            logger.With(log.String("cat", "Job Worker")),
		stopped:        atomic.NewBool(false),
	}

	for i := 0; i < workersNum; i++ {
		res.workers[i] = NewWorker(i+1, res)
	}

	return res
}

// InitGlobalPool sets the current pool as the global pool.
func (p *Pool) InitGlobalPool() {
	pool = p
}

// Listen creates and starts a worker pool.
func Listen(ctx context.Context, workersNum int, log *log.Logger) (*Pool, error) {
	p := newPool(ctx, workersNum, log)

	for i := range p.workers {
		go p.workers[i].Listen(p.ctx)
	}

	go func() {
		ticker := time.NewTicker(checkScheduledJobsInterval)
		defer ticker.Stop()

		for {
			select {
			case <-p.ctx.Done():
				return
			case <-p.shutdownCh:
				return
			case <-ticker.C:
				p.queueScheduledJobs()
			}
		}
	}()

	p.log.Info("pool is listening")

	return p, nil
}

// QueueJob adds a job to the waiting queue to be processed by a worker.
func (p *Pool) QueueJob(j *Job) {
	j.state = StateWaiting

	if p.stopped.Load() {
		p.log.Info("jobs worker pool is stopped")
		return
	}

	if j.delay > 0 {
		p.log.Debug("job is delayed", log.String("job", j.name), log.Duration("delay", j.delay))
		go p.enqueueJobAfterDelay(j)
		return
	}

	p.enqueueJob(j)
}

func (p *Pool) enqueueJobAfterDelay(j *Job) {
	timer := time.NewTimer(j.delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		p.enqueueJob(j)
	case <-p.ctx.Done():
		return
	case <-p.shutdownCh:
		return
	}
}

func (p *Pool) enqueueJob(j *Job) {
	log.Debug("job is queued", log.String("job", j.name))

	defer func() {
		if r := recover(); r != nil {
			var err error
			switch t := r.(type) {
			case error:
				err = t
			default:
				err = fmt.Errorf("%v", t)
			}
			log.Error("job queueing failed", err, log.String("job", j.name), log.Any("panic", r))
		}
	}()

	if p.stopped.Load() {
		log.Info("jobs worker pool is stopped")
		return
	}

	p.waitingJobs <- j
}

// ScheduleJob adds a scheduled job to the pool.
func (p *Pool) ScheduleJob(j *ScheduledJob) {
	j.state = StateNew

	p.scheduledMu.Lock()
	defer p.scheduledMu.Unlock()

	p.scheduledJobs = append(p.scheduledJobs, j)
}

func (p *Pool) queueScheduledJobs() {
	p.scheduledMu.RLock()
	defer p.scheduledMu.RUnlock()

	for _, j := range p.scheduledJobs {
		if j.isItTime() {
			if j.couldBeProcessed() {
				p.QueueJob(j.Job)
			}
			j.queueItForLater()
		}
	}
}

// Shutdown gracefully stops the worker pool, waiting for all workers to finish their current jobs.
func (p *Pool) Shutdown() error {
	p.shutdownOnce.Do(func() {
		p.stopped.Store(true)
		close(p.shutdownCh)
		close(p.waitingJobs)

		// waiting for workers are finished
		for i := 0; i < len(p.workers); i++ {
			<-p.workerFinished
		}
	})
	p.log.Info("pool is stopped")

	return nil
}
