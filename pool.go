package jobworker

import (
	"context"
	"log/slog"
	"sync"
	"time"

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
	// sendMu makes "check stopped, then send on waitingJobs" atomic with respect to
	// Shutdown closing waitingJobs: senders hold the read lock, Shutdown the write lock.
	sendMu sync.RWMutex

	log *slog.Logger

	stopped *atomic.Bool
}

// newPool creates a new worker pool with the specified number of workers.
// A nil logger means slog.Default().
func newPool(ctx context.Context, workersNum int, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}

	res := &Pool{
		ctx:            ctx,
		workers:        make([]Worker, workersNum),
		waitingJobs:    make(chan *Job, queueSize),
		workerFinished: make(chan struct{}),
		shutdownCh:     make(chan struct{}),
		log:            logger.With("component", "jobworker"),
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

// Listen creates and starts a worker pool. A nil logger means slog.Default().
func Listen(ctx context.Context, workersNum int, logger *slog.Logger) (*Pool, error) {
	p := newPool(ctx, workersNum, logger)

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
	j.setState(StateWaiting)

	if p.stopped.Load() {
		p.log.Info("jobs worker pool is stopped")
		return
	}

	if j.delay > 0 {
		p.log.Debug("job is delayed", "job", j.name, "delay", j.delay)
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
	p.log.Debug("job is queued", "job", j.name)

	p.sendMu.RLock()
	defer p.sendMu.RUnlock()

	if p.stopped.Load() {
		p.log.Info("jobs worker pool is stopped")
		return
	}

	select {
	case p.waitingJobs <- j:
	case <-p.shutdownCh:
		// Shutdown started while the queue was full: the job is dropped, as any job
		// queued after Shutdown is.
		p.log.Info("jobs worker pool is stopped", "job", j.name)
	}
}

// ScheduleJob adds a scheduled job to the pool.
func (p *Pool) ScheduleJob(j *ScheduledJob) {
	j.setState(StateNew)

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
// Jobs already in the queue are still processed; jobs queued after Shutdown starts are dropped.
func (p *Pool) Shutdown() error {
	p.shutdownOnce.Do(func() {
		p.stopped.Store(true)
		// Stops the scheduler loop and unblocks senders waiting on a full queue.
		close(p.shutdownCh)
		// Waits for in-flight sends; afterwards no goroutine can send on waitingJobs.
		p.sendMu.Lock()
		close(p.waitingJobs)
		p.sendMu.Unlock()

		// waiting for workers are finished
		for i := 0; i < len(p.workers); i++ {
			<-p.workerFinished
		}
	})
	p.log.Info("pool is stopped")

	return nil
}
