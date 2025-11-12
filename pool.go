package jobworker

import (
	"context"
	"sync"
	"time"

	"github.com/Cery-Tech/log"
	"go.uber.org/atomic"
)

const queueSize = 10
const checkScheduledJobsInterval = 5 * time.Second

// Pool manages a collection of workers and a queue of jobs to be processed.
type Pool struct {
	workers        []Worker
	waitingJobs    chan *Job
	workerFinished chan struct{}
	scheduledJobs  []*ScheduledJob
	scheduledMu    sync.RWMutex

	stopped *atomic.Bool
}

// NewPool creates a new worker pool with the specified number of workers.
func NewPool(workersNum int) *Pool {
	res := &Pool{
		workers:        make([]Worker, workersNum),
		waitingJobs:    make(chan *Job, queueSize),
		workerFinished: make(chan struct{}),
		stopped:        atomic.NewBool(false),
	}

	for i := 0; i < workersNum; i++ {
		res.workers[i] = NewWorker(i+1, res.waitingJobs, res.workerFinished)
	}

	return res
}

// InitGlobalPool sets the current pool as the global pool.
func (p *Pool) InitGlobalPool() {
	pool = p
}

// Listen starts the worker pool and begins processing jobs.
// It also starts a ticker to check for scheduled jobs.
func (p *Pool) Listen(ctx context.Context) error {
	for i := range p.workers {
		go p.workers[i].Listen(ctx)
	}

	go func() {
		ticker := time.NewTicker(checkScheduledJobsInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.queueScheduledJobs()
			}
		}
	}()

	<-ctx.Done()

	return nil
}

// QueueJob adds a job to the waiting queue to be processed by a worker.
func (p *Pool) QueueJob(j *Job) {
	j.state = StateWaiting

	if p.stopped.Load() {
		log.Infof(`Jobs worker pool is stopped.`)
		return
	}

	log.Debugf(`JOB <%s> is queued`, j.name)

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
	p.stopped.Store(true)
	close(p.waitingJobs)

	// waiting for workers are finished
	for i := 0; i < len(p.workers); i++ {
		<-p.workerFinished
	}

	return nil
}
