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

type Pool struct {
	workers        []Worker
	waitingJobs    chan *Job
	workerFinished chan struct{}
	scheduledJobs  []*ScheduledJob
	scheduledMu    sync.RWMutex

	stopped *atomic.Bool
}

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

func (p *Pool) InitGlobalPool() {
	pool = p
}

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

func (p *Pool) QueueJob(j *Job) {
	j.state = StateWaiting

	if p.stopped.Load() {
		log.Infof(`Jobs worker pool is stopped.`)
		return
	}

	log.Debugf(`JOB <%s> is queued`, j.name)

	p.waitingJobs <- j
}

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

func (p *Pool) Shutdown() error {
	p.stopped.Store(true)
	close(p.waitingJobs)

	// waiting for workers are finished
	for i := 0; i < len(p.workers); i++ {
		<-p.workerFinished
	}

	return nil
}
