package jobworker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Cery-Tech/log/v2"
	"github.com/stretchr/testify/assert"
)

// TestListen tests the creation of a new Pool.
func TestListen(t *testing.T) {
	numWorkers := 5
	pool, err := Listen(context.Background(), numWorkers, log.New())

	assert.NoError(t, err)
	assert.NotNil(t, pool)
	assert.Len(t, pool.workers, numWorkers)
	assert.NotNil(t, pool.waitingJobs)
	assert.NotNil(t, pool.workerFinished)
	assert.NotNil(t, pool.stopped)
	assert.False(t, pool.stopped.Load())

	assert.NoError(t, pool.Shutdown())
}

// TestPoolListenAndShutdown tests the lifecycle of the pool.
func TestPoolListenAndShutdown(t *testing.T) {
	pool, err := Listen(context.Background(), 2, log.New())
	assert.NoError(t, err)
	assert.NotNil(t, pool)
	assert.NoError(t, pool.Shutdown())
}

// TestPoolQueueJob tests queuing a job to the pool.
func TestPoolQueueJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool, err := Listen(ctx, 1, log.New())
	defer cancel()
	assert.NoError(t, err)
	assert.NotNil(t, pool)

	done := make(chan struct{})
	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		close(done)
		return nil, nil
	})

	pool.QueueJob(job)

	select {
	case <-done:
		// Job processed successfully
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for job to be processed")
	}

	pool.Shutdown()
}

// TestPoolQueueJob_WhenStopped tests that jobs are not queued when pool is stopped.
func TestPoolQueueJob_WhenStopped(t *testing.T) {
	pool, err := Listen(context.Background(), 1, log.New())
	assert.NoError(t, err)
	assert.NotNil(t, pool)

	pool.stopped.Store(true)

	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		return nil, nil
	})

	// This should not block since the pool is stopped
	pool.QueueJob(job)

	assert.Equal(t, StateWaiting, job.state)
}

// TestPoolScheduleJob tests scheduling a job.
func TestPoolScheduleJob(t *testing.T) {
	pool, err := Listen(context.Background(), 1, log.New())
	assert.NoError(t, err)
	assert.NotNil(t, pool)

	scheduledJob := NewScheduledJob(func(ctx context.Context) (fmt.Stringer, error) {
		return nil, nil
	}, 100*time.Millisecond)

	pool.ScheduleJob(scheduledJob)

	pool.scheduledMu.RLock()
	assert.Len(t, pool.scheduledJobs, 1)
	assert.Equal(t, StateNew, scheduledJob.state)
	pool.scheduledMu.RUnlock()

	assert.NoError(t, pool.Shutdown())
}

// TestPoolInitGlobalPool tests setting the pool as global.
func TestPoolInitGlobalPool(t *testing.T) {
	p, err := Listen(context.Background(), 1, log.New())
	assert.NoError(t, err)
	p.InitGlobalPool()

	assert.Equal(t, p, pool)

	assert.NoError(t, p.Shutdown())
}

// TestPoolMultipleWorkers tests that multiple workers can process jobs concurrently.
func TestPoolMultipleWorkers(t *testing.T) {
	numWorkers := 3
	ctx, cancel := context.WithCancel(context.Background())
	p, err := Listen(ctx, numWorkers, log.New())
	defer cancel()
	assert.NoError(t, err)
	assert.NotNil(t, p)

	var wg sync.WaitGroup
	processedCount := 0
	var mu sync.Mutex

	for i := 0; i < 5; i++ {
		wg.Add(1)
		job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
			mu.Lock()
			processedCount++
			mu.Unlock()
			wg.Done()
			return nil, nil
		})
		p.QueueJob(job)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		assert.Equal(t, 5, processedCount)
		mu.Unlock()
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for jobs to be processed")
	}

	_ = p.Shutdown()
}
