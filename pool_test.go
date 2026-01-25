package jobworker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestNewPool tests the creation of a new Pool.
func TestNewPool(t *testing.T) {
	numWorkers := 5
	pool := NewPool(numWorkers)

	assert.NotNil(t, pool)
	assert.Len(t, pool.workers, numWorkers)
	assert.NotNil(t, pool.waitingJobs)
	assert.NotNil(t, pool.workerFinished)
	assert.NotNil(t, pool.stopped)
	assert.False(t, pool.stopped.Load())
}

// TestPoolListenAndShutdown tests the lifecycle of the pool.
func TestPoolListenAndShutdown(t *testing.T) {
	pool := NewPool(2)
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.Listen(ctx)
	}()

	// Allow some time for the pool to start listening
	time.Sleep(100 * time.Millisecond)

	cancel()
	err := pool.Shutdown()
	assert.NoError(t, err)
	wg.Wait() // Wait for Listen to return
}

// TestPoolQueueJob tests queuing a job to the pool.
func TestPoolQueueJob(t *testing.T) {
	pool := NewPool(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Listen(ctx)

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
	pool := NewPool(1)

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
	pool := NewPool(1)

	scheduledJob := NewScheduledJob(func(ctx context.Context) (fmt.Stringer, error) {
		return nil, nil
	}, 100*time.Millisecond)

	pool.ScheduleJob(scheduledJob)

	pool.scheduledMu.RLock()
	assert.Len(t, pool.scheduledJobs, 1)
	assert.Equal(t, StateNew, scheduledJob.state)
	pool.scheduledMu.RUnlock()
}

// TestPoolInitGlobalPool tests setting the pool as global.
func TestPoolInitGlobalPool(t *testing.T) {
	p := NewPool(1)
	p.InitGlobalPool()

	assert.Equal(t, p, pool)
}

// TestPoolMultipleWorkers tests that multiple workers can process jobs concurrently.
func TestPoolMultipleWorkers(t *testing.T) {
	numWorkers := 3
	p := NewPool(numWorkers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go p.Listen(ctx)

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

	p.Shutdown()
}
