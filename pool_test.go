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

	var processed bool
	var mu sync.Mutex
	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		mu.Lock()
		processed = true
		mu.Unlock()
		return nil, nil
	})

	pool.QueueJob(job)

	// Wait for the job to be processed
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	assert.True(t, processed, "Job should have been processed")
	mu.Unlock()

	pool.Shutdown()
}

// TestPoolScheduleJob tests scheduling a job.
func TestPoolScheduleJob(t *testing.T) {
	pool := NewPool(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pool.Listen(ctx)

	var processed bool
	var mu sync.Mutex
	scheduledJob := NewScheduledJob(func(ctx context.Context) (fmt.Stringer, error) {
		mu.Lock()
		processed = true
		mu.Unlock()
		return nil, nil
	}, 100*time.Millisecond)

	pool.ScheduleJob(scheduledJob)

	// Wait for the job to be scheduled and processed
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	assert.True(t, processed, "Scheduled job should have been processed")
	mu.Unlock()

	pool.Shutdown()
}
