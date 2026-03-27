package jobworker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueueJob(t *testing.T) {
	p := NewPool(1)
	p.InitGlobalPool()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Listen(ctx)

	var processed bool
	var mu sync.Mutex
	done := make(chan struct{})

	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		mu.Lock()
		processed = true
		mu.Unlock()
		close(done)
		return nil, nil
	})

	QueueJob(job)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for job to be processed")
	}

	mu.Lock()
	assert.True(t, processed)
	mu.Unlock()

	p.Shutdown()
}

func TestQueueJob_WithOptions(t *testing.T) {
	p := NewPool(1)
	p.InitGlobalPool()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Listen(ctx)

	done := make(chan struct{})

	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		close(done)
		return nil, nil
	})

	customTimeout := 30 * time.Second
	customName := "test-job-with-options"
	QueueJob(job, JobWithTimeout(customTimeout), JobWithName(customName))

	assert.Equal(t, customTimeout, job.timeout)
	assert.Equal(t, customName, job.name)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for job to be processed")
	}

	p.Shutdown()
}

func TestQueueJob_WithDelay(t *testing.T) {
	p := NewPool(1)
	p.InitGlobalPool()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Listen(ctx)

	done := make(chan struct{})
	startedAt := time.Now()
	var processedAt time.Time

	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		processedAt = time.Now()
		close(done)
		return nil, nil
	})

	QueueJob(job, JobWithDelay(50*time.Millisecond))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for delayed job to be processed")
	}

	assert.GreaterOrEqual(t, processedAt.Sub(startedAt), 50*time.Millisecond)

	p.Shutdown()
}

func TestQueueJobIf_True(t *testing.T) {
	p := NewPool(1)
	p.InitGlobalPool()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Listen(ctx)

	var processed bool
	var mu sync.Mutex
	done := make(chan struct{})

	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		mu.Lock()
		processed = true
		mu.Unlock()
		close(done)
		return nil, nil
	})

	ifCh := make(chan bool, 1)
	QueueJobIf(job, ifCh)
	ifCh <- true

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for job to be processed")
	}

	mu.Lock()
	assert.True(t, processed)
	mu.Unlock()

	p.Shutdown()
}

func TestQueueJobIf_False(t *testing.T) {
	p := NewPool(1)
	p.InitGlobalPool()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Listen(ctx)

	var processed bool
	var mu sync.Mutex

	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		mu.Lock()
		processed = true
		mu.Unlock()
		return nil, nil
	})

	ifCh := make(chan bool, 1)
	QueueJobIf(job, ifCh)
	ifCh <- false

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.False(t, processed, "Job should not have been processed when condition is false")
	mu.Unlock()

	p.Shutdown()
}

func TestJobWithTimeout(t *testing.T) {
	job := &Job{}
	opt := JobWithTimeout(5 * time.Second)
	opt(job)
	assert.Equal(t, 5*time.Second, job.timeout)
}

func TestJobWithDelay(t *testing.T) {
	job := &Job{}
	opt := JobWithDelay(time.Minute)
	opt(job)
	assert.Equal(t, time.Minute, job.delay)
}

func TestJobWithName(t *testing.T) {
	job := &Job{}
	opt := JobWithName("my-job")
	opt(job)
	assert.Equal(t, "my-job", job.name)
}
