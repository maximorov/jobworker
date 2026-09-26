package jobworker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestAfter_QueueAndNotify_Success tests that jobs are queued when Notify is called with a nil error.
func TestAfter_QueueAndNotify_Success(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool, err := Listen(ctx, 1, discardLogger())
	assert.NoError(t, err)
	pool.InitGlobalPool()
	defer cancel()

	after := NewAfter()

	var wg sync.WaitGroup
	wg.Add(1)

	job := func(ctx context.Context) (fmt.Stringer, error) {
		wg.Done()
		return nil, nil
	}

	after.Queue(job)
	after.Notify(nil) // Notify with success

	// Wait for the job to be processed
	wg.Wait()

	pool.Shutdown()
}

// TestAfter_QueueAndNotify_Error tests that jobs are not queued when Notify is called with an error.
func TestAfter_QueueAndNotify_Error(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool, err := Listen(ctx, 1, discardLogger())
	assert.NoError(t, err)
	pool.InitGlobalPool()
	defer cancel()

	after := NewAfter()

	processed := false
	var mu sync.Mutex

	job := func(ctx context.Context) (fmt.Stringer, error) {
		mu.Lock()
		processed = true
		mu.Unlock()
		return nil, nil
	}

	after.Queue(job)
	after.Notify(fmt.Errorf("some error")) // Notify with an error

	// Wait a bit to ensure the job is not processed
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.False(t, processed, "Job should not have been processed")
	mu.Unlock()

	pool.Shutdown()
}
