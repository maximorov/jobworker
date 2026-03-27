package jobworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Cery-Tech/log/v2"
	"github.com/stretchr/testify/assert"
)

type SuccessResponse struct {
}

func (r SuccessResponse) String() string {
	return `success`
}

// TestWorker_Listen tests the worker's ability to receive and process a job.
func TestWorker_Listen(t *testing.T) {
	p, err := Listen(context.Background(), 1, log.New())
	assert.NoError(t, err)
	defer p.Shutdown()

	worker := NewWorker(1, p)

	ctx, cancel := context.WithCancel(context.Background())
	go worker.Listen(ctx)

	var mu sync.Mutex
	processed := false
	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		mu.Lock()
		processed = true
		mu.Unlock()
		return nil, nil
	})

	p.waitingJobs <- job

	// Give the worker time to process the job
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.True(t, processed, "Worker should have processed the job")
	mu.Unlock()

	cancel()
	<-p.workerFinished // Wait for the worker to finish
}

// TestWorker_PerformJob_Success tests the successful performance of a job.
func TestWorker_PerformJob_Success(t *testing.T) {
	p, err := Listen(context.Background(), 1, log.New())
	assert.NoError(t, err)
	defer p.Shutdown()

	worker := NewWorker(1, p)
	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		return &SuccessResponse{}, nil
	})

	res, err := worker.performJob(context.Background(), job)

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, "success", res.String())
	assert.Equal(t, StateProcessed, job.state)
}

// TestWorker_PerformJob_Error tests a job that returns an error.
func TestWorker_PerformJob_Error(t *testing.T) {
	p, err := Listen(context.Background(), 1, log.New())
	assert.NoError(t, err)
	defer p.Shutdown()

	worker := NewWorker(1, p)
	jobErr := errors.New("job failed")
	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		return nil, jobErr
	})

	res, err := worker.performJob(context.Background(), job)

	assert.Error(t, err)
	assert.Equal(t, jobErr, err)
	assert.Nil(t, res)
	assert.Equal(t, StateProcessed, job.state)
}

// TestWorker_PerformJob_Timeout tests a job that times out.
func TestWorker_PerformJob_Timeout(t *testing.T) {
	p, err := Listen(context.Background(), 1, log.New())
	assert.NoError(t, err)
	defer p.Shutdown()

	worker := NewWorker(1, p)
	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return nil, nil
		}
	}, JobWithTimeout(50*time.Millisecond))

	_, err = worker.performJob(context.Background(), job)

	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.Equal(t, StateProcessed, job.state)
}

// internalError implements InternalErrorHolder interface for testing
type internalError struct {
	msg      string
	internal error
}

func (e *internalError) Error() string {
	return e.msg
}

func (e *internalError) GetInternal() error {
	return e.internal
}

// TestWorker_PerformJob_InternalError tests a job that returns an error with internal error.
func TestWorker_PerformJob_InternalError(t *testing.T) {
	p, err := Listen(context.Background(), 1, log.New())
	assert.NoError(t, err)
	defer p.Shutdown()

	worker := NewWorker(1, p)
	internalErr := errors.New("database connection failed")
	jobErr := &internalError{
		msg:      "job failed",
		internal: internalErr,
	}

	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		return nil, jobErr
	})

	res, err := worker.performJob(context.Background(), job)

	assert.Error(t, err)
	assert.Nil(t, res)
	assert.Equal(t, StateProcessed, job.state)

	iErr, ok := err.(InternalErrorHolder)
	assert.True(t, ok, "Error should implement InternalErrorHolder")
	assert.Equal(t, internalErr, iErr.GetInternal())
}

// TestWorker_Listen_ContextCancel tests that worker stops when context is canceled.
func TestWorker_Listen_ContextCancel(t *testing.T) {
	p, err := Listen(context.Background(), 1, log.New())
	assert.NoError(t, err)
	defer p.Shutdown()

	p.workerFinished = make(chan struct{}, 1)
	worker := NewWorker(1, p)

	ctx, cancel := context.WithCancel(context.Background())

	go worker.Listen(ctx)

	cancel()

	select {
	case <-p.workerFinished:
		// Worker finished as expected
	case <-time.After(time.Second):
		t.Fatal("Worker did not finish after context cancel")
	}
}
