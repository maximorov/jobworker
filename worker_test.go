package jobworker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type SuccessResponse struct {
}

func (r SuccessResponse) String() string {
	return `success`
}

// TestWorker_Listen tests the worker's ability to receive and process a job.
func TestWorker_Listen(t *testing.T) {
	jobs := make(chan *Job, 1)
	finished := make(chan struct{}, 1)
	worker := NewWorker(1, jobs, finished)

	ctx, cancel := context.WithCancel(context.Background())
	go worker.Listen(ctx)

	processed := false
	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		processed = true
		return nil, nil
	})

	jobs <- job

	// Give the worker time to process the job
	time.Sleep(100 * time.Millisecond)

	assert.True(t, processed, "Worker should have processed the job")

	cancel()
	<-finished // Wait for the worker to finish
}

// TestWorker_PerformJob_Success tests the successful performance of a job.
func TestWorker_PerformJob_Success(t *testing.T) {
	worker := Worker{}
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
	worker := Worker{}
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
	worker := Worker{}
	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		time.Sleep(200 * time.Millisecond) // This will take longer than the timeout
		return nil, nil
	}, JobWithTimeout(50*time.Millisecond))

	_, err := worker.performJob(context.Background(), job)

	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.Equal(t, StateProcessed, job.state)
}
