package jobworker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// TestNewJob tests the creation of a new Job with default and custom options.
func TestNewJob(t *testing.T) {
	business := func(ctx context.Context) (fmt.Stringer, error) {
		return nil, nil
	}

	// Test with default options
	job := NewJob(business)
	assert.NotNil(t, job)
	assert.Equal(t, StateNew, job.getState())
	assert.NotNil(t, job.business)
	assert.Equal(t, defaultJobTimeout, job.timeout)
	_, err := uuid.Parse(job.name)
	assert.NoError(t, err, "job name should be a valid UUID by default")

	// Test with custom options
	customTimeout := 20 * time.Second
	customName := "custom-job"
	customDelay := 45 * time.Second
	jobWithOpts := NewJob(business, JobWithTimeout(customTimeout), JobWithDelay(customDelay), JobWithName(customName))
	assert.NotNil(t, jobWithOpts)
	assert.Equal(t, customTimeout, jobWithOpts.timeout)
	assert.Equal(t, customDelay, jobWithOpts.delay)
	assert.Equal(t, customName, jobWithOpts.name)
}

// TestJobCouldBeProcessed tests the logic of the couldBeProcessed method.
func TestJobCouldBeProcessed(t *testing.T) {
	job := &Job{}

	job.setState(StateNew)
	assert.True(t, job.couldBeProcessed(), "Job with 'new' state should be processable")

	job.setState(StateProcessed)
	assert.True(t, job.couldBeProcessed(), "Job with 'processed' state should be processable")

	job.setState(StateWaiting)
	assert.False(t, job.couldBeProcessed(), "Job with 'waiting' state should not be processable")

	job.setState(StateProcessing)
	assert.False(t, job.couldBeProcessed(), "Job with 'processing' state should not be processable")
}
