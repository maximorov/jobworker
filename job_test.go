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
	assert.Equal(t, StateNew, job.state)
	assert.NotNil(t, job.business)
	assert.Equal(t, defaultJobTimeout, job.timeout)
	_, err := uuid.Parse(job.name)
	assert.NoError(t, err, "job name should be a valid UUID by default")

	// Test with custom options
	customTimeout := 20 * time.Second
	customName := "custom-job"
	jobWithOpts := NewJob(business, JobWithTimeout(customTimeout), JobWithName(customName))
	assert.NotNil(t, jobWithOpts)
	assert.Equal(t, customTimeout, jobWithOpts.timeout)
	assert.Equal(t, customName, jobWithOpts.name)
}

// TestJobCouldBeProcessed tests the logic of the couldBeProcessed method.
func TestJobCouldBeProcessed(t *testing.T) {
	job := &Job{}

	job.state = StateNew
	assert.True(t, job.couldBeProcessed(), "Job with 'new' state should be processable")

	job.state = StateProcessed
	assert.True(t, job.couldBeProcessed(), "Job with 'processed' state should be processable")

	job.state = StateWaiting
	assert.False(t, job.couldBeProcessed(), "Job with 'waiting' state should not be processable")

	job.state = StateProcessing
	assert.False(t, job.couldBeProcessed(), "Job with 'processing' state should not be processable")
}
