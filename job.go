package jobworker

import (
	"context"
	"time"
)

const defaultJobTimeout = time.Second * 10

type jobState string

const (
	StateNew        jobState = "new"
	StateWaiting    jobState = "waiting"
	StateProcessing jobState = `processing`
	StateProcessed  jobState = `processed`
)

type Job struct {
	state    jobState
	timeout  time.Duration
	business business
}

type business func(context.Context) error

func NewJob(b business) *Job {
	return &Job{
		state:    StateNew,
		business: b,
		timeout:  defaultJobTimeout,
	}
}
