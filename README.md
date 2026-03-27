# Jobworker

This project implements a background job processing system in Go. It allows you to queue and process jobs asynchronously using a pool of workers.

## Features

*   **Job Queueing:** Easily queue jobs for background processing.
*   **Worker Pool:** A configurable pool of workers to process jobs concurrently.
*   **Scheduled Jobs:** Schedule jobs to run at specific intervals or using cron expressions.
*   **Conditional Jobs:** Queue jobs that only run if a specific condition is met.
*   **Graceful Shutdown:** The worker pool can be shut down gracefully, ensuring all jobs are completed.
*   **Context-aware:** Jobs are processed with a context that can be used for cancellation.
*   **Customizable Jobs:** Jobs can be customized with timeouts and names.

## Usage

### Initialization

First, create a new worker pool with the desired number of workers:

```go
pool := jobworker.NewPool(5) // Creates a pool with 5 workers
```

Then, initialize the global pool and start listening for jobs:

```go
pool.InitGlobalPool()
go pool.Listen(context.Background())
```

### Queueing a Job

To queue a job, create a new job with a business function and add it to the pool:

```go
job := jobworker.NewJob(func(ctx context.Context) (fmt.Stringer, error) {
    // Your job logic here
    return nil, nil
})
jobworker.QueueJob(job)
```

If you want to postpone execution, pass `JobWithDelay` with a `time.Duration`:

```go
jobworker.QueueJob(job, jobworker.JobWithDelay(time.Minute))
```

### Scheduled Jobs

You can schedule jobs to run at a specific interval:

```go
scheduledJob := jobworker.NewScheduledJob(func(ctx context.Context) (fmt.Stringer, error) {
    // Your job logic here
    return nil, nil
}, 5 * time.Minute) // Runs every 5 minutes
jobworker.ScheduleJob(scheduledJob)
```

Or using a cron expression:

```go
cronJob := jobworker.NewScheduledCronJob(func(ctx context.Context) (fmt.Stringer, error) {
    // Your job logic here
    return nil, nil
}, "0 * * * *") // Runs at the beginning of every hour
jobworker.ScheduleJob(cronJob)
```

### Conditional Jobs with `After`

You can queue jobs that will only be executed if a certain condition is met. This is useful for scenarios like running jobs after a database transaction successfully commits.

```go
after := jobworker.NewAfter()

after.Queue(func(ctx context.Context) (fmt.Stringer, error) {
    // This job will run only if Notify(nil) is called.
    return nil, nil
})

// In a real scenario, you would call Notify based on the outcome of an operation.
// For example, after a database transaction.
var txErr error
// ... perform transaction ...
after.Notify(txErr) // If txErr is nil, the queued jobs will be executed.
```

### Graceful Shutdown

To shut down the worker pool gracefully, call the `Shutdown` method:

```go
pool.Shutdown()
```

This will wait for all workers to finish their current jobs before exiting.

## Components

### `Pool`

The `Pool` manages the workers and the job queue. It's responsible for distributing jobs to the workers and handling scheduled jobs.

### `Worker`

A `Worker` is responsible for processing jobs. It listens for jobs on the job channel and executes them.

### `Job`

A `Job` represents a unit of work to be done. It contains the business logic to be executed.

### `ScheduledJob`

A `ScheduledJob` is a job that is scheduled to run at a later time. It can be scheduled to run at a specific interval or using a cron expression.

### `After`

The `After` struct provides a way to queue jobs that are conditional on the successful completion of another operation. It holds a set of jobs and only queues them to the worker pool when its `Notify` method is called with a `nil` error.
