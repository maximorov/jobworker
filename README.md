# Jobworker

`github.com/maximorov/jobworker` is a small in-process background job runner for Go: a pool of
workers, a queue, interval and cron schedules, and graceful shutdown. Jobs live in memory only;
durable work belongs in your database (for example an outbox table polled by a scheduled job).

```sh
go get github.com/maximorov/jobworker@v1.8.0
```

## Features

*   **Job Queueing:** Easily queue jobs for background processing.
*   **Worker Pool:** A configurable pool of workers to process jobs concurrently.
*   **Scheduled Jobs:** Schedule jobs to run at specific intervals or using cron expressions.
*   **Conditional Jobs:** Queue jobs that only run if a specific condition is met.
*   **Graceful Shutdown:** The worker pool can be shut down gracefully, ensuring all jobs are completed.
*   **Context-aware:** Jobs are processed with a context that can be used for cancellation.
*   **Customizable Jobs:** Jobs can be customized with timeouts and names.
*   **Structured logging:** The pool logs through the standard library `log/slog`.

## Usage

### Initialization

Create a worker pool with the desired number of workers and a `*slog.Logger`
(`nil` means `slog.Default()`):

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

pool, err := jobworker.Listen(context.Background(), 5, logger) // 5 workers, started
if err != nil {
    panic(err)
}
```

To use the package-level helpers (`QueueJob`, `QueueJobIf`, `After`), make the pool global:

```go
pool.InitGlobalPool()
```

### Logging

Every record carries `component=jobworker`. Job records add these attributes:

| Attribute | Value |
|---|---|
| `job` | job name (`JobWithName`, or a random UUID) |
| `worker_id` | worker number, on panics |
| `delay` | the `JobWithDelay` duration, on delayed jobs |
| `result` | `String()` of the job result, on success |
| `error` | error message (string), on failures and panics |
| `internal` | `GetInternal().Error()` when the error implements `InternalErrorHolder` |

### Queueing a Job

To queue a job, create a new job with a business function and add it to the pool:

```go
job := jobworker.NewJob(func(ctx context.Context) (fmt.Stringer, error) {
    // Your job logic here
    return nil, nil
})
pool.QueueJob(job)
```

With a global pool, `jobworker.QueueJob(job, opts...)` does the same and applies options.
To postpone execution, pass `JobWithDelay`:

```go
jobworker.QueueJob(job, jobworker.JobWithDelay(time.Minute))
```

### Scheduled Jobs

Schedule a job to run at an interval:

```go
scheduledJob := jobworker.NewScheduledJob(func(ctx context.Context) (fmt.Stringer, error) {
    // Your job logic here
    return nil, nil
}, 5*time.Minute, jobworker.JobWithName("cleanup"), jobworker.JobWithTimeout(time.Minute))
pool.ScheduleJob(scheduledJob)
```

Or with a cron expression (minute, hour, day of month, month, day of week):

```go
cronJob, err := jobworker.NewScheduledCronJob(func(ctx context.Context) (fmt.Stringer, error) {
    // Your job logic here
    return nil, nil
}, "0 * * * *") // at the beginning of every hour
if err != nil {
    panic(err)
}
pool.ScheduleJob(cronJob)
```

A scheduled job is not queued again while its previous run is still waiting or processing.
Schedules run in every process that registers them; to run a schedule once per cluster, guard
the job body with a database lock (for example `pg_try_advisory_lock`) in your application.

### Conditional Jobs with `After`

You can queue jobs that will only be executed if a certain condition is met (global pool only):

```go
after := jobworker.NewAfter()

after.Queue(func(ctx context.Context) (fmt.Stringer, error) {
    // This job will run only if Notify(nil) is called.
    return nil, nil
})

var txErr error
// run the database transaction and keep its error in txErr
after.Notify(txErr) // If txErr is nil, the queued jobs will be executed.
```

### Graceful Shutdown

```go
if err := pool.Shutdown(); err != nil {
    return err
}
```

`Shutdown` stops accepting jobs and waits for every worker to finish its current job.

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

## Changes

See [CHANGELOG.md](CHANGELOG.md).
