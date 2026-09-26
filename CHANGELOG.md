# Changelog

All notable changes to this module are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.8.0] - 2026-09-26

First release whose `go.mod` declares `github.com/maximorov/jobworker`. Earlier tags declare
`github.com/Cery-Tech/jobworker` and cannot be fetched under this path.

### Changed

- Module path is `github.com/maximorov/jobworker` (was `github.com/Cery-Tech/jobworker`).
- **Breaking:** `Listen(ctx context.Context, workersNum int, logger *slog.Logger) (*Pool, error)`
  takes a standard library `*slog.Logger` instead of `*github.com/Cery-Tech/log/v2.Logger`.
  A `nil` logger means `slog.Default()`.
- Pool records carry `component=jobworker` (was `cat="Job Worker"`). Job attributes are `job`,
  `worker_id`, `delay`, `result`, `error` and `internal`; `error`, `internal` and `result` are
  strings.

### Removed

- The dependency on `github.com/Cery-Tech/log/v2` and, with it, on `getsentry/sentry-go`.

Every other exported identifier keeps its signature.

### Fixed

- `Pool.Shutdown` no longer races concurrent `QueueJob` calls: the queue is closed only after
  in-flight sends finish, and a sender blocked on a full queue gives up when shutdown starts
  (previously a send on the closed channel panicked and was logged as "job queueing failed").
