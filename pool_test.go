package jobworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListen tests the creation of a new Pool.
func TestListen(t *testing.T) {
	numWorkers := 5
	pool, err := Listen(context.Background(), numWorkers, discardLogger())

	assert.NoError(t, err)
	assert.NotNil(t, pool)
	assert.Len(t, pool.workers, numWorkers)
	assert.NotNil(t, pool.waitingJobs)
	assert.NotNil(t, pool.workerFinished)
	assert.NotNil(t, pool.stopped)
	assert.False(t, pool.stopped.Load())

	assert.NoError(t, pool.Shutdown())
}

// TestPoolListenAndShutdown tests the lifecycle of the pool.
func TestPoolListenAndShutdown(t *testing.T) {
	pool, err := Listen(context.Background(), 2, discardLogger())
	assert.NoError(t, err)
	assert.NotNil(t, pool)
	assert.NoError(t, pool.Shutdown())
}

// TestPoolQueueJob tests queuing a job to the pool.
func TestPoolQueueJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool, err := Listen(ctx, 1, discardLogger())
	defer cancel()
	assert.NoError(t, err)
	assert.NotNil(t, pool)

	done := make(chan struct{})
	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		close(done)
		return nil, nil
	})

	pool.QueueJob(job)

	select {
	case <-done:
		// Job processed successfully
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for job to be processed")
	}

	pool.Shutdown()
}

// TestPoolQueueJob_WhenStopped tests that jobs are not queued when pool is stopped.
func TestPoolQueueJob_WhenStopped(t *testing.T) {
	pool, err := Listen(context.Background(), 1, discardLogger())
	assert.NoError(t, err)
	assert.NotNil(t, pool)

	pool.stopped.Store(true)

	job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		return nil, nil
	})

	// This should not block since the pool is stopped
	pool.QueueJob(job)

	assert.Equal(t, StateWaiting, job.getState())
}

// TestPoolScheduleJob tests scheduling a job.
func TestPoolScheduleJob(t *testing.T) {
	pool, err := Listen(context.Background(), 1, discardLogger())
	assert.NoError(t, err)
	assert.NotNil(t, pool)

	scheduledJob := NewScheduledJob(func(ctx context.Context) (fmt.Stringer, error) {
		return nil, nil
	}, 100*time.Millisecond)

	pool.ScheduleJob(scheduledJob)

	pool.scheduledMu.RLock()
	assert.Len(t, pool.scheduledJobs, 1)
	assert.Equal(t, StateNew, scheduledJob.getState())
	pool.scheduledMu.RUnlock()

	assert.NoError(t, pool.Shutdown())
}

// TestPoolInitGlobalPool tests setting the pool as global.
func TestPoolInitGlobalPool(t *testing.T) {
	p, err := Listen(context.Background(), 1, discardLogger())
	assert.NoError(t, err)
	p.InitGlobalPool()

	assert.Equal(t, p, pool)

	assert.NoError(t, p.Shutdown())
}

// TestPoolMultipleWorkers tests that multiple workers can process jobs concurrently.
func TestPoolMultipleWorkers(t *testing.T) {
	numWorkers := 3
	ctx, cancel := context.WithCancel(context.Background())
	p, err := Listen(ctx, numWorkers, discardLogger())
	defer cancel()
	assert.NoError(t, err)
	assert.NotNil(t, p)

	var wg sync.WaitGroup
	processedCount := 0
	var mu sync.Mutex

	for i := 0; i < 5; i++ {
		wg.Add(1)
		job := NewJob(func(ctx context.Context) (fmt.Stringer, error) {
			mu.Lock()
			processedCount++
			mu.Unlock()
			wg.Done()
			return nil, nil
		})
		p.QueueJob(job)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		assert.Equal(t, 5, processedCount)
		mu.Unlock()
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for jobs to be processed")
	}

	_ = p.Shutdown()
}

// discardLogger returns a logger that drops every record.
func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// lockedBuffer is an io.Writer that is safe to read while workers write to it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// jsonLogger returns a debug-level JSON logger writing into buf.
func jsonLogger(buf *lockedBuffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// waitForRecord waits until buf holds a JSON record with the given message and returns it.
func waitForRecord(t *testing.T, buf *lockedBuffer, msg string) map[string]any {
	t.Helper()
	var found map[string]any
	require.Eventually(t, func() bool {
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			var rec map[string]any
			if json.Unmarshal([]byte(line), &rec) == nil && rec["msg"] == msg {
				found = rec
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "no %q record in:\n%s", msg, buf.String())
	return found
}

// TestListen_NilLoggerUsesSlogDefault tests that a nil logger falls back to slog.Default().
func TestListen_NilLoggerUsesSlogDefault(t *testing.T) {
	var buf lockedBuffer
	// slog.SetDefault also redirects the standard log package (output and
	// flags), and restoring the previous slog logger does not undo that, so
	// restore all three.
	previous, previousOutput, previousFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(jsonLogger(&buf))
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(previousOutput)
		log.SetFlags(previousFlags)
	})

	p, err := Listen(context.Background(), 1, nil)
	require.NoError(t, err)
	require.NoError(t, p.Shutdown())

	rec := waitForRecord(t, &buf, "pool is listening")
	assert.Equal(t, "jobworker", rec["component"])
}

// TestPool_LogsDelayedJobs tests the job and delay attributes of the delay log.
func TestPool_LogsDelayedJobs(t *testing.T) {
	var buf lockedBuffer
	p, err := Listen(context.Background(), 1, jsonLogger(&buf))
	require.NoError(t, err)
	defer func() { _ = p.Shutdown() }()

	p.QueueJob(NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		return nil, nil
	}, JobWithName("delayed-job"), JobWithDelay(time.Hour)))

	rec := waitForRecord(t, &buf, "job is delayed")
	assert.Equal(t, "jobworker", rec["component"])
	assert.Equal(t, "delayed-job", rec["job"])
	assert.Equal(t, float64(time.Hour), rec["delay"])
}

// TestPool_ShutdownWaitsForRunningJob tests graceful shutdown: Shutdown returns
// only after the job that a worker is running has finished.
func TestPool_ShutdownWaitsForRunningJob(t *testing.T) {
	p, err := Listen(context.Background(), 1, discardLogger())
	require.NoError(t, err)

	started := make(chan struct{})
	finished := make(chan struct{})
	p.QueueJob(NewJob(func(ctx context.Context) (fmt.Stringer, error) {
		close(started)
		time.Sleep(200 * time.Millisecond)
		close(finished)
		return nil, nil
	}, JobWithName("slow")))

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the job never started")
	}
	require.NoError(t, p.Shutdown())

	select {
	case <-finished:
	default:
		t.Fatal("Shutdown returned before the running job finished")
	}
}

// TestPool_ShutdownConcurrentWithQueueJob hammers QueueJob from several goroutines while
// Shutdown runs. Before the fix, `go test -race` reports a data race between close(waitingJobs)
// and a send, and a send on the closed channel logs "job queueing failed".
func TestPool_ShutdownConcurrentWithQueueJob(t *testing.T) {
	noop := func(context.Context) (fmt.Stringer, error) { return nil, nil }

	for round := 0; round < 50; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		logs := &lockedBuffer{}
		logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

		p, err := Listen(ctx, 2, logger)
		require.NoError(t, err)

		stop := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						p.QueueJob(NewJob(noop))
					}
				}
			}()
		}

		time.Sleep(2 * time.Millisecond)
		require.NoError(t, p.Shutdown())
		close(stop)
		wg.Wait()
		cancel()

		require.NotContains(t, logs.String(), "job queueing failed", "round %d", round)
	}
}

// TestPool_ShutdownWithSenderBlockedOnFullQueue covers the shutdown order of an application:
// its context is cancelled first, so the workers stop draining the queue, and Shutdown runs
// while a sender (a QueueJob caller or the scheduler tick) is blocked on the full queue. The
// sender must give up when shutdown starts; otherwise it keeps the read lock of sendMu and
// Shutdown waits for the write lock forever.
func TestPool_ShutdownWithSenderBlockedOnFullQueue(t *testing.T) {
	noop := func(context.Context) (fmt.Stringer, error) { return nil, nil }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := Listen(ctx, 2, discardLogger())
	require.NoError(t, err)

	cancel()
	time.Sleep(20 * time.Millisecond) // the workers leave their loop and stop draining the queue

	for i := 0; i < queueSize; i++ {
		p.QueueJob(NewJob(noop))
	}
	sent := make(chan struct{})
	go func() {
		p.QueueJob(NewJob(noop)) // blocks: the queue is full and nobody drains it
		close(sent)
	}()
	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		_ = p.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown deadlocked with a sender blocked on the full queue")
	}
	<-sent
}

// shutdownOnDropHandler calls Shutdown of its pool from inside every "jobs worker pool is
// stopped" record, i.e. it calls back into the pool while the pool is logging.
type shutdownOnDropHandler struct {
	pool atomic.Pointer[Pool]
}

func (h *shutdownOnDropHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *shutdownOnDropHandler) Handle(_ context.Context, r slog.Record) error {
	if p := h.pool.Load(); p != nil && r.Message == "jobs worker pool is stopped" {
		_ = p.Shutdown()
	}
	return nil
}

func (h *shutdownOnDropHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *shutdownOnDropHandler) WithGroup(string) slog.Handler { return h }

// TestPool_ShutdownWhenLoggerCallsBackIntoPool checks that a sender does not call the logger
// while it holds the read lock of sendMu. Otherwise a handler that calls back into the pool
// (here: Shutdown, which waits for the Shutdown already running) waits for that Shutdown, and
// that Shutdown waits for the handler's read lock.
func TestPool_ShutdownWhenLoggerCallsBackIntoPool(t *testing.T) {
	noop := func(context.Context) (fmt.Stringer, error) { return nil, nil }

	h := &shutdownOnDropHandler{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := Listen(ctx, 2, slog.New(h))
	require.NoError(t, err)
	h.pool.Store(p)

	cancel()
	time.Sleep(20 * time.Millisecond) // the workers leave their loop and stop draining the queue

	for i := 0; i < queueSize; i++ {
		p.QueueJob(NewJob(noop))
	}
	sent := make(chan struct{})
	go func() {
		p.QueueJob(NewJob(noop)) // blocks on the full queue, then logs that the job is dropped
		close(sent)
	}()
	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		_ = p.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown deadlocked with a logger that calls back into the pool")
	}
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("the dropped sender never returned")
	}
}
