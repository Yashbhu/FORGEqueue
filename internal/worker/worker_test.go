package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"forgequeue/internal/model"
)

const testRedisAddr = "localhost:6379"

type noopHandler struct{}

func (noopHandler) Handle(ctx context.Context, task *model.TaskMetaData) error {
	return nil
}

type flakyHandler struct {
	mu    sync.Mutex
	calls int
}

// Handle fails the first two executions, then succeeds.
// This simulates a task that gets requeued by recovery once, then completes.
func (h *flakyHandler) Handle(ctx context.Context, task *model.TaskMetaData) error {
	h.mu.Lock()
	h.calls++
	calls := h.calls
	h.mu.Unlock()

	if calls <= 2 {
		return errors.New("boom")
	}
	return nil
}

func (h *flakyHandler) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func enqueueTask(t *testing.T, wp *WorkerPool, taskID string) {
	t.Helper()

	data, err := json.Marshal(model.TaskMetaData{
		ID:         taskID,
		TaskType:   "noop",
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}

	if err := wp.redisClient.Set(wp.ctx, "task:"+taskID, data, 0).Err(); err != nil {
		t.Fatalf("set task body: %v", err)
	}
	if err := wp.redisClient.LPush(wp.ctx, "queue:tasks:immediate", taskID).Err(); err != nil {
		t.Fatalf("enqueue task: %v", err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func zcard(t testing.TB, wp *WorkerPool, key string) int64 {
	t.Helper()
	n, err := wp.redisClient.ZCard(wp.ctx, key).Result()
	if err != nil {
		t.Fatalf("zcard %s: %v", key, err)
	}
	return n
}

func llen(t testing.TB, wp *WorkerPool, key string) int64 {
	t.Helper()
	n, err := wp.redisClient.LLen(wp.ctx, key).Result()
	if err != nil {
		t.Fatalf("llen %s: %v", key, err)
	}
	return n
}

// TestWorkerAckPreventsRequeue verifies the happy path:
// READY -> CLAIM -> IN-FLIGHT -> HANDLER SUCCESS -> ACK -> removed from IN-FLIGHT.
// After the lease expires, recovery must NOT requeue the ACK'd task.
func TestWorkerAckPreventsRequeue(t *testing.T) {
	wp, err := NewWorkerPool(testRedisAddr, 1)
	if err != nil {
		t.Fatalf("new worker pool: %v", err)
	}
	wp.leaseDuration = 200 * time.Millisecond
	wp.RegisterHandler("noop", noopHandler{})

	if err := wp.redisClient.FlushDB(wp.ctx).Err(); err != nil {
		t.Fatalf("flush db: %v", err)
	}
	wp.Start()

	enqueueTask(t, wp, uuid.NewString())

	// Task must leave IN-FLIGHT (ACK'd).
	waitFor(t, 5*time.Second, "task to be ACK'd (IN-FLIGHT empty)", func() bool {
		return zcard(t, wp, "queue:tasks:inflight") == 0
	})
	// Immediate queue must be empty too (nothing pending).
	waitFor(t, 5*time.Second, "immediate queue to drain", func() bool {
		return llen(t, wp, "queue:tasks:immediate") == 0
	})

	// Wait well past the lease so recovery has run many times.
	time.Sleep(6 * time.Second)

	if n := llen(t, wp, "queue:tasks:immediate"); n != 0 {
		t.Fatalf("ACK'd task was requeued by recovery: %d item(s) in immediate queue", n)
	}
	if n := zcard(t, wp, "queue:tasks:inflight"); n != 0 {
		t.Fatalf("ACK'd task still in IN-FLIGHT: %d item(s)", n)
	}

	wp.Stop()
}

// TestWorkerRequeueAfterFailure verifies the error path:
// the task stays IN-FLIGHT, recovery requeues it after the lease expires,
// and it only gets ACK'd once the handler finally succeeds.
func TestWorkerRequeueAfterFailure(t *testing.T) {
	wp, err := NewWorkerPool(testRedisAddr, 1)
	if err != nil {
		t.Fatalf("new worker pool: %v", err)
	}
	wp.leaseDuration = 200 * time.Millisecond
	handler := &flakyHandler{}
	wp.RegisterHandler("noop", handler)

	if err := wp.redisClient.FlushDB(wp.ctx).Err(); err != nil {
		t.Fatalf("flush db: %v", err)
	}
	wp.Start()

	enqueueTask(t, wp, uuid.NewString())

	// The handler must be invoked 3 times: 2 failures (requeued by recovery)
	// then 1 success that gets ACK'd.
	waitFor(t, 10*time.Second, "handler to run 3 times and ACK", func() bool {
		return handler.Count() >= 3 &&
			zcard(t, wp, "queue:tasks:inflight") == 0 &&
			llen(t, wp, "queue:tasks:immediate") == 0
	})

	// Give recovery + workers a chance to incorrectly re-run the task.
	time.Sleep(6 * time.Second)

	if got := handler.Count(); got != 3 {
		t.Fatalf("handler ran %d times, want exactly 3 (2 fails + 1 success)", got)
	}

	wp.Stop()
}

// BenchmarkWorkerThroughput measures end-to-end throughput:
// enqueue N tasks, let the pool process them, report tasks/sec.
func BenchmarkWorkerThroughput(b *testing.B) {
	wp, err := NewWorkerPool(testRedisAddr, 4)
	if err != nil {
		b.Fatalf("new worker pool: %v", err)
	}
	wp.leaseDuration = time.Second
	wp.RegisterHandler("noop", noopHandler{})

	if err := wp.redisClient.FlushDB(context.Background()).Err(); err != nil {
		b.Fatalf("flush db: %v", err)
	}
	wp.Start()
	defer wp.Stop()

	// Enqueue b.N tasks, then time until the pool drains them.
	//
	// The pool starts consuming as we enqueue, which is the realistic
	// steady-state behaviour we want to measure.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("bench-%d", i)
		if err := wp.redisClient.Set(wp.ctx, "task:"+id, `{"id":"`+id+`","task_type":"noop"}`, 0).Err(); err != nil {
			b.Fatalf("set task %d: %v", i, err)
		}
		if err := wp.redisClient.LPush(wp.ctx, "queue:tasks:immediate", id).Err(); err != nil {
			b.Fatalf("enqueue task %d: %v", i, err)
		}
	}

	total := int64(b.N)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if (llen(b, wp, "queue:tasks:immediate") + zcard(b, wp, "queue:tasks:inflight")) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.StopTimer()

	if n := llen(b, wp, "queue:tasks:immediate") + zcard(b, wp, "queue:tasks:inflight"); n != 0 {
		b.Fatalf("pool failed to drain %d task(s) within timeout", n)
	}
	b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "tasks/sec")
}
