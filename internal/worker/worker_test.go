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
	"github.com/redis/go-redis/v9"

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

// alwaysFailHandler fails every execution, so a task with a finite retry
// budget eventually exhausts it and lands in the dead-letter queue.
type alwaysFailHandler struct {
	mu    sync.Mutex
	calls int
}

func (h *alwaysFailHandler) Handle(ctx context.Context, task *model.TaskMetaData) error {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	return errors.New("boom")
}

func (h *alwaysFailHandler) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// countingHandler succeeds every time and only tracks how often it ran,
// useful for asserting a delayed task is promoted and processed exactly once.
type countingHandler struct {
	mu sync.Mutex
	n  int
}

func (h *countingHandler) Handle(ctx context.Context, task *model.TaskMetaData) error {
	h.mu.Lock()
	h.n++
	h.mu.Unlock()
	return nil
}

func (h *countingHandler) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

func enqueueTask(t *testing.T, wp *WorkerPool, taskID string, maxRetries int32) {
	t.Helper()

	data, err := json.Marshal(model.TaskMetaData{
		ID:         taskID,
		TaskType:   "noop",
		MaxRetries: maxRetries,
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

	enqueueTask(t, wp, uuid.NewString(), 0)

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
// a failing handler burns a retry budget. The task goes into backoff, gets
// promoted again, and is only ACK'd once the handler finally succeeds.
func TestWorkerRequeueAfterFailure(t *testing.T) {
	wp, err := NewWorkerPool(testRedisAddr, 1)
	if err != nil {
		t.Fatalf("new worker pool: %v", err)
	}
	wp.leaseDuration = 200 * time.Millisecond
	wp.backoffBase = 200 * time.Millisecond
	handler := &flakyHandler{}
	wp.RegisterHandler("noop", handler)

	if err := wp.redisClient.FlushDB(wp.ctx).Err(); err != nil {
		t.Fatalf("flush db: %v", err)
	}
	wp.Start()

	// max_retries=3 leaves room for 2 failures + the final success.
	enqueueTask(t, wp, uuid.NewString(), 3)

	// The handler must be invoked 3 times: 2 failures (each requeued with
	// backoff) then 1 success that gets ACK'd.
	waitFor(t, 10*time.Second, "handler to run 3 times and ACK", func() bool {
		return handler.Count() >= 3 &&
			zcard(t, wp, "queue:tasks:inflight") == 0 &&
			llen(t, wp, "queue:tasks:immediate") == 0 &&
			zcard(t, wp, "queue:tasks:scheduled") == 0
	})

	// Give recovery + workers a chance to incorrectly re-run the task.
	time.Sleep(6 * time.Second)

	if got := handler.Count(); got != 3 {
		t.Fatalf("handler ran %d times, want exactly 3 (2 fails + 1 success)", got)
	}

	wp.Stop()
}

// TestWorkerDeadLettersAfterRetryBudget verifies the retry-budget cap:
// a task that keeps failing moves to the dead-letter queue once its
// max_retries budget runs out, instead of being retried forever.
func TestWorkerDeadLettersAfterRetryBudget(t *testing.T) {
	wp, err := NewWorkerPool(testRedisAddr, 1)
	if err != nil {
		t.Fatalf("new worker pool: %v", err)
	}
	wp.leaseDuration = 200 * time.Millisecond
	wp.backoffBase = 200 * time.Millisecond
	handler := &alwaysFailHandler{}
	wp.RegisterHandler("noop", handler)

	if err := wp.redisClient.FlushDB(wp.ctx).Err(); err != nil {
		t.Fatalf("flush db: %v", err)
	}
	wp.Start()
	defer wp.Stop()

	enqueueTask(t, wp, uuid.NewString(), 2)

	// With max_retries=2 the handler runs exactly twice, then the task must
	// sit in the dead-letter queue and leave every active queue alone.
	waitFor(t, 10*time.Second, "task to reach the dead-letter queue", func() bool {
		return handler.Count() >= 2 && llen(t, wp, "queue:tasks:dead") == 1
	})

	// Give the subsystems a chance to wrongly re-run the task.
	time.Sleep(3 * time.Second)

	if got := handler.Count(); got != 2 {
		t.Fatalf("handler ran %d times, want exactly 2 (retry budget)", got)
	}
	if n := llen(t, wp, "queue:tasks:dead"); n != 1 {
		t.Fatalf("dead-letter queue has %d item(s), want 1", n)
	}
	if n := zcard(t, wp, "queue:tasks:scheduled"); n != 0 {
		t.Fatalf("task still scheduled: %d item(s)", n)
	}
	if n := llen(t, wp, "queue:tasks:immediate"); n != 0 {
		t.Fatalf("task still in immediate queue: %d item(s)", n)
	}
	if n := zcard(t, wp, "queue:tasks:inflight"); n != 0 {
		t.Fatalf("task still in-flight: %d item(s)", n)
	}
}

// TestWorkerDelayedTaskPromotion verifies the scheduled set is consumed:
// a task enqueued with a delay must be promoted by the promotion loop and
// processed exactly once.
func TestWorkerDelayedTaskPromotion(t *testing.T) {
	wp, err := NewWorkerPool(testRedisAddr, 1)
	if err != nil {
		t.Fatalf("new worker pool: %v", err)
	}
	wp.leaseDuration = 200 * time.Millisecond
	wp.backoffBase = 200 * time.Millisecond
	handler := &countingHandler{}
	wp.RegisterHandler("noop", handler)

	if err := wp.redisClient.FlushDB(wp.ctx).Err(); err != nil {
		t.Fatalf("flush db: %v", err)
	}
	wp.Start()
	defer wp.Stop()

	taskID := uuid.NewString()
	body, err := json.Marshal(model.TaskMetaData{
		ID:         taskID,
		TaskType:   "noop",
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	if err := wp.redisClient.Set(wp.ctx, "task:"+taskID, body, 0).Err(); err != nil {
		t.Fatalf("set task body: %v", err)
	}
	// Schedule it 1 second in the future, exactly like the gateway does.
	if err := wp.redisClient.ZAdd(wp.ctx, "queue:tasks:scheduled", redis.Z{
		Score:  float64(time.Now().Unix() + 1),
		Member: taskID,
	}).Err(); err != nil {
		t.Fatalf("schedule task: %v", err)
	}

	// It must be promoted and land exactly once in the handler (1 success).
	waitFor(t, 8*time.Second, "delayed task to be processed once", func() bool {
		return handler.Count() == 1 &&
			llen(t, wp, "queue:tasks:immediate") == 0 &&
			zcard(t, wp, "queue:tasks:scheduled") == 0 &&
			zcard(t, wp, "queue:tasks:inflight") == 0
	})
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

// BenchmarkWorkerThroughputPreloaded measures processing throughput only:
// all N tasks are enqueued before timing starts, so the timer captures the
// pure drain rate (no enqueue work pollutes the measurement).
func BenchmarkWorkerThroughputPreloaded(b *testing.B) {
	wp, err := NewWorkerPool(testRedisAddr, 4)
	if err != nil {
		b.Fatalf("new worker pool: %v", err)
	}
	wp.leaseDuration = time.Second
	wp.RegisterHandler("noop", noopHandler{})

	if err := wp.redisClient.FlushDB(context.Background()).Err(); err != nil {
		b.Fatalf("flush db: %v", err)
	}

	// Enqueue all N tasks before timing begins.
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("bench-%d", i)
		if err := wp.redisClient.Set(wp.ctx, "task:"+id, `{"id":"`+id+`","task_type":"noop"}`, 0).Err(); err != nil {
			b.Fatalf("set task %d: %v", i, err)
		}
		if err := wp.redisClient.LPush(wp.ctx, "queue:tasks:immediate", id).Err(); err != nil {
			b.Fatalf("enqueue task %d: %v", i, err)
		}
	}

	wp.Start()
	defer wp.Stop()

	b.ResetTimer()
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
