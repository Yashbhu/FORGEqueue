package worker

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"forgequeue/internal/durable"
	"forgequeue/internal/model"
)

// pgPool opens a raw connection used to inspect/clean the journal, skipping
// the test when Postgres is not reachable.
func pgPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("FORGEQUEUE_PG_DSN")
	if dsn == "" {
		dsn = durable.DefaultDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("postgres unavailable: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// enqueueJournaled mirrors the gateway's storage-split write: journal the
// task first (pending), then put its body and ID into Redis.
func enqueueJournaled(
	t *testing.T,
	wp *WorkerPool,
	store durable.Store,
	id string,
	taskType string,
	maxRetries int32,
) {
	t.Helper()

	body, err := json.Marshal(model.TaskMetaData{
		ID:         id,
		TaskType:   taskType,
		MaxRetries: maxRetries,
	})
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	if err := store.Enqueue(wp.ctx, durable.Task{
		ID:         id,
		TaskType:   taskType,
		Payload:    body,
		MaxRetries: maxRetries,
		State:      durable.StateEnqueued,
	}); err != nil {
		t.Fatalf("journal enqueue: %v", err)
	}
	if err := wp.redisClient.Set(wp.ctx, "task:"+id, body, 0).Err(); err != nil {
		t.Fatalf("set task body: %v", err)
	}
	if err := wp.redisClient.LPush(wp.ctx, "queue:tasks:immediate", id).Err(); err != nil {
		t.Fatalf("enqueue task: %v", err)
	}
}

// TestWorkerJournalsLifecycle proves the journal follows a task through a
// real execution: acknowledged tasks end up 'succeeded', tasks that burn
// their retry budget end up 'dead' with the right attempt counter.
func TestWorkerJournalsLifecycle(t *testing.T) {
	pool := pgPool(t)
	if _, err := pool.Exec(context.Background(), "TRUNCATE queue_tasks"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := durable.NewPostgres(ctx, durable.DefaultDSN)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	defer store.Close()

	wp, err := NewWorkerPool(testRedisAddr, 1)
	if err != nil {
		t.Fatalf("new worker pool: %v", err)
	}
	wp.leaseDuration = 200 * time.Millisecond
	wp.backoffBase = 200 * time.Millisecond
	counting := &countingHandler{}
	failing := &alwaysFailHandler{}
	wp.RegisterHandler("once", counting)
	wp.RegisterHandler("failing", failing)
	wp.WithStore(store)

	if err := wp.redisClient.FlushDB(wp.ctx).Err(); err != nil {
		t.Fatalf("flush db: %v", err)
	}
	wp.Start()
	defer wp.Stop()

	// Task 1 succeeds on the first run and must land as 'succeeded'.
	idOK := "journal-ok"
	enqueueJournaled(t, wp, store, idOK, "once", 3)
	waitFor(t, 8*time.Second, "task to be acknowledged", func() bool {
		return counting.Count() == 1 && zcard(t, wp, "queue:tasks:inflight") == 0
	})
	var state string
	if err := pool.QueryRow(context.Background(),
		"SELECT state FROM queue_tasks WHERE id = $1", idOK).Scan(&state); err != nil {
		t.Fatalf("query ok task: %v", err)
	}
	if state != string(durable.StateSucceeded) {
		t.Fatalf("journal state = %s, want succeeded", state)
	}

	// Task 2 fails forever: max_retries=2 means exactly two runs, then dead.
	idDead := "journal-dead"
	enqueueJournaled(t, wp, store, idDead, "failing", 2)
	waitFor(t, 8*time.Second, "task to dead-letter", func() bool {
		return failing.Count() == 2 && llen(t, wp, "queue:tasks:dead") == 1
	})
	var state2 string
	var attempts2 int32
	if err := pool.QueryRow(context.Background(),
		"SELECT state, attempts FROM queue_tasks WHERE id = $1", idDead).Scan(&state2, &attempts2); err != nil {
		t.Fatalf("query dead task: %v", err)
	}
	if state2 != string(durable.StateDead) || attempts2 != 2 {
		t.Fatalf("journal state = %s attempts = %d, want dead/2", state2, attempts2)
	}
}

// TestWorkerReconcilerHealsFailedEnqueue proves the double-write heal works
// end to end: a task the gateway journaled but never managed to push into
// Redis (simulated by writing the journal only) is still processed, because
// the pool's reconciler re-materializes it.
func TestWorkerReconcilerHealsFailedEnqueue(t *testing.T) {
	pool := pgPool(t)
	if _, err := pool.Exec(context.Background(), "TRUNCATE queue_tasks"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := durable.NewPostgres(ctx, durable.DefaultDSN)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	defer store.Close()

	wp, err := NewWorkerPool(testRedisAddr, 1)
	if err != nil {
		t.Fatalf("new worker pool: %v", err)
	}
	wp.leaseDuration = 200 * time.Millisecond
	wp.backoffBase = 200 * time.Millisecond
	counting := &countingHandler{}
	wp.RegisterHandler("once", counting)
	wp.WithStore(store)

	if err := wp.redisClient.FlushDB(wp.ctx).Err(); err != nil {
		t.Fatalf("flush db: %v", err)
	}
	wp.Start()
	defer wp.Stop()

	// Only the journal knows about this task. Redis never got the LPUSH.
	id := "journal-only"
	body, err := json.Marshal(model.TaskMetaData{
		ID:         id,
		TaskType:   "once",
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	if err := store.Enqueue(wp.ctx, durable.Task{
		ID:         id,
		TaskType:   "once",
		Payload:    body,
		MaxRetries: 3,
		State:      durable.StateEnqueued,
	}); err != nil {
		t.Fatalf("journal enqueue: %v", err)
	}

	// The reconciler (running inside the pool) must notice the pending task,
	// materialize it into Redis, and the worker must then run it exactly once.
	waitFor(t, 8*time.Second, "journaled task to be materialized and run", func() bool {
		return counting.Count() == 1 && zcard(t, wp, "queue:tasks:inflight") == 0
	})
	if counting.Count() != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", counting.Count())
	}
}
