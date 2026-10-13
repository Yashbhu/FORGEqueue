package durable

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// newTestRedis connects to local Redis and flushes it, so each reconciler
// scenario starts from a clean hot store.
func newTestRedis(t testing.TB) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flush db: %v", err)
	}
	return c
}

func TestReconcileMaterializesImmediate(t *testing.T) {
	rdb := newTestRedis(t)
	p := newTestDB(t)
	truncateTasks(t, p)
	ctx := context.Background()

	id := "rec-immediate"
	body := []byte(`{"id":"rec-immediate","task_type":"noop"}`)
	if err := p.Enqueue(ctx, Task{
		ID: id, TaskType: "noop", Payload: body, State: StateEnqueued,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Redis is empty (simulates a gateway that journaled a task and then
	// died before writing to the hot store), so the reconciler must put the
	// task on the ready list with its body in place.
	repaired, _, _, err := ReconcileOnce(ctx, rdb, p, Cursor{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if repaired != 1 {
		t.Fatalf("repaired = %d, want 1", repaired)
	}

	if n, _ := rdb.LLen(ctx, "queue:tasks:immediate").Result(); n != 1 {
		t.Fatalf("immediate queue has %d item(s), want 1", n)
	}
	if n, _ := rdb.Exists(ctx, "task:"+id).Result(); n != 1 {
		t.Fatalf("body task:%s missing after materialize", id)
	}
	got, err := rdb.Get(ctx, "task:"+id).Result()
	if err != nil || got != string(body) {
		t.Fatalf("body = %q, want %q (err=%v)", got, body, err)
	}

	// A second sweep must be a no-op: the task is still non-terminal, but it
	// is already in Redis so nothing is repaired and no duplicate appears.
	repaired, _, _, err = ReconcileOnce(ctx, rdb, p, Cursor{})
	if err != nil {
		t.Fatalf("reconcile again: %v", err)
	}
	if repaired != 0 {
		t.Fatalf("second sweep repaired = %d, want 0", repaired)
	}
	if n, _ := rdb.LLen(ctx, "queue:tasks:immediate").Result(); n != 1 {
		t.Fatalf("immediate queue has %d item(s) after second sweep, want 1", n)
	}
}

func TestReconcileMaterializesDelayed(t *testing.T) {
	rdb := newTestRedis(t)
	p := newTestDB(t)
	truncateTasks(t, p)
	ctx := context.Background()

	id := "rec-delayed"
	body := []byte(`{"id":"rec-delayed","task_type":"noop"}`)
	due := time.Now().Add(3 * time.Minute)
	if err := p.Enqueue(ctx, Task{
		ID: id, TaskType: "noop", Payload: body, State: StateEnqueued, ScheduleAt: due,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, _, _, err := ReconcileOnce(ctx, rdb, p, Cursor{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if n, _ := rdb.LLen(ctx, "queue:tasks:immediate").Result(); n != 0 {
		t.Fatalf("immediate queue has %d item(s), want 0 for a delayed task", n)
	}
	score, err := rdb.ZScore(ctx, "queue:tasks:scheduled", id).Result()
	if err != nil {
		t.Fatalf("task not scheduled: %v", err)
	}
	if int64(score) != due.Unix() {
		t.Fatalf("scheduled score = %d, want %d", int64(score), due.Unix())
	}
}

func TestReconcileSkipsTaskAlreadyInRedis(t *testing.T) {
	rdb := newTestRedis(t)
	p := newTestDB(t)
	truncateTasks(t, p)
	ctx := context.Background()

	id := "rec-present"
	body := []byte(`{"id":"rec-present","task_type":"noop"}`)

	// The full gateway happy path: journal AND Redis both have the task.
	if err := p.Enqueue(ctx, Task{
		ID: id, TaskType: "noop", Payload: body, State: StateEnqueued,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := rdb.Set(ctx, "task:"+id, body, 0).Err(); err != nil {
		t.Fatalf("set body: %v", err)
	}
	if err := rdb.LPush(ctx, "queue:tasks:immediate", id).Err(); err != nil {
		t.Fatalf("lpush: %v", err)
	}

	repaired, _, _, err := ReconcileOnce(ctx, rdb, p, Cursor{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if repaired != 0 {
		t.Fatalf("repaired = %d, want 0 (task already in Redis)", repaired)
	}

	// No duplicate was pushed onto the ready list.
	if n, _ := rdb.LLen(ctx, "queue:tasks:immediate").Result(); n != 1 {
		t.Fatalf("immediate queue has %d item(s), want 1 (no dupes)", n)
	}
}

func TestReconcileRebuildsAfterRedisWipe(t *testing.T) {
	rdb := newTestRedis(t)
	p := newTestDB(t)
	truncateTasks(t, p)
	ctx := context.Background()

	id := "rec-wipe"
	body := []byte(`{"id":"rec-wipe","task_type":"noop"}`)

	// A worker claimed and materialized this task (so redis_pending is
	// already FALSE), then Redis lost everything. Because the reconciler
	// re-verifies every non-terminal task, not just redis_pending rows, it
	// must bring the task back and schedule it for a re-run (past-now score),
	// like recovery would.
	if _, err := p.pool.Exec(ctx, `
		INSERT INTO queue_tasks (id, task_type, payload, max_retries, attempts, state, redis_pending)
		VALUES ($1, 'noop', $2, 3, 1, 'processing', FALSE)
	`, id, body); err != nil {
		t.Fatalf("seed processing row: %v", err)
	}

	if _, _, _, err := ReconcileOnce(ctx, rdb, p, Cursor{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	score, err := rdb.ZScore(ctx, "queue:tasks:scheduled", id).Result()
	if err != nil {
		t.Fatalf("task not re-scheduled: %v", err)
	}
	ub := time.Now().Add(2 * time.Second).Unix()
	if int64(score) > ub {
		t.Fatalf("rescheduled score = %d, want now (<= %d)", int64(score), ub)
	}
}
