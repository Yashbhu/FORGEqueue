package durable

import (
	"context"
	"os"
	"testing"
	"time"
)

// newTestDB connects to Postgres and skips the test when it's unreachable,
// so machines without Postgres can still run the rest of the suite.
func newTestDB(t testing.TB) *Postgres {
	t.Helper()
	dsn := os.Getenv("FORGEQUEUE_PG_DSN")
	if dsn == "" {
		dsn = DefaultDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Skipf("postgres unavailable (set FORGEQUEUE_PG_DSN to run): %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func truncateTasks(t testing.TB, p *Postgres) {
	t.Helper()
	if _, err := p.pool.Exec(context.Background(), "TRUNCATE queue_tasks"); err != nil {
		t.Fatalf("truncate queue_tasks: %v", err)
	}
}

func rowState(t testing.TB, p *Postgres, id string) (string, int32) {
	t.Helper()
	var state string
	var attempts int32
	if err := p.pool.QueryRow(
		context.Background(),
		"SELECT state, attempts FROM queue_tasks WHERE id = $1",
		id,
	).Scan(&state, &attempts); err != nil {
		t.Fatalf("query state for %s: %v", id, err)
	}
	return state, attempts
}

func TestPostgresLifecycle(t *testing.T) {
	p := newTestDB(t)
	truncateTasks(t, p)
	ctx := context.Background()

	id := "life-1"
	body := []byte(`{"id":"life-1","task_type":"noop"}`)

	// Enqueue leaves redis_pending = true so the reconciler can prove it.
	if err := p.Enqueue(ctx, Task{
		ID: id, TaskType: "noop", Payload: body, State: StateEnqueued,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	pending, err := p.Pending(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != id || pending[0].ScheduleAt != (time.Time{}) {
		t.Fatalf("pending = %+v, want one immediate task %s", pending, id)
	}

	// Materialized proves Redis has it -> no longer pending.
	if err := p.Materialized(ctx, id); err != nil {
		t.Fatalf("materialized: %v", err)
	}
	if pending, _ := p.Pending(ctx); len(pending) != 0 {
		t.Fatalf("pending has %d task(s) after materialized, want 0", len(pending))
	}

	// The journal follows the worker's view: processing -> retrying -> done.
	if err := p.Processing(ctx, id); err != nil {
		t.Fatalf("processing: %v", err)
	}
	if st, _ := rowState(t, p, id); st != string(StateProcessing) {
		t.Fatalf("state = %s, want processing", st)
	}

	if err := p.Retrying(ctx, id, 1, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("retrying: %v", err)
	}
	if st, a := rowState(t, p, id); st != string(StateRetrying) || a != 1 {
		t.Fatalf("state = %s attempts = %d, want retrying/1", st, a)
	}

	if err := p.Succeeded(ctx, id); err != nil {
		t.Fatalf("succeeded: %v", err)
	}
	if st, _ := rowState(t, p, id); st != string(StateSucceeded) {
		t.Fatalf("state = %s, want succeeded", st)
	}

	// A terminal state must never surface as pending.
	if pending, _ := p.Pending(ctx); len(pending) != 0 {
		t.Fatalf("pending has %d task(s) after terminal state, want 0", len(pending))
	}
}

func TestPostgresEnqueueIsUpsert(t *testing.T) {
	p := newTestDB(t)
	truncateTasks(t, p)
	ctx := context.Background()

	id := "upsert-1"
	body := []byte(`{"id":"upsert-1","task_type":"noop"}`)
	task := Task{ID: id, TaskType: "noop", Payload: body, State: StateEnqueued}

	if err := p.Enqueue(ctx, task); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	// Re-enqueueing the same id (e.g. a client retry at the gateway) must
	// update the row and set pending again, not explode on the PK.
	if err := p.Enqueue(ctx, task); err != nil {
		t.Fatalf("second enqueue: %v", err)
	}

	var state string
	if err := p.pool.QueryRow(ctx, `
		SELECT state FROM queue_tasks WHERE id = $1
	`, id).Scan(&state); err != nil {
		t.Fatalf("query: %v", err)
	}

	pending, _ := p.Pending(ctx)
	if len(pending) != 1 {
		t.Fatalf("pending has %d task(s), want 1", len(pending))
	}
	if state != string(StateEnqueued) {
		t.Fatalf("state = %s, want enqueued", state)
	}
}
