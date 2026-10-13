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

	// Enqueue leaves the task non-terminal, so the reconciler sees it.
	if err := p.Enqueue(ctx, Task{
		ID: id, TaskType: "noop", Payload: body, State: StateEnqueued,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	active, err := p.Active(ctx, Cursor{}, 100)
	if err != nil {
		t.Fatalf("active: %v", err)
	}
	if len(active) != 1 || active[0].ID != id || active[0].ScheduleAt != (time.Time{}) {
		t.Fatalf("active = %+v, want one immediate task %s", active, id)
	}

	// Materialized clears the redis_pending hint, but the task is still
	// non-terminal and must keep being re-verified: that is what lets the
	// reconciler rebuild Redis after a flush.
	if err := p.Materialized(ctx, id); err != nil {
		t.Fatalf("materialized: %v", err)
	}
	if active, _ := p.Active(ctx, Cursor{}, 100); len(active) != 1 {
		t.Fatalf("active has %d task(s) after materialized, want 1", len(active))
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

	// A terminal state must never surface as active.
	if active, _ := p.Active(ctx, Cursor{}, 100); len(active) != 0 {
		t.Fatalf("active has %d task(s) after terminal state, want 0", len(active))
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

	active, _ := p.Active(ctx, Cursor{}, 100)
	if len(active) != 1 {
		t.Fatalf("active has %d task(s), want 1", len(active))
	}
	if state != string(StateEnqueued) {
		t.Fatalf("state = %s, want enqueued", state)
	}
}

// Active must walk rows in keyset order so a sweep can page through a large
// active set without re-reading or skipping rows.
func TestPostgresActivePaginates(t *testing.T) {
	p := newTestDB(t)
	truncateTasks(t, p)
	ctx := context.Background()

	for _, id := range []string{"page-1", "page-2", "page-3"} {
		if err := p.Enqueue(ctx, Task{
			ID: id, TaskType: "noop", Payload: []byte(`{}`), State: StateEnqueued,
		}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	var seen []string
	cursor := Cursor{}
	for {
		page, err := p.Active(ctx, cursor, 1)
		if err != nil {
			t.Fatalf("active: %v", err)
		}
		if len(page) == 0 {
			break
		}
		seen = append(seen, page[0].ID)
		cursor = Cursor{CreatedAt: page[0].CreatedAt, ID: page[0].ID}
	}

	if len(seen) != 3 {
		t.Fatalf("paged over %v, want all 3 tasks", seen)
	}
}
