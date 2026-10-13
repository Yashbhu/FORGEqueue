package durable

import (
	"context"
	"time"
)

// State is the lifecycle state of a task as the journal records it. The
// journal is the durable side of the queue, plain Postgres rows that survive
// a crash or a Redis flush.
type State string

const (
	// StateEnqueued means "accepted by the gateway, not yet run".
	StateEnqueued State = "enqueued"
	// StateProcessing means "claimed by a worker right now".
	StateProcessing State = "processing"
	// StateRetrying means "in backoff, scheduled for another attempt".
	StateRetrying State = "retrying"
	// StateSucceeded means "handler returned nil and the task was ACKed".
	// Terminal.
	StateSucceeded State = "succeeded"
	// StateDead means "retry budget exhausted, moved to the dead-letter
	// queue". Terminal.
	StateDead State = "dead"
)

// Task is the durable record for one task. Payload is the exact JSON body
// Redis stores at task:<id>, which lets the reconciler rebuild Redis from
// scratch after a crash or flush.
type Task struct {
	ID         string
	TaskType   string
	Payload    []byte
	MaxRetries int32
	Attempts   int32
	State      State
	// ScheduleAt is when an enqueued/retrying task is due. Zero means no
	// delay, so it goes to the immediate queue.
	ScheduleAt time.Time
	CreatedAt  time.Time
}

// Cursor is a keyset pagination position over (CreatedAt, ID). The zero value
// means "start from the beginning".
type Cursor struct {
	CreatedAt time.Time
	ID        string
}

// Store is the durable half of the queue. It is best-effort: queue
// correctness lives in Redis and the journal only exists to repair it. A Nop
// Store absorbs calls and does nothing.
type Store interface {
	// Enqueue records a newly accepted task and leaves redis_pending = true
	// until the reconciler confirms Redis holds it.
	Enqueue(ctx context.Context, t Task) error
	// Processing marks a task as claimed by a worker.
	Processing(ctx context.Context, id string) error
	// Succeeded marks a task as ACKed. Terminal.
	Succeeded(ctx context.Context, id string) error
	// Dead marks a task as dead-lettered. Terminal.
	Dead(ctx context.Context, id string, attempts int32) error
	// Retrying records the new attempt count and backoff schedule for a task
	// already put back into Redis.
	Retrying(ctx context.Context, id string, attempts int32, scheduleAt time.Time) error
	// Active returns a page of non-terminal tasks (enqueued, processing,
	// retrying) ordered by (created_at, id) after the cursor. The reconciler
	// re-verifies each one against Redis, so a Redis restart or flush that
	// drops an already-materialized task can still be healed. Terminal tasks
	// are never returned.
	Active(ctx context.Context, after Cursor, limit int) ([]Task, error)
	// Materialized clears redis_pending after the task is in Redis.
	Materialized(ctx context.Context, id string) error
	Close() error
}

// Nop is a Store that does nothing. It is the default when no Postgres is
// configured.
type Nop struct{}

func (Nop) Enqueue(context.Context, Task) error                      { return nil }
func (Nop) Processing(context.Context, string) error                 { return nil }
func (Nop) Succeeded(context.Context, string) error                  { return nil }
func (Nop) Dead(context.Context, string, int32) error                { return nil }
func (Nop) Retrying(context.Context, string, int32, time.Time) error { return nil }
func (Nop) Active(context.Context, Cursor, int) ([]Task, error)      { return nil, nil }
func (Nop) Materialized(context.Context, string) error               { return nil }
func (Nop) Close() error                                             { return nil }
