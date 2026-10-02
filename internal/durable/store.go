package durable

import (
	"context"
	"time"
)

// State is the lifecycle state of a task as the journal records it.
//
// The journal is the durable side of the queue: plain rows in Postgres that
// survive crashes, Redis restarts and even a Redis flush. The hot side of
// the queue (Redis lists and sorted sets) is the part that moves fast; this
// table is the part that remembers.
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

// Task is the durable record for one task, as far as the journal cares.
//
// Payload is the exact JSON body Redis stores at task:<id>. Keeping the body
// in Postgres is what lets the reconciler rebuild Redis state from scratch
// after a crash or a Redis flush: it re-SETs the body and re-inserts the ID
// into whichever queue the state says it belongs in.
type Task struct {
	ID         string
	TaskType   string
	Payload    []byte
	MaxRetries int32
	Attempts   int32
	State      State
	// ScheduleAt is when an enqueued/retrying task is due. The zero value
	// signals "no delay" - the task belongs in the immediate queue.
	ScheduleAt time.Time
	CreatedAt  time.Time
}

// Store is the durable half of the queue. The contract with the caller is
// best-effort: queue correctness lives in Redis, this journal exists so Redis
// state can be repaired. Every method is safe to treat as optional - a Nop
// Store absorbs the call and does nothing.
type Store interface {
	// Enqueue records a newly accepted task. It leaves redis_pending = true,
	// meaning "the journal knows about this task but can't yet prove Redis
	// does" - the reconciler closes that gap.
	Enqueue(ctx context.Context, t Task) error
	// Processing marks a task as claimed by a worker.
	Processing(ctx context.Context, id string) error
	// Succeeded marks a task as ACKed. Terminal.
	Succeeded(ctx context.Context, id string) error
	// Dead marks a task as dead-lettered. Terminal.
	Dead(ctx context.Context, id string, attempts int32) error
	// Retrying records the new attempt count and backoff schedule for a task
	// the worker already put back into Redis, so redis_pending is cleared.
	Retrying(ctx context.Context, id string, attempts int32, scheduleAt time.Time) error
	// Pending returns the tasks redis_pending = true: know by the journal,
	// unproven in Redis. The reconciler works through this list.
	Pending(ctx context.Context) ([]Task, error)
	// Materialized clears redis_pending after the task was (re)written into
	// Redis.
	Materialized(ctx context.Context, id string) error
	Close() error
}

// Nop is a Store that does nothing. It is the default, so a queue without
// Postgres configured behaves exactly as it did before the storage split.
type Nop struct{}

func (Nop) Enqueue(context.Context, Task) error                      { return nil }
func (Nop) Processing(context.Context, string) error                 { return nil }
func (Nop) Succeeded(context.Context, string) error                  { return nil }
func (Nop) Dead(context.Context, string, int32) error                { return nil }
func (Nop) Retrying(context.Context, string, int32, time.Time) error { return nil }
func (Nop) Pending(context.Context) ([]Task, error)                  { return nil, nil }
func (Nop) Materialized(context.Context, string) error               { return nil }
func (Nop) Close() error                                             { return nil }
