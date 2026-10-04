// Package handlers provides the task handlers the worker binary registers
// out of the box.
//
// The queue is a delivery mechanism: it moves a JSON body from a producer to
// whoever knows what to do with it. That "who" is application code, so a
// real deployment registers its own handlers and this package is only the
// runnable default.
//
// The three handlers here exist so a fresh checkout has something that
// actually processes tasks end to end, and so each of the worker's telemetry
// signals can be provoked deliberately:
//
//	"echo"  succeeds immediately. Exercises the claim -> handle -> ACK path.
//	"sleep" runs for a duration. Exercises heartbeats and lease renewal, and
//	        shows what happens when a handler is cancelled mid-flight.
//	"fail"  always fails. Exercises the retry budget, backoff, and the
//	        dead-letter queue.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"forgequeue/internal/model"
)

// Task types registered by the worker binary.
const (
	TypeEcho  = "echo"
	TypeSleep = "sleep"
	TypeFail  = "fail"
)

// sleepRequest is the payload the "sleep" handler expects.
type sleepRequest struct {
	// DurationMS is how long to run for.
	DurationMS int `json:"duration_ms"`
}

// Echo completes immediately without doing any work.
//
// Useful as a liveness probe for the queue: enqueue one and confirm
// tasks_processed_total climbs without touching anything else.
type Echo struct{}

// Handle implements worker.TaskHandler.
func (Echo) Handle(ctx context.Context, task *model.TaskMetaData) error {
	log.Printf(
		"echo: task %s payload=%s",
		task.ID,
		string(task.Payload),
	)
	return nil
}

// Sleep blocks for a duration and then succeeds.
//
// This is how you exercise the heartbeat path without writing slow real code:
// a task longer than the lease duration proves the lease is being renewed,
// because otherwise recovery would reclaim it mid-flight.
//
// The wait is context-aware. When the heartbeat loses the lease the worker
// cancels the task context, and returning promptly on ctx.Done() is what lets
// the worker stop work it no longer owns. A handler that ignores its context
// would keep running against a task another worker is now executing.
type Sleep struct{}

// Handle implements worker.TaskHandler.
func (Sleep) Handle(ctx context.Context, task *model.TaskMetaData) error {
	var req sleepRequest

	// An unparseable payload is a permanent error: retrying it would fail
	// identically forever and just burn the retry budget.
	if len(task.Payload) > 0 {
		if err := json.Unmarshal(task.Payload, &req); err != nil {
			return fmt.Errorf("sleep: bad payload: %w", err)
		}
	}

	d := time.Duration(req.DurationMS) * time.Millisecond

	log.Printf("sleep: task %s running for %s", task.ID, d)

	select {
	case <-time.After(d):
		log.Printf("sleep: task %s done", task.ID)
		return nil

	case <-ctx.Done():
		// Returning the context error marks the attempt as failed, so the
		// task is requeued or dead-lettered by the normal path. It is not
		// special-cased as a lease loss here; the worker's heartbeat loop
		// already recorded that separately.
		return ctx.Err()
	}
}

// Fail always returns an error.
//
// It exists to exercise the failure paths on demand: enqueue it with a
// max_retries budget and watch tasks_retried_total climb, then
// tasks_dead_lettered_total once the budget is spent.
type Fail struct{}

// Handle implements worker.TaskHandler.
func (Fail) Handle(ctx context.Context, task *model.TaskMetaData) error {
	return fmt.Errorf("fail: task %s failed on purpose", task.ID)
}

// Default returns the task type to handler mapping the worker binary
// registers.
//
// Returning the map rather than registering one by one keeps cmd/worker
// honest about what it supports, and gives a caller a way to add to it
// before handing it over:
//
//	m := handlers.Default()
//	m["email"] = myEmailHandler{}
//	wp.RegisterAll(m)
func Default() map[string]WorkerHandler {
	return map[string]WorkerHandler{
		TypeEcho:  Echo{},
		TypeSleep: Sleep{},
		TypeFail:  Fail{},
	}
}

// WorkerHandler mirrors worker.TaskHandler without importing it.
//
// The interface is structural in Go, so any type with this method satisfies
// worker.TaskHandler. Declaring it here keeps this package free of a
// dependency on the worker package, which would be circular once the worker
// binary imports both.
type WorkerHandler interface {
	Handle(ctx context.Context, task *model.TaskMetaData) error
}
