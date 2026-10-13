// Package handlers provides the default task handlers the worker binary
// registers.
//
// The queue just moves a JSON body to whoever knows what to do with it, so a
// real deployment registers its own handlers. These three are here so a fresh
// checkout runs end to end:
//
//	echo  succeeds immediately.
//	sleep runs for a duration.
//	fail  always fails.
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

// Echo completes immediately. Handy as a liveness probe for the queue.
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

// Sleep blocks for a duration then succeeds. A task longer than the lease
// proves heartbeats are renewing it.
//
// The wait is context-aware: when the heartbeat loses the lease the worker
// cancels the context, and returning on ctx.Done() stops work the worker no
// longer owns.
type Sleep struct{}

// Handle implements worker.TaskHandler.
func (Sleep) Handle(ctx context.Context, task *model.TaskMetaData) error {
	var req sleepRequest

	// A bad payload is a permanent error, retrying would just burn the
	// retry budget.
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
		// Returning the error marks the attempt failed, so it is requeued
		// or dead-lettered. The heartbeat loop records the lease loss.
		return ctx.Err()
	}
}

// Fail always returns an error, for exercising the retry and dead-letter paths.
type Fail struct{}

// Handle implements worker.TaskHandler.
func (Fail) Handle(ctx context.Context, task *model.TaskMetaData) error {
	return fmt.Errorf("fail: task %s failed on purpose", task.ID)
}

// Default returns the task type to handler mapping the worker registers. A
// caller can add to it before handing it over:
//
//	m := handlers.Default()
//	m["email"] = myEmailHandler{}
func Default() map[string]WorkerHandler {
	return map[string]WorkerHandler{
		TypeEcho:  Echo{},
		TypeSleep: Sleep{},
		TypeFail:  Fail{},
	}
}

// WorkerHandler mirrors worker.TaskHandler without importing it. Go
// interfaces are structural, so declaring it here avoids a circular import.
type WorkerHandler interface {
	Handle(ctx context.Context, task *model.TaskMetaData) error
}
