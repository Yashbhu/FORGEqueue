package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"forgequeue/internal/model"
)

// payload builds a JSON payload for a handler.
func payload(t *testing.T, v any) []byte {
	t.Helper()

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	return b
}

func TestEchoSucceeds(t *testing.T) {
	task := &model.TaskMetaData{
		ID:      "task-1",
		Payload: []byte(`{"hello":"world"}`),
	}

	if err := (Echo{}).Handle(context.Background(), task); err != nil {
		t.Errorf("Echo.Handle() error = %v, want nil", err)
	}
}

// Echo must not care about its context: it finishes instantly, so a
// cancelled context is irrelevant and should not turn a success into a
// failure.
func TestEchoIgnoresCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	task := &model.TaskMetaData{ID: "task-1", Payload: []byte(`{}`)}

	if err := (Echo{}).Handle(ctx, task); err != nil {
		t.Errorf("Echo.Handle() with a cancelled context = %v, want nil", err)
	}
}

func TestSleepRunsForTheRequestedDuration(t *testing.T) {
	task := &model.TaskMetaData{
		ID:      "task-sleep",
		Payload: payload(t, sleepRequest{DurationMS: 40}),
	}

	start := time.Now()

	if err := (Sleep{}).Handle(context.Background(), task); err != nil {
		t.Fatalf("Sleep.Handle() error = %v, want nil", err)
	}

	elapsed := time.Since(start)
	if elapsed < 40*time.Millisecond {
		t.Errorf("Sleep.Handle() returned after %s, want at least 40ms", elapsed)
	}
}

// The whole point of a context-aware sleep is lease loss. When the heartbeat
// is rejected the worker cancels this context, and the handler must return
// promptly rather than run to completion on a task another worker now owns.
func TestSleepReturnsPromptlyOnCancellation(t *testing.T) {
	task := &model.TaskMetaData{
		ID:      "task-cancelled",
		Payload: payload(t, sleepRequest{DurationMS: 60000}),
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- (Sleep{}).Handle(ctx, task)
	}()

	// Give the handler time to reach its select before cancelling.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Sleep.Handle() on cancel = %v, want context.Canceled", err)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("Sleep.Handle() did not return within 2s of cancellation")
	}
}

// A bad payload can never succeed on retry, so it must fail rather than
// silently sleep for zero and report success.
func TestSleepRejectsUnparseablePayload(t *testing.T) {
	task := &model.TaskMetaData{
		ID:      "task-bad",
		Payload: []byte(`not json at all`),
	}

	err := (Sleep{}).Handle(context.Background(), task)
	if err == nil {
		t.Fatal("Sleep.Handle() with a bad payload = nil, want an error")
	}
}

// An empty payload is treated as a zero duration rather than an error, so a
// bare sleep task is still a valid task.
func TestSleepAcceptsEmptyPayload(t *testing.T) {
	task := &model.TaskMetaData{ID: "task-empty"}

	if err := (Sleep{}).Handle(context.Background(), task); err != nil {
		t.Errorf("Sleep.Handle() with no payload = %v, want nil", err)
	}
}

func TestFailAlwaysFails(t *testing.T) {
	task := &model.TaskMetaData{ID: "task-fail"}

	if err := (Fail{}).Handle(context.Background(), task); err == nil {
		t.Error("Fail.Handle() = nil, want an error")
	}
}

// Default must cover the documented task types, and callers must be able to
// extend the map without touching this package.
func TestDefaultIsExtensible(t *testing.T) {
	m := Default()

	for _, want := range []string{TypeEcho, TypeSleep, TypeFail} {
		if _, ok := m[want]; !ok {
			t.Errorf("Default() missing task type %q", want)
		}
	}

	m["email"] = Echo{}

	if _, ok := m["email"]; !ok {
		t.Error("Default() map is not extensible by the caller")
	}

	// Mutating the result must not leak back into the package default.
	if _, leaked := Default()["email"]; leaked {
		t.Error("mutating the Default() map affected later callers")
	}
}
