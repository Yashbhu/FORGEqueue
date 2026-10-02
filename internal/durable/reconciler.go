package durable

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// The reconciler is the answer to the storage split's core problem: you
// cannot atomically write Postgres AND Redis. Every write is ordered, so
// there is always a window in which one store has the task and the other
// does not. The reconciler works through redis_pending rows and makes sure
// the task actually exists in Redis - the hot store converges to what the
// durable store says it should be.
//
// This is the "journal first" half of the design: Postgres decides what
// should exist, Redis executes it, and a crash anywhere in between gets
// repaired on the next sweep.

// materializeScript moves one task into Redis from its journal record.
//
// It is one atomic script so "check presence, then write" has no race:
//
//   - LPOS/ZSCORE against the three hot structures. If the task is anywhere
//     in Redis already, there is nothing to do and the script returns 0
//     ("already present"). This is what stops a crashed-but-retried worker
//     path from ever doubling a task.
//   - otherwise the body is re-SET (Redis may have lost everything), and the
//     ID is placed on the ready list or the scheduled set depending on the
//     journal state.
//
// KEYS[1] --> queue:tasks:immediate
// KEYS[2] --> queue:tasks:inflight
// KEYS[3] --> queue:tasks:scheduled
// KEYS[4] --> task:<id>   (the body key)
// ARGV[1] --> task ID
// ARGV[2] --> journal state ("enqueued" | "processing" | "retrying")
// ARGV[3] --> schedule score (unix seconds) when the task is time-bound,
//
//	empty otherwise
//
// ARGV[4] -> the payload (exact JSON body Redis should hold)
const materializeScript = `
    local inImmediate = redis.call("LPOS", KEYS[1], ARGV[1])
    local inInflight  = redis.call("ZSCORE", KEYS[2], ARGV[1])
    local inScheduled = redis.call("ZSCORE", KEYS[3], ARGV[1])

    if (inImmediate or inInflight or inScheduled) then
        return 0
    end

    redis.call("SET", KEYS[4], ARGV[4])

    if ARGV[3] == "" then
        -- an immediate enqueue belongs on the ready list.
        redis.call("LPUSH", KEYS[1], ARGV[1])
    else
        -- delayed, in backoff, or a crashed processing task: it waits in
        -- the scheduled set until its score is due.
        redis.call("ZADD", KEYS[3], tonumber(ARGV[3]), ARGV[1])
    end

    return 1
`

// ReconcileOnce does a single sweep: read every pending task from the
// journal, materialize the missing ones into Redis, and clear redis_pending.
// It returns how many tasks it actually repaired.
func ReconcileOnce(ctx context.Context, rdb *redis.Client, store Store) (int, error) {
	pending, err := store.Pending(ctx)
	if err != nil {
		return 0, err
	}

	repaired := 0
	for _, t := range pending {
		ok, err := materialize(ctx, rdb, t)
		if err != nil {
			log.Printf("reconcile: failed to materialize task %s: %v", t.ID, err)
			continue
		}
		if ok {
			repaired++
		}
		if err := store.Materialized(ctx, t.ID); err != nil {
			log.Printf("reconcile: failed to clear pending for task %s: %v", t.ID, err)
		}
	}
	return repaired, nil
}

// ReconcileLoop runs reconciliations forever until ctx is cancelled. One
// sweep per second is plenty: pending is near-empty in steady state and only
// fills up when something crashed.
func ReconcileLoop(ctx context.Context, rdb *redis.Client, store Store) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, err := ReconcileOnce(ctx, rdb, store); err != nil {
				log.Printf("reconcile: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// materialize writes one pending task into Redis, or confirms it is already
// there. It returns true when it actually (re)materialized the task.
func materialize(ctx context.Context, rdb *redis.Client, t Task) (bool, error) {
	// Decide where a task whose hot state is gone should land.
	//
	// - enqueued, no delay: ready list.
	// - enqueued, delayed, or retrying: scheduled set, at its due time.
	// - processing: the worker claiming it died/was wiped; re-run it like
	//   recovery would -> scheduled at "now".
	score := ""
	switch t.State {
	case StateProcessing:
		score = strconv.FormatInt(time.Now().Unix(), 10)
	case StateEnqueued, StateRetrying:
		if !t.ScheduleAt.IsZero() {
			score = strconv.FormatInt(t.ScheduleAt.Unix(), 10)
		}
	}

	result, err := redis.NewScript(materializeScript).Run(
		ctx,
		rdb,
		[]string{
			"queue:tasks:immediate",
			"queue:tasks:inflight",
			"queue:tasks:scheduled",
			"task:" + t.ID,
		},
		t.ID,
		string(t.State),
		score,
		t.Payload,
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}
