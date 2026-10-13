package durable

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Postgres and Redis cannot be written atomically, so there is always a
// window where one has the task and the other doesn't. The reconciler sweeps
// every non-terminal task and makes sure each still exists in Redis. Because
// it re-checks tasks that were already materialized, it also heals a Redis
// restart or flush that dropped them, not just the enqueue crash window.

// reconcilePageSize bounds how many journal rows one sweep verifies, so a
// large active set is walked in pages rather than one unbounded query.
const reconcilePageSize = 1000

// materializeScript moves one task from its journal record into Redis. If the
// task is already in one of the hot structures it returns 0, otherwise it
// re-SETs the body and adds the ID to ready or scheduled depending on state.
//
// KEYS[1] queue:tasks:immediate
// KEYS[2] queue:tasks:inflight
// KEYS[3] queue:tasks:scheduled
// KEYS[4] task:<id>
// ARGV[1] task ID
// ARGV[2] journal state
// ARGV[3] schedule score, empty when not time-bound
// ARGV[4] payload JSON
const materializeScript = `
    local inImmediate = redis.call("LPOS", KEYS[1], ARGV[1])
    local inInflight  = redis.call("ZSCORE", KEYS[2], ARGV[1])
    local inScheduled = redis.call("ZSCORE", KEYS[3], ARGV[1])

    if (inImmediate or inInflight or inScheduled) then
        return 0
    end

    redis.call("SET", KEYS[4], ARGV[4])

    if ARGV[3] == "" then
        -- immediate enqueue -> ready list.
        redis.call("LPUSH", KEYS[1], ARGV[1])
    else
        -- delayed, in backoff, or crashed processing -> scheduled set.
        redis.call("ZADD", KEYS[3], tonumber(ARGV[3]), ARGV[1])
    end

    return 1
`

// ReconcileOnce verifies one page of non-terminal tasks starting just after
// cursor. It returns how many tasks it had to re-materialize, the cursor to
// resume from on the next call, and whether this page was the last one (fewer
// than a full page of rows came back).
func ReconcileOnce(ctx context.Context, rdb *redis.Client, store Store, cursor Cursor) (repaired int, next Cursor, done bool, err error) {
	page, err := store.Active(ctx, cursor, reconcilePageSize)
	if err != nil {
		return 0, cursor, true, err
	}
	next = cursor

	for _, t := range page {
		ok, err := materialize(ctx, rdb, t)
		if err != nil {
			log.Printf("reconcile: failed to materialize task %s: %v", t.ID, err)
		} else if ok {
			repaired++
		}
		if err := store.Materialized(ctx, t.ID); err != nil {
			log.Printf("reconcile: failed to clear pending for task %s: %v", t.ID, err)
		}
		next = Cursor{CreatedAt: t.CreatedAt, ID: t.ID}
	}

	return repaired, next, len(page) < reconcilePageSize, nil
}

// ReconcileLoop reconciles until ctx is cancelled. One page per second is
// plenty: the loop walks the whole active set over successive ticks and wraps
// around once it reaches the end.
func ReconcileLoop(ctx context.Context, rdb *redis.Client, store Store) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var cursor Cursor
	for {
		select {
		case <-ticker.C:
			_, next, done, err := ReconcileOnce(ctx, rdb, store, cursor)
			if err != nil {
				log.Printf("reconcile: %v", err)
			}
			if done {
				cursor = Cursor{}
			} else {
				cursor = next
			}
		case <-ctx.Done():
			return
		}
	}
}

// materialize writes one pending task into Redis. Returns true if it actually
// wrote it, false if it was already there.
func materialize(ctx context.Context, rdb *redis.Client, t Task) (bool, error) {
	// Where a task lands when its hot state is gone. processing means the
	// worker died, so re-run it now via scheduled.
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
