package durable

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultDSN points at a local Postgres running the forgequeue database.
// Override it with FORGEQUEUE_PG_DSN when Postgres lives elsewhere
// (docker-compose, CI, a real deployment).
const DefaultDSN = "postgres://localhost:5432/forgequeue"

// The schema is one table. Every enqueued task gets a row; the row follows
// the task through its lifecycle until the task is succeeded or dead.
//
// payload is BYTEA (not JSONB) on purpose: it is the byte-for-byte JSON body
// Redis stores, so the reconciler can rebuild Redis without re-encoding the
// task. JSONB would be nicer for querying, but querying bodies is not what
// this table is for.
const schema = `
CREATE TABLE IF NOT EXISTS queue_tasks (
    id            TEXT PRIMARY KEY,
    task_type     TEXT NOT NULL,
    payload       BYTEA NOT NULL,
    max_retries   INT  NOT NULL DEFAULT 0,
    attempts      INT  NOT NULL DEFAULT 0,
    state         TEXT NOT NULL,
    schedule_at   TIMESTAMPTZ,
    redis_pending BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The reconciler only ever scans the rows that need Redis materialized, so
-- the scan should not touch the finished history.
CREATE INDEX IF NOT EXISTS queue_tasks_pending_idx
    ON queue_tasks (redis_pending)
    WHERE redis_pending = TRUE;
`

// Postgres is a Store backed by PostgreSQL.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres connects to Postgres, fails fast if it is unreachable, and
// makes sure the schema exists.
func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("durable: parse dsn: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("durable: ping postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("durable: ensure schema: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}

// Enqueue upserts the task and always leaves redis_pending = true. The
// gateway calls this before touching Redis: if the process dies in between,
// the reconciler re-materializes the task and nothing is lost.
func (p *Postgres) Enqueue(ctx context.Context, t Task) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO queue_tasks
			(id, task_type, payload, max_retries, attempts, state, schedule_at, redis_pending)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, TRUE)
		ON CONFLICT (id) DO UPDATE SET
			task_type     = EXCLUDED.task_type,
			payload       = EXCLUDED.payload,
			max_retries   = EXCLUDED.max_retries,
			attempts      = EXCLUDED.attempts,
			state         = EXCLUDED.state,
			schedule_at   = EXCLUDED.schedule_at,
			redis_pending = TRUE,
			updated_at    = now()
	`, t.ID, t.TaskType, t.Payload, t.MaxRetries, t.Attempts,
		string(t.State), nullableTime(t.ScheduleAt))
	return err
}

func (p *Postgres) Processing(ctx context.Context, id string) error {
	return p.setState(ctx, id, string(StateProcessing), -1, nil)
}

func (p *Postgres) Succeeded(ctx context.Context, id string) error {
	return p.setState(ctx, id, string(StateSucceeded), -1, nil)
}

func (p *Postgres) Dead(ctx context.Context, id string, attempts int32) error {
	return p.setState(ctx, id, string(StateDead), attempts, nil)
}

// Retrying records the backoff schedule the worker just wrote into Redis.
// attempts and schedule_at come from the requeue script's own math, so the
// journal mirrors what Redis actually decided - never the other way around.
func (p *Postgres) Retrying(ctx context.Context, id string, attempts int32, scheduleAt time.Time) error {
	return p.setState(ctx, id, string(StateRetrying), attempts, &scheduleAt)
}

// setState is the shared UPDATE. attempts = -1 leaves the counter untouched
// (the worker did not change it).
func (p *Postgres) setState(ctx context.Context, id string, state string, attempts int32, scheduleAt *time.Time) error {
	_, err := p.pool.Exec(ctx, `
		UPDATE queue_tasks
		SET state         = $2,
		    attempts      = CASE WHEN $3 >= 0 THEN $3 ELSE attempts END,
		    schedule_at   = COALESCE($4, schedule_at),
		    redis_pending = FALSE,
		    updated_at    = now()
		WHERE id = $1
	`, id, state, attempts, scheduleAt)
	return err
}

// Pending returns the tasks the journal knows about but Redis has not proven
// it holds. Only active states can need materialization - a succeeded or
// dead task never goes back into a queue.
func (p *Postgres) Pending(ctx context.Context) ([]Task, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, task_type, payload, max_retries, attempts, state, schedule_at, created_at
		FROM queue_tasks
		WHERE redis_pending = TRUE
		  AND state IN ('`+string(StateEnqueued)+`', '`+string(StateProcessing)+`', '`+string(StateRetrying)+`')
		ORDER BY created_at
		LIMIT 1000
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		var state string
		var sched *time.Time
		if err := rows.Scan(&t.ID, &t.TaskType, &t.Payload, &t.MaxRetries,
			&t.Attempts, &state, &sched, &t.CreatedAt); err != nil {
			return nil, err
		}
		t.State = State(state)
		if sched != nil {
			t.ScheduleAt = *sched
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// Materialized clears redis_pending now that Redis provably holds the task.
func (p *Postgres) Materialized(ctx context.Context, id string) error {
	_, err := p.pool.Exec(ctx, `
		UPDATE queue_tasks SET redis_pending = FALSE, updated_at = now() WHERE id = $1
	`, id)
	return err
}

// nullableTime converts a Go time to the *time.Time Postgres wants for a
// NULL-able column. The zero time means "immediate" and maps to NULL.
func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
