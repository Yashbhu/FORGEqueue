package durable

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultDSN points at a local Postgres running the forgequeue database.
// Override with FORGEQUEUE_PG_DSN.
const DefaultDSN = "postgres://localhost:5432/forgequeue"

// Migration 1: one table, one row per enqueued task, following it until
// succeeded or dead. payload is BYTEA not JSONB: it is the exact body Redis
// stores, so the reconciler can rebuild Redis without re-encoding.
//
// Every statement is idempotent (IF NOT EXISTS) so a database created by an
// older binary, before this file tracked versions, adopts cleanly.
const migration1QueueTasks = `
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

-- The reconciler scans non-terminal tasks in (created_at, id) order, so the
-- index only covers those rows. Terminal history is skipped entirely.
CREATE INDEX IF NOT EXISTS queue_tasks_active_idx
    ON queue_tasks (created_at, id)
    WHERE state IN ('enqueued', 'processing', 'retrying');

DROP INDEX IF EXISTS queue_tasks_pending_idx;
`

// migrations is the ordered, append-only list of schema changes. To evolve the
// schema, add the next version here; never edit an applied one. Each runs
// exactly once and is recorded in schema_migrations.
var migrations = []migration{
	{Version: 1, Name: "queue_tasks", SQL: migration1QueueTasks},
}

type migration struct {
	Version int
	Name    string
	SQL     string
}

// migrationsTable records which migrations have run. It is created before any
// migration so the very first one is tracked too.
const migrationsTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INT PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

// Postgres is a Store backed by PostgreSQL.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres connects to Postgres, applies any pending migrations, and returns
// the store. Returns an error if Postgres is unreachable.
//
// advisoryLock serializes migrations across pods. Multiple replicas (gateway +
// workers) all call NewPostgres on boot, and executing the same DDL
// concurrently raises transient catalog races (e.g. duplicate key on
// pg_type_typname_nsp_index). The lock is transaction-scoped, so it releases
// when the commit ends.
const advisoryLock int64 = 884422

func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("durable: parse dsn: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("durable: ping postgres: %w", err)
	}
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("durable: migrate: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// migrate creates the ledger, then applies every migration that has not run,
// in order, each guarded by an advisory lock. A transient failure (a peer
// mid-migration) is retried a few times before giving up.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if err := migrateOnce(ctx, pool); err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		return nil
	}
	return lastErr
}

func migrateOnce(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, migrationsTable); err != nil {
		return err
	}

	// Which migrations are already applied?
	applied := map[int]bool{}
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", advisoryLock); err != nil {
			tx.Rollback(ctx)
			return err
		}
		// Re-check under the lock: a peer may have applied it while we waited.
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`,
			m.Version).Scan(&exists); err != nil {
			tx.Rollback(ctx)
			return err
		}
		if exists {
			tx.Rollback(ctx)
			continue
		}
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("migration %d (%s): %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
			m.Version, m.Name); err != nil {
			tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}

// Enqueue upserts the task and always leaves redis_pending = true. The
// gateway calls this before touching Redis, so a crash in between is healed
// by the reconciler.
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
// attempts and schedule_at come from the requeue script, so the journal
// mirrors Redis rather than the other way around.
func (p *Postgres) Retrying(ctx context.Context, id string, attempts int32, scheduleAt time.Time) error {
	return p.setState(ctx, id, string(StateRetrying), attempts, &scheduleAt)
}

// setState is the shared UPDATE. attempts = -1 leaves the counter untouched.
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

// Active returns a page of non-terminal tasks ordered by (created_at, id),
// strictly after the cursor. Every non-terminal task is re-verified against
// Redis on every sweep, so a task that was materialized and then lost when
// Redis restarted is rebuilt. Rows are keyset-paginated so a large active set
// is walked a page at a time instead of starved by an OFFSET/LIMIT window.
func (p *Postgres) Active(ctx context.Context, after Cursor, limit int) ([]Task, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := p.pool.Query(ctx, `
		SELECT id, task_type, payload, max_retries, attempts, state, schedule_at, created_at
		FROM queue_tasks
		WHERE state IN ('`+string(StateEnqueued)+`', '`+string(StateProcessing)+`', '`+string(StateRetrying)+`')
		  AND (created_at, id) > ($1, $2)
		ORDER BY created_at, id
		LIMIT $3
	`, after.CreatedAt, after.ID, limit)
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

// Materialized clears redis_pending once Redis holds the task.
func (p *Postgres) Materialized(ctx context.Context, id string) error {
	_, err := p.pool.Exec(ctx, `
		UPDATE queue_tasks SET redis_pending = FALSE, updated_at = now() WHERE id = $1
	`, id)
	return err
}

// nullableTime maps a Go time to a nullable column. Zero means immediate, so
// it becomes NULL.
func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
