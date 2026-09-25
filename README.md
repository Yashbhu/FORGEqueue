# ForgeQueue

ForgeQueue is a Redis-backed task queue written in Go. It was built as a
learning exercise in distributed systems: to understand, from first
principles, how job queues provide durable, at-least-once execution — leases,
heartbeats, crash recovery, and ownership fencing — by implementing them
yourself instead of reaching for an off-the-shelf system.

There are no external "hard" dependencies beyond Go and a single Redis
instance. The queue is a library (`internal/worker`) plus a gRPC ingestion
gateway (`internal/gateway`); there is no separate worker binary yet.

```
Client ──gRPC──▶ Gateway ──▶ Redis ──▶ Worker pool ──▶ Your handler
                 (router)   (queue)   (consumers)
```

## Architecture

ForgeQueue is split into two components that communicate only through Redis.

```
internal/gateway/   gRPC server. Validates EnqueueTask requests, generates a
                    UUID per task, and routes the task into Redis. Fail-fast
                    on startup: it pings Redis and refuses to boot if the
                    store is unreachable. Listens on :50051, exits cleanly on
                    SIGINT/SIGTERM (GracefulStop).

internal/worker/    The actual queue engine. A WorkerPool spins up
                    concurrencyLimit worker goroutines plus one recovery
                    goroutine. Workers claim tasks, dispatch them to handlers
                    registered by task type, and renew the task's lease while
                    the handler runs.

internal/redisutil/ Thin wrapper around go-redis: builds a client and fails
                    fast with a Ping.

internal/model/     TaskMetaData: the JSON payload actually stored in Redis.

proto/v1/           Protobuf definitions and generated Go stubs for the
                    QueueService.
```

The protobuf schema is minimal and deliberate — a production queue exposes a
lot of knobs, but the wire contract for enqueueing a task is small:

```proto
message EnqueueTaskRequest {
  string task_type = 1;
  bytes  payload    = 2;
  int32  max_retries = 3;
  int32  delay_seconds = 4;
}
message EnqueueTaskResponse {
  string task_id = 1;
  string status  = 2;
  int64  timestamp = 3;
}
service QueueService {
  rpc EnqueueTask(EnqueueTaskRequest) returns (EnqueueTaskResponse);
}
```

## Task lifecycle

A task moves through Redis in four distinct states, each represented by a
different structure (see the next section for the exact keys):

```
ENQUEUE  Gateway writes the body to task:<id> and pushes the id onto the
         ready list (or, for delayed tasks, into the scheduled set).
CLAIM    One atomic Lua script: LPOP the ready list, register the task in
         the in-flight set with a lease expiry, store a fresh lease token,
         and read the task body. Returns lease + body together.
WORK     The handler runs. A background heartbeat renews the lease on a
         fixed interval.
ACK      One atomic Lua script: verify the caller still owns the lease,
         then remove the task from the in-flight set and the leases hash.
```

When the lease expires without an ACK, the recovery loop moves the task back
to the ready list for another attempt.

## Redis state model

| Key | Type | What it holds |
| --- | --- | --- |
| `queue:tasks:immediate` | List | Task IDs ready to run now |
| `queue:tasks:inflight` | Sorted set | Task IDs, scored by lease expiry timestamp |
| `queue:tasks:leases` | Hash | `taskID → leaseID`, the proof of ownership |
| `queue:tasks:scheduled` | Sorted set | Task bodies, scored by execution timestamp (used only for delayed enqueue; nothing promotes these yet) |
| `task:<id>` | String | The JSON task body |

Every state transition is a Lua script, so it happens atomically in a single
round trip — no worker can observe a half-claimed task.

## Reliability model

The core question a queue has to answer: how do you avoid losing work when
the machine running the work dies? ForgeQueue's answer is leases. Everything
else hangs off that decision.

- **Claims are leases, not membership.** Claiming a task does not "assign" it
  to a process; it creates a short-lived lease in Redis with a unique
  `LeaseID` token and a hard expiry. Ownership is always re-verified against
  the stored token, never assumed. This is what makes the system work across
  process boundaries: Redis, not any worker's memory, is the source of truth.

- **Heartbeats keep leases alive for long-running tasks.** Each task starts a
  background loop that renews its lease every `leaseDuration / 3`. A handler
  that runs for minutes doesn't get its task stolen by recovery.

- **Fencing on every destructive operation.** Heartbeat and ACK both re-check
  the lease token inside the same Lua script that mutates state. A stale
  worker — one whose task was already requeued and re-claimed by someone else
  after the lease expired — cannot ACK or extend a lease it no longer owns.
  This is the classic fenced-leader problem, solved for the single key case.

- **Lease loss cancels the handler.** If a heartbeat is rejected, the task's
  context is cancelled so the handler can stop early instead of writing
  results for a task it no longer owns.

- **Recovery is atomic and idempotent.** Once a second, the recovery goroutine
  scans the in-flight set for tasks whose expiry has passed, then requeues
  them. The requeue re-verifies the expiry *inside* the Lua script against
  Redis's own clock rather than trusting the scan, closing the race where a
  heartbeat renews the lease between the scan and the requeue (and prevents
  a just-heartbeat task from being double-enqueued).

- **At-least-once semantics.** Because recovery re-runs tasks whose lease
  expired mid-processing, handlers must be able to cope with an occasional
  duplicate. No attempt is made to provide exactly-once delivery; that is not
  a property task queues offer.

## Failure handling

Honest inventory of what happens in each failure mode:

| Failure | Behaviour |
| --- | --- |
| Worker crashes mid-handler | Lease expires, recovery requeues the task. It will run again (potentially twice). |
| Handler returns an error | The task is not ACK'd, its lease expires, recovery requeues it. Attempts are repeated indefinitely — there is no retry cap or backoff yet, and `max_retries` is stored but not enforced. |
| Stale worker tries to ACK after requeue | ACK rejected by the fencing check. |
| Heartbeat fails (Redis unreachable or lease lost) | Task context cancelled; handler told to stop early. |
| Task in-flight when Redis restarts | In-flight entries survive (they are just data), leases expire against real time, recovery requeues them after downtime. |
| Redis unreachable at start | Both the gateway (fail-fast Ping) and `NewWorkerPool` (Ping) refuse to start. |
| Task body missing or corrupt JSON | Claimed, logged, skipped (`continue`) — the task remains in-flight and will be requeued forever. |
| No handler for the task type | Claimed, logged, skipped, requeued forever on the next expiry. |
| Queue idle | Workers poll every 1 ms instead of blocking on BRPOP. Fine against local Redis, suboptimal for production; a blocking variant is planned. |

Known gaps beyond those above: completed tasks' bodies (`task:<id>`) are never
deleted, and there is no authentication or TLS for either Redis or gRPC. This
is a learning system, not yet a hardened service.

## Performance

### Methodology

The `BenchmarkWorkerThroughputPreloaded` benchmark measures *pure drain rate*:
it preloads `b.N` tasks into Redis, starts the pool, and times until every
task is processed. Each task costs two round trips to Redis — one claim (which
also fetches the body) and one ACK — plus one heartbeat round trip every
`leaseDuration / 3` per task. The handler is a no-op, so the measurement is
entirely the queue machinery.

Setup: Apple Silicon (M2), macOS, locally running Redis on `localhost:6379`,
concurrency 4, `leaseDuration` 1 s.

### Numbers

A clean run (`-benchtime=10s -benchmem`):

```
BenchmarkWorkerThroughputPreloaded-8  ~12,171 tasks/s
                                      ~82,160 ns/op
                                      ~3,419 B/op
                                      ~67 allocs/op
```

Throughput varies noticeably between runs (roughly 8,000–12,000 tasks/s,
typically ~8,000–8,500 on an otherwise quiet box). Treat these as "millions of
round trips a second on localhost Redis" rather than a stable spec. There is
no persistent storage involved; do not extrapolate to production hardware or
real handler workloads.

### What moved the needle

Workers originally made three round trips per task — claim (LPOP + lease
registration), fetch the body (GET), then ACK. Folding the body fetch into the
claim Lua script cut it to two round trips per task and measurably improved
throughput (roughly 5% in a controlled series of runs).

The larger wins are still on the table: batching claims and ACKs across
multiple tasks per round trip, which is the likely path to 2–3x throughput.

### ForgeQueue vs BullMQ

A head-to-head comparison was run against BullMQ (Redis-backed, Node) on the
same machine and quiet box. BullMQ benchmark scripts are in the run's scratch
directory, not in this repo.

| Implementation | Mode | Throughput |
| --- | --- | --- |
| ForgeQueue | pool drain only | ~11,000 tasks/s |
| BullMQ | process jobs only | ~12,900 jobs/s |
| BullMQ | full enqueue + process | ~9,500 jobs/s |

The comparisons are not apples-to-apples: different runtimes (Go vs Node),
different measurement windows, and BullMQ's numbers reflect its own
configuration. Take them as order-of-magnitude context, not a verdict —
ForgeQueue is not being claimed as faster.

### Running the benchmarks

Redis must be running (the benchmarks flush the database):

```sh
go test ./internal/worker/ -run=^$ -bench BenchmarkWorkerThroughputPreloaded -benchtime=10s -benchmem
```

## Testing

Tests live in `internal/worker/worker_test.go` and use a real Redis instance
(`localhost:6379`). They `FlushDB` before running, so point them at a
throwaway database.

- `TestWorkerAckPreventsRequeue` — happy path. Enqueues one task, waits for it
  to be ACK'd out of the in-flight set, then sleeps well past the lease and
  asserts recovery never re-enqueued it.
- `TestWorkerRequeueAfterFailure` — error path. A flaky handler fails twice
  (each failure leaving the task in-flight), recovery requeues it each time,
  and the third attempt succeeds and ACKs. Asserts exactly three handler
  calls and no extra runs after recovery.

Run them:

```sh
go test ./internal/worker/
```

## Running locally

Prerequisites: Go 1.26+ and a Redis instance (default `localhost:6379`).

```sh
# 1. Start Redis
redis-server

# 2. Start the gateway (gRPC on :50051)
go run ./cmd/gateway
```

### Using the worker

The worker is a library: register handlers by task type, then start the pool.

```go
pool, err := worker.NewWorkerPool("localhost:6379", 4)
if err != nil {
	log.Fatal(err)
}
pool.RegisterHandler("send_email", emailHandler{})
pool.Start()
defer pool.Stop()
```

### Enqueueing over gRPC

```go
conn, _ := grpc.NewClient("localhost:50051")
client := queuev1.NewQueueServiceClient(conn)

resp, err := client.EnqueueTask(ctx, &queuev1.EnqueueTaskRequest{
	TaskType:     "send_email",
	Payload:      []byte(`{"to":"a@b.com"}`),
	MaxRetries:   3,
	DelaySeconds: 0,
})
```

## Configuration

Configuration is currently compile-time constants and constructor arguments;
there is no config file.

| Setting | Where | Default |
| --- | --- | --- |
| Gateway Redis address | hardcoded in `gateway.Start()` | `localhost:6379` |
| Gateway Redis pool size | hardcoded | 50 |
| Worker Redis address | `NewWorkerPool(addr, concurrency)` | — |
| Worker concurrency | `NewWorkerPool(addr, concurrency)` | — |
| Worker Redis pool size | derived | `concurrency * 2` |
| Lease duration | `wp.leaseDuration` (unexported) | 10 s |
| Heartbeat interval | derived | `leaseDuration / 3` |
| Recovery interval | fixed in `recoveryLoop` | 1 s |

Making the lease duration and addresses externally configurable is future
work.

## Project status

**Implemented**

- gRPC gateway: validation, UUID generation, immediate + delayed enqueue into
  Redis, fail-fast startup, graceful shutdown.
- Worker pool: concurrent consumers, handler dispatch by task type, clean
  shutdown via quit channel + context cancellation + WaitGroup.
- Leases: unique per-claim token stored in Redis, verified on every
  destructive operation (heartbeat, ACK).
- Heartbeats: per-task background renewal while the handler runs.
- Recovery: 1 s crash/expiry requeue loop, race-safe via atomic expiry
  re-check.
- Cancellation: handlers interrupted when their lease is lost.
- Round-trip optimization: body fetched inside the claim script (3 → 2 round
  trips per task).
- Tests for ACK/no-requeue and flush-fail-requeue flows; throughput
  benchmarks.

**In progress**

- Batched claims/ACKs (multiple tasks per round trip) — the design note
  below sketches the direction; not yet implemented.
- A standing ForgeQueue-vs-BullMQ benchmark harness.

**Planned**

- Retry budget: enforce `max_retries` with exponential backoff.
- Delayed-task promotion: move due tasks out of `queue:tasks:scheduled` into
  the ready list (enqueueing delayed tasks works; nothing runs them yet).
- Dead-letter queue for tasks that exhaust their retry budget.
- PostgreSQL as the durable metadata store, with Redis remaining the hot
  queue state.
- Telemetry: throughput, latency, lease-loss counters.
- Smarter idle polling (blocking BRPOP instead of the 1 ms poll).
- Multi-instance deployment story; today one Redis instance is assumed.

## Roadmap

1. Retry budget + backoff, then DLQ. Until these exist, an erroring handler
   causes infinite requeue.
2. Delayed-task promotion, to make `delay_seconds` actually work.
3. Batched claims/ACKs for 2–3x throughput.
4. PostgreSQL metadata store + telemetry.
5. Observability, auth/TLS, and multi-node hardening.

## Design notes

The notable implementation decision is the reliance on Lua scripts. Every
state transition (claim, heartbeat, ACK, requeue) is a single atomic script
instead of several client round trips. This buys both correctness — the
fencing and recovery checks are atomic with the writes — and speed:

- **Claim** (1 round trip): LPOP ready list → ZADD in-flight with expiry →
  HSET lease token → GET body → return `{taskID, body}`.
- **Heartbeat** (1 round trip): HGET lease → verify token → ZADD in-flight
  with new expiry from Redis's own clock.
- **ACK** (1 round trip): HGET lease → verify token → ZREM in-flight → HDEL
  lease.
- **Requeue** (1 round trip per expired task): ZSCORE → compare against
  Redis `TIME` → ZREM → HDEL → LPUSH.

The remaining cost is transmission, not atomicity: two round trips per
completed task. Batching by claiming N tasks per script call is the intended
next step.

## Contributing

This is primarily a learning project. If you find a bug or a race, a
reproduction and a failing test are the most useful contribution. The tests
require a local Redis; anything that needs `FlushDB` is documented as such.

## License

No license file is present. Until one is added, the code is all-rights-
reserved by default; please ask before reusing it.