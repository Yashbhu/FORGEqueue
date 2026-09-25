# FORGEqueue

A lightweight, Redis-backed distributed task queue with a gRPC API and a concurrent worker pool, written in Go.

## Architecture

FORGEqueue splits into two components that talk through Redis:

```
Client ──gRPC──▶ Gateway ──▶ Redis ──▶ Workers
                 (router)   (queue)   (consumer pool)
```

- **Gateway** (`cmd/gateway`, `internal/gateway`): a gRPC server that accepts
  `EnqueueTask` requests, assigns each a UUID, and routes it into Redis.
- **Redis** (`internal/redisutil`): the queue backend. Immediate tasks go into a
  blocking list, delayed tasks into a sorted set keyed by execution timestamp.
- **Workers** (`internal/worker`): a `WorkerPool` of concurrently running
  consumers that claim tasks, fetch their bodies, and dispatch them to
  registered handlers by task type — while continuously renewing ownership.
- **Proto** (`proto/v1`): the gRPC service and message definitions.

## How a task moves through the system

```
ENQUEUE → task:<id> + ready list   (gateway writes)
CLAIM  → LPOP + ZADD in-flight + HSET lease   (one atomic Lua script)
WORK   → handler runs, heartbeat renews the lease every leaseDuration/3
ACK    → HGET lease, verify ownership, ZREM + HDEL   (one atomic Lua script)
```

Redis state is split across three structures:

| Key | Type | Meaning |
| --- | --- | --- |
| `queue:tasks:immediate` | List | Tasks ready to run right now |
| `queue:tasks:inflight` | Sorted set | Claimed tasks, scored by lease expiry |
| `queue:tasks:leases` | Hash | `taskID → leaseID`, the ownership proof |

## Features

- gRPC API with fail-fast validation and graceful shutdown
- UUID task IDs with per-task metadata (type, payload, max retries)
- Immediate execution via a Redis list
- Delayed enqueue via a Redis sorted set (`queue:tasks:scheduled`)
- Concurrent worker pool with clean shutdown (context cancellation + WaitGroup)
- Handler dispatch by task type with graceful handling of unregistered types
- **Lease-based ownership** — every claim gets a unique lease token stored in
  Redis, so tasks are never "owned" in name only
- **Heartbeats** — each task renews its lease in the background while the
  handler runs, so long tasks aren't stolen by recovery
- **Task cancellation** — if a heartbeat is rejected (lease lost), the task
  context is cancelled so the handler can stop early
- **Atomic recovery** — expired leases are found and requeued in one Lua
  script that re-verifies the expiry, closing the scan-vs-requeue race
- **Fenced ACK/claim** — claim returns the task body in the same atomic script
  as the lease (one round trip saved); ACK verifies ownership before removing
- **Benchmarked against BullMQ** — see below

## Prerequisites

- Go 1.26+
- A running Redis instance (default `localhost:6379`)

## Getting Started

```sh
# Start Redis (example using the redis CLI)
redis-server

# Run the gateway
go run ./cmd/gateway
```

The gateway listens on `:50051`.

## Enqueueing Tasks

Submit tasks over gRPC using the generated client in `proto/v1`:

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

### Protocol

```proto
service QueueService {
  rpc EnqueueTask(EnqueueTaskRequest) returns (EnqueueTaskResponse);
}
```

| Field         | Type   | Notes                          |
| ------------- | ------ | ------------------------------ |
| `task_type`   | string | Required; validated by server  |
| `payload`     | bytes  | Opaque task payload            |
| `max_retries` | int32  | Retry budget for the task      |
| `delay_seconds`| int32 | Schedule delay; `0` = immediate |

## Component Layout

```
cmd/gateway/          # entrypoint for the gRPC gateway
internal/gateway/     # gRPC server + task routing into Redis
internal/model/       # TaskMetaData model
internal/redisutil/   # Redis client construction (fail-fast ping)
internal/worker/      # worker pool, leases, heartbeats, recovery
proto/v1/             # protobuf definitions and generated code
docs/                 # design notes (e.g. Redis round-trip analysis)
```

## Benchmarks

Run the worker throughput benchmark (Redis must be running):

```sh
go test ./internal/worker/ -run=^$ -bench BenchmarkWorkerThroughputPreloaded -benchtime=15s
```

Latest numbers (Apple M2, local Redis, concurrency 4, no-op handler,
process-only):

| Implementation | Throughput |
| --- | --- |
| FORGEqueue | ~11,000 tasks/s |
| BullMQ | ~12,900 jobs/s |

The claim script returning the body in one round trip moved FORGEqueue from
~7,900 → ~11,000 tasks/s at equal concurrency. Details and the next
optimizations (batched claims/ACKs) are in
[`docs/redis-roundtrips.md`](docs/redis-roundtrips.md).

## Final Scope & Progress

FORGEqueue's end-state is a complete distributed task system, mapped to the
original architecture with current progress:

### Gateway
- [x] gRPC API (`EnqueueTask`) with validation and fail-fast startup
- [x] UUID task IDs, routing into Redis, graceful shutdown

### Core Engine
- [x] Concurrent worker pool (`WorkerPool`, Start/Stop/RegisterHandler)
- [x] Redis queue backend (ready / in-flight / lease state split)
- [x] Task claiming (atomic LPOP + lease registration)
- [x] Handler execution dispatched by task type
- [x] Graceful shutdown (quit channel + context cancellation + WaitGroup)

### Resilience
- [x] Lease-based ownership (`TaskLease`, per-task `LeaseID`)
- [x] Heartbeats (auto-renew the lease while a handler runs)
- [x] Crash/expiry recovery loop (atomic, race-safe requeue)
- [x] Lease fencing (ACK verifies ownership before removing)
- [x] Task cancellation on lease loss (`taskCtx`)
- [ ] Retries with exponential backoff
- [ ] Delayed-task promotion (`queue:tasks:scheduled` → ready queue)
- [ ] Dead-letter queue for exhausted retry budgets

### Storage Split
- [x] Redis: hot queue state (ready / in-flight / leases)
- [ ] PostgreSQL: durable metadata store

### Telemetry
- [ ] Metrics (throughput, latency, lease losses)
- [ ] Task lifecycle events
- [ ] Monitoring/alerting

### Performance
- [x] Claim + body fetched in one atomic round trip (3 → 2 ops/task)
- [ ] Batched claims/ACKs (target 2-3x throughput)
- [ ] Benchmark suite vs BullMQ

**Next up:** retries + backoff, then delayed-task promotion, then DLQ.