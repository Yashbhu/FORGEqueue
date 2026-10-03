# ForgeQueue

ForgeQueue is a Redis-backed task queue written in Go. It implements durable, at-least-once task execution with leases, heartbeats, crash recovery, and ownership fencing from first principles. 

Key features:
- At-least-once execution with lease-based ownership
- Automatic retries with exponential backoff
- Dead letter queue for failed tasks
- Delayed task scheduling
- Optional Postgres journal for extra durability and reconciliation
- gRPC API for task ingestion
- Fully atomic state transitions using Lua scripts

## Quick Start

### Prerequisites

- Go 1.26 or later
- Redis running on `localhost:6379`

### Run the Gateway

```bash
go run ./cmd/gateway
```

The gRPC gateway listens on port `50051`.

### Use the Worker (Library)

```go
pool, err := worker.NewWorkerPool("localhost:6379", 4)
if err != nil {
	log.Fatal(err)
}

pool.RegisterHandler("send_email", emailHandler{})
pool.Start()
defer pool.Stop()
```

### Enqueue a Task

```go
conn, err := grpc.NewClient("localhost:50051")
if err != nil {
	log.Fatal(err)
}
defer conn.Close()

client := queuev1.NewQueueServiceClient(conn)
resp, err := client.EnqueueTask(ctx, &queuev1.EnqueueTaskRequest{
	TaskType:     "send_email",
	Payload:      []byte(`{"to":"a@b.com"}`),
	MaxRetries:   3,
	DelaySeconds: 0,
})
if err != nil {
	log.Fatal(err)
}

fmt.Printf("Enqueued task %s\n", resp.TaskId)
```

## Architecture

ForgeQueue consists of two main components that communicate through Redis:

| Component | Description |
| --- | --- |
| `internal/gateway` | gRPC server that validates requests, generates UUIDs, and enqueues tasks to Redis. Performs fail-fast ping on startup and graceful shutdown on SIGINT/SIGTERM. |
| `internal/worker` | Queue engine with a worker pool (concurrency-limited goroutines), a recovery goroutine, and a promotion goroutine for scheduled tasks. |
| `internal/redisutil` | Thin wrapper around go-redis with fail-fast ping. |
| `internal/model` | Defines `TaskMetaData` (the JSON payload stored in Redis). |
| `internal/durable` | Optional Postgres journal. Records accepted tasks before writing to Redis and reconciles state on startup. Requires `FORGEQUEUE_PG_DSN`. |
| `proto/v1` | Protobuf definitions and generated Go stubs. |

Data flow: `Client -> Gateway (gRPC) -> Redis -> Worker Pool -> Handler`

## API

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

## Redis State

| Key | Type | Purpose |
| --- | --- | --- |
| `queue:tasks:immediate` | List | Task IDs ready to execute immediately |
| `queue:tasks:inflight` | Sorted Set | Task IDs scored by lease expiry timestamp |
| `queue:tasks:leases` | Hash | Maps `taskID` to `leaseID` for ownership verification |
| `queue:tasks:scheduled` | Sorted Set | Delayed or backoff tasks scored by execution timestamp |
| `queue:tasks:dead` | List | Tasks that exhausted retry budget, with full bodies |
| `task:<id>` | String | JSON-encoded task body (including attempts counter) |

All state transitions are executed atomically via Lua scripts to prevent race conditions.

## Task Lifecycle

1. **Enqueue**: Gateway writes the task body to `task:<id>` and pushes the ID to the ready list (or scheduled set if delayed).
2. **Claim**: An atomic Lua script pops a task from the ready list, registers it in the inflight set with a lease expiry, stores a lease token, and returns the task body.
3. **Work**: The handler executes while a background heartbeat renews the lease at intervals.
4. **ACK**: An atomic Lua script verifies ownership via the lease token and removes the task from inflight and leases.
5. **Fail/Retry**: On error, the lease is released immediately. If retries remain, the task is rescheduled with exponential backoff; otherwise it goes to the dead letter queue.
6. **Recovery**: A recovery loop scans for expired leases and requeues or dead-letters them after re-verifying expiry atomically.

## Reliability

- **Lease-based ownership**: Each claim generates a unique lease token stored in Redis. All destructive operations verify this token, making Redis the source of truth across process boundaries.
- **Heartbeats**: Long-running tasks renew their leases every `leaseDuration / 3`, preventing premature reclamation.
- **Fencing**: Heartbeat, ACK, and requeue operations verify lease ownership atomically. Stale workers cannot mutate tasks they no longer own.
- **Lease loss cancellation**: If a heartbeat fails, the handler's context is cancelled to allow early termination.
- **Race-safe recovery**: Recovery re-verifies expiry inside Redis at reclamation time, closing the window between scan and requeue.
- **Exponential backoff**: Retries use `base * 2^(attempts-1)` with a configurable cap. Tasks with `max_retries = 0` retry indefinitely.
- **At-least-once semantics**: Due to lease expiry and crash recovery, handlers may receive duplicate executions and must be idempotent.

## Configuration

| Setting | Location | Default |
| --- | --- | --- |
| Gateway Redis address | `gateway.Start()` | `localhost:6379` |
| Gateway Redis pool size | `gateway.Start()` | 50 |
| Worker Redis address | `NewWorkerPool(addr, concurrency)` | (required) |
| Worker concurrency | `NewWorkerPool(addr, concurrency)` | (required) |
| Worker Redis pool size | Derived | `concurrency * 2` |
| Lease duration | `wp.leaseDuration` (unexported) | 10s |
| Heartbeat interval | Derived | `leaseDuration / 3` |
| Recovery interval | `recoveryLoop` | 1s |
| Promotion interval | `promotionLoop` | 1s |
| Backoff base | `wp.backoffBase` (unexported) | 1s |
| Backoff cap | `wp.backoffCap` (unexported) | 60s |

Set `FORGEQUEUE_PG_DSN` to enable the optional Postgres journal.

## Testing

Tests require a local Redis instance on `localhost:6379` and will flush the database.

```bash
go test ./internal/worker/
```

Notable tests:
- `TestWorkerAckPreventsRequeue`: Verifies ACK prevents recovery requeue
- `TestWorkerRequeueAfterFailure`: Tests retry with backoff on handler errors
- `TestWorkerDeadLettersAfterRetryBudget`: Confirms tasks go to DLQ after exhausting retries
- `TestWorkerDelayedTaskPromotion`: Verifies delayed tasks are promoted correctly

Journal and reconciler tests exist under `internal/durable/` and require Postgres.

## Benchmarks

Measure pure drain throughput with a no-op handler:

```bash
go test ./internal/worker/ -run=^$ -bench BenchmarkWorkerThroughputPreloaded -benchtime=10s -benchmem
```

Typical results on Apple Silicon (M2) with local Redis: ~8,000 to 12,000 tasks/s. The benchmark measures queue overhead only (two Redis round trips per task plus heartbeats); real workloads will differ.

## Failure Modes

| Scenario | Behavior |
| --- | --- |
| Worker crash mid-handler | Lease expires; recovery requeues the task with backoff (counts as an attempt). |
| Handler returns error | Attempt increments; lease released immediately. Task retries or goes to DLQ based on budget. |
| Stale worker tries to ACK | Fencing check rejects the operation. |
| Heartbeat fails (Redis unreachable or lease lost) | Handler context is cancelled. |
| Inflight tasks at Redis restart | Leases expire in real time; recovery handles them after downtime. |
| Redis unreachable at startup | Gateway and WorkerPool fail fast with ping. |
| Missing task body | Claimed but dropped by recovery as "no-body". |
| Corrupt JSON in task body | Sent to dead letter queue. |
| No handler for task type | Task is retried until budget exhausted, then dead-lettered. |

## Limitations

- Completed task bodies (`task:<id>`) are not deleted automatically.
- No authentication or TLS for Redis or gRPC.
- Idle polling uses a 1ms interval; blocking variants are planned for production use.

## Development Status

**Implemented:**
- gRPC gateway with validation, UUIDs, immediate/delayed enqueue, fail-fast startup, graceful shutdown
- Worker pool with concurrent consumers, handler dispatch, clean shutdown
- Lease-based ownership with fencing on all destructive operations
- Heartbeats, race-safe recovery, and context cancellation on lease loss
- Retry budget with exponential backoff and dead letter queue
- Delayed task promotion
- Body fetch folded into claim (2 Redis round trips per completed task)
- Optional Postgres journal with reconciliation

**In Progress:**
- Batching claims and ACKs for improved throughput

**Planned:**
- Telemetry (throughput, latency, lease loss counters)
- Smarter idle polling (blocking operations)
- Multi-instance deployment hardening

## Design Notes

All critical operations use Lua scripts for atomicity:
- **Claim** (1 round trip): Pop from ready list, add to inflight with expiry, store lease token, fetch body.
- **Heartbeat** (1 round trip): Verify lease token and extend expiry using Redis time.
- **ACK** (1 round trip): Verify lease token and remove from inflight/leases.
- **Requeue/DLQ** (1 round trip): Verify expiry or release lease, load body, increment attempts, schedule backoff or push to DLQ.
- **Promote** (1 round trip): Check scheduled task time against Redis TIME and move to ready list.

The main remaining cost is round trips; batching multiple claims/ACKs per script is the planned optimization for higher throughput.

## License

This project does not currently include a license file. All rights reserved unless a license is added. Please contact the author before reusing this code.
