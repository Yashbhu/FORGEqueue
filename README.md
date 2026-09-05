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
  consumers that block on the immediate queue, parse tasks, and dispatch them to
  registered handlers by task type.
- **Proto** (`proto/v1`): the gRPC service and message definitions.

## Features

- gRPC API with fail-fast validation and graceful shutdown
- UUID task IDs with per-task metadata (type, payload, max retries)
- Immediate execution via a blocking Redis list
- Scheduled/delayed execution via a Redis sorted set (ZSET)
- Concurrent worker pool with clean shutdown (context cancellation + WaitGroup)
- Handler dispatch by task type with graceful handling of unregistered types

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
internal/worker/      # concurrent worker pool and task handlers
proto/v1/             # protobuf definitions and generated code
```

## Planned

- Task execution/retry logic in workers (`leaseDuration` plumbing in progress)
- Delayed-task promotion from the scheduled set back into the ready queue