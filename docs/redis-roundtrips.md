# Cutting Redis round trips per task

## The problem

Every task a worker processes costs Redis round trips. Each round trip is a
full request/response over the TCP connection to Redis. The worker's
throughput is capped by the number of trips, not by CPU — the queue plumbing
does almost no compute.

Before this change, one task cost **3 round trips**:

```
worker                              Redis
  │   LPOP + ZADD + HSET              │     claim the task ("I'll take it")
  │ ────────────────────────────────► │
  │   "task-123"                      │
  │ ◄──────────────────────────────── │
  │                                   │
  │   GET task:task-123               │     fetch the JSON body separately
  │ ────────────────────────────────► │
  │   {payload...}                    │
  │ ◄──────────────────────────────── │
  │                                   │
  │   HGET + ZREM + HDEL              │     ack it (verify ownership, remove)
  │ ────────────────────────────────► │
  │   1                               │
  │ ◄──────────────────────────────── │
```

Redis is single-threaded: it processes one command at a time from all
connected clients. That gives a hard ceiling of roughly **100–150k ops/sec**,
no matter how fast the worker code is. So fewer ops per task is the single
most direct way to raise throughput.

## The concept: move the decision into Redis

The trick is to fold work into Redis's Lua scripts. A Lua script runs
**atomically** — nothing else can interleave between its commands — and it is
sent as **one round trip**.

The claim already did `LPOP + ZADD + HSET` in one script. That script now also
reads the task body (`GET task:<id>`) and returns `{taskID, body}` together.
The worker no longer sends the separate `GET`.

```
worker                              Redis
  │  LPOP + ZADD + HSET + GET          │     claim + read body atomically
  │ ────────────────────────────────► │
  │  {task-123, {payload...}}          │
  │ ◄──────────────────────────────── │
  │                                   │
  │  HGET + ZREM + HDEL               │     ack
  │ ────────────────────────────────► │
```

**3 round trips → 2.** That is ~1.5x upside on the round-trip-bound portion.

The body is read by the same script that registers the lease, so this also
removes a window where a task could be claimed but not yet readable.

## What changed (code)

`internal/worker/worker.go`:

- Added `TaskClaim` struct — embeds `TaskLease` (TaskID + LeaseID) plus
  `Body []byte`, so the caller gets ownership and payload in one object.
- `claimTask()` returns `(*TaskClaim, error)` instead of `(*TaskLease, error)`.
  Signature diff: `*/TaskLease, error` → `*TaskClaim, error` (1 line).
- Claim Lua script: new `local body = redis.call("GET", ARGV[3] .. taskID)`
  and returns `{taskID, body}` instead of just `taskID`. (~4 Lua lines)
- `script.Run(...)` passes one more argument: `"task:"` as `ARGV[3]` (the key
  prefix used to build `task:<id>`). (~2 lines)
- Return block parses the array: type-asserts `{taskID, body}` and rejects a
  malformed result. (~12 lines)
- `workerLoop()`: deleted the whole `redisClient.Get("task:"+lease.TaskID)`
  round trip and its error handling (~14 lines removed), replaced with a
  `len(lease.Body) == 0` guard and `json.Unmarshal(lease.Body, &task)`.

Net: **~10 net-new lines, one round trip removed from the hot path.**

## Measured result

`BenchmarkWorkerThroughputPreloaded` (concurrency 4, no-op handler,
same local Redis, 3 runs each):

| | Before | After | Delta |
|---|---|---|---|
| Throughput | ~7,571–8,181/s | ~8,220–9,257/s | **~+13%** |

The gain is less than the theoretical 1.5x because the path isn't purely
round-trip-bound (go-redis overhead, handler dispatch, log lines all add
fixed cost). It is, however, the foundation for the bigger wins below, and it
makes the healthy path *one op shorter* — fewer per-task ops also means Redis
saturates later at higher concurrency.

## Next steps (not yet implemented)

| Change | Ops/task | Expected gain |
|---|---|---|
| (done) claim returns body | 3 → 2 | ~1.13x measured |
| Batch claims — `LPOP`/`ZPOPMIN` N tasks, buffer in worker, one heartbeat renews the batch | ~1–1.5 | **~2–3x** |
| Batch ACKs — one Lua removes N finished tasks | ~1 | ~3x cumulative |
| Stop — multiple shards (Redis Cluster) / PG split | — | past single-Redis ceiling |

Batching is the real lever to decisively pass BullMQ; it changes the claim
semantics (a worker holds several leases at once), so it needs care with the
per-task heartbeat/cancellation we already built.