# ForgeQueue

ForgeQueue is a Redis-backed task queue written in Go. It implements durable,
at-least-once task execution with leases, heartbeats, crash recovery, and
ownership fencing from first principles.

> **What this is.** A learning / portfolio project that implements a task queue
> the way a real one works: atomic Lua state transitions, lease ownership,
> heartbeats, recovery, retries and a dead-letter queue. It is deliberately
> small and readable rather than production-hardened. It is **not** a drop-in
> replacement for Redis-backed job systems like Asynq, Celery, BullMQ or
> Sidekiq — see [Limitations](#limitations) for the honest list of what it does
> not do.

Key features:
- At-least-once execution with lease-based ownership and fencing
- Automatic retries with exponential backoff
- Dead letter queue with retry/delete/purge controls
- Delayed task scheduling
- Batched claims and ACKs (configurable `BATCH_SIZE`) with per-task heartbeat renewal
- Optional Postgres journal for extra durability and reconciliation
- gRPC API for task ingestion
- HTTP/JSON read + control API and a small React/TanStack web console
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

## Run with Docker

Build every image from source — multi-stage, nothing is copied in pre-built:

```bash
docker build -f dockerfiles/worker.Dockerfile    -t forgequeue/worker:latest    .
docker build -f dockerfiles/gateway.Dockerfile   -t forgequeue/gateway:latest   .
docker build -f dockerfiles/apiserver.Dockerfile -t forgequeue/apiserver:latest .
docker build -f dockerfiles/web.Dockerfile       -t forgequeue/web:latest       .
```

The four Go services are distroless static binaries running as a non-root user;
the console is a Node SSR server (Nitro `node-server` output), not a static
bundle. Bring the stack up on one network:

```bash
docker network create forgequeue
docker run -d --name redis --network forgequeue redis:7.4-alpine --appendonly yes
docker run -d --name gateway   --network forgequeue -e FORGEQUEUE_REDIS_ADDR=redis:6379 forgequeue/gateway:latest
docker run -d --name worker    --network forgequeue -e FORGEQUEUE_REDIS_ADDR=redis:6379 \
  -e FORGEQUEUE_WORKER_CONCURRENCY=16 -e FORGEQUEUE_BATCH_SIZE=4 forgequeue/worker:latest
docker run -d --name apiserver --network forgequeue -p 8080:8080 \
  -e FORGEQUEUE_REDIS_ADDR=redis:6379 -e FORGEQUEUE_AUTH_SECRET=devsecret \
  -e FORGEQUEUE_ADMIN_PASSWORD=devpassword forgequeue/apiserver:latest
docker run -d --name web --network forgequeue -p 3000:3000 forgequeue/web:latest
```

The console is then on http://localhost:3000 and the API on
http://localhost:8080. The web image bakes `VITE_API_BASE_URL=/api` by default,
which assumes a reverse proxy in front; to hit a browser-reachable API origin
directly, rebuild it with
`--build-arg VITE_API_BASE_URL=http://localhost:8080/api`.

## Kubernetes

A Kustomize tree lives under `deploy/k8s`. `base/` holds Redis and the four
services as Deployments (non-root, read-only root filesystem, dropped
capabilities, health probes, resource limits, a Redis-gated init container),
plus an Ingress, HPAs and PodDisruptionBudgets. Two overlays:

- `overlays/kind` — local kind cluster: `:latest` images loaded from your daemon
  and a **dev-only** Secret generated in-cluster.
- `overlays/prod` — `ghcr.io/yashbhu/forgequeue-*:v1.0.0`, scaled replicas, and
  no Secret (apply `forgequeue-secrets` out of band, e.g. SOPS + age).

```bash
kind create cluster --name forgequeue --config deploy/k8s/kind-config.yaml
kind load docker-image forgequeue/worker:latest forgequeue/gateway:latest \
  forgequeue/apiserver:latest forgequeue/web:latest --name forgequeue
kubectl apply -k deploy/k8s/overlays/kind
kubectl -n forgequeue get pods
```

Redis runs a single replica on a PersistentVolumeClaim with AOF enabled, so
queued tasks survive a pod restart. Worker and apiserver autoscale on CPU with
the HPAs in `base/` — the cluster needs `metrics-server` for those targets to
resolve. Note that the worker idle-polls, so a **CPU-based** HPA scales on
polling overhead rather than queue depth; a queue-depth scaler (e.g. KEDA) would
be the accurate signal. Metrics are scraped via the `ServiceMonitor` in
`deploy/k8s/observability`, which is applied separately **after** installing
kube-prometheus-stack (the `monitoring.coreos.com` CRDs must exist). The Ingress
targets the `forgequeue.local` host and needs an ingress-nginx controller.

Launch hardening baked into the manifests:

- **TLS at the edge.** The Ingress terminates TLS (cert-manager
  `letsencrypt-prod` by default) and redirects HTTP to HTTPS; the console and
  `/api` share one origin, so the API needs no CORS grant in production.
- **Redis AUTH.** The Redis StatefulSet runs with `requirepass` sourced from
  `forgequeue-secrets` (`FORGEQUEUE_REDIS_PASSWORD`); every service reads it
  through `redisutil`. `maxmemory-policy noeviction` bounds memory without
  silently dropping queued tasks.
- **Alerting.** `observability/prometheusrule.yaml` ships alerts for backlog
  growth, dead-lettering, lease losses, a down scrape target, failed backups
  and filling persistent volumes.
- **Queue-depth autoscaling.** `observability/keda-scaledobject.yaml` scales
  workers on the ready-list length (KEDA); it supersedes the CPU HPA.
- **Off-cluster backups.** `base/backup.yaml` adds a daily `pg_dump` to object
  storage in addition to the on-PVC copy, and no-ops when unconfigured.

Images are built and signed (SBOM + provenance + cosign) by
`.github/workflows/release.yml` on a `v*` tag. Pin the printed digests in
`overlays/prod` before deploying.

## Architecture

ForgeQueue consists of two main components that communicate through Redis:

| Component | Description |
| --- | --- |
| `internal/gateway` | gRPC server that validates requests, generates UUIDs, and enqueues tasks to Redis. Performs fail-fast ping on startup and graceful shutdown on SIGINT/SIGTERM. |
| `internal/worker` | Queue engine with a worker pool (concurrency-limited goroutines), a recovery goroutine, and a promotion goroutine for scheduled tasks. |
| `internal/httpapi` | HTTP/JSON read + control API used by the web console (stats, queues, jobs, workers, DLQ, delayed, pause/resume/purge). |
| `internal/auth` | JWT (HS256) issuance/verification and role-based access control for the HTTP API. |
| `internal/redisutil` | Thin wrapper around go-redis with fail-fast ping. |
| `internal/model` | Defines `TaskMetaData` (the JSON payload stored in Redis). |
| `internal/durable` | Optional Postgres journal. Records accepted tasks before writing to Redis and reconciles state on startup. Requires `FORGEQUEUE_PG_DSN`. |
| `proto/v1` | Protobuf definitions and generated Go stubs. |
| `web/` | React + TanStack Start console: logs in to the HTTP API and polls it. |

Data flow: `Client -> Gateway (gRPC) -> Redis -> Worker Pool -> Handler`

The console talks only to the HTTP API: `Browser -> HTTP API -> Redis`.

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

### HTTP / JSON API (console backend)

The `apiserver` (`cmd/apiserver`) exposes read and control endpoints on
`:8080` (`FORGEQUEUE_API_ADDR`). Every endpoint except the login endpoints
(`POST /api/auth/login`, `GET /api/auth/config`, and the `GET
/api/auth/oidc/*` SSO flow) requires a JWT bearer token and a role that carries
the relevant permission.

| Method | Path | Permission | Purpose |
| --- | --- | --- | --- |
| POST | `/api/auth/login` | — | Exchange username/password for a bearer token |
| GET | `/api/auth/config` | — | Which login methods are enabled (password / SSO) |
| GET | `/api/auth/oidc/login` | — | Start the SSO redirect (auth-code + PKCE) |
| GET | `/api/auth/oidc/callback` | — | SSO provider callback; returns a one-time code |
| POST | `/api/auth/oidc/exchange` | — | Trade the one-time code for a bearer token |
| GET | `/api/auth/me` | (any authenticated) | Current identity + roles |
| GET | `/api/system/stats` | `system:read` | Totals: enqueued, processing, delayed, completed |
| GET | `/api/system/version` | `system:read` | Build/version info |
| GET | `/api/queues` | `queues:read` | Queue list with depth + paused status |
| GET | `/api/queues/{name}` | `queues:read` | One queue |
| GET | `/api/queues/{name}/jobs` | `jobs:read` | Jobs in a queue |
| POST | `/api/queues/{name}/pause` \| `/resume` | `queues:write` | Pause / resume claiming |
| POST | `/api/queues/{name}/purge` | `queues:write` | Drop all pending tasks |
| GET | `/api/jobs/{id}` | `jobs:read` | One job |
| POST | `/api/jobs/{id}/retry` \| `/requeue` | `jobs:write` | Re-enqueue a job |
| DELETE | `/api/jobs/{id}` | `jobs:write` | Delete a job |
| GET | `/api/workers` `/api/workers/{id}` | `workers:read` | Live workers and their leases |
| GET | `/api/dlq` | `dlq:read` | Dead-letter entries |
| POST | `/api/dlq/{id}/retry` | `dlq:write` | Retry one dead task |
| DELETE | `/api/dlq/{id}` | `dlq:write` | Delete one dead task |
| POST | `/api/dlq/purge` | `dlq:write` | Clear the DLQ |
| GET | `/api/delayed` | `delayed:read` | Scheduled tasks |
| POST | `/api/delayed/{id}/promote` | `delayed:write` | Run a delayed task now |
| DELETE | `/api/delayed/{id}` | `delayed:write` | Cancel a delayed task |
| GET | `/api/metrics/throughput` | `metrics:read` | Time-bucketed throughput series |

Auth is configured from the environment (see [Configuration](#configuration)).
The recommended setup is **single sign-on**: point the server at any OpenID
Connect provider (Google, Okta, Auth0, Keycloak, Entra ID) with
`FORGEQUEUE_OIDC_ISSUER` and the console offers a "Continue with …" button — the
provider owns accounts, passwords, MFA and revocation, so there is nothing to
create or store here. Roles are assigned by email allowlist
(`FORGEQUEUE_OIDC_ADMIN_EMAILS`, `FORGEQUEUE_OIDC_OPERATOR_EMAILS`) or a default
role for everyone else.

For a local, provider-less deployment you can instead use username/password
accounts. If nothing is configured, the server bootstraps a single `admin` user
and **logs the generated password** to stdout — pin it with
`FORGEQUEUE_ADMIN_PASSWORD` or declare real users with `FORGEQUEUE_USERS`. When
SSO is enabled and no local accounts are configured, the bootstrap admin is
skipped entirely (set `FORGEQUEUE_ADMIN_PASSWORD` or `FORGEQUEUE_USERS` if you
want a break-glass account).

> Because successful ACKs delete `task:<id>`, a **completed** job no longer has a
> body and `GET /api/jobs/{id}` returns `404`. Completed jobs are meant to be
> observed through the metrics/throughput endpoints, not looked up individually.

## Redis State

| Key | Type | Purpose |
| --- | --- | --- |
| `queue:tasks:immediate` | List | Task IDs ready to execute immediately |
| `queue:tasks:inflight` | Sorted Set | Task IDs scored by lease expiry timestamp |
| `queue:tasks:leases` | Hash | Maps `taskID` to `leaseID` for ownership verification |
| `queue:tasks:scheduled` | Sorted Set | Delayed or backoff tasks scored by execution timestamp |
| `queue:tasks:dead` | List | Tasks that exhausted retry budget, with full bodies |
| `queue:paused` | String | Presence means the queue is paused (workers stop claiming) |
| `task:<id>` | String | JSON-encoded task body (including attempts counter); deleted on successful ACK |

All state transitions are executed atomically via Lua scripts to prevent race conditions.

## Task Lifecycle

1. **Enqueue**: Gateway writes the task body to `task:<id>` and pushes the ID to the ready list (or scheduled set if delayed).
2. **Claim**: An atomic Lua script pops up to `BATCH_SIZE` tasks from the ready list, registers each in the inflight set with a lease expiry, stores a lease token per task, and returns the bodies. It returns nothing while `queue:paused` exists.
3. **Work**: A heartbeat goroutine is started for **every** claimed task the moment the batch is claimed, not just the one currently executing. It renews each lease at `leaseDuration / 3` using Redis's own clock.
4. **ACK**: Successful tasks are ACKed together in one fenced script that verifies each lease token, removes the task from inflight/leases, and deletes its body.
5. **Fail/Retry**: On error, the lease is released immediately (fenced). If retries remain, the task is rescheduled with exponential backoff; otherwise it goes to the dead letter queue.
6. **Recovery**: A recovery loop scans for expired leases and requeues or dead-letters them after re-verifying expiry atomically.

The batch's heartbeats are only stopped **after** the ACK/requeue is committed. If they were stopped when the handler returned, a task that finished early would sit un-renewed while the rest of the batch drained; with a lease shorter than the batch drain time that lease would expire and recovery would run the task a second time.

## Reliability

- **Lease-based ownership**: Each claim generates a unique lease token stored in Redis. All destructive operations verify this token, making Redis the source of truth across process boundaries.
- **Batched execution**: Each worker can claim up to `BATCH_SIZE` tasks in one atomic script and ACK them together. A heartbeat is started for every claimed task at claim time and kept alive until that task's ACK/requeue is committed, so tasks waiting behind the one executing cannot lose their lease.
- **Heartbeats**: Long-running tasks renew their leases every `leaseDuration / 3` using Redis's clock, preventing premature reclamation.
- **Sub-second lease precision**: Lease expiries are stored as fractional-second scores, so short leases are not silently truncated.
- **Fencing**: Heartbeat, ACK, and requeue operations verify lease ownership atomically. Stale workers cannot mutate tasks they no longer own.
- **Lease loss cancellation**: If a heartbeat fails, the handler's context is cancelled to allow early termination.
- **Race-safe recovery**: Recovery re-verifies expiry inside Redis at reclamation time, closing the window between scan and requeue.
- **Exponential backoff**: Retries use `base * 2^(attempts-1)` with a configurable cap. Tasks with `max_retries = 0` retry indefinitely.
- **Body cleanup**: A successfully ACKed task's `task:<id>` body is deleted in the same atomic ACK script.
- **Graceful drain**: `Stop()` cancels the infra loops, waits up to the drain timeout for in-flight handlers to finish, then cancels the rest and de-registers the worker.
- **At-least-once semantics**: Due to lease expiry and crash recovery, handlers may receive duplicate executions and must be idempotent. There is no exactly-once guarantee.

## Configuration

| Setting | Location | Default |
| --- | --- | --- |
| Redis address | `FORGEQUEUE_REDIS_ADDR` | `localhost:6379` |
| Redis password (AUTH) | `FORGEQUEUE_REDIS_PASSWORD` | none |
| Redis database index | `FORGEQUEUE_REDIS_DB` | `0` |
| Redis TLS | `FORGEQUEUE_REDIS_TLS` | off |
| Redis TLS verify skip (dev) | `FORGEQUEUE_REDIS_TLS_INSECURE` | off |
| Redis TLS CA bundle | `FORGEQUEUE_REDIS_TLS_CA` | system roots |
| gRPC TLS cert / key | `FORGEQUEUE_GRPC_TLS_CERT` / `_KEY` | off (plaintext) |
| gRPC TLS client CA (mTLS) | `FORGEQUEUE_GRPC_TLS_CLIENT_CA` | off |
| API CORS origins (comma list) | `FORGEQUEUE_CORS_ORIGINS` | same-origin only |
| API max request body | `FORGEQUEUE_MAX_BODY_BYTES` | 1 MiB |
| Postgres sslmode (DSN) | `FORGEQUEUE_PG_SSLMODE` | `disable` |
| Gateway gRPC listen | `gateway.Start()` (hardcoded) | `:50051` |
| Gateway metrics address | `FORGEQUEUE_METRICS_ADDR` | disabled |
| HTTP API listen | `FORGEQUEUE_API_ADDR` | `:8080` |
| Worker concurrency | `FORGEQUEUE_WORKER_CONCURRENCY` | 4 |
| Worker batch size | `FORGEQUEUE_BATCH_SIZE` | 1 |
| Worker metrics address | `FORGEQUEUE_METRICS_ADDR` | disabled |
| Postgres journal | `FORGEQUEUE_PG_DSN` | disabled |
| API auth secret | `FORGEQUEUE_AUTH_SECRET` | ephemeral (random, warns) |
| API auth token TTL | `FORGEQUEUE_AUTH_TTL` | 12h |
| API users (JSON) | `FORGEQUEUE_USERS` | bootstrapped admin |
| Bootstrap admin user | `FORGEQUEUE_ADMIN_USER` | `admin` |
| Bootstrap admin password | `FORGEQUEUE_ADMIN_PASSWORD` | random (logged) |
| SSO issuer (enables OIDC) | `FORGEQUEUE_OIDC_ISSUER` | disabled |
| SSO client id | `FORGEQUEUE_OIDC_CLIENT_ID` | — |
| SSO client secret | `FORGEQUEUE_OIDC_CLIENT_SECRET` | — (PKCE; optional) |
| SSO callback URL | `FORGEQUEUE_OIDC_REDIRECT_URL` | — |
| SSO provider label | `FORGEQUEUE_OIDC_PROVIDER_NAME` | issuer host |
| SSO scopes | `FORGEQUEUE_OIDC_SCOPES` | `openid email profile` |
| SSO admin emails | `FORGEQUEUE_OIDC_ADMIN_EMAILS` | — |
| SSO operator emails | `FORGEQUEUE_OIDC_OPERATOR_EMAILS` | — |
| SSO default role | `FORGEQUEUE_OIDC_DEFAULT_ROLE` | `viewer` (`none` to deny) |
| SSO allowed return origins | `FORGEQUEUE_OIDC_ALLOWED_ORIGINS` | web URL only |
| Console web URL | `FORGEQUEUE_WEB_URL` | `/` |
| Lease duration | `wp.leaseDuration` (unexported) | 10s |
| Heartbeat interval | Derived | `leaseDuration / 3` |
| Recovery interval | `wp.recoveryInterval` | 1s |
| Drain timeout | `WithDrainTimeout` | 30s |
| Promotion interval | `promotionLoop` | 1s |
| Backoff base | `wp.backoffBase` (unexported) | 1s |
| Backoff cap | `wp.backoffCap` (unexported) | 60s |

## Running the whole stack

```bash
go build ./cmd/...
redis-server &                       # or use your own Redis

FORGEQUEUE_REDIS_ADDR=localhost:6379 ./gateway
FORGEQUEUE_REDIS_ADDR=localhost:6379 \
  FORGEQUEUE_ADMIN_PASSWORD=devpassword \
  FORGEQUEUE_AUTH_SECRET=devsecret ./apiserver
FORGEQUEUE_REDIS_ADDR=localhost:6379 \
  FORGEQUEUE_WORKER_CONCURRENCY=16 \
  FORGEQUEUE_BATCH_SIZE=4 \
  FORGEQUEUE_METRICS_ADDR=:9100 ./worker &
```

(`apiserver` refuses to start without a working auth config; with no
`FORGEQUEUE_USERS` it bootstraps an `admin` user — set `FORGEQUEUE_ADMIN_PASSWORD`
here so you know the password. Then `POST /api/auth/login` to get a token.)

To run behind SSO instead, register a confidential/web client with your provider
and set the callback to `<api>/api/auth/oidc/callback`:

```bash
FORGEQUEUE_REDIS_ADDR=localhost:6379 \
  FORGEQUEUE_AUTH_SECRET=devsecret \
  FORGEQUEUE_OIDC_ISSUER=https://accounts.google.com \
  FORGEQUEUE_OIDC_CLIENT_ID=xxxx.apps.googleusercontent.com \
  FORGEQUEUE_OIDC_CLIENT_SECRET=xxxx \
  FORGEQUEUE_OIDC_REDIRECT_URL=http://localhost:8080/api/auth/oidc/callback \
  FORGEQUEUE_WEB_URL=http://localhost:3000 \
  FORGEQUEUE_OIDC_ADMIN_EMAILS=you@example.com ./apiserver
```

For local testing without a real provider, [Dex](https://dexidp.io) or Keycloak
both work; point `FORGEQUEUE_OIDC_ISSUER` at their discovery URL.

Then load-test with the bundled generator (talks to the gateway over gRPC):

```bash
./loadgen -rate 200 -concurrency 8 -duration 30s \
  -echo 60 -sleep 20 -fail 12 -delay 8 -sleep-max-ms 4000
```

## Testing

Tests require a local Redis on `localhost:6379` and will flush that database —
run them against a Redis you don't mind clearing.

```bash
go test ./internal/worker/          # engine tests
go test -race ./internal/worker/    # race check
go test ./...                       # everything (Postgres tests skip unless set)
```

Notable tests:
- `TestWorkerAckPreventsRequeue`: Verifies ACK prevents recovery requeue
- `TestWorkerRequeueAfterFailure`: Tests retry with backoff on handler errors
- `TestWorkerDeadLettersAfterRetryBudget`: Confirms tasks go to DLQ after exhausting retries
- `TestWorkerDelayedTaskPromotion`: Verifies delayed tasks are promoted correctly
- `TestWorkerBatchClaimsAndAcks` / `TestWorkerBatchMixedSuccessAndFailure`: Batched claim/ACK paths
- `TestWorkerBatchLeaseKeptAlive`: Regression test for the batch lease bug — asserts every task in a slow batch runs exactly once

Journal and reconciler tests exist under `internal/durable/` and require Postgres.

## Benchmarks

Measure pure drain throughput with a no-op handler (worker concurrency 4, 50k
preloaded tasks):

```bash
go test ./internal/worker/ -run=^$ -bench BenchmarkWorkerThroughputPreloaded -benchtime=10s -benchmem
go test ./internal/worker/ -run=^$ -bench BenchmarkWorkerThroughputBatched     -benchtime=10s -benchmem
```

Results on an Apple M2 with a local Redis. Handlers are no-ops, so this is pure
queue overhead (Redis round trips plus heartbeats):

| Implementation | Claim style | Throughput |
| --- | --- | --- |
| ForgeQueue | one task per claim (`BATCH_SIZE=1`) | ~4,100 tasks/s |
| ForgeQueue | batch claim of 16 (`BATCH_SIZE=16`) | ~14,600 tasks/s |
| BullMQ 5 | one job per claim (concurrency 4) | ~12,600 jobs/s |

The batch claim is where ForgeQueue pulls ahead: at concurrency 4 a one-task
claim pays a full Redis round trip per task, while a 16-task claim amortizes one
round trip across the batch. Out of the box (`BATCH_SIZE=1`) it is *slower* than
BullMQ — the win only shows up once batching is on. Reproduce the BullMQ side
with the bundled same-machine harness:

```bash
cd bench/bullmq && npm install && JOBS=50000 CONCURRENCY=4 node bench.js
```

Real handlers dominate throughput in practice, so treat these as an upper bound.

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
| Queue paused (`queue:paused`) | Claim script returns nothing; in-flight tasks keep draining. |
| Worker shutdown (SIGINT/SIGTERM) | Infrastructure loops stop, in-flight handlers drain up to the drain timeout, then are cancelled and the worker is de-registered. |

## Limitations

- **No exactly-once.** Delivery is at-least-once: a lease can expire (worker crash, Redis blip, GC pause) and a task can run twice. Handlers must be idempotent.
- **Single Redis.** No replication, sharding, or failover logic — durability is whatever the Redis deployment provides. The bundled Kubernetes manifest runs one AOF-backed Redis on a PersistentVolumeClaim, but that is still a single point of failure.
- **Partial auth.** The HTTP API and the gRPC gateway authenticate with JWT
  bearer tokens (RBAC on the API, per-method permissions on gRPC), with
  optional OIDC/SSO login. Transport TLS is available but **off by default**:
  enable it on gRPC with the `FORGEQUEUE_GRPC_TLS_*` vars and terminate HTTP
  TLS at the ingress (see the Kubernetes section). Redis supports
  `requirepass` and TLS through its env vars. On a trusted pod network the
  defaults are plaintext; do not expose any port publicly without enabling TLS.
- **No multi-tenancy / queue isolation.** "Queues" are a single logical namespace, not separate Redis keyspaces or broker connections.
- **Completed bodies are deleted**, so a finished job cannot be looked up by ID afterwards (see the HTTP API note above).
- **Idle polling** uses a short sleep interval per worker; blocking variants (e.g. `BLMOVE`) are not implemented.
- **Bounded console.** The web UI logs in against the API and shows recent items; it is not a full admin plane (no user management, no audit log).
- **Not battle-tested.** No soak testing, formal fault injection, or large-scale benchmarks beyond the simple drain benchmark below.

## Development Status

**Implemented:**
- gRPC gateway with validation, UUIDs, immediate/delayed enqueue, fail-fast startup, graceful shutdown
- Worker pool with concurrent consumers, handler dispatch, clean shutdown, and graceful drain
- Lease-based ownership with fencing on all destructive operations
- Per-task heartbeats from claim time; batched claims/ACKs (`BATCH_SIZE`)
- Sub-second lease precision with regression coverage
- Retry budget with exponential backoff and dead letter queue
- Delayed task promotion
- Body fetch folded into claim and body cleanup on ACK
- Optional Postgres journal with reconciliation
- Prometheus metrics (worker + gateway) and a throughput/stats HTTP API
- HTTP/JSON control API (pause/resume/purge, retry/requeue/delete, DLQ ops, delayed ops)
- JWT + RBAC auth for the HTTP API
- OIDC / SSO login (authorization code + PKCE) with email-based role mapping
- React + TanStack Start web console with login
- gRPC load generator

**Planned / explicitly out of scope:**
- Smarter idle polling (blocking operations)
- HA Redis (replication/Sentinel) — the Kubernetes manifest runs a single AOF-backed Redis
- AuthN/AuthZ and multi-tenancy
- A streaming/Kafka-style log layer (deliberately not pursued — different problem)

## Design Notes

All critical operations use Lua scripts for atomicity:
- **Claim** (1 round trip): Pop up to `BATCH_SIZE` IDs from the ready list, add each to inflight with an expiry, store a lease token per task, fetch each body. Returns nothing while paused.
- **Heartbeat** (1 round trip): Verify the lease token and extend the expiry using Redis's TIME (fractional seconds).
- **ACK** (1 round trip for the batch): Verify each lease token, remove from inflight/leases, and delete the body for each ACKed task.
- **Requeue/DLQ** (1 round trip): Verify expiry or release the lease, load the body, increment attempts, schedule backoff or push to DLQ.
- **Promote** (1 round trip): Check the scheduled task's time against Redis TIME and move it to the ready list.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
Copyright 2026 Yash Bahuguna.
