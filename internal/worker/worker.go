package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"forgequeue/internal/durable"
	"forgequeue/internal/metrics"
	"forgequeue/internal/model"
)

// Worker registry keys. A worker pool publishes its identity here on Start
// and refreshes it on a heartbeat, so the dashboard can show live workers
// without the API and the worker sharing a process.
const (
	// workerRegistrySet holds the IDs of every worker that has registered.
	workerRegistrySet = "forgequeue:workers"
	// workerMetaPrefix builds the per-worker keys: <prefix><id>:meta and
	// <prefix><id>:leases.
	workerMetaPrefix = "worker:"
	// workerMetaTTL bounds how long a registration outlives its last
	// heartbeat. A crashed worker simply disappears once the TTL lapses.
	workerMetaTTL = 30 * time.Second
	// workerHeartbeatEvery is how often a live worker refreshes its keys.
	workerHeartbeatEvery = 10 * time.Second
)

// workerMeta is the JSON document published at worker:<id>:meta.
type workerMeta struct {
	ID            string `json:"id"`
	Hostname      string `json:"hostname"`
	Version       string `json:"version"`
	Status        string `json:"status"`
	StartedAt     string `json:"startedAt"`
	LastHeartbeat string `json:"lastHeartbeatAt"`
	Concurrency   int    `json:"concurrency"`
}

type WorkerPool struct {
	// The shared Redis client used for all queue operations.
	redisClient *redis.Client
	// Number of worker goroutines that process tasks in parallel.
	concurrencyLimit int
	// WaitGroup keeps track of how many workers are still running.
	// Stop() uses it to wait until every worker has finished.
	wg sync.WaitGroup

	// Closing this channel tells all workers that the pool is shutting down.
	quit chan struct{}

	// Shared context for Redis operations.
	// Stop() cancels this context so a worker waiting inside Redis
	// doesn't have to wait for the full BRPop timeout.
	ctx context.Context

	// Function used to cancel the shared context during shutdown.
	cancel context.CancelFunc

	//handler is jsut an arbitary value
	// used to map task types to their corresponding handlers.
	handlers map[string]TaskHandler
	// time to wait for a lease before giving up on a task.
	leaseDuration time.Duration

	// starting delay before the first retry after a failure.
	// every retry doubles it (1s, 2s, 4s, ...) until backoffCap.
	backoffBase time.Duration

	// ceiling for the exponential backoff delay so a flaky task
	// can't schedule itself minutes into the future.
	backoffCap time.Duration

	// how many tasks one worker claims per round trip. 1 keeps the old
	// single-task behaviour; larger batches trade round trips for latency
	// (tasks claimed together wait for each other). Opt-in via WithBatchSize.
	batchSize int

	// The durable journal (Postgres). It mirrors task lifecycles so Redis
	// state can be rebuilt after a crash. Nop by default: without a store
	// the pool behaves exactly as it always did.
	store durable.Store

	// Observability. Never nil: the default pool gets no-op instruments so
	// instrumentation call sites stay free of nil checks.
	metrics *metrics.Metrics

	// Identity of this pool instance. Published to Redis on Start so the
	// dashboard can attribute leases and heartbeats to a specific worker.
	id        string
	hostname  string
	startedAt time.Time
	version   string

	// bgCtx/bgCancel govern the background infrastructure loops (queue-depth
	// sampling, the durable reconciler) that stop only on context
	// cancellation. They are deliberately separate from ctx so Stop can drain
	// in-flight handlers (which run under ctx) before tearing the background
	// down.
	bgCtx    context.Context
	bgCancel context.CancelFunc

	// drainTimeout bounds how long Stop waits for in-flight handlers to
	// finish before cancelling them.
	drainTimeout time.Duration

	// recoveryInterval is how often the recovery loop looks for expired
	// leases. Defaults to one second; tests shrink it to exercise expiry
	// without waiting.
	recoveryInterval time.Duration
}

// taskRuntime holds the per-task lifecycle of one claimed task: the context
// its handler runs under and the cancellation hooks for the handler and its
// heartbeat.
//
// A batch is handled sequentially, so the runtime for every task in it is
// created and its heartbeat started the moment the batch is claimed. Without
// that, tasks waiting behind the one being executed would have no heartbeat
// and their leases would expire mid-batch, letting recovery hand them to
// another worker while this one was still running them.
type taskRuntime struct {
	ctx      context.Context
	cancel   context.CancelFunc
	stopBeat context.CancelFunc
}

// TaskLease is the proof of ownership a worker holds over a claimed task.
//
// TaskID identifies the task. LeaseID is a unique token generated at claim
// time; Redis stores the pair in the leases hash so later operations
// (heartbeat, ACK) can verify this worker still owns the task.
type TaskLease struct {
	TaskID  string
	LeaseID string
}

// TaskClaim is what a worker receives from claimBatch(): the ownership lease
// plus the task's JSON body. The claim Lua script reads the body in the same
// atomic call, so fetching it does not cost a second round trip to Redis.
type TaskClaim struct {
	TaskLease
	Body []byte
}

func NewWorkerPool(addr string, concurrencyLimit int) (*WorkerPool, error) {
	// Create the Redis client used by all workers.
	client := redis.NewClient(&redis.Options{
		Addr: addr,

		// Give Redis enough connections for the workers
		// and their concurrent Redis operations.
		PoolSize: concurrencyLimit * 2,
	})

	// Create one context for the whole worker pool.
	//
	// This context will be passed to Redis operations.
	// When Stop() calls cancel(), operations using this context
	// can be interrupted.
	ctx, cancel := context.WithCancel(context.Background())

	// Separate context for the background infrastructure loops, so they can
	// be stopped at the end of a graceful drain without cancelling the
	// handlers still finishing under ctx.
	bgCtx, bgCancel := context.WithCancel(context.Background())

	// Check that Redis is reachable before returning the pool.
	if err := client.Ping(context.Background()).Err(); err != nil {
		// If Redis isn't reachable, cancel the contexts we just created
		// because the worker pool won't be started.
		cancel()
		bgCancel()
		return nil, err
	}

	hostname, _ := os.Hostname()

	return &WorkerPool{
		redisClient:      client,
		concurrencyLimit: concurrencyLimit,
		store:            durable.Nop{},
		id:               uuid.New().String(),
		hostname:         hostname,
		startedAt:        time.Now(),
		version:          "v0.9.1",

		// sync.WaitGroup starts with a counter of 0.
		// Start() will add one for every worker it creates.
		wg: sync.WaitGroup{},

		// Channel used as a shutdown signal.
		quit: make(chan struct{}),

		// Shared context and its cancellation function.
		ctx:              ctx,
		cancel:           cancel,
		bgCtx:            bgCtx,
		bgCancel:         bgCancel,
		drainTimeout:     30 * time.Second,
		recoveryInterval: 1 * time.Second,
		handlers:         make(map[string]TaskHandler),
		leaseDuration:    10 * time.Second,
		backoffBase:      1 * time.Second,
		backoffCap:       60 * time.Second,
		batchSize:        1,
		metrics:          metrics.NewNop(),
	}, nil
}

// WithMetrics attaches a telemetry sink to the pool. Call it before Start().
// Passing nil is a no-op, so callers can forward an optional *metrics.Metrics
// without branching.
func (wp *WorkerPool) WithMetrics(m *metrics.Metrics) *WorkerPool {
	if m == nil {
		return wp
	}
	wp.metrics = m
	return wp
}

// workerLoop is the work performed by one worker.
//
// workerID identifies which worker is running this loop.
// For example, Start() can create worker 0, worker 1, worker 2, etc.
//
// One iteration handles a whole batch: claim up to batchSize tasks in a
// single round trip, run each task's handler sequentially, then ACK all the
// successes in a single round trip. With batchSize = 1 this degenerates
// exactly to the original claim-one/ack-one loop.
func (wp *WorkerPool) workerLoop(workerID int) {
	for {
		select {
		// If the quit channel is closed, the worker stops.
		case <-wp.quit:
			return

		default:
			claims, err := wp.claimBatch()

			// claimBatch returns nothing when the queue is empty.
			// Sleep briefly so workers don't spin hot on an idle queue.
			if len(claims) == 0 {
				time.Sleep(1 * time.Millisecond)
				continue
			}
			if err != nil {
				log.Printf(
					"worker %d: failed to claim tasks: %v",
					workerID,
					err,
				)
				continue
			}

			var ackIDs []string
			var ackLeases []string

			// Record every claimed task against this worker so the
			// dashboard can show what it is processing.
			claimedIDs := make([]string, 0, len(claims))
			for _, claim := range claims {
				if claim.TaskID != "" {
					claimedIDs = append(claimedIDs, claim.TaskID)
				}
			}
			wp.markLeases(claimedIDs)

			// Start a heartbeat for every claimed task up front. The batch
			// is handled sequentially, so a task waiting behind the one
			// being executed would otherwise have no heartbeat and its lease
			// would expire mid-batch - exactly the window in which recovery
			// hands the task to a second worker and it runs twice.
			runtimes := make(map[string]*taskRuntime, len(claims))
			for _, claim := range claims {
				if claim.TaskID == "" {
					continue
				}
				taskCtx, cancelTask := context.WithCancel(wp.ctx)
				beatCtx, stopBeat := context.WithCancel(wp.ctx)
				runtimes[claim.TaskID] = &taskRuntime{
					ctx:      taskCtx,
					cancel:   cancelTask,
					stopBeat: stopBeat,
				}
				go wp.heartbeatLoop(beatCtx, claim.TaskID, claim.LeaseID, cancelTask)
			}

			// stopTask ends the heartbeat and execution context for one task.
			// Idempotent, so it is safe on every exit path and again in the
			// cleanup sweep below.
			stopTask := func(id string) {
				if rt, ok := runtimes[id]; ok {
					rt.stopBeat()
					rt.cancel()
				}
			}

			for _, claim := range claims {

				// Defensive guard: a claimed task must always have an ID.
				if claim.TaskID == "" {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// The task body was already read by the claim script.
				// An empty body means the task data was missing.
				if len(claim.Body) == 0 {
					log.Printf(
						"worker %d: failed to get task %s: no task body",
						workerID,
						claim.TaskID,
					)
					stopTask(claim.TaskID)
					continue
				}

				var task model.TaskMetaData

				// Convert the JSON stored in Redis into TaskMetaData.
				//
				// &task gives Unmarshal the address of task so that
				// it can fill the struct.
				if err := json.Unmarshal(claim.Body, &task); err != nil {
					log.Printf(
						"worker %d: failed to parse task %s: %v",
						workerID,
						claim.TaskID,
						err,
					)
					stopTask(claim.TaskID)
					continue
				}

				// Look up the handler for this task's type.
				//
				// ok is true when a handler was registered for task.TaskType
				// (e.g. "email"). If none is registered, the task can't be
				// processed, so skip it and let it expire/requeue.
				handler, ok := wp.handlers[task.TaskType]
				if !ok {
					log.Printf(
						"worker %d: no handler registered for task type %s",
						workerID,
						task.TaskType,
					)
					stopTask(claim.TaskID)
					continue
				}

				// The task is now owned by this worker, so the journal should
				// know it is being executed. Best-effort: if Postgres is down
				// the task still runs, the record is just stale.
				if err := wp.store.Processing(wp.ctx, claim.TaskID); err != nil {
					log.Printf(
						"journal: failed to mark task %s as processing: %v",
						claim.TaskID,
						err,
					)
				}

				handlerStart := time.Now()

				// The handler runs under the task's pre-created context, whose
				// heartbeat is already renewing the lease in the background.
				// The heartbeat is deliberately left running after the handler
				// returns: the ACK is deferred to the single batched call at
				// the end of the loop, so stopping it here would let an
				// early-finished task's lease expire while it waits its turn.
				rt := runtimes[claim.TaskID]
				handlerErr := handler.Handle(rt.ctx, &task)

				// How long the handler actually ran. Recorded for both
				// outcomes: a slow success and a slow failure are very
				// different signals when you are looking at p99.
				wp.metrics.TaskDuration.Record(
					wp.ctx,
					time.Since(handlerStart).Seconds(),
				)

				if handlerErr == nil {
					ackIDs = append(ackIDs, claim.TaskID)
					ackLeases = append(ackLeases, claim.LeaseID)
					continue
				}

				log.Printf(
					"worker %d: failed to execute task %s: %v",
					workerID,
					task.ID,
					handlerErr,
				)

				// A handler error counts as a failed attempt. Release the
				// lease right away (fenced, like an ACK) instead of waiting
				// for it to expire, so the retry happens on the backoff
				// delay instead of whenever the lease runs out.
				status, rerr := wp.requeueOrDead(task.ID, claim.LeaseID)
				if rerr != nil {
					log.Printf(
						"worker %d: failed to schedule retry for task %s: %v",
						workerID,
						task.ID,
						rerr,
					)
					continue
				}
				log.Printf(
					"worker %d: task %s after failure: %s",
					workerID,
					task.ID,
					status,
				)
				switch status {
				case "scheduled":
					wp.metrics.TasksRetried.Add(wp.ctx, 1)
					wp.reportRetry(task.ID)
				case "dead":
					wp.metrics.TasksDLQ.Add(wp.ctx, 1)
					wp.reportDead(task.ID)
					now := time.Now().Unix()
					wp.redisClient.ZAdd(wp.ctx, "metrics:failed", redis.Z{Score: float64(now), Member: task.ID})
					wp.redisClient.Expire(wp.ctx, "metrics:failed", 24*time.Hour)
				}

				// The lease was released by requeueOrDead, so there is
				// nothing left for the heartbeat to keep alive.
				stopTask(task.ID)
			}

			// Everything is decided for this batch now, so ACK the successes
			// in one fenced script. Only after the ACK lands do we stop the
			// remaining heartbeats: an early-finished success has to keep
			// heartbeating until its ACK is committed, otherwise its lease
			// expires while it waits for the rest of the batch and recovery
			// hands the task to a second worker.
			//
			// The script fences per task, so a batch can be partially ACKed:
			// if one task's lease really did expire, recovery may already
			// have handed it to another worker and rotated its lease. That
			// task is skipped, the rest are ACKed, and the returned ids are
			// the ones that actually completed.
			if len(ackIDs) > 0 {
				ackedIDs, ackErr := wp.ackBatch(ackIDs, ackLeases)
				if ackErr != nil {
					log.Printf(
						"worker %d: failed to ACK %d task(s): %v",
						workerID,
						len(ackIDs),
						ackErr,
					)
				} else {
					if len(ackedIDs) < len(ackIDs) {
						// Worth a warning: this is the at-least-once duplicate
						// path. A silent overcount here would look like healthy
						// throughput while tasks were actually running twice.
						log.Printf(
							"worker %d: only %d of %d task(s) ACKed, %d lost their lease "+
								"and may run again elsewhere",
							workerID, len(ackedIDs), len(ackIDs), len(ackIDs)-len(ackedIDs),
						)
					}

					// ACKed means done. Counted from the script's return value
					// rather than len(ackIDs), so a fenced task is not reported
					// as processed while it is still outstanding elsewhere.
					wp.metrics.TasksProcessed.Add(wp.ctx, int64(len(ackedIDs)))

					// ACKed means done: record it so the journal stops tracking
					// it. Only the tasks the script actually ACKed are counted,
					// so a task that lost its lease is not double-counted as
					// completed.
					now := time.Now().Unix()
					for _, id := range ackedIDs {
						if err := wp.store.Succeeded(wp.ctx, id); err != nil {
							log.Printf(
								"journal: failed to mark task %s as succeeded: %v",
								id,
								err,
							)
						}
						wp.redisClient.ZAdd(wp.ctx, "metrics:completed", redis.Z{Score: float64(now), Member: id})
					}
					wp.redisClient.Expire(wp.ctx, "metrics:completed", 24*time.Hour)
				}
			}

			// Every task in this batch has reached an outcome by now (ACKed,
			// retried or dead-lettered), so stop any heartbeat still running
			// and drop them from this worker's lease set.
			for id := range runtimes {
				stopTask(id)
			}
			wp.clearLeases(claimedIDs)
		}
	}
}

// WithStore attaches the durable journal to the pool and makes Start() run
// the reconciler alongside the workers. Call it before Start().
func (wp *WorkerPool) WithStore(s durable.Store) *WorkerPool {
	wp.store = s
	return wp
}

// WithBatchSize configures how many tasks each worker claims per round trip.
// 1 (the default) preserves the old single-task behaviour. Values above 1
// claim a whole batch atomically and collect ACKs, cutting Redis round trips
// well below one per task.
func (wp *WorkerPool) WithBatchSize(n int) *WorkerPool {
	if n < 1 {
		n = 1
	}
	wp.batchSize = n
	return wp
}

// WithDrainTimeout bounds how long Stop waits for in-flight handlers to finish
// before cancelling them. Call it before Start().
func (wp *WorkerPool) WithDrainTimeout(d time.Duration) *WorkerPool {
	if d > 0 {
		wp.drainTimeout = d
	}
	return wp
}

// Start creates and starts the workers.
//
// The number of workers is controlled by concurrencyLimit.
func (wp *WorkerPool) Start() {
	// Publish this pool's identity and keep it fresh with a heartbeat, so
	// the dashboard can see the worker without sharing its process.
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		wp.registrationLoop()
	}()

	for i := 0; i < wp.concurrencyLimit; i++ {

		// Tell the WaitGroup that another worker is about to start.
		wp.wg.Add(1)

		// Start the worker as a goroutine.
		//
		// i is passed into the anonymous function so each worker
		// receives its own worker ID.
		go func(i int) {

			// Done() is called automatically when this worker exits,
			// even if workerLoop returns from the quit signal.
			defer wp.wg.Done()

			// Run the actual worker loop.
			wp.workerLoop(i)

		}(i)
	}
	// The recovery loop runs separately from the workers: it has its own
	// goroutine so it keeps requeueing expired tasks even while every
	// worker is busy executing.
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		wp.recoveryLoop()
	}()

	// The promotion loop is separate too: it watches the scheduled set and
	// moves tasks whose delay has passed into the ready queue.
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		wp.promotionLoop()
	}()

	// The reconciler heals the journal's write gap: it re-materializes into
	// Redis any task the journal knows about that Redis is missing. It only
	// exists when a durable store is attached, otherwise it would be a
	// pointless wire to an empty table every second.
	if _, isNop := wp.store.(durable.Nop); !isNop {
		wp.wg.Add(1)
		go func() {
			defer wp.wg.Done()
			durable.ReconcileLoop(wp.bgCtx, wp.redisClient, wp.store)
		}()
	}

	// Queue depth is sampled from Redis on a timer. Doing it in-process
	// would need a matching increment and decrement for every transition,
	// and one missed transition would leave the gauge permanently wrong.
	if wp.metrics.Ready {
		wp.wg.Add(1)
		go func() {
			defer wp.wg.Done()
			metrics.NewDepthSampler(
				wp.metrics,
				wp.redisClient,
				metrics.DefaultDepthInterval,
				func(err error) {
					log.Printf("queue depth sample failed: %v", err)
				},
			).Run(wp.bgCtx)
		}()
	}
}

// Stop gracefully shuts down the worker pool.
//
// It works in three phases so in-flight work is drained rather than aborted:
//
//  1. close quit and stop the background infrastructure. Workers stop claiming
//     new tasks at the top of their loop, but a worker already inside a batch
//     keeps running it.
//  2. wait for every goroutine to return. Handlers run under ctx, which is
//     still alive, so they finish normally instead of being cancelled.
//  3. cancel ctx and deregister. By now no handler is running, so cancelling
//     is safe.
//
// A slow or stuck handler must not hold shutdown open forever, so phase 2 is
// bounded by drainTimeout; past it the context is cancelled to force the
// handlers to unwind.
func (wp *WorkerPool) Stop() {
	// Phase 1: stop accepting new claims and stop the loops that would
	// otherwise run for the lifetime of the pool.
	close(wp.quit)
	wp.bgCancel()

	// Phase 2: wait for workers to finish the batch they are inside, while
	// their contexts are still valid.
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(wp.drainTimeout):
		log.Printf(
			"shutdown: in-flight tasks still running after %s, cancelling them",
			wp.drainTimeout,
		)
	}

	// Phase 3: nothing should be executing now, so tear the context down and
	// remove the registration so the dashboard reflects the shutdown. In the
	// normal path the wait above already returned, so the pool is fully idle.
	wp.cancel()
	wp.deregister()
}

func (wp *WorkerPool) RegisterHandler(
	taskType string,
	handler TaskHandler,
) {
	//happens in start where user stores the tasktype in the map
	// then in redis when a task is received, the task type is used to look up the handler in the map
	wp.handlers[taskType] = handler
}

// TaskHandler defines the interface for task handlers.
type TaskHandler interface {
	//create a taskhandler type which needs to handle and have these
	Handle(ctx context.Context, task *model.TaskMetaData) error
}

// claimBatch pops up to batchSize tasks off the immediate queue, claims
// ownership of them all, and reads their bodies — in one atomic Lua script.
//
// Every task still costs a claim and an ACK, but they no longer cost one
// round trip each: a whole batch is claimed together and ACKed together, so
// the cost collapses to ~2 round trips per batch. The script returns the
// lease (ID + ownership token sent in ARGV) together with each stored JSON
// body, so the worker never sends a separate GET.
//
// Lease IDs are generated here in Go and passed in ARGV; the script assigns
// them to whatever it pops (ARGV[3+i] goes to the i-th popped task), and the
// caller pairs the returned bodies back to those IDs by index.
func (wp *WorkerPool) claimBatch() ([]TaskClaim, error) {
	claimStart := time.Now()

	// The expiry is the deadline for the lease. If the worker doesn't
	// ACK or heartbeat before this, recovery will requeue the task.
	// Keep fractional seconds: truncating to whole seconds would silently
	// shave up to a second off short leases, expiring them early.
	leaseExpiry := float64(time.Now().UnixNano())/1e9 + wp.leaseDuration.Seconds()

	batch := wp.batchSize
	if batch < 1 {
		batch = 1
	}
	leaseIDs := make([]string, batch)
	args := make([]interface{}, 0, batch+3)
	args = append(args, batch, strconv.FormatFloat(leaseExpiry, 'f', -1, 64), "task:")
	for i := 0; i < batch; i++ {
		leaseIDs[i] = uuid.New().String()
		args = append(args, leaseIDs[i])
	}

	// KEYS[1] -> queue:tasks:immediate  (source of ready tasks)
	// KEYS[2] -> queue:tasks:inflight   (sorted set: taskID -> expiry)
	// KEYS[3] -> queue:tasks:leases     (hash: taskID -> leaseID)
	// KEYS[4] -> queue:paused           (presence means "stop claiming")
	// ARGV[1] -> how many tasks to try to claim (the batch size)
	// ARGV[2] -> lease expiry (Unix seconds, fractional)
	// ARGV[3] -> "task:" prefix used to build the body's key
	// ARGV[4..] -> one leaseID per possible claim, in order
	script := redis.NewScript(`
        if redis.call("EXISTS", KEYS[4]) == 1 then
            return ""
        end

        local count = 0
        local ids = {}

        for i = 1, tonumber(ARGV[1]) do
            local taskID = redis.call("LPOP", KEYS[1])
            if not taskID then
                break
            end
            count = count + 1
            ids[count] = taskID

            redis.call("ZADD", KEYS[2], ARGV[2], taskID)
            redis.call("HSET", KEYS[3], taskID, ARGV[3 + i])
        end

        if count == 0 then
            return ""
        end

        local out = {}
        for i = 1, count do
            out[(i - 1) * 2 + 1] = ids[i]
            out[(i - 1) * 2 + 2] = redis.call("GET", ARGV[3] .. ids[i])
        end
        return out
    `)

	result, err := script.Run(
		wp.ctx,
		wp.redisClient,
		[]string{
			"queue:tasks:immediate",
			"queue:tasks:inflight",
			"queue:tasks:leases",
			"queue:paused",
		},
		args...,
	).Result()

	if err != nil {
		return nil, err
	}

	// The script returns an empty string when the queue was empty.
	if result == "" {
		// Deliberately not recorded. An idle worker polls this script every
		// millisecond, so those round trips outnumber real claims by orders
		// of magnitude and complete in microseconds. Averaging them into the
		// same histogram drags every percentile toward zero and hides the
		// claim latency that actually matters. The poll rate is still
		// visible as Redis command traffic in Prometheus.
		return nil, nil
	}

	// A claim that returned work is worth timing: it is the queue's floor on
	// per-task overhead.
	wp.metrics.ClaimLatency.Record(
		wp.ctx,
		time.Since(claimStart).Seconds(),
	)

	// Otherwise it returns {id1, body1, id2, body2, ...}.
	values := result.([]interface{})
	claims := make([]TaskClaim, 0, len(values)/2)

	for i := 0; i+1 < len(values); i += 2 {
		taskID := ""
		if values[i] != nil {
			taskID = values[i].(string)
		}

		body := []byte{}
		if values[i+1] != nil {
			body = []byte(values[i+1].(string))
		}

		// The i-th returned task got the i-th leaseID.
		claims = append(claims, TaskClaim{
			TaskLease: TaskLease{
				TaskID:  taskID,
				LeaseID: leaseIDs[i/2],
			},
			Body: body,
		})
	}

	return claims, nil
}

// findExpiredTasks returns the task IDs whose lease has expired.
//
// The inflight sorted set stores taskID -> lease expiry. Asking for
// everything between "-inf" and the current Unix time ("now") returns
// exactly the tasks whose deadline has passed and are ready for requeue.
func (wp *WorkerPool) findExpiredTasks() ([]string, error) {
	now := float64(time.Now().UnixNano()) / 1e9
	return wp.redisClient.ZRangeArgs(
		wp.ctx,
		redis.ZRangeArgs{
			Key:   "queue:tasks:inflight",
			Start: "-inf",
			// Stop is a float score, so it has to be converted to a string.
			Stop:    strconv.FormatFloat(now, 'f', -1, 64),
			ByScore: true,
		},
	).Result()
}

// requeueOrDead decides what happens to a failed or expired task.
//
// Two different callers share this one script:
//
//   - the worker, right after a handler returns an error. It passes the
//     leaseID so the script fences like an ACK: if the worker no longer owns
//     the task, the call is rejected and the task is left alone.
//   - the recovery loop, for a task whose lease expired while nobody was
//     processing it. It passes "" and instead relies on re-checking the
//     inflight score against Redis's own clock, exactly like the old
//     removeExpiredTasks did.
//
// Whichever caller it is, the script then:
//
//  1. loads the task body, increments the "attempts" counter, and rewrites
//     the body with the new counter either way.
//  2. decides between two outcomes:
//     - "scheduled": still below its retry budget. The task goes into the
//     scheduled set with a backoff score (now + base * 2^(attempts-1),
//     capped).
//     - "dead": the budget is exhausted. The full body (with attempts) is
//     pushed onto the dead-letter queue so a human can inspect it.
//
// A task with max_retries = 0 has an unlimited budget and always retries.
//
// Return values: "fenced" (worker lost the lease), "gone" (nothing in
// inflight), "renewed" (lease extended before us), "no-body" (dropped),
// "dead" (moved to the dead-letter queue), "scheduled" (retry queued).
//
// KEYS[1] -> queue:tasks:inflight
// KEYS[2] -> queue:tasks:leases
// KEYS[3] -> queue:tasks:scheduled
// KEYS[4] -> queue:tasks:dead
// ARGV[1] -> "task:" prefix (builds the body key)
// ARGV[2] -> taskID
// ARGV[3] -> leaseID to fence with, or "" when called from recovery
// ARGV[4] -> backoff base in seconds
// ARGV[5] -> backoff cap in seconds
func (wp *WorkerPool) requeueOrDead(taskID string, leaseID string) (string, error) {
	script := redis.NewScript(`
        local now = redis.call("TIME")
        local nowf = tonumber(now[1]) + tonumber(now[2]) / 1000000

        if ARGV[3] ~= "" then
            -- called by the worker: fence like an ACK.
            local current = redis.call("HGET", KEYS[2], ARGV[2])
            if current ~= ARGV[3] then
                return "fenced"
            end
            redis.call("ZREM", KEYS[1], ARGV[2])
            redis.call("HDEL", KEYS[2], ARGV[2])
        else
            -- called by recovery: re-check the expiry inside Redis.
            local expiry = redis.call("ZSCORE", KEYS[1], ARGV[2])
            if not expiry then
                return "gone"
            end
            if tonumber(expiry) > nowf then
                return "renewed"
            end
            redis.call("ZREM", KEYS[1], ARGV[2])
            redis.call("HDEL", KEYS[2], ARGV[2])
        end

        local body = redis.call("GET", ARGV[1] .. ARGV[2])
        if not body then
            return "no-body"
        end

        local ok, task = pcall(cjson.decode, body)
        -- Un-decodable bodies can't be retried or counted, so they go
        -- straight to the dead-letter queue instead of vanishing.
        if not ok or type(task) ~= "table" then
            redis.call("LPUSH", KEYS[4], body)
            return "dead"
        end

        local attempts = (task.attempts or 0) + 1
        task.attempts = attempts
        local newBody = cjson.encode(task)

        -- The body always reflects the latest attempt counter, whether the
        -- task is retried or dead-lettered. Makes the attempt count visible
        -- anywhere the body is read from.
        redis.call("SET", ARGV[1] .. ARGV[2], newBody)

        local maxRetries = task.max_retries or 0
        if maxRetries > 0 and attempts >= maxRetries then
            redis.call("LPUSH", KEYS[4], newBody)
            return "dead"
        end

        -- backoff delay: base * 2^(attempts-1), capped.
        local base = tonumber(ARGV[4])
        local cap = tonumber(ARGV[5])
        local delay = base
        local i = 2
        while i <= attempts do
            delay = delay * 2
            i = i + 1
        end
        if delay > cap then
            delay = cap
        end

        local retryAt = nowf + delay

        redis.call("ZADD", KEYS[3], retryAt, ARGV[2])
        return "scheduled"
    `)

	result, err := script.Run(
		wp.ctx,
		wp.redisClient,
		[]string{
			"queue:tasks:inflight",
			"queue:tasks:leases",
			"queue:tasks:scheduled",
			"queue:tasks:dead",
		},
		"task:",
		taskID,
		leaseID,
		strconv.FormatFloat(wp.backoffBase.Seconds(), 'f', -1, 64),
		strconv.FormatFloat(wp.backoffCap.Seconds(), 'f', -1, 64),
	).Result()

	if err != nil {
		return "", err
	}
	return result.(string), nil
}

// backoffDelay mirrors the delay computed inside the requeueOrDead script:
// base * 2^(attempts-1), capped. The two halves must keep the same math, or
// the journal would record a different schedule than Redis actually applied.
func backoffDelay(attempts int32, base, cap time.Duration) time.Duration {
	delay := base
	for i := int32(2); i <= attempts; i++ {
		delay *= 2
	}
	if delay > cap {
		return cap
	}
	return delay
}

// reportRetry tells the journal a task was put into backoff. Redis already
// holds the task (the requeue script scheduled it), so the journal only
// records what Redis decided - the counter and the new schedule. Reading the
// body back is one extra round trip, but only on the failure path.
func (wp *WorkerPool) reportRetry(taskID string) {
	t, ok := wp.readTaskForJournal(taskID)
	if !ok {
		return
	}
	due := time.Now().Add(backoffDelay(t.Attempts, wp.backoffBase, wp.backoffCap))
	if err := wp.store.Retrying(wp.ctx, taskID, t.Attempts, due); err != nil {
		log.Printf("journal: failed to record retry for task %s: %v", taskID, err)
	}
}

// reportDead tells the journal a task exhausted its retry budget. The body
// (with its final attempt count) is already in the dead-letter queue.
func (wp *WorkerPool) reportDead(taskID string) {
	t, ok := wp.readTaskForJournal(taskID)
	if !ok {
		return
	}
	if err := wp.store.Dead(wp.ctx, taskID, t.Attempts); err != nil {
		log.Printf("journal: failed to record dead task %s: %v", taskID, err)
	}
}

// readTaskForJournal fetches the current body of a task so its post-script
// attempt count can be reported. The body is undefined when the queue has no
// record of it, which is fine - there is nothing useful to report then.
func (wp *WorkerPool) readTaskForJournal(taskID string) (model.TaskMetaData, bool) {
	body, err := wp.redisClient.Get(wp.ctx, "task:"+taskID).Result()
	if err != nil {
		log.Printf("journal: failed to read body for task %s: %v", taskID, err)
		return model.TaskMetaData{}, false
	}
	var t model.TaskMetaData
	if err := json.Unmarshal([]byte(body), &t); err != nil {
		log.Printf("journal: failed to parse task %s: %v", taskID, err)
		return model.TaskMetaData{}, false
	}
	return t, true
}

// recoveryLoop watches for expired leases and handles them.
//
// Every second it asks Redis which in-flight tasks have expired, then hands
// each one to requeueOrDead, which either schedules a backoff retry or dead-
// letters it once the retry budget is gone. It runs in its own goroutine and
// stops when the pool shuts down.
func (wp *WorkerPool) recoveryLoop() {
	// Re-check for expired tasks on the configured interval (one second by
	// default).
	ticker := time.NewTicker(wp.recoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			taskIDs, err := wp.findExpiredTasks()
			if err != nil {
				log.Printf("recovery: failed to find expired tasks: %v", err)
				continue
			}

			for _, taskID := range taskIDs {
				status, err := wp.requeueOrDead(taskID, "")
				if err != nil {
					log.Printf("recovery: failed to handle task %s: %v", taskID, err)
					continue
				}
				switch status {
				case "scheduled":
					wp.metrics.TasksRetried.Add(wp.ctx, 1)
					wp.reportRetry(taskID)
				case "dead":
					log.Printf("recovery: task %s exhausted its retry budget, moved to dead-letter queue", taskID)
					wp.metrics.TasksDLQ.Add(wp.ctx, 1)
					wp.reportDead(taskID)
					now := time.Now().Unix()
					wp.redisClient.ZAdd(wp.ctx, "metrics:failed", redis.Z{Score: float64(now), Member: taskID})
					wp.redisClient.Expire(wp.ctx, "metrics:failed", 24*time.Hour)
				case "no-body":
					log.Printf("recovery: task %s has no body, dropped", taskID)
				}
				// "renewed", "gone" and "scheduled" are the quiet cases:
				// the lease was extended, someone else handled it, or it
				// was scheduled for another attempt.
			}

		case <-wp.quit:
			return
		}
	}
}

// findDueTasks returns task IDs in the scheduled set whose delay has passed.
//
// Being "due" as reported here is only a candidate: promoteTask re-checks the
// score against Redis's own clock atomically, just like recovery does.
func (wp *WorkerPool) findDueTasks() ([]string, error) {
	now := float64(time.Now().UnixNano()) / 1e9
	return wp.redisClient.ZRangeArgs(
		wp.ctx,
		redis.ZRangeArgs{
			Key:     "queue:tasks:scheduled",
			Start:   "-inf",
			Stop:    strconv.FormatFloat(now, 'f', -1, 64),
			ByScore: true,
		},
	).Result()
}

// promoteTask moves one task from the scheduled set to the immediate queue.
//
// The task body lives at task:<id> (both the gateway's delayed path and
// requeueOrDead write it there), so promotion is just a list move. The score
// is re-checked against TIME inside the script so a task is never promoted
// early and never promoted twice.
//
// KEYS[1] -> queue:tasks:scheduled
// KEYS[2] -> queue:tasks:immediate
// ARGV[1] -> taskID
func (wp *WorkerPool) promoteTask(taskID string) (bool, error) {
	script := redis.NewScript(`
        local score = redis.call("ZSCORE", KEYS[1], ARGV[1])
        if not score then
            return 0
        end
        local now = redis.call("TIME")
        local nowf = tonumber(now[1]) + tonumber(now[2]) / 1000000
        if tonumber(score) > nowf then
            return 0
        end
        redis.call("ZREM", KEYS[1], ARGV[1])
        redis.call("LPUSH", KEYS[2], ARGV[1])
        return 1
    `)

	result, err := script.Run(
		wp.ctx,
		wp.redisClient,
		[]string{
			"queue:tasks:scheduled",
			"queue:tasks:immediate",
		},
		taskID,
	).Result()

	if err != nil {
		return false, err
	}
	return result.(int64) == 1, nil
}

// promotionLoop watches the scheduled set and moves due tasks to the ready
// queue. Delayed enqueues and backoff retries both land here. It runs in its
// own goroutine and stops when the pool shuts down.
func (wp *WorkerPool) promotionLoop() {
	// Check for due tasks once per second.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			taskIDs, err := wp.findDueTasks()
			if err != nil {
				log.Printf("promotion: failed to find due tasks: %v", err)
				continue
			}
			for _, taskID := range taskIDs {
				if _, err := wp.promoteTask(taskID); err != nil {
					log.Printf("promotion: failed to promote task %s: %v", taskID, err)
				}
			}

		case <-wp.quit:
			return
		}
	}
}

// ackBatch tells Redis that a list of tasks finished successfully.
//
// Every task is fenced individually: it is only removed if the presented
// leaseID still matches the owner in the leases hash, so an ACK from a worker
// whose lease was overwritten (e.g. the task was requeued after expiry) is
// rejected without touching anything else. One round trip for the whole list.
//
// A successfully ACKed task has no further use for its body, so the script
// deletes task:<id> in the same atomic call. That is what keeps the task
// store from growing without bound. The returned slice holds exactly the IDs
// that were ACKed, so callers never count a fenced task as completed.
//
// KEYS[1] -> queue:tasks:inflight (sorted set: taskID -> expiry)
// KEYS[2] -> queue:tasks:leases   (hash: taskID -> leaseID)
// KEYS[3] -> "task:" prefix (builds the body key to delete)
// ARGV[1..] -> flat pairs: taskID1, leaseID1, taskID2, leaseID2, ...
func (wp *WorkerPool) ackBatch(taskIDs []string, leaseIDs []string) ([]string, error) {
	args := make([]interface{}, 0, 2*len(taskIDs))
	for i := range taskIDs {
		args = append(args, taskIDs[i], leaseIDs[i])
	}

	script := redis.NewScript(`
        local acked = {}
        local i = 1
        while i <= #ARGV do
            local taskID = ARGV[i]
            local leaseID = ARGV[i + 1]

            if redis.call("HGET", KEYS[2], taskID) == leaseID then
                redis.call("ZREM", KEYS[1], taskID)
                redis.call("HDEL", KEYS[2], taskID)
                redis.call("DEL", KEYS[3] .. taskID)
                acked[#acked + 1] = taskID
            end

            i = i + 2
        end
        return acked
    `)

	ackStart := time.Now()
	result, err := script.Run(
		wp.ctx,
		wp.redisClient,
		[]string{
			"queue:tasks:inflight",
			"queue:tasks:leases",
			"task:",
		},
		args...,
	).Result()

	wp.metrics.ACKLatency.Record(
		wp.ctx,
		time.Since(ackStart).Seconds(),
	)

	if err != nil {
		return nil, err
	}

	values := result.([]interface{})
	acked := make([]string, 0, len(values))
	for _, v := range values {
		if v != nil {
			acked = append(acked, v.(string))
		}
	}
	return acked, nil
}

// errLeaseLost is returned by heartbeat when Redis answered but the lease
// token no longer matches. A distinct sentinel (rather than a generic
// error) so the caller can tell "somebody else owns this task now" apart
// from "Redis was unreachable", which is a transport problem rather than a
// lost lease and should not be counted as one.
var errLeaseLost = errors.New("heartbeat rejected: lease no longer held")

// heartbeat renews the lease for a task that is still being processed.
//
// It checks ownership first (HGET) and, if the worker still owns the task,
// pushes the expiry forward using Redis's own clock rather than the
// worker's clock.
//
// KEYS[1] -> queue:tasks:leases   (hash: taskID -> leaseID)
// KEYS[2] -> queue:tasks:inflight (sorted set: taskID -> expiry)
// ARGV[1] -> taskID
// ARGV[2] -> leaseID presented by the worker
// ARGV[3] -> lease duration in seconds (fractional)
func (wp *WorkerPool) heartbeat(taskID string, leaseID string) error {
	beatStart := time.Now()
	script := redis.NewScript(`
        local currentLease = redis.call("HGET", KEYS[1], ARGV[1])

        if currentLease ~= ARGV[2] then
            return 0
        end

        local now = redis.call("TIME")
        local nowf = tonumber(now[1]) + tonumber(now[2]) / 1000000
        local newExpiry = nowf + tonumber(ARGV[3])

        redis.call("ZADD", KEYS[2], newExpiry, ARGV[1])

        return 1
    `)

	result, err := script.Run(
		wp.ctx,
		wp.redisClient,
		[]string{
			"queue:tasks:leases",
			"queue:tasks:inflight",
		},
		taskID,
		leaseID,
		strconv.FormatFloat(wp.leaseDuration.Seconds(), 'f', -1, 64),
	).Result()

	// Recorded on both paths: a slow heartbeat is what causes lease
	// expiry in the first place, so the failures matter too.
	wp.metrics.HeartbeatLatency.Record(
		wp.ctx,
		time.Since(beatStart).Seconds(),
	)

	if err != nil {
		return err
	}

	// return 0 means the worker no longer holds the lease,
	// so the heartbeat is rejected.
	if result.(int64) == 0 {
		return errLeaseLost
	}

	return nil
}

// heartbeatLoop keeps a task's lease alive while it is being handled.
//
// heartbeat() extends the lease once; this loop calls it repeatedly at a
// fraction of the lease duration (leaseDuration / 3) so a long-running task
// never expires while it's still making progress.
//
// It stops either when the per-task context is cancelled (the handler
// finished) or when a heartbeat is rejected (the lease was lost). When the
// lease is lost, it also cancels the task context so the running handler
// is told to stop early.
func (wp *WorkerPool) heartbeatLoop(
	ctx context.Context,
	taskID string,
	leaseID string,
	cancelTask context.CancelFunc,
) {
	ticker := time.NewTicker(wp.leaseDuration / 3)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := wp.heartbeat(taskID, leaseID); err != nil {
				// A heartbeat that raced with the batch's own ACK (the lease
				// was removed the instant we stopped) is not a real loss: the
				// stop already happened. Only report it when the loop is still
				// meant to be running.
				if ctx.Err() != nil {
					return
				}

				log.Printf(
					"heartbeat failed for task %s: %v",
					taskID,
					err,
				)

				// Only a rejected lease counts as a lease loss. A
				// transport error or a shutdown-induced cancellation
				// is a different failure and would otherwise inflate
				// the metric during a Redis blip or a rolling restart.
				if errors.Is(err, errLeaseLost) {
					wp.metrics.LeaseLosses.Add(wp.ctx, 1)
				}

				cancelTask()
				return
			}

		case <-ctx.Done():
			return
		}
	}
}
