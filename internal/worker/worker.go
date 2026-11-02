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
	"forgequeue/internal/redisutil"
	"forgequeue/internal/version"
)

// worker registry keys. a pool publishes its identity on Start and keeps it
// fresh with a heartbeat so the dashboard can show live workers.
const (
	// ids of every worker that has registered.
	workerRegistrySet = "forgequeue:workers"
	// builds the per-worker keys: <prefix><id>:meta and <prefix><id>:leases.
	workerMetaPrefix = "worker:"
	// how long a registration outlives its last heartbeat. a crashed worker
	// disappears once the TTL lapses.
	workerMetaTTL = 30 * time.Second
	// how often a live worker refreshes its keys.
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
	// shared Redis client for all queue operations.
	redisClient *redis.Client
	// number of worker goroutines processing tasks in parallel.
	concurrencyLimit int
	// WaitGroup keeps track of how many workers are still running.
	// Stop() uses it to wait until every worker has finished.
	wg sync.WaitGroup

	// closing this channel tells every worker the pool is shutting down.
	quit chan struct{}

	// shared context for Redis operations. Stop() cancels it so a worker
	// stuck inside Redis doesn't wait out the full BRPop timeout.
	ctx context.Context

	// cancels the shared context during shutdown.
	cancel context.CancelFunc

	//handler is jsut an arbitary value
	// used to map task types to their corresponding handlers.
	handlers map[string]TaskHandler
	// time to wait for a lease before giving up on a task.
	leaseDuration time.Duration

	// delay before the first retry. doubles every attempt (1s, 2s, 4s...)
	// up to backoffCap.
	backoffBase time.Duration

	// ceiling on the backoff delay so a flaky task can't retry minutes out.
	backoffCap time.Duration

	// tasks one worker claims per round trip. 1 is the old single-task
	// behaviour; bigger batches cut round trips but tasks wait on each other.
	batchSize int

	// durable journal (Postgres). mirrors task lifecycles so Redis state can
	// be rebuilt after a crash. Nop by default.
	store durable.Store

	// observability. never nil, the default pool gets no-op instruments so
	// call sites skip nil checks.
	metrics *metrics.Metrics

	// this pool's identity. published to Redis on Start so the dashboard can
	// attribute leases and heartbeats to a worker.
	id        string
	hostname  string
	startedAt time.Time
	version   string

	// govern the background loops (depth sampling, the reconciler) that stop
	// only on context cancellation. separate from ctx so Stop can drain
	// in-flight handlers before tearing these down.
	bgCtx    context.Context
	bgCancel context.CancelFunc

	// how long Stop waits for in-flight handlers before cancelling them.
	drainTimeout time.Duration

	// how often the recovery loop looks for expired leases. tests shrink it
	// to exercise expiry without waiting.
	recoveryInterval time.Duration
}

// taskRuntime holds the lifecycle of one claimed task: the context its
// handler runs under, plus the cancels for the handler and its heartbeat.
//
// a batch runs sequentially, so every task starts its heartbeat the moment
// the batch is claimed. a task waiting behind the running one would otherwise
// have no heartbeat, its lease would expire, and recovery would hand it to
// another worker while this one still had it.
type taskRuntime struct {
	ctx      context.Context
	cancel   context.CancelFunc
	stopBeat context.CancelFunc
}

// TaskLease is a worker's proof of ownership over a claimed task.
//
// LeaseID is a token made at claim time, stored with the taskID in the leases
// hash so heartbeat and ACK can check this worker still owns it.
type TaskLease struct {
	TaskID  string
	LeaseID string
}

// TaskClaim is what claimBatch() returns: the lease plus the task's JSON
// body, read in the same atomic script call so there is no second round trip.
type TaskClaim struct {
	TaskLease
	Body []byte
}

func NewWorkerPool(addr string, concurrencyLimit int) (*WorkerPool, error) {
	// One context for the whole pool, passed to every Redis operation so
	// Stop() can interrupt them. It also bounds the connection check below.
	ctx, cancel := context.WithCancel(context.Background())

	// enough connections for the workers and their concurrent Redis ops.
	// redisutil applies auth/TLS from the environment in one place.
	client, err := redisutil.NewClient(addr, concurrencyLimit*2, ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	// separate context for the background loops, so a graceful drain can stop
	// them without cancelling handlers still finishing under ctx.
	bgCtx, bgCancel := context.WithCancel(context.Background())

	hostname, _ := os.Hostname()

	return &WorkerPool{
		redisClient:      client,
		concurrencyLimit: concurrencyLimit,
		store:            durable.Nop{},
		id:               uuid.New().String(),
		hostname:         hostname,
		startedAt:        time.Now(),
		version:          version.Version,

		wg: sync.WaitGroup{},

		quit:             make(chan struct{}),
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

// WithMetrics attaches a telemetry sink. call before Start(). passing nil is
// a no-op.
func (wp *WorkerPool) WithMetrics(m *metrics.Metrics) *WorkerPool {
	if m == nil {
		return wp
	}
	wp.metrics = m
	return wp
}

// workerLoop runs one worker.
//
// workerID just labels which worker this is (0, 1, 2...).
//
// one iteration handles a whole batch: claim up to batchSize tasks, run
// their handlers one at a time, then ACK the successes together.
func (wp *WorkerPool) workerLoop(workerID int) {
	for {
		select {
		case <-wp.quit:
			return

		default:
			claims, err := wp.claimBatch()

			// empty queue: sleep a little so idle workers don't spin.
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

			// record every claimed task so the dashboard can show what this
			// worker is processing.
			claimedIDs := make([]string, 0, len(claims))
			for _, claim := range claims {
				if claim.TaskID != "" {
					claimedIDs = append(claimedIDs, claim.TaskID)
				}
			}
			wp.markLeases(claimedIDs)

			// start a heartbeat for every claimed task up front. the batch
			// runs one at a time, so a task waiting behind the running one
			// would otherwise have no heartbeat and its lease would expire,
			// letting recovery hand it to a second worker.
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
			// idempotent, so it is safe on every exit path.
			stopTask := func(id string) {
				if rt, ok := runtimes[id]; ok {
					rt.stopBeat()
					rt.cancel()
				}
			}

			for _, claim := range claims {

				// a claimed task should always have an ID.
				if claim.TaskID == "" {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// the claim script already read the body. empty means the
				// task data was missing.
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

				// decode the JSON from Redis into task.
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

				// look up the handler for this task type. nothing registered
				// means it can't run, so skip and let the lease expire.
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

				// this worker owns the task now, so tell the journal it is
				// running. best effort: if Postgres is down the task still runs.
				if err := wp.store.Processing(wp.ctx, claim.TaskID); err != nil {
					log.Printf(
						"journal: failed to mark task %s as processing: %v",
						claim.TaskID,
						err,
					)
				}

				handlerStart := time.Now()

				// the handler runs under the task's own context, whose
				// heartbeat is already renewing the lease. the heartbeat keeps
				// running after the handler returns, because the ACK waits for
				// the batched call at the end of the loop. stopping it here
				// would expire an early-finished task while it waits its turn.
				rt := runtimes[claim.TaskID]
				handlerErr := handler.Handle(rt.ctx, &task)

				// how long the handler ran. recorded for successes too, since
				// a slow success and a slow failure look very different at p99.
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

				// a handler error is a failed attempt. release the lease now
				// (fenced, like an ACK) instead of letting it expire, so the
				// retry lands on the backoff delay.
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

				// requeueOrDead released the lease, nothing left to keep alive.
				stopTask(task.ID)
			}

			// ACK the successes in one fenced script. only after it lands do
			// we stop the other heartbeats, otherwise an early-finished task
			// would expire while it waits for the rest of the batch.
			//
			// the script fences per task, so a batch can be partially ACKed:
			// tasks that lost their lease are skipped and the returned ids are
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
						// worth a warning: this is the at-least-once duplicate
						// path and would otherwise look like healthy throughput.
						log.Printf(
							"worker %d: only %d of %d task(s) ACKed, %d lost their lease "+
								"and may run again elsewhere",
							workerID, len(ackedIDs), len(ackIDs), len(ackIDs)-len(ackedIDs),
						)
					}

					// counted from the script's return value, not len(ackIDs),
					// so a fenced task isn't reported as processed.
					wp.metrics.TasksProcessed.Add(wp.ctx, int64(len(ackedIDs)))

					// record the ACKed tasks so the journal stops tracking them.
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

			// every task has an outcome now (ACKed, retried or dead), so stop
			// any heartbeat left and drop them from this worker's lease set.
			for id := range runtimes {
				stopTask(id)
			}
			wp.clearLeases(claimedIDs)
		}
	}
}

// WithStore attaches the durable journal. call before Start().
func (wp *WorkerPool) WithStore(s durable.Store) *WorkerPool {
	wp.store = s
	return wp
}

// WithBatchSize sets how many tasks a worker claims per round trip. 1 is the
// default single-task behaviour.
func (wp *WorkerPool) WithBatchSize(n int) *WorkerPool {
	if n < 1 {
		n = 1
	}
	wp.batchSize = n
	return wp
}

// WithDrainTimeout bounds how long Stop waits for in-flight handlers before
// cancelling them. call before Start().
func (wp *WorkerPool) WithDrainTimeout(d time.Duration) *WorkerPool {
	if d > 0 {
		wp.drainTimeout = d
	}
	return wp
}

// Start creates and starts the workers, concurrencyLimit of them.
func (wp *WorkerPool) Start() {
	// publish this pool's identity and keep it fresh with a heartbeat so the
	// dashboard can see the worker.
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		wp.registrationLoop()
	}()

	for i := 0; i < wp.concurrencyLimit; i++ {

		wp.wg.Add(1)

		// i is captured so each worker gets its own ID.
		go func(i int) {

			// Done() runs even if workerLoop returns via the quit signal.
			defer wp.wg.Done()

			wp.workerLoop(i)

		}(i)
	}
	// recovery runs in its own goroutine so expired tasks keep getting
	// requeued even while every worker is busy.
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		wp.recoveryLoop()
	}()

	// promotion is separate too: it moves due tasks from the scheduled set
	// into the ready queue.
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		wp.promotionLoop()
	}()

	// the reconciler heals the journal's write gap: it re-materializes into
	// Redis any task the journal knows about but Redis doesn't. only runs
	// when a durable store is attached.
	if _, isNop := wp.store.(durable.Nop); !isNop {
		wp.wg.Add(1)
		go func() {
			defer wp.wg.Done()
			durable.ReconcileLoop(wp.bgCtx, wp.redisClient, wp.store)
		}()
	}

	// sample queue depth from Redis on a timer. doing it in-process would
	// need an inc/dec for every transition, and one miss leaves the gauge
	// wrong for good.
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

// Stop gracefully shuts down the worker pool in three phases so in-flight
// work drains instead of being aborted.
//
//  1. close quit and stop the background loops. workers stop claiming new
//     tasks but finish a batch already running.
//  2. wait for every goroutine. handlers run under ctx, still alive, so they
//     finish normally.
//  3. cancel ctx and deregister.
//
// phase 2 is bounded by drainTimeout so a stuck handler can't hold shutdown
// open forever.
func (wp *WorkerPool) Stop() {
	// phase 1: stop new claims and the loops that would otherwise run for the
	// life of the pool.
	close(wp.quit)
	wp.bgCancel()

	// phase 2: let workers finish the batch they are in while their contexts
	// are still valid.
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

	// phase 3: nothing should be running now, so cancel the context and
	// deregister so the dashboard sees the shutdown.
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

type TaskHandler interface {
	//create a taskhandler type which needs to handle and have these
	Handle(ctx context.Context, task *model.TaskMetaData) error
}

// claimBatch pops up to batchSize tasks off the immediate queue, claims them
// all, and reads their bodies in one atomic Lua script, so a batch costs
// about two round trips instead of one per task.
//
// lease IDs are made here in Go and passed in ARGV; the script hands them to
// whatever it pops and the caller pairs the returned bodies back by index.
func (wp *WorkerPool) claimBatch() ([]TaskClaim, error) {
	claimStart := time.Now()

	// lease deadline. no ACK or heartbeat before this and recovery requeues
	// the task. keep fractional seconds: truncating would shave up to a
	// second off short leases and expire them early.
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

	// empty string means the queue was empty.
	if result == "" {
		// not timed on purpose. an idle worker polls this every millisecond,
		// so averaging those in would drag every percentile toward zero and
		// hide the claim latency that matters.
		return nil, nil
	}

	// a claim that returned work is worth timing: it is the queue's floor on
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
// the inflight set maps taskID -> lease expiry, so everything scored up to
// now is expired and ready for requeue.
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
// two callers share this script: the worker after a handler error passes the
// leaseID so it fences like an ACK (a task it no longer owns is rejected);
// the recovery loop passes "" and lets the script re-check the inflight score
// against Redis's clock.
//
// either way the script loads the body, bumps "attempts", rewrites the body,
// then schedules a retry with a backoff score (now + base * 2^(attempts-1),
// capped) or pushes it to the dead-letter queue once the budget is gone.
// max_retries = 0 means retry forever.
//
// returns: "fenced", "gone", "renewed", "no-body", "dead", "scheduled".
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

// backoffDelay mirrors the delay the requeueOrDead script computes:
// base * 2^(attempts-1), capped. keep the two in sync or the journal records
// a different schedule than Redis applied.
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

// reportRetry tells the journal a task went into backoff. Redis already
// scheduled it, so the journal only records the counter and the new due time.
// reading the body back is one extra round trip, only on the failure path.
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

// reportDead tells the journal a task ran out of retries. the body with its
// final attempt count is already in the dead-letter queue.
func (wp *WorkerPool) reportDead(taskID string) {
	t, ok := wp.readTaskForJournal(taskID)
	if !ok {
		return
	}
	if err := wp.store.Dead(wp.ctx, taskID, t.Attempts); err != nil {
		log.Printf("journal: failed to record dead task %s: %v", taskID, err)
	}
}

// readTaskForJournal fetches a task body so its post-script attempt count can
// be reported. missing means nothing to report.
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

// recoveryLoop watches for expired leases. on every tick it asks Redis which
// in-flight tasks expired and hands each to requeueOrDead.
func (wp *WorkerPool) recoveryLoop() {
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
				// "renewed", "gone" and "scheduled" are quiet: the lease was
				// extended, someone else handled it, or a retry was queued.
			}

		case <-wp.quit:
			return
		}
	}
}

// findDueTasks returns scheduled task IDs whose delay has passed.
//
// "due" here is only a candidate: promoteTask re-checks the score against
// Redis's own clock.
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
// the body already lives at task:<id>, so this is just a list move. the score
// is re-checked against TIME in the script so a task is never promoted early
// or twice.
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

// promotionLoop moves due tasks from the scheduled set to the ready queue.
// delayed enqueues and backoff retries both land here.
func (wp *WorkerPool) promotionLoop() {
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
// each task is fenced on its own: it is only removed if the presented leaseID
// still matches the leases hash, so a stale ACK is rejected. one round trip
// for the whole list.
//
// an ACKed task has no use for its body, so task:<id> is deleted in the same
// atomic call, keeping the task store from growing without bound. the returned
// slice is exactly the IDs that were ACKed.
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

// errLeaseLost means Redis answered but the lease token no longer matches. a
// sentinel so the caller can tell "someone else owns this task now" apart
// from "Redis was unreachable", which shouldn't count as a lost lease.
var errLeaseLost = errors.New("heartbeat rejected: lease no longer held")

// heartbeat renews the lease for a task still being processed.
//
// it checks ownership first (HGET) and, if we still own the task, pushes the
// expiry forward using Redis's clock, not the worker's.
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

	// recorded on both paths: a slow heartbeat is what causes lease expiry,
	// so the failures matter too.
	wp.metrics.HeartbeatLatency.Record(
		wp.ctx,
		time.Since(beatStart).Seconds(),
	)

	if err != nil {
		return err
	}

	// 0 means we no longer hold the lease, so the heartbeat is rejected.
	if result.(int64) == 0 {
		return errLeaseLost
	}

	return nil
}

// heartbeatLoop keeps a task's lease alive while it is handled.
//
// it calls heartbeat() every leaseDuration/3 so a long task never expires.
// it stops when the task context is cancelled (the handler finished) or a
// heartbeat is rejected. on a rejected lease it also cancels the task
// context so the handler stops early.
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
				// a heartbeat that raced with the batch's own ACK (lease gone
				// the instant we stopped) is not a real loss. only report when
				// the loop is still meant to be running.
				if ctx.Err() != nil {
					return
				}

				log.Printf(
					"heartbeat failed for task %s: %v",
					taskID,
					err,
				)

				// only a rejected lease is a lease loss. a transport error or
				// shutdown cancellation is different and shouldn't inflate the
				// metric during a Redis blip.
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
