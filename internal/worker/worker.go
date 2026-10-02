package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"forgequeue/internal/durable"
	"forgequeue/internal/model"
)

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

	// Check that Redis is reachable before returning the pool.
	if err := client.Ping(context.Background()).Err(); err != nil {
		// If Redis isn't reachable, cancel the context we just created
		// because the worker pool won't be started.
		cancel()
		return nil, err
	}

	return &WorkerPool{
		redisClient:      client,
		concurrencyLimit: concurrencyLimit,
		store:            durable.Nop{},

		// sync.WaitGroup starts with a counter of 0.
		// Start() will add one for every worker it creates.
		wg: sync.WaitGroup{},

		// Channel used as a shutdown signal.
		quit: make(chan struct{}),

		// Shared context and its cancellation function.
		ctx:           ctx,
		cancel:        cancel,
		handlers:      make(map[string]TaskHandler),
		leaseDuration: 10 * time.Second,
		backoffBase:   1 * time.Second,
		backoffCap:    60 * time.Second,
		batchSize:     1,
	}, nil
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

			// Successfully handled tasks are ACKed together at the end of
			// the batch, so their finishes only cost one round trip.
			var ackIDs []string
			var ackLeases []string

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

				// This task's own cancellation signal. If the lease is lost
				// mid-execution, this context can be cancelled so the handler
				// has a chance to stop early. Canceled explicitly (not via
				// defer) because this is a loop - deferred calls would pile up.
				taskCtx, cancelTask := context.WithCancel(wp.ctx)

				// A per-task context so cancelling it only stops this
				// task's heartbeat, not the whole worker pool.
				heartbeatCtx, stopHeartbeat := context.WithCancel(wp.ctx)

				// Run the heartbeat in the background while the handler
				// is executing, so the lease is renewed for long tasks.
				go wp.heartbeatLoop(
					heartbeatCtx,
					claim.TaskID,
					claim.LeaseID,
					cancelTask,
				)

				handlerErr := handler.Handle(taskCtx, &task)

				// Stop the heartbeats as soon as this execution attempt
				// is over, even if the handler failed. The attempt is done,
				// so renewing the lease would only delay recovery.
				stopHeartbeat()
				cancelTask()

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
					wp.reportRetry(task.ID)
				case "dead":
					wp.reportDead(task.ID)
				}
			}

			if len(ackIDs) == 0 {
				continue
			}

			// ACK every successful task of the batch in one fenced script.
			if _, err := wp.ackBatch(ackIDs, ackLeases); err != nil {
				log.Printf(
					"worker %d: failed to ACK %d task(s): %v",
					workerID,
					len(ackIDs),
					err,
				)
				continue
			}

			// ACKed means done: record it so the journal stops tracking it.
			for _, id := range ackIDs {
				if err := wp.store.Succeeded(wp.ctx, id); err != nil {
					log.Printf(
						"journal: failed to mark task %s as succeeded: %v",
						id,
						err,
					)
				}
			}
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

// Start creates and starts the workers.
//
// The number of workers is controlled by concurrencyLimit.
func (wp *WorkerPool) Start() {
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
			durable.ReconcileLoop(wp.ctx, wp.redisClient, wp.store)
		}()
	}
}

// Stop gracefully shuts down the worker pool.
func (wp *WorkerPool) Stop() {
	// Tell all workers that shutdown has been requested.
	//
	// Closing a channel wakes every goroutine waiting to receive
	// from that channel.
	close(wp.quit)

	// Cancel the shared context.
	//
	// This is important if a worker is currently blocked inside
	// BRPop. It allows the Redis operation to stop instead of
	// waiting for the full 2-second timeout.
	wp.cancel()

	// Wait until every worker has returned from workerLoop()
	// and called wg.Done().
	//
	// This makes Stop() wait until shutdown is actually complete.
	wp.wg.Wait()
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
	// The expiry is the deadline for the lease. If the worker doesn't
	// ACK or heartbeat before this, recovery will requeue the task.
	leaseExpiry := time.Now().Add(wp.leaseDuration).Unix()

	batch := wp.batchSize
	if batch < 1 {
		batch = 1
	}
	leaseIDs := make([]string, batch)
	args := make([]interface{}, 0, batch+3)
	args = append(args, batch, leaseExpiry, "task:")
	for i := 0; i < batch; i++ {
		leaseIDs[i] = uuid.New().String()
		args = append(args, leaseIDs[i])
	}

	// KEYS[1] -> queue:tasks:immediate  (source of ready tasks)
	// KEYS[2] -> queue:tasks:inflight   (sorted set: taskID -> expiry)
	// KEYS[3] -> queue:tasks:leases     (hash: taskID -> leaseID)
	// ARGV[1] -> how many tasks to try to claim (the batch size)
	// ARGV[2] -> lease expiry (Unix seconds)
	// ARGV[3] -> "task:" prefix used to build the body's key
	// ARGV[4..] -> one leaseID per possible claim, in order
	script := redis.NewScript(`
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
		},
		args...,
	).Result()

	if err != nil {
		return nil, err
	}

	// The script returns an empty string when the queue was empty.
	if result == "" {
		return nil, nil
	}

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
	now := time.Now().Unix()
	return wp.redisClient.ZRangeArgs(
		wp.ctx,
		redis.ZRangeArgs{
			Key:   "queue:tasks:inflight",
			Start: "-inf",
			// Stop is an integer, so it has to be converted to a string.
			Stop:    strconv.FormatInt(now, 10),
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
            local now = redis.call("TIME")[1]
            if tonumber(expiry) > tonumber(now) then
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

        local retryAt = redis.call("TIME")[1] + delay

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
		int64(wp.backoffBase/time.Second),
		int64(wp.backoffCap/time.Second),
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
	// Re-check for expired tasks once per second.
	ticker := time.NewTicker(1 * time.Second)
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
					wp.reportRetry(taskID)
				case "dead":
					log.Printf("recovery: task %s exhausted its retry budget, moved to dead-letter queue", taskID)
					wp.reportDead(taskID)
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
	now := time.Now().Unix()
	return wp.redisClient.ZRangeArgs(
		wp.ctx,
		redis.ZRangeArgs{
			Key:     "queue:tasks:scheduled",
			Start:   "-inf",
			Stop:    strconv.FormatInt(now, 10),
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
        local now = redis.call("TIME")[1]
        if tonumber(score) > tonumber(now) then
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
// KEYS[1] -> queue:tasks:inflight (sorted set: taskID -> expiry)
// KEYS[2] -> queue:tasks:leases   (hash: taskID -> leaseID)
// ARGV[1..] -> flat pairs: taskID1, leaseID1, taskID2, leaseID2, ...
func (wp *WorkerPool) ackBatch(taskIDs []string, leaseIDs []string) (int64, error) {
	args := make([]interface{}, 0, 2*len(taskIDs))
	for i := range taskIDs {
		args = append(args, taskIDs[i], leaseIDs[i])
	}

	script := redis.NewScript(`
        local acked = 0
        local i = 1
        while i <= #ARGV do
            local taskID = ARGV[i]
            local leaseID = ARGV[i + 1]

            if redis.call("HGET", KEYS[2], taskID) == leaseID then
                redis.call("ZREM", KEYS[1], taskID)
                redis.call("HDEL", KEYS[2], taskID)
                acked = acked + 1
            end

            i = i + 2
        end
        return acked
    `)

	result, err := script.Run(
		wp.ctx,
		wp.redisClient,
		[]string{
			"queue:tasks:inflight",
			"queue:tasks:leases",
		},
		args...,
	).Result()

	if err != nil {
		return 0, err
	}

	return result.(int64), nil
}

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
// ARGV[3] -> lease duration in seconds
func (wp *WorkerPool) heartbeat(taskID string, leaseID string) error {
	script := redis.NewScript(`
        local currentLease = redis.call("HGET", KEYS[1], ARGV[1])

        if currentLease ~= ARGV[2] then
            return 0
        end

        local newExpiry = redis.call("TIME")[1] + ARGV[3]

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
		int64(wp.leaseDuration/time.Second),
	).Result()

	if err != nil {
		return err
	}

	// return 0 means the worker no longer holds the lease,
	// so the heartbeat is rejected.
	if result.(int64) == 0 {
		return errors.New("heartbeat rejected: lease no longer held")
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
				log.Printf(
					"heartbeat failed for task %s: %v",
					taskID,
					err,
				)

				cancelTask()
				return
			}

		case <-ctx.Done():
			return
		}
	}
}
