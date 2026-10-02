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

// TaskClaim is what a worker receives from claimTask(): the ownership lease
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
	}, nil
}

// workerLoop is the work performed by one worker.
//
// workerID identifies which worker is running this loop.
// For example, Start() can create worker 0, worker 1, worker 2, etc.
func (wp *WorkerPool) workerLoop(workerID int) {
	for {
		select {
		// If the quit channel is closed, the worker stops.
		case <-wp.quit:
			return

		default:
			lease, err := wp.claimTask()

			// claimTask returns nil when the queue is empty.
			// Sleep briefly so workers don't spin hot on an idle queue.
			if lease == nil {
				time.Sleep(1 * time.Millisecond)
				continue
			}
			if err != nil {
				log.Printf(
					"worker %d: failed to claim task: %v",
					workerID,
					err,
				)
				continue
			}

			// Defensive guard: a claimed task must always have an ID.
			if lease.TaskID == "" {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// The task body was already read by the claim script.
			// An empty body means the task data was missing.
			if len(lease.Body) == 0 {
				log.Printf(
					"worker %d: failed to get task %s: no task body",
					workerID,
					lease.TaskID,
				)
				continue
			}

			var task model.TaskMetaData

			// Convert the JSON stored in Redis into TaskMetaData.
			//
			// &task gives Unmarshal the address of task so that
			// it can fill the struct.
			if err := json.Unmarshal(lease.Body, &task); err != nil {
				log.Printf(
					"worker %d: failed to parse task %s: %v",
					workerID,
					lease.TaskID,
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

			// This task's own cancellation signal. If the lease is lost
			// mid-execution, this context can be cancelled so the handler
			// has a chance to stop early.
			taskCtx, cancelTask := context.WithCancel(wp.ctx)
			defer cancelTask()

			// A per-task context so cancelling it only stops this
			// task's heartbeat, not the whole worker pool.
			heartbeatCtx, stopHeartbeat := context.WithCancel(wp.ctx)

			// Run the heartbeat in the background while the handler
			// is executing, so the lease is renewed for long tasks.
			go wp.heartbeatLoop(
				heartbeatCtx,
				lease.TaskID,
				lease.LeaseID,
				cancelTask,
			)

			err = handler.Handle(taskCtx, &task)

			// Stop the heartbeats as soon as this execution attempt
			// is over, even if the handler failed. The attempt is done,
			// so renewing the lease would only delay recovery.
			stopHeartbeat()

			if err != nil {
				log.Printf(
					"worker %d: failed to execute task %s: %v",
					workerID,
					task.ID,
					err,
				)

				// A handler error counts as a failed attempt. Release the
				// lease right away (fenced, like an ACK) instead of waiting
				// for it to expire, so the retry happens on the backoff
				// delay instead of whenever the lease runs out.
				retry, rerr := wp.requeueOrDead(task.ID, lease.LeaseID)
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
					retry,
				)
				continue
			}
			// get acknowledgement for the task
			if err := wp.ackTask(task.ID, lease.LeaseID); err != nil {
				log.Printf(
					"worker %d: failed to ACK task %s: %v",
					workerID,
					task.ID,
					err,
				)
				continue
			}
		}
	}
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

// claimTask pops a task off the immediate queue, claims ownership of it,
// and reads its body — all in one atomic Lua script.
//
// Every task costs two Redis round trips instead of three: the claim script
// returns the lease (ID + ownership token) together with the stored JSON body,
// so the worker never sends a separate GET for it.
func (wp *WorkerPool) claimTask() (*TaskClaim, error) {
	// The expiry is the deadline for the lease. If the worker doesn't
	// ACK or heartbeat before this, recovery will requeue the task.
	leaseExpiry := time.Now().Add(wp.leaseDuration).Unix()
	leaseID := uuid.New().String()

	// KEYS[1] -> queue:tasks:immediate  (source of ready tasks)
	// KEYS[2] -> queue:tasks:inflight   (sorted set: taskID -> expiry)
	// KEYS[3] -> queue:tasks:leases     (hash: taskID -> leaseID)
	// ARGV[1] -> lease expiry (Unix seconds)
	// ARGV[2] -> leaseID (ownership token)
	// ARGV[3] -> "task:" prefix used to build the body's key
	script := redis.NewScript(`
        local taskID = redis.call("LPOP", KEYS[1])

        if not taskID then
            return ""
        end

        redis.call("ZADD", KEYS[2], ARGV[1], taskID)

        redis.call("HSET", KEYS[3], taskID, ARGV[2])

        local body = redis.call("GET", ARGV[3] .. taskID)

        return {taskID, body}
    `)

	result, err := script.Run(
		wp.ctx,
		wp.redisClient,
		[]string{
			"queue:tasks:immediate",
			"queue:tasks:inflight",
			"queue:tasks:leases",
		},
		leaseExpiry,
		leaseID,
		"task:",
	).Result()

	if err != nil {
		return nil, err
	}

	// The script returns an empty string when the queue is empty.
	if result == "" {
		return nil, nil
	}

	// Otherwise it returns {taskID, body}.
	values := result.([]interface{})
	if len(values) < 2 {
		return nil, errors.New("claim script returned malformed result")
	}

	body := ""
	if values[1] != nil {
		body = values[1].(string)
	}

	return &TaskClaim{
		TaskLease: TaskLease{
			TaskID:  values[0].(string),
			LeaseID: leaseID,
		},
		Body: []byte(body),
	}, nil
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
//  1. loads the task body, and increments the "attempts" counter.
//  2. decides between two outcomes:
//     - "scheduled": still below its retry budget. The body is rewritten
//     with the new attempt count and the task goes into the scheduled set
//     with a backoff score (now + base * 2^(attempts-1), capped).
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

        local maxRetries = task.max_retries or 0
        if maxRetries > 0 and attempts >= maxRetries then
            redis.call("LPUSH", KEYS[4], cjson.encode(task))
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

        redis.call("SET", ARGV[1] .. ARGV[2], cjson.encode(task))
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
				case "dead":
					log.Printf("recovery: task %s exhausted its retry budget, moved to dead-letter queue", taskID)
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

// ackTask tells Redis that a task finished successfully.
//
// It only removes the task if the presented leaseID still matches the owner
// stored in the leases hash. That way an ACK from a worker whose lease was
// overwritten (e.g. the task was requeued after expiry) is rejected.
//
// KEYS[1] -> queue:tasks:inflight (sorted set: taskID -> expiry)
// KEYS[2] -> queue:tasks:leases   (hash: taskID -> leaseID)
// ARGV[1] -> taskID
// ARGV[2] -> leaseID presented by the worker
func (wp *WorkerPool) ackTask(taskID string, leaseID string) error {
	script := redis.NewScript(`
        local currentLease = redis.call("HGET", KEYS[2], ARGV[1])

        if currentLease ~= ARGV[2] then
            return 0
        end

        redis.call("ZREM", KEYS[1], ARGV[1])
        redis.call("HDEL", KEYS[2], ARGV[1])

        return 1
    `)

	result, err := script.Run(
		wp.ctx,
		wp.redisClient,
		[]string{
			"queue:tasks:inflight",
			"queue:tasks:leases",
		},
		taskID,
		leaseID,
	).Result()

	if err != nil {
		return err
	}

	// return 0 means the worker no longer holds the lease,
	// so the ACK is rejected.
	if result.(int64) == 0 {
		return errors.New("ack rejected: lease no longer held")
	}

	return nil
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
