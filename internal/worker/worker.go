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
			// Wait for a task in Redis.
			//
			// BRPop blocks until a task is available or
			// the timeout expires.
			//
			// We use the worker pool's shared context here
			// so Stop() can cancel this Redis operation.
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

			// Fetch the actual task body we stored with SET.
			serializedData, err := wp.redisClient.Get(
				wp.ctx,
				"task:"+lease.TaskID,
			).Result()

			if err != nil {
				log.Printf(
					"worker %d: failed to get task %s: %v",
					workerID,
					lease.TaskID,
					err,
				)
				continue
			}

			var task model.TaskMetaData

			// Convert the JSON stored in Redis into TaskMetaData.
			//
			// &task gives Unmarshal the address of task so that
			// it can fill the struct.
			if err := json.Unmarshal([]byte(serializedData), &task); err != nil {
				log.Printf(
					"worker %d: failed to parse task %s: %v",
					workerID,
					lease.TaskID,
					err,
				)
				continue
			}

			// For now, we only confirm that the worker successfully
			// dequeued and parsed the task.
			//
			// Actual task execution will be added later.
			log.Printf(
				"worker %d: dequeued task %s (%s)",
				workerID,
				task.ID,
				task.TaskType,
			)
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
			log.Printf(
				"worker %d: successfully executed task %s",
				workerID,
				task.ID,
			)
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

// claimTask pops a task off the immediate queue and claims ownership of it.
//
// This is one atomic Lua script, so there's no gap between popping the task
// and registering it as in-flight/owned.
func (wp *WorkerPool) claimTask() (*TaskLease, error) {
	// The expiry is the deadline for the lease. If the worker doesn't
	// ACK or heartbeat before this, recovery will requeue the task.
	leaseExpiry := time.Now().Add(wp.leaseDuration).Unix()
	leaseID := uuid.New().String()

	// KEYS[1] → queue:tasks:immediate  (source of ready tasks)
	// KEYS[2] → queue:tasks:inflight   (sorted set: taskID → expiry)
	// KEYS[3] → queue:tasks:leases     (hash: taskID → leaseID)
	script := redis.NewScript(`
        local taskID = redis.call("LPOP", KEYS[1])

        if not taskID then
            return ""
        end

        redis.call("ZADD", KEYS[2], ARGV[1], taskID)

        redis.call("HSET", KEYS[3], taskID, ARGV[2])

        return taskID
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
	).Result()

	if err != nil {
		return nil, err
	}

	// The script returns an empty string when the queue is empty.
	if result == "" {
		return nil, nil
	}

	return &TaskLease{
		TaskID:  result.(string),
		LeaseID: leaseID,
	}, nil
}

// findExpiredTasks returns the task IDs whose lease has expired.
//
// The inflight sorted set stores taskID → lease expiry. Asking for
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

// removeExpiredTasks returns expired tasks to the immediate queue.
//
// findExpiredTasks() only produces candidates: the scan can go stale, since
// a heartbeat may renew the lease between the scan and this call. So this
// function re-checks the expiry inside Redis, atomically, right before
// requeueing. If the task is no longer expired, it's left alone.
//
// KEYS[1] → queue:tasks:inflight
// KEYS[2] → queue:tasks:immediate
// KEYS[3] → queue:tasks:leases
// ARGV[1] → the task ID to requeue
func (wp *WorkerPool) removeExpiredTasks(taskIDs []string) error {
	script := redis.NewScript(`
        local expiry = redis.call("ZSCORE", KEYS[1], ARGV[1])

        if not expiry then
            return 0
        end

        local now = redis.call("TIME")[1]

        if tonumber(expiry) > tonumber(now) then
            return 0
        end

        redis.call("ZREM", KEYS[1], ARGV[1])
        redis.call("HDEL", KEYS[3], ARGV[1])
        redis.call("LPUSH", KEYS[2], ARGV[1])

        return 1
    `)
	// Requeue each expired task, one Lua call at a time.
	for _, taskID := range taskIDs {
		_, err := script.Run(
			wp.ctx,
			wp.redisClient,
			[]string{
				"queue:tasks:inflight",
				"queue:tasks:immediate",
				"queue:tasks:leases",
			},
			taskID,
		).Result()

		if err != nil {
			return err
		}
	}
	return nil
}

// recoveryLoop watches for expired leases and requeues them.
//
// Every second it asks Redis which in-flight tasks have expired and moves
// those back to the immediate queue for another attempt. It runs in its
// own goroutine and stops when the pool shuts down.
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

			if len(taskIDs) == 0 {
				continue
			}

			if err := wp.removeExpiredTasks(taskIDs); err != nil {
				log.Printf("recovery: failed to requeue expired tasks: %v", err)
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
// KEYS[1] → queue:tasks:inflight (sorted set: taskID → expiry)
// KEYS[2] → queue:tasks:leases   (hash: taskID → leaseID)
// ARGV[1] → taskID
// ARGV[2] → leaseID presented by the worker
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
// KEYS[1] → queue:tasks:leases   (hash: taskID → leaseID)
// KEYS[2] → queue:tasks:inflight (sorted set: taskID → expiry)
// ARGV[1] → taskID
// ARGV[2] → leaseID presented by the worker
// ARGV[3] → lease duration in seconds
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
