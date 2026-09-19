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
	redisClient      *redis.Client
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

type TaskLease struct {
	//for ownership by defining seperate lease TaskID
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
			//handler value under key and ok is bool to confirm if key exists or not ( handler would be email or any which comes under taskhandler)
			// while task.type is the key used to look up the handler in the handlers map which is indeed comes from redis while worker loopup
			handler, ok := wp.handlers[task.TaskType]
			if !ok {
				log.Printf(
					//place holder %d for base 10 integer and %s for string
					"worker %d: no handler registered for task type %s",
					workerID,
					task.TaskType,
				)
				continue
			}
			//execute the handler with the task
			// handler is just an arbiatry value like x and is used as atype of taskhandelr which have a handler paramter
			err = handler.Handle(wp.ctx, &task)

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

func (wp *WorkerPool) claimTask() (*TaskLease, error) {
	leaseExpiry := time.Now().Add(wp.leaseDuration).Unix()
	leaseID := uuid.New().String()

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

func (wp *WorkerPool) findExpiredTasks() ([]string, error) {
	now := time.Now().Unix()
	//key is queue:tasks:inflight to sort them so that the task which get retrivered are <= now which is the time when its gonna expire lease
	//
	return wp.redisClient.ZRangeArgs(
		wp.ctx,
		redis.ZRangeArgs{
			Key:   "queue:tasks:inflight",
			Start: "-inf",
			//int64 → string using base 10
			Stop:    strconv.FormatInt(now, 10),
			ByScore: true,
		},
	).Result()
}

// KEYS[1] → queue:tasks:inflight
// KEYS[2] → queue:tasks:immediate
// taskIDs is the slice of task IDs to remove from the inflight queue and add to the immediate queue.
// // []int{a,b,c} bracket empty in slice
// //KEYS = Redis keys that the script is going to operate on.
// ARGV = ordinary values the script needs.
func (wp *WorkerPool) removeExpiredTasks(taskIDs []string) error {
	script := redis.NewScript(`
        local taskID = ARGV[1]

        redis.call("ZREM", KEYS[1], taskID)
        redis.call("LPUSH", KEYS[2], taskID)

        return taskID
    `)
	//ignoring the index and focusing on value
	// run loop for each task ID to remove it from the inflight queue and add it to the immediate queue.
	for _, taskID := range taskIDs {
		_, err := script.Run(
			wp.ctx,
			wp.redisClient,
			[]string{
				"queue:tasks:inflight",
				"queue:tasks:immediate",
			},
			taskID,
		).Result()

		if err != nil {
			return err
		}
	}
	//func successfully no error
	return nil
}

func (wp *WorkerPool) recoveryLoop() {
	//Create a timer that produces an event every 1 second.
	//
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	// Keep checking for events forever.
	// Each iteration waits until one of the select cases is ready.
	for {
		//until any of these cases are ready
		select {

		// <- without any vairbale meaning we dont care about value but just signal
		// //each second ticker.C will produce a signal
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
			//shutdown
		case <-wp.quit:
			return
		}
	}
}

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
