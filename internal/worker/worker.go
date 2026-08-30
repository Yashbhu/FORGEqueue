package worker

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

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

	// used to map task types to their corresponding handlers.
	handlers map[string]TaskHandler
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
	handlers := make(map[string]TaskHandler)

	return &WorkerPool{
		redisClient:      client,
		concurrencyLimit: concurrencyLimit,

		// sync.WaitGroup starts with a counter of 0.
		// Start() will add one for every worker it creates.
		wg: sync.WaitGroup{},

		// Channel used as a shutdown signal.
		quit: make(chan struct{}),

		// Shared context and its cancellation function.
		ctx:    ctx,
		cancel: cancel,
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
			results, err := wp.redisClient.BRPop(
				wp.ctx,
				2*time.Second,
				"queue:tasks:immediate",
			).Result()

			if err != nil {
				// If the context was cancelled during shutdown,
				// go back to the top of the loop where the quit
				// channel will be checked.
				continue
			}

			// BRPop returns:
			//
			// results[0] → list name
			// results[1] → value stored in the list
			//
			// The value is the JSON representation of our task.
			var task model.TaskMetaData

			// Convert the JSON stored in Redis into TaskMetaData.
			//
			// &task gives Unmarshal the address of task so that
			// it can fill the struct.
			if err := json.Unmarshal([]byte(results[1]), &task); err != nil {
				log.Printf(
					"worker %d: failed to parse task: %v",
					workerID,
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

// TaskHandler defines the interface for task handlers.
type TaskHandler interface {
	Handle(ctx context.Context, task *model.TaskMetaData) error
}
