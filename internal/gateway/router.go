package gateway

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"forgequeue/internal/durable"
	"forgequeue/internal/model"
	"forgequeue/internal/redisutil"
)

// task router should be public
// Every TaskRouter object will have a field named redisClient
type TaskRouter struct {
	redisClient *redis.Client // a pointer to the external redis client assigning to the redisclient field
	// durable journal (Postgres). It records every accepted task BEFORE
	// Redis is touched, so a crash mid-enqueue never loses the task - the
	// reconciler finishes the write. Nop by default (no database configured).
	store durable.Store
}

// constructor function which returns a pointer to struct
// we assign it like router := NewTaskRouter("")
func NewTaskRouter(ctx context.Context, addr string) (*TaskRouter, error) {

	// fail fast
	//creating the redis client and pinging the redis server to check if it's available
	client, err := redisutil.NewClient(addr, 50, ctx)
	if err != nil {
		return nil, err
	}
	return &TaskRouter{ // returns a pointer to a new TaskRouter struct with the redis client assigned to the redisClient field
		redisClient: client,
		store:       durable.Nop{},
	}, nil
}

// WithStore attaches the durable journal to the router. Call it before
// serving requests. Tasks are journaled first and materialized into Redis
// afterwards; a crash in between is repaired by the reconciler.
func (tr *TaskRouter) WithStore(s durable.Store) *TaskRouter {
	tr.store = s
	return tr
}

// method belonging to taskrouter we call it like tr.routeTask it doesnt exist itself
func (tr *TaskRouter) RouteTask(
	ctx context.Context,
	id string,
	taskType string,
	payload []byte,
	maxRetries int32,
	delaySeconds int64,
) error {
	// creating a struct in memory
	data := model.TaskMetaData{
		ID:         id,
		TaskType:   taskType,
		Payload:    payload,
		MaxRetries: maxRetries,
	}
	// serialisation or if error return it
	serializedData, err := json.Marshal(data)
	// checking error if the function fails
	if err != nil {
		return err
	}
	// checking if delay seconds is greater than 0 if so we schedule the task to be executed at a later time
	if delaySeconds > 0 {
		targetTime := time.Now().Unix() + delaySeconds

		// Journal the task first. If we die before the Redis writes below,
		// the reconciler sees redis_pending and materializes the task for
		// us. This is the whole "journal first, then hot store" order.
		if err := tr.store.Enqueue(ctx, durable.Task{
			ID:         id,
			TaskType:   taskType,
			Payload:    serializedData,
			MaxRetries: maxRetries,
			State:      durable.StateEnqueued,
			ScheduleAt: time.Unix(targetTime, 0),
		}); err != nil {
			return err
		}

		// Write the body first so it is already in place when the worker's
		// promotion loop moves the ID out of the scheduled set. The scheduled
		// set only stores the task ID, matching the immediate path: the body
		// always lives at task:<id>, no matter which path enqueued it.
		err := tr.redisClient.Set(
			ctx,
			"task:"+id,
			serializedData,
			0,
		).Err()
		if err != nil {
			return err
		}

		err = tr.redisClient.ZAdd(
			ctx,
			"queue:tasks:scheduled",
			redis.Z{
				Score:  float64(targetTime),
				Member: id,
			},
		).Err()
		if err != nil {
			return err
		}

		// Redis now provably holds the task; the journal can stop treating
		// it as pending.
		return tr.store.Materialized(ctx, id)
	}

	// immediate enqueue.
	if err := tr.store.Enqueue(ctx, durable.Task{
		ID:         id,
		TaskType:   taskType,
		Payload:    serializedData,
		MaxRetries: maxRetries,
		State:      durable.StateEnqueued,
	}); err != nil {
		return err
	}

	err = tr.redisClient.Set(
		ctx,
		"task:"+id,
		serializedData,
		0,
	).Err()
	if err != nil {
		return err
	}

	err = tr.redisClient.LPush(
		ctx,
		"queue:tasks:immediate",
		id,
	).Err()

	if err != nil {
		return err
	}

	return tr.store.Materialized(ctx, id)
}
