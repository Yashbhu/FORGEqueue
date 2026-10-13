package gateway

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"forgequeue/internal/durable"
	"forgequeue/internal/metrics"
	"forgequeue/internal/model"
	"forgequeue/internal/redisutil"
)

// task router should be public
// Every TaskRouter object will have a field named redisClient
type TaskRouter struct {
	redisClient *redis.Client
	// durable journal. Records every accepted task before Redis is touched,
	// so a crash mid-enqueue is healed by the reconciler. Nop by default.
	store durable.Store
	// Observability. Never nil, defaults to no-op instruments.
	metrics *metrics.Metrics
}

// constructor function which returns a pointer to struct
// we assign it like router := NewTaskRouter("")
func NewTaskRouter(ctx context.Context, addr string) (*TaskRouter, error) {

	//creating the redis client and pinging the redis server to check if it's available
	client, err := redisutil.NewClient(addr, 50, ctx)
	if err != nil {
		return nil, err
	}
	return &TaskRouter{
		redisClient: client,
		store:       durable.Nop{},
		metrics:     metrics.NewNop(),
	}, nil
}

// WithStore attaches the durable journal. Call it before serving requests.
func (tr *TaskRouter) WithStore(s durable.Store) *TaskRouter {
	tr.store = s
	return tr
}

// WithMetrics attaches a telemetry sink. Call it before serving requests.
func (tr *TaskRouter) WithMetrics(m *metrics.Metrics) *TaskRouter {
	if m == nil {
		return tr
	}
	tr.metrics = m
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
	if err != nil {
		return err
	}
	// checking if delay seconds is greater than 0 if so we schedule the task to be executed at a later time
	if delaySeconds > 0 {
		targetTime := time.Now().Unix() + delaySeconds

		// Journal first. If we die before the Redis writes below, the
		// reconciler sees redis_pending and materializes the task.
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

		// Write the body first so it is in place when the promotion loop
		// moves the ID out of the scheduled set. The set only stores the ID,
		// the body always lives at task:<id>.
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

		// Redis holds the task now, so clear pending.
		if err := tr.store.Materialized(ctx, id); err != nil {
			return err
		}

		// Counted after Redis accepts it. Depth is not adjusted here, the
		// gauges sample Redis which already counted the push.
		tr.metrics.TasksEnqueued.Add(ctx, 1)
		now := time.Now().Unix()
		tr.redisClient.ZAdd(ctx, "metrics:enqueued", redis.Z{Score: float64(now), Member: id})
		tr.redisClient.Expire(ctx, "metrics:enqueued", 24*time.Hour)
		return nil
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

	if err := tr.store.Materialized(ctx, id); err != nil {
		return err
	}

	tr.metrics.TasksEnqueued.Add(ctx, 1)
	now := time.Now().Unix()
	tr.redisClient.ZAdd(ctx, "metrics:enqueued", redis.Z{Score: float64(now), Member: id})
	tr.redisClient.Expire(ctx, "metrics:enqueued", 24*time.Hour)
	return nil
}
