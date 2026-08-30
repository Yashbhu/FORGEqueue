package gateway

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"forgequeue/internal/model"
	"forgequeue/internal/redisutil"
)

// task router should be public
// Every TaskRouter object will have a field named redisClient
type TaskRouter struct {
	redisClient *redis.Client // a pointer to the external redis client assigning to the redisclient field
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
	}, nil
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
		err := tr.redisClient.ZAdd(
			ctx,
			"queue:tasks:scheduled",
			redis.Z{
				Score:  float64(targetTime),
				Member: serializedData,
			},
		).Err()
		if err != nil {
			return err
		}
	} else {
		// if delay seconds is 0 or less we execute the task immediately
		err := tr.redisClient.LPush(
			ctx,
			"queue:tasks:immediate",
			serializedData,
		).Err()
		if err != nil {
			return err
		}
	}
	return nil
}
