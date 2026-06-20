package gateway

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type TaskMetaData struct {
	ID         string `json:"id"`          // 16 byte
	TaskType   string `json:"task_type"`   // 16 byte
	Payload    []byte `json:"payload"`     // 24 bytes
	MaxRetries int32  `json:"max_retries"` // 4 bytes
}

// task router should be public
type TaskRouter struct {
	redisClient *redis.Client // a pointer to the external redis client assigning to the redisclient field
}

// constructor function which returns a pointer to struct
// we assign it like router := NewTaskRouter("")
func NewTaskRouter(addr string, ctx context.Context) (*TaskRouter, error) {
	// creating a redis client instance
	client := redis.NewClient(&redis.Options{
		// redis client returns a pointer
		Addr:     addr,
		PoolSize: 50,
	})
	// fail fast
	//pinging the redis server to check if it's available
	err := client.Ping(ctx).Err()
	if err != nil {
		return nil, err
	}
	return &TaskRouter{ // returns a pointer to a new TaskRouter struct with the redis client assigned to the redisClient field
		redisClient: client, // created actual task router client value from the instance taking its address as well
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
	data := TaskMetaData{
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
