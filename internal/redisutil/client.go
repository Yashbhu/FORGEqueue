package redisutil

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// NewClient creates a redis client and checks the connection
func NewClient(addr string, poolSize int, ctx context.Context) (*redis.Client, error) {
	// creating a redis client instance
	client := redis.NewClient(&redis.Options{
		// redis.NewClient already returns a pointer
		Addr:     addr,
		PoolSize: poolSize,
	})
	// fail fast: ping the redis server to check if it's available
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return client, nil
}
