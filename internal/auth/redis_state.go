package auth

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore is a StateStore backed by Redis so OIDC flow state survives across
// API instances and restarts.
type RedisStore struct {
	rdb *redis.Client
}

func NewRedisStore(rdb *redis.Client) *RedisStore {
	return &RedisStore{rdb: rdb}
}

func (s *RedisStore) Save(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return s.rdb.Set(ctx, key, value, ttl).Err()
}

func (s *RedisStore) Load(ctx context.Context, key string) ([]byte, bool, error) {
	b, err := s.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (s *RedisStore) Delete(ctx context.Context, key string) error {
	return s.rdb.Del(ctx, key).Err()
}
