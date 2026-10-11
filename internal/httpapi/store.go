package httpapi

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	rdb *redis.Client
}

func NewRedisStore(rdb *redis.Client) *RedisStore {
	return &RedisStore{rdb: rdb}
}

func (s *RedisStore) readTask(id string) (TaskMetaData, bool, error) {
	var t TaskMetaData
	b, err := s.rdb.Get(context.Background(), "task:"+id).Bytes()
	if err == redis.Nil {
		return t, false, nil
	}
	if err != nil {
		return t, false, err
	}
	if err := json.Unmarshal(b, &t); err != nil {
		return t, false, err
	}
	return t, true, nil
}

func (s *RedisStore) listIDs(key string) ([]string, error) {
	ids, err := s.rdb.LRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *RedisStore) zrangeIDs(key string) ([]string, error) {
	ids, err := s.rdb.ZRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *RedisStore) zrangeWithScores(key string) ([]redis.Z, error) {
	res, err := s.rdb.ZRangeWithScores(context.Background(), key, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	return res, nil
}
