package rdbClient

import (
	"context"
	"github.com/go-redis/redis/v9"
	logger "github.com/qtyty/delay-mq/v1/internal/log"
	"time"
)

type RedisClient struct {
	ctx context.Context
	rdb *redis.Client
}

func NewRedisClient() *RedisClient {
	return &RedisClient{
		ctx: context.Background(),
		rdb: redis.NewClient(&redis.Options{
			Addr:     "116.62.153.187:6379",
			Password: "",
			DB:       0,
		}),
	}
}

func (rdb *RedisClient) Set(key, value string, expire time.Duration) {
	err := rdb.rdb.Set(rdb.ctx, key, value, expire).Err()
	if err != nil {
		logger.Logger.Error(err.Error())
	}
}

func (rdb *RedisClient) Get(key string) (string, error) {
	return rdb.rdb.Get(rdb.ctx, key).Result()
}

//list

func (rdb *RedisClient) LLen(key string) (int64, error) {
	return rdb.rdb.LLen(rdb.ctx, key).Result()
}

func (rdb *RedisClient) LPush(key string, value string) error {
	return rdb.rdb.LPush(rdb.ctx, key, value).Err()
}

func (rdb *RedisClient) RPop(key string) (string, error) {
	return rdb.rdb.RPop(rdb.ctx, key).Result()
}

//ZSet

func (rdb *RedisClient) ZCard(key string) (int64, error) {
	return rdb.rdb.ZCard(rdb.ctx, key).Result()
}

func (rdb *RedisClient) ZAdd(key string, value string, score int) error {
	return rdb.rdb.ZAdd(rdb.ctx, key, redis.Z{
		Score:  float64(score),
		Member: value,
	}).Err()
}

// lua script

func (rdb *RedisClient) RunScript(script string, keys []string, args ...any) (any, error) {
	return rdb.rdb.Eval(rdb.ctx, script, keys, args).Result()
}
