package redisclient

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisClient struct {
	Ctx    context.Context
	Client *redis.Client
}

func New(uri string, context context.Context) (*RedisClient, error) {

	var redisClient *RedisClient = nil
	opts, err := redis.ParseURL(os.Getenv("REDIS_URI"))
	if err != nil {
		return redisClient, err
	}

	redisClient.Client = redis.NewClient(opts)
	redisClient.Ctx = context

	return redisClient, nil
}
func (redisClient *RedisClient) Exists(key string) (bool, error) {

	exists, err := redisClient.Client.Exists(redisClient.Ctx, key).Result()
	if err != nil {
		return false, err
	}

	if exists > 0 {
		return true, nil
	}
	return false, nil
}
func (redisClient *RedisClient) Connect(uri string, context context.Context) error {
	opts, err := redis.ParseURL(uri)
	if err != nil {
		return err
	}
	redisClient.Client = redis.NewClient(opts)
	redisClient.Ctx = context
	return nil
}

func (redisClient *RedisClient) Set(key string, value interface{}, expires time.Duration) error {

	jsonString, _ := json.Marshal(value)

	return redisClient.Client.Set(redisClient.Ctx, key, jsonString, expires).Err()
}

func (redisClient *RedisClient) GetJSON(key string, obj interface{}) error {

	value, err := redisClient.Client.Get(redisClient.Ctx, key).Result()
	if err != nil {
		return err
	}
	json.Unmarshal([]byte(value), &obj)
	return nil
}

func (redisClient *RedisClient) Get(key string) (string, error) {

	value, err := redisClient.Client.Get(redisClient.Ctx, key).Result()
	if err != nil {
		return "", err
	}
	return value, nil

}
