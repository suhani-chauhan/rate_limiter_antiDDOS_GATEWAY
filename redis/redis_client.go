package redis

import (
	"context"
	"github.com/redis/go-redis/v9"
)

var Client *redis.Client

func ConnectRedis() error {
	Client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()

	if err := Client.Ping(ctx).Err(); err != nil {
		return err
	}

	return nil
}
