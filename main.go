package main

import (
	rdbClient "github.com/qtyty/delay-mq/v1/internal/redis"
	"time"
)

func main() {
	redisClient := rdbClient.NewRedisClient()

	redisClient.Set("key", "value", time.Minute)

}
