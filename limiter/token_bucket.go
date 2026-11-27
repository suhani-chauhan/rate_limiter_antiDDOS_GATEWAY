package limiter

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type TokenBucket struct {
	Client     *redis.Client
	Capacity   float64
	RefillRate float64
	KeyPrefix  string
}

var tokenBucketScript = redis.NewScript(`
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local data = redis.call("HMGET", KEYS[1], "tokens", "last_time")

local tokens = tonumber(data[1])
local last_time = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_time = now
end

local elapsed = math.max(0, now - last_time)

tokens = math.min(capacity, tokens + (elapsed * refill_rate / 1000))

local allowed = 0

if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call("HSET", KEYS[1],
    "tokens", tokens,
    "last_time", now
)

redis.call("PEXPIRE", KEYS[1], 60000)

return {allowed, tostring(tokens)}
`)

func (tb *TokenBucket) Allow(
	ctx context.Context,
	ip string,
) (bool, float64, error) {

	key := tb.KeyPrefix + ip

	now := time.Now().UnixMilli()

	result, err := tokenBucketScript.Run(
		ctx,
		tb.Client,
		[]string{key},
		tb.Capacity,
		tb.RefillRate,
		now,
	).Slice()

	if err != nil {
		return false, 0, err
	}

	allowedValue := fmt.Sprint(result[0])
	remainingValue := fmt.Sprint(result[1])

	allowed := allowedValue == "1"

	remaining, err := strconv.ParseFloat(remainingValue, 64)

	if err != nil {
		return false, 0, err
	}

	return allowed, remaining, nil
}
