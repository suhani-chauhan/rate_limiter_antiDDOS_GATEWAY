package blocking

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type IPBlocker struct {
	Client *redis.Client
}

func (b *IPBlocker) IsBlocked(
	ctx context.Context,
	ip string,
) (bool, error) {

	key := "blocked:" + ip

	result, err := b.Client.Exists(ctx, key).Result()

	if err != nil {
		return false, err
	}

	return result > 0, nil
}

func (b *IPBlocker) BlockIP(
	ctx context.Context,
	ip string,
	duration time.Duration,
) error {

	key := "blocked:" + ip

	if duration <= 0 {
		return b.Client.Set(ctx, key, "1", 0).Err()
	}

	return b.Client.Set(ctx, key, "1", duration).Err()
}

func (b *IPBlocker) UnblockIP(
	ctx context.Context,
	ip string,
) error {

	key := "blocked:" + ip

	return b.Client.Del(ctx, key).Err()
}
