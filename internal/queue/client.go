package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	*redis.Client
	queueKey    string
	arrivalsKey string
	timeout     time.Duration
}

func New(rdb *redis.Client, queueKey string, arrivalsKey string, timeout time.Duration) *Queue {
	return &Queue{
		Client:      rdb,
		queueKey:    queueKey,
		arrivalsKey: arrivalsKey,
		timeout:     timeout,
	}
}

func (q *Queue) query(ctx context.Context, fn func(context.Context) (int64, error)) (int64, error) {
	redisCtx, cancel := context.WithTimeoutCause(ctx, q.timeout, ErrRedisClientTimeout)
	defer cancel()

	v, err := fn(redisCtx)
	if err != nil {
		// parent context died - controller shutting down.
		if ctx.Err() != nil {
			return 0, context.Cause(ctx)
		}

		// redisCtx died
		if cause := context.Cause(redisCtx); cause != nil {
			return 0, fmt.Errorf("%w: %w", ErrRedisUnavailable, cause)
		}

		return 0, classifyErr(err)
	}
	return v, nil
}

func (q *Queue) Depth(ctx context.Context) (int64, error) {
	return q.query(ctx, func(ctx context.Context) (int64, error) {
		return q.LLen(ctx, q.queueKey).Result()
	})
}

func (q *Queue) ArrivalsTotal(ctx context.Context) (int64, error) {
	n, err := q.query(ctx, func(rctx context.Context) (int64, error) {
		return q.Get(rctx, q.arrivalsKey).Int64()
	})
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return n, err
}
