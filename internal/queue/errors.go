package queue

import (
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var (
	ErrRedisUnavailable   = errors.New("redis unavailable")
	ErrKeyNoExist         = errors.New("key does not exist")
	ErrRedisClientTimeout = errors.New("redis client deadline exceeded")
	ErrRedisLoading       = errors.New("redis loading dataset")
	ErrWrongType          = errors.New("wrong type for key")
	ErrRedisConfig        = errors.New("redis rejected client")
)

func classifyErr(err error) error {
	var rErr redis.Error
	if !errors.As(err, &rErr) {
		// never received redis response
		return fmt.Errorf("%w: %w", ErrRedisUnavailable, err)
	}

	switch {
	case redis.HasErrorPrefix(err, "WRONGTYPE"):
		return fmt.Errorf("%w: %w", ErrWrongType, err)

	case redis.IsLoadingError(err):
		return fmt.Errorf("%w: %w", ErrRedisLoading, err)

	case redis.IsMasterDownError(err),
		redis.IsMaxClientsError(err),
		redis.IsTryAgainError(err):
		return fmt.Errorf("%w: %w", ErrRedisUnavailable, err)

	case redis.IsAuthError(err), redis.IsPermissionError(err):
		return fmt.Errorf("%w: %w", ErrRedisConfig, err)

	default:
		return err
	}
}
