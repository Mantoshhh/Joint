package joint

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type redisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
}

type Lock struct {
	clients []redisClient
	key     string
	ttl     time.Duration
	value   string
}

func (l *Lock) fanOut(op func(redisClient) (bool, error)) (successCount int, errs []error) {
	results := make(chan bool, len(l.clients))
	errCh := make(chan error, len(l.clients))
	for _, client := range l.clients {
		go func() {
			success, err := op(client)
			results <- success
			errCh <- err
		}()
	}
	for i := 0; i < len(l.clients); i++ {
		if <-results {
			successCount++
		}
		if err := <-errCh; err != nil {
			errs = append(errs, err)
		}
	}
	return successCount, errs
}

func New(addrs []string, key string, ttl time.Duration) *Lock {
	clients := make([]redisClient, len(addrs))
	for i, addr := range addrs {
		clients[i] = redis.NewClient(&redis.Options{Addr: addr})
	}
	return &Lock{clients: clients, key: key, ttl: ttl}
}

func (l *Lock) Acquire(ctx context.Context) (bool, int64, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return false, 0, err
	}
	l.value = id.String()

	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, l.ttl)
	defer cancel()

	successCount, collectedErrs := l.fanOut(func(client redisClient) (bool, error) {
		return client.SetNX(ctx, l.key, l.value, l.ttl).Result()
	})

	majority := len(l.clients)/2 + 1
	if len(collectedErrs) >= majority {
		l.releaseAll(context.Background())
		return false, 0, errors.Join(collectedErrs...)
	}
	elapsed := time.Since(start)
	acquired := successCount >= majority && elapsed < l.ttl
	if !acquired {
		l.releaseAll(context.Background())
		return false, 0, nil
	}
	return true, time.Now().UnixNano(), nil
}

func (l *Lock) Extend(ctx context.Context) (bool, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, l.ttl)
	defer cancel()

	majority := len(l.clients)/2 + 1
	successCount, collectedErrs := l.fanOut(func(client redisClient) (bool, error) {
		res, err := client.Eval(ctx, extendScript, []string{l.key}, l.value, l.ttl.Milliseconds()).Int64()
		return res == 1, err
	})
	if len(collectedErrs) >= majority {
		return false, errors.Join(collectedErrs...)
	}
	elapsed := time.Since(start)
	return successCount >= majority && elapsed < l.ttl, nil
}

func (l *Lock) releaseAll(ctx context.Context) {
	l.fanOut(func(client redisClient) (bool, error) {
		res, err := client.Eval(ctx, releaseScript, []string{l.key}, l.value).Int64()
		return res == 1, err
	})
}

func (l *Lock) Release(ctx context.Context) (bool, error) {
	majority := len(l.clients)/2 + 1
	successCount, collectedErrs := l.fanOut(func(client redisClient) (bool, error) {
		res, err := client.Eval(ctx, releaseScript, []string{l.key}, l.value).Int64()
		return res == 1, err
	})
	if len(collectedErrs) >= majority {
		return false, errors.Join(collectedErrs...)
	}
	return successCount >= majority, nil
}
