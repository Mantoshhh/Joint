package joint

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type redisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
}

type Lock struct {
	clients     []redisClient
	key         string
	ttl         time.Duration
	value       string
	quorum      int
	maxAttempts int
	baseDelay   time.Duration
}

type Option func(*Lock)

func New(addrs []string, key string, ttl time.Duration, opts ...Option) *Lock {
	clients := make([]redisClient, len(addrs))
	for i, addr := range addrs {
		clients[i] = redis.NewClient(&redis.Options{Addr: addr})
	}
	l := &Lock{clients: clients, key: key, ttl: ttl,
		quorum: len(clients)/2 + 1, maxAttempts: 1}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

func (l *Lock) Acquire(ctx context.Context) (bool, int64, error) {
	var lastErr error
	for attempt := 0; attempt < l.maxAttempts; attempt++ {
		acquired, token, err := l.tryAcquire(ctx)
		if err == nil {
			return acquired, token, nil
		}
		lastErr = err
		if attempt < l.maxAttempts-1 {
			select {
			case <-ctx.Done():
				return false, 0, ctx.Err()
			case <-time.After(backoffWithJitter(l.baseDelay, attempt)):
			}
		}
	}
	return false, 0, lastErr
}

func (l *Lock) Extend(ctx context.Context) (bool, error) {
	var lastErr error
	for attempt := 0; attempt < l.maxAttempts; attempt++ {
		extended, err := l.tryExtend(ctx)
		if err == nil {
			return extended, nil
		}
		lastErr = err
		if attempt < l.maxAttempts-1 {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(backoffWithJitter(l.baseDelay, attempt)):
			}
		}
	}
	return false, lastErr
}

func (l *Lock) Release(ctx context.Context) (bool, error) {
	var lastErr error
	for attempt := 0; attempt < l.maxAttempts; attempt++ {
		released, err := l.tryRelease(ctx)
		if err == nil {
			return released, nil
		}
		lastErr = err
		if attempt < l.maxAttempts-1 {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(backoffWithJitter(l.baseDelay, attempt)):
			}
		}
	}
	return false, lastErr
}

func WithRetry(maxAttempts int, baseDelay time.Duration) Option {
	return func(l *Lock) {
		l.maxAttempts = maxAttempts
		l.baseDelay = baseDelay
	}
}

func backoffWithJitter(base time.Duration, attempt int) time.Duration {
	exp := base * time.Duration(1<<attempt)
	half := exp / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
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

func (l *Lock) releaseAll(ctx context.Context) {
	l.fanOut(func(client redisClient) (bool, error) {
		res, err := client.Eval(ctx, releaseScript, []string{l.key}, l.value).Int64()
		return res == 1, err
	})
}

func (l *Lock) tryAcquire(ctx context.Context) (bool, int64, error) {
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

	if len(collectedErrs) >= l.quorum {
		l.releaseAll(context.Background())
		return false, 0, errors.Join(collectedErrs...)
	}
	elapsed := time.Since(start)
	acquired := successCount >= l.quorum && elapsed < l.ttl
	if !acquired {
		l.releaseAll(context.Background())
		return false, 0, nil
	}
	return true, time.Now().UnixNano(), nil
}

func (l *Lock) tryExtend(ctx context.Context) (bool, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, l.ttl)
	defer cancel()

	successCount, collectedErrs := l.fanOut(func(client redisClient) (bool, error) {
		res, err := client.Eval(ctx, extendScript, []string{l.key}, l.value, l.ttl.Milliseconds()).Int64()
		return res == 1, err
	})
	if len(collectedErrs) >= l.quorum {
		return false, errors.Join(collectedErrs...)
	}
	elapsed := time.Since(start)
	return successCount >= l.quorum && elapsed < l.ttl, nil
}

func (l *Lock) tryRelease(ctx context.Context) (bool, error) {
	successCount, collectedErrs := l.fanOut(func(client redisClient) (bool, error) {
		res, err := client.Eval(ctx, releaseScript, []string{l.key}, l.value).Int64()
		return res == 1, err
	})
	if len(collectedErrs) >= l.quorum {
		return false, errors.Join(collectedErrs...)
	}
	return successCount >= l.quorum, nil
}
