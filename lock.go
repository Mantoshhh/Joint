package joint

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Lock struct {
	clients []*redis.Client
	key     string
	ttl     time.Duration
	value   string
}

const script string = `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
`

func New(addrs []string, key string, ttl time.Duration) *Lock {
	clients := make([]*redis.Client, len(addrs))
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
	value := id.String()
	l.value = value
	success, err := l.clients[0].SetNX(ctx, l.key, l.value, l.ttl).Result()
	if success {
		return true, 0, err
	}
	return false, 0, err
}

func (l *Lock) Release(ctx context.Context) (bool, error) {
	luaCmd := l.clients[0].Eval(ctx, script, []string{l.key}, l.value)
	res, err := luaCmd.Int64()
	if err != nil {
		return false, err
	}
	if res == 0 {
		return false, nil
	}
	return true, nil
}
