package joint

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type fakeRedisClient struct {
	setNXResult bool
	setNXErr    error
	setNXDelay  time.Duration
	evalResult  int64
	evalErr     error
	evalDelay   time.Duration
}

func (f *fakeRedisClient) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd {
	if f.setNXDelay > 0 {
		time.Sleep(f.setNXDelay)
	}
	cmd := redis.NewBoolCmd(ctx)
	cmd.SetVal(f.setNXResult)
	cmd.SetErr(f.setNXErr)
	return cmd
}

func (f *fakeRedisClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	if f.evalDelay > 0 {
		time.Sleep(f.evalDelay)
	}
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(f.evalResult)
	cmd.SetErr(f.evalErr)
	return cmd
}

func TestAcquireSucceedsWithFakeMajority(t *testing.T) {
	lock := &Lock{
		clients: []redisClient{
			&fakeRedisClient{setNXResult: true},
			&fakeRedisClient{setNXResult: true},
			&fakeRedisClient{setNXResult: true},
			&fakeRedisClient{setNXResult: false},
			&fakeRedisClient{setNXResult: false},
		},
		key: "test",
		ttl: 5 * time.Second,
	}

	acquired, token, err := lock.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire returned error: %v", err)
	}
	if !acquired {
		t.Errorf("expected acquired=true, got false")
	}
	if token == 0 {
		t.Errorf("expected non-zero token")
	}
}

func TestAcquireFailsWithFakeMinority(t *testing.T) {
	lock := &Lock{
		clients: []redisClient{
			&fakeRedisClient{setNXResult: true},
			&fakeRedisClient{setNXResult: true},
			&fakeRedisClient{setNXResult: false},
			&fakeRedisClient{setNXResult: false},
			&fakeRedisClient{setNXResult: false},
		},
		key: "test",
		ttl: 5 * time.Second,
	}

	acquired, token, err := lock.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire returned error: %v", err)
	}
	if acquired {
		t.Errorf("expected acquired=false, got true")
	}
	if token != 0 {
		t.Errorf("expected token=0, got %d", token)
	}
}

func TestAcquireFailsWhenElapsedExceedsTTL(t *testing.T) {
	lock := &Lock{
		clients: []redisClient{
			&fakeRedisClient{setNXResult: true},
			&fakeRedisClient{setNXResult: true},
			&fakeRedisClient{setNXResult: true},
			&fakeRedisClient{setNXResult: true, setNXDelay: 50 * time.Millisecond},
			&fakeRedisClient{setNXResult: true, setNXDelay: 50 * time.Millisecond},
		},
		key: "test",
		ttl: 10 * time.Millisecond,
	}

	acquired, token, err := lock.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire returned error: %v", err)
	}
	if acquired {
		t.Errorf("expected acquired=false because elapsed time exceeded ttl, got true")
	}
	if token != 0 {
		t.Errorf("expected token=0, got %d", token)
	}
}

func TestAcquireReturnsJoinedErrorWhenMajorityError(t *testing.T) {
	errA := errors.New("boom-a")
	errB := errors.New("boom-b")
	errC := errors.New("boom-c")
	lock := &Lock{
		clients: []redisClient{
			&fakeRedisClient{setNXErr: errA},
			&fakeRedisClient{setNXErr: errB},
			&fakeRedisClient{setNXErr: errC},
			&fakeRedisClient{setNXResult: true},
			&fakeRedisClient{setNXResult: true},
		},
		key: "test",
		ttl: 5 * time.Second,
	}

	acquired, _, err := lock.Acquire(context.Background())
	if acquired {
		t.Errorf("expected acquired=false, got true")
	}
	if !errors.Is(err, errA) || !errors.Is(err, errB) || !errors.Is(err, errC) {
		t.Errorf("expected joined error to wrap all three original errors, got: %v", err)
	}
}

func TestReleaseSucceedsWithFakeMajority(t *testing.T) {
	lock := &Lock{
		clients: []redisClient{
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 0},
			&fakeRedisClient{evalResult: 0},
		},
		key: "test",
		ttl: 5 * time.Second,
	}

	released, err := lock.Release(context.Background())
	if err != nil {
		t.Fatalf("Release returned error: %v", err)
	}
	if !released {
		t.Errorf("expected released=true, got false")
	}
}

func TestReleaseFailsWhenMinorityMatch(t *testing.T) {
	lock := &Lock{
		clients: []redisClient{
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 0},
			&fakeRedisClient{evalResult: 0},
			&fakeRedisClient{evalResult: 0},
			&fakeRedisClient{evalResult: 0},
		},
		key: "test",
		ttl: 5 * time.Second,
	}

	released, err := lock.Release(context.Background())
	if err != nil {
		t.Fatalf("Release returned error: %v", err)
	}
	if released {
		t.Errorf("expected released=false, got true")
	}
}

func TestReleaseReturnsJoinedErrorWhenMajorityError(t *testing.T) {
	errA := errors.New("boom-a")
	errB := errors.New("boom-b")
	errC := errors.New("boom-c")
	lock := &Lock{
		clients: []redisClient{
			&fakeRedisClient{evalErr: errA},
			&fakeRedisClient{evalErr: errB},
			&fakeRedisClient{evalErr: errC},
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 1},
		},
		key: "test",
		ttl: 5 * time.Second,
	}

	released, err := lock.Release(context.Background())
	if released {
		t.Errorf("expected released=false, got true")
	}
	if !errors.Is(err, errA) || !errors.Is(err, errB) || !errors.Is(err, errC) {
		t.Errorf("expected joined error to wrap all three original errors, got: %v", err)
	}
}

func TestExtendSucceedsWithFakeMajority(t *testing.T) {
	lock := &Lock{
		clients: []redisClient{
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 0},
			&fakeRedisClient{evalResult: 0},
		},
		key: "test",
		ttl: 5 * time.Second,
	}

	extended, err := lock.Extend(context.Background())
	if err != nil {
		t.Fatalf("Extend returned error: %v", err)
	}
	if !extended {
		t.Errorf("expected extended=true, got false")
	}
}

func TestExtendFailsWhenKeyDoesNotExist(t *testing.T) {
	lock := &Lock{
		clients: []redisClient{
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 0},
			&fakeRedisClient{evalResult: 0},
			&fakeRedisClient{evalResult: 0},
			&fakeRedisClient{evalResult: 0},
		},
		key: "test",
		ttl: 5 * time.Second,
	}

	extended, err := lock.Extend(context.Background())
	if err != nil {
		t.Fatalf("Extend returned error: %v", err)
	}
	if extended {
		t.Errorf("expected extended=false, got true")
	}
}

func TestExtendFailsWhenElapsedExceedsTTL(t *testing.T) {
	lock := &Lock{
		clients: []redisClient{
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 1, evalDelay: 50 * time.Millisecond},
			&fakeRedisClient{evalResult: 1, evalDelay: 50 * time.Millisecond},
		},
		key: "test",
		ttl: 10 * time.Millisecond,
	}

	extended, err := lock.Extend(context.Background())
	if err != nil {
		t.Fatalf("Extend returned error: %v", err)
	}
	if extended {
		t.Errorf("expected extended=false because elapsed time exceeded ttl, got true")
	}
}

func TestExtendReturnsJoinedErrorWhenMajorityError(t *testing.T) {
	errA := errors.New("boom-a")
	errB := errors.New("boom-b")
	errC := errors.New("boom-c")
	lock := &Lock{
		clients: []redisClient{
			&fakeRedisClient{evalErr: errA},
			&fakeRedisClient{evalErr: errB},
			&fakeRedisClient{evalErr: errC},
			&fakeRedisClient{evalResult: 1},
			&fakeRedisClient{evalResult: 1},
		},
		key: "test",
		ttl: 5 * time.Second,
	}

	extended, err := lock.Extend(context.Background())
	if extended {
		t.Errorf("expected extended=false, got true")
	}
	if !errors.Is(err, errA) || !errors.Is(err, errB) || !errors.Is(err, errC) {
		t.Errorf("expected joined error to wrap all three original errors, got: %v", err)
	}
}
