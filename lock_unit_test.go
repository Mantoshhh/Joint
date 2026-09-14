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

	// failFirst, if > 0, makes the first failFirst calls to the relevant
	// method (SetNX has its own counter, Eval has its own) return
	// transientErr instead of the fields above, then fall back to normal
	// behavior. setNXCalls/evalCalls track invocation counts separately,
	// since a single failed Acquire attempt calls both SetNX (the attempt
	// itself) and Eval (releaseAll's cleanup) on every client.
	failFirst    int
	transientErr error
	setNXCalls   int
	evalCalls    int
}

func (f *fakeRedisClient) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd {
	f.setNXCalls++
	if f.setNXDelay > 0 {
		time.Sleep(f.setNXDelay)
	}
	cmd := redis.NewBoolCmd(ctx)
	if f.setNXCalls <= f.failFirst {
		cmd.SetErr(f.transientErr)
		return cmd
	}
	cmd.SetVal(f.setNXResult)
	cmd.SetErr(f.setNXErr)
	return cmd
}

func (f *fakeRedisClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	f.evalCalls++
	if f.evalDelay > 0 {
		time.Sleep(f.evalDelay)
	}
	cmd := redis.NewCmd(ctx)
	if f.evalCalls <= f.failFirst {
		cmd.SetErr(f.transientErr)
		return cmd
	}
	cmd.SetVal(f.evalResult)
	cmd.SetErr(f.evalErr)
	return cmd
}

// lockOp normalizes Acquire/Extend/Release to the same (bool, error) shape
// so retry-loop behavior can be tested once, across all three, via a table.
// attemptCalls reads whichever counter actually corresponds to "one attempt"
// for that method — Acquire's is setNXCalls, Extend/Release's is evalCalls
// (Acquire's evalCalls also includes releaseAll's cleanup calls on failure,
// so it isn't a 1:1 attempt count for that method).
type lockOp struct {
	name         string
	call         func(l *Lock, ctx context.Context) (bool, error)
	attemptCalls func(f *fakeRedisClient) int
}

var lockOps = []lockOp{
	{"Acquire", func(l *Lock, ctx context.Context) (bool, error) {
		acquired, _, err := l.Acquire(ctx)
		return acquired, err
	}, func(f *fakeRedisClient) int { return f.setNXCalls }},
	{"Extend", func(l *Lock, ctx context.Context) (bool, error) {
		return l.Extend(ctx)
	}, func(f *fakeRedisClient) int { return f.evalCalls }},
	{"Release", func(l *Lock, ctx context.Context) (bool, error) {
		return l.Release(ctx)
	}, func(f *fakeRedisClient) int { return f.evalCalls }},
}

func TestAcquireSucceeds(t *testing.T) {
	clients := []redisClient{
		&fakeRedisClient{setNXResult: true},
		&fakeRedisClient{setNXResult: true},
		&fakeRedisClient{setNXResult: true},
		&fakeRedisClient{setNXResult: false},
		&fakeRedisClient{setNXResult: false}}

	lock := &Lock{
		clients:     clients,
		key:         t.Name(),
		ttl:         5 * time.Second,
		quorum:      len(clients)/2 + 1,
		maxAttempts: 1,
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

func TestAcquireFails(t *testing.T) {
	clients := []redisClient{
		&fakeRedisClient{setNXResult: true},
		&fakeRedisClient{setNXResult: true},
		&fakeRedisClient{setNXResult: false},
		&fakeRedisClient{setNXResult: false},
		&fakeRedisClient{setNXResult: false}}

	lock := &Lock{
		clients:     clients,
		key:         t.Name(),
		ttl:         5 * time.Second,
		quorum:      len(clients)/2 + 1,
		maxAttempts: 1,
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
	clients := []redisClient{
		&fakeRedisClient{setNXResult: true},
		&fakeRedisClient{setNXResult: true},
		&fakeRedisClient{setNXResult: true},
		&fakeRedisClient{setNXResult: true, setNXDelay: 50 * time.Millisecond},
		&fakeRedisClient{setNXResult: true, setNXDelay: 50 * time.Millisecond}}

	lock := &Lock{
		clients:     clients,
		key:         t.Name(),
		ttl:         10 * time.Millisecond,
		quorum:      len(clients)/2 + 1,
		maxAttempts: 1,
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

func TestAcquireReturnsJoinedErrorWhenQuorumErrorsOut(t *testing.T) {
	errA := errors.New("boom-a")
	errB := errors.New("boom-b")
	errC := errors.New("boom-c")

	clients := []redisClient{
		&fakeRedisClient{setNXErr: errA},
		&fakeRedisClient{setNXErr: errB},
		&fakeRedisClient{setNXErr: errC},
		&fakeRedisClient{setNXResult: true},
		&fakeRedisClient{setNXResult: true}}

	lock := &Lock{
		clients:     clients,
		key:         t.Name(),
		ttl:         5 * time.Second,
		quorum:      len(clients)/2 + 1,
		maxAttempts: 1,
	}

	acquired, _, err := lock.Acquire(context.Background())
	if acquired {
		t.Errorf("expected acquired=false, got true")
	}
	if !errors.Is(err, errA) || !errors.Is(err, errB) || !errors.Is(err, errC) {
		t.Errorf("expected joined error to wrap all three original errors, got: %v", err)
	}
}

func TestReleaseSucceeds(t *testing.T) {
	clients := []redisClient{
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 0},
		&fakeRedisClient{evalResult: 0}}

	lock := &Lock{
		clients:     clients,
		key:         t.Name(),
		ttl:         5 * time.Second,
		quorum:      len(clients)/2 + 1,
		maxAttempts: 1,
	}

	released, err := lock.Release(context.Background())
	if err != nil {
		t.Fatalf("Release returned error: %v", err)
	}
	if !released {
		t.Errorf("expected released=true, got false")
	}
}

func TestReleaseFails(t *testing.T) {
	clients := []redisClient{
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 0},
		&fakeRedisClient{evalResult: 0},
		&fakeRedisClient{evalResult: 0},
		&fakeRedisClient{evalResult: 0},
	}

	lock := &Lock{
		clients:     clients,
		key:         t.Name(),
		ttl:         5 * time.Second,
		quorum:      len(clients)/2 + 1,
		maxAttempts: 1,
	}

	released, err := lock.Release(context.Background())
	if err != nil {
		t.Fatalf("Release returned error: %v", err)
	}
	if released {
		t.Errorf("expected released=false, got true")
	}
}

func TestReleaseReturnsJoinedErrorWhenQuorumErrorsOut(t *testing.T) {
	errA := errors.New("boom-a")
	errB := errors.New("boom-b")
	errC := errors.New("boom-c")

	clients := []redisClient{
		&fakeRedisClient{evalErr: errA},
		&fakeRedisClient{evalErr: errB},
		&fakeRedisClient{evalErr: errC},
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 1},
	}

	lock := &Lock{
		clients:     clients,
		key:         t.Name(),
		ttl:         5 * time.Second,
		quorum:      len(clients)/2 + 1,
		maxAttempts: 1,
	}

	released, err := lock.Release(context.Background())
	if released {
		t.Errorf("expected released=false, got true")
	}
	if !errors.Is(err, errA) || !errors.Is(err, errB) || !errors.Is(err, errC) {
		t.Errorf("expected joined error to wrap all three original errors, got: %v", err)
	}
}

func TestExtendSucceeds(t *testing.T) {
	clients := []redisClient{
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 0},
		&fakeRedisClient{evalResult: 0},
	}

	lock := &Lock{
		clients:     clients,
		key:         t.Name(),
		ttl:         5 * time.Second,
		quorum:      len(clients)/2 + 1,
		maxAttempts: 1,
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
	clients := []redisClient{
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 0},
		&fakeRedisClient{evalResult: 0},
		&fakeRedisClient{evalResult: 0},
		&fakeRedisClient{evalResult: 0},
	}

	lock := &Lock{
		clients:     clients,
		key:         t.Name(),
		ttl:         5 * time.Second,
		quorum:      len(clients)/2 + 1,
		maxAttempts: 1,
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
	clients := []redisClient{
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 1, evalDelay: 50 * time.Millisecond},
		&fakeRedisClient{evalResult: 1, evalDelay: 50 * time.Millisecond},
	}

	lock := &Lock{
		clients:     clients,
		key:         t.Name(),
		ttl:         10 * time.Millisecond,
		quorum:      len(clients)/2 + 1,
		maxAttempts: 1,
	}

	extended, err := lock.Extend(context.Background())
	if err != nil {
		t.Fatalf("Extend returned error: %v", err)
	}
	if extended {
		t.Errorf("expected extended=false because elapsed time exceeded ttl, got true")
	}
}

func TestExtendReturnsJoinedErrorWhenQuorumErrorsOut(t *testing.T) {
	errA := errors.New("boom-a")
	errB := errors.New("boom-b")
	errC := errors.New("boom-c")

	clients := []redisClient{
		&fakeRedisClient{evalErr: errA},
		&fakeRedisClient{evalErr: errB},
		&fakeRedisClient{evalErr: errC},
		&fakeRedisClient{evalResult: 1},
		&fakeRedisClient{evalResult: 1},
	}

	lock := &Lock{
		clients:     clients,
		key:         t.Name(),
		ttl:         5 * time.Second,
		quorum:      len(clients)/2 + 1,
		maxAttempts: 1,
	}

	extended, err := lock.Extend(context.Background())
	if extended {
		t.Errorf("expected extended=false, got true")
	}
	if !errors.Is(err, errA) || !errors.Is(err, errB) || !errors.Is(err, errC) {
		t.Errorf("expected joined error to wrap all three original errors, got: %v", err)
	}
}

func TestRetryRecoversFromTransientError(t *testing.T) {
	for _, op := range lockOps {
		t.Run(op.name, func(t *testing.T) {
			transientErr := errors.New("transient")
			clients := []redisClient{
				&fakeRedisClient{setNXResult: true, evalResult: 1, failFirst: 1, transientErr: transientErr},
				&fakeRedisClient{setNXResult: true, evalResult: 1, failFirst: 1, transientErr: transientErr},
				&fakeRedisClient{setNXResult: true, evalResult: 1, failFirst: 1, transientErr: transientErr},
				&fakeRedisClient{setNXResult: true, evalResult: 1},
				&fakeRedisClient{setNXResult: true, evalResult: 1},
			}
			lock := &Lock{
				clients:     clients,
				key:         t.Name(),
				ttl:         5 * time.Second,
				quorum:      len(clients)/2 + 1,
				maxAttempts: 2,
				baseDelay:   time.Millisecond,
			}

			ok, err := op.call(lock, context.Background())
			if err != nil {
				t.Fatalf("%s returned error: %v", op.name, err)
			}
			if !ok {
				t.Errorf("expected %s to succeed after retry, got false", op.name)
			}
		})
	}
}

func TestRetryExhaustsAttemptsAndReturnsLastError(t *testing.T) {
	for _, op := range lockOps {
		t.Run(op.name, func(t *testing.T) {
			transientErr := errors.New("always transient")
			erroring := []*fakeRedisClient{
				{setNXErr: transientErr, evalErr: transientErr},
				{setNXErr: transientErr, evalErr: transientErr},
				{setNXErr: transientErr, evalErr: transientErr},
			}
			clients := []redisClient{
				erroring[0], erroring[1], erroring[2],
				&fakeRedisClient{setNXResult: true, evalResult: 1},
				&fakeRedisClient{setNXResult: true, evalResult: 1},
			}
			lock := &Lock{
				clients:     clients,
				key:         t.Name(),
				ttl:         5 * time.Second,
				quorum:      len(clients)/2 + 1,
				maxAttempts: 3,
				baseDelay:   time.Millisecond,
			}

			ok, err := op.call(lock, context.Background())
			if ok {
				t.Errorf("expected %s to fail, got success", op.name)
			}
			if err == nil {
				t.Fatalf("expected non-nil error after exhausting retries")
			}
			for i, f := range erroring {
				if got := op.attemptCalls(f); got != lock.maxAttempts {
					t.Errorf("erroring client[%d] calls = %d, want %d (expected a retry on every attempt)", i, got, lock.maxAttempts)
				}
			}
		})
	}
}

func TestRetryDoesNotRetryOnCleanFailure(t *testing.T) {
	for _, op := range lockOps {
		t.Run(op.name, func(t *testing.T) {
			fakes := []*fakeRedisClient{
				{setNXResult: false, evalResult: 0},
				{setNXResult: false, evalResult: 0},
				{setNXResult: false, evalResult: 0},
				{setNXResult: false, evalResult: 0},
				{setNXResult: false, evalResult: 0},
			}
			clients := make([]redisClient, len(fakes))
			for i, f := range fakes {
				clients[i] = f
			}
			lock := &Lock{
				clients:     clients,
				key:         t.Name(),
				ttl:         5 * time.Second,
				quorum:      len(clients)/2 + 1,
				maxAttempts: 3,
				baseDelay:   time.Millisecond,
			}

			ok, err := op.call(lock, context.Background())
			if err != nil {
				t.Fatalf("%s returned error: %v", op.name, err)
			}
			if ok {
				t.Errorf("expected %s to report false (clean failure), got true", op.name)
			}
			for i, f := range fakes {
				if got := op.attemptCalls(f); got != 1 {
					t.Errorf("fake[%d] calls = %d, want 1 (expected no retry on a clean, non-transient failure)", i, got)
				}
			}
		})
	}
}

func TestRetryRespectsContextCancellation(t *testing.T) {
	for _, op := range lockOps {
		t.Run(op.name, func(t *testing.T) {
			transientErr := errors.New("always transient")
			clients := []redisClient{
				&fakeRedisClient{setNXErr: transientErr, evalErr: transientErr},
				&fakeRedisClient{setNXErr: transientErr, evalErr: transientErr},
				&fakeRedisClient{setNXErr: transientErr, evalErr: transientErr},
				&fakeRedisClient{setNXErr: transientErr, evalErr: transientErr},
				&fakeRedisClient{setNXErr: transientErr, evalErr: transientErr},
			}
			lock := &Lock{
				clients:     clients,
				key:         t.Name(),
				ttl:         5 * time.Second,
				quorum:      len(clients)/2 + 1,
				maxAttempts: 10,
				baseDelay:   time.Second,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			start := time.Now()
			_, err := op.call(lock, ctx)
			elapsed := time.Since(start)

			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected context.DeadlineExceeded, got %v", err)
			}
			if elapsed > 500*time.Millisecond {
				t.Errorf("expected early return on context cancellation, took %v", elapsed)
			}
		})
	}
}

func TestBackoffWithJitter(t *testing.T) {
	base := 100 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		d := backoffWithJitter(base, attempt)
		if d < 0 {
			t.Errorf("attempt %d: backoffWithJitter returned negative duration: %v", attempt, d)
		}
		maxExpected := base * time.Duration(1<<attempt)
		if d > maxExpected {
			t.Errorf("attempt %d: backoffWithJitter returned %v, want <= %v", attempt, d, maxExpected)
		}
	}
}
