//go:build integration

package joint

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func testAddrs() []string {
	if v := os.Getenv("JOINT_TEST_REDIS_ADDRS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:6379", "localhost:6380",
		"localhost:6381", "localhost:6382", "localhost:6383"}
}

func TestAcquireRelease(t *testing.T) {
	lock := New(testAddrs()[:1], t.Name(), 5*time.Second)
	acquired, _, err := lock.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire returned error: %v", err)
	}
	if !acquired {
		t.Errorf("expected acquired=true, got false")
	}
	t.Cleanup(func() {
		if _, err := lock.Release(context.Background()); err != nil {
			t.Logf("cleanup: Release failed: %v", err)
		}
	})
}

func TestAcquireThenExtend(t *testing.T) {
	lock := New(testAddrs()[:1], t.Name(), 5*time.Second)
	acquired, _, err := lock.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire returned error: %v", err)
	}
	if !acquired {
		t.Fatalf("expected acquired=true, got false")
	}
	t.Cleanup(func() {
		if _, err := lock.Release(context.Background()); err != nil {
			t.Logf("cleanup: Release failed: %v", err)
		}
	})

	extended, err := lock.Extend(context.Background())
	if err != nil {
		t.Fatalf("Extend returned error: %v", err)
	}
	if !extended {
		t.Errorf("expected extended=true, got false")
	}
}

func TestAcquireFailsWhileHeld(t *testing.T) {
	holder := New(testAddrs()[:1], t.Name(), 5*time.Second)
	contender := New(testAddrs()[:1], t.Name(), 5*time.Second)
	holderAcquired, _, err := holder.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire for holder returned error: %v", err)
	}
	if !holderAcquired {
		t.Fatalf("expected holderAcquired=true, got false")
	}
	contenderAcquired, _, err := contender.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire for contenderAcquired return error: %v", err)
	}
	if contenderAcquired {
		t.Errorf("expected contenderAcquired=false, got true")
	}
	t.Cleanup(func() {
		if _, err := holder.Release(context.Background()); err != nil {
			t.Logf("cleanup: Release failed: %v", err)
		}
	})
}

func TestReleaseWithoutOwnership(t *testing.T) {
	lock := New(testAddrs()[:1], t.Name(), 5*time.Second)
	released, err := lock.Release(context.Background())
	if err != nil {
		t.Fatalf("Release for lock returned error: %v", err)
	}
	if released {
		t.Errorf("expected released=false, got true")
	}
}

func TestQuorumSucceedsAgainstMinorityContention(t *testing.T) {
	addrs := testAddrs()
	contended := New(addrs[:2], t.Name(), 5*time.Second)
	contendedAcquired, _, err := contended.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire for contended returned error: %v", err)
	}
	if !contendedAcquired {
		t.Fatalf("expected contendedAcquired=true, got false")
	}
	main := New(addrs, t.Name(), 5*time.Second)
	mainAcquired, _, err := main.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire for main returned error: %v", err)
	}
	if !mainAcquired {
		t.Errorf("expected mainAcquired=true, got false")
	}
	t.Cleanup(func() {
		if _, err := contended.Release(context.Background()); err != nil {
			t.Logf("cleanup: contended.Release failed: %v", err)
		}
		if _, err := main.Release(context.Background()); err != nil {
			t.Logf("cleanup: main.Release failed: %v", err)
		}
	})
}

func TestQuorumFailsAgainstMajorityContention(t *testing.T) {
	addrs := []string{"localhost:6379", "localhost:6380",
		"localhost:6381", "localhost:6382", "localhost:6383"}
	contended := New(addrs[:3], t.Name(), 5*time.Second)
	contendedAcquired, _, err := contended.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire for contended returned error: %v", err)
	}
	if !contendedAcquired {
		t.Fatalf("expected contendedAcquired=true, got false")
	}
	main := New(addrs, t.Name(), 5*time.Second)
	mainAcquired, _, err := main.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire for main returned error: %v", err)
	}
	if mainAcquired {
		t.Errorf("expected mainAcquired=false, got true")
	}
	check := New(addrs[3:], t.Name(), 5*time.Second)
	checkAcquired, _, err := check.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire for check returned error: %v", err)
	}
	if !checkAcquired {
		t.Errorf("expected checkAcquired=true, got false")
	}
	t.Cleanup(func() {
		if _, err := contended.Release(context.Background()); err != nil {
			t.Logf("cleanup: contended.Release failed: %v", err)
		}
		if _, err := main.Release(context.Background()); err != nil {
			t.Logf("cleanup: main.Release failed: %v", err)
		}
		if _, err := check.Release(context.Background()); err != nil {
			t.Logf("cleanup: check.Release failed: %v", err)
		}
	})
}

func TestFencingTokenIncreasesAcrossAcquisitions(t *testing.T) {
	addrs := testAddrs()[:1]
	lock := New(addrs, t.Name(), 5*time.Second)
	_, token1, err := lock.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first Acquire returned error: %v", err)
	}
	if _, err := lock.Release(context.Background()); err != nil {
		t.Fatalf("Release between acquisitions returned error: %v", err)
	}
	_, token2, err := lock.Acquire(context.Background())
	if err != nil {
		t.Fatalf("second Acquire returned error: %v", err)
	}
	t.Cleanup(func() {
		if _, err := lock.Release(context.Background()); err != nil {
			t.Logf("cleanup: Release failed: %v", err)
		}
	})
	if token2 <= token1 {
		t.Errorf("expected token2 > token1, got token1=%d, token2=%d", token1, token2)
	}
}
