package joint

import (
	"context"
	"testing"
	"time"
)

func TestAcquireRelease(t *testing.T) {
	lock := New([]string{"localhost:6379"}, t.Name(), 5*time.Second)
	acquired, _, err := lock.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire returned error: %v", err)
	}
	if !acquired {
		t.Errorf("expected acquired=true, got false")
	}
	t.Cleanup(func() {
		lock.Release(context.Background())
	})
}

func TestAcquireFailsWhileHeld(t *testing.T) {
	holder := New([]string{"localhost:6379"}, t.Name(), 5*time.Second)
	contender := New([]string{"localhost:6379"}, t.Name(), 5*time.Second)
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
		holder.Release(context.Background())
	})
}

func TestReleaseWithoutOwnership(t *testing.T) {
	lock := New([]string{"localhost:6379"}, t.Name(), 5*time.Second)
	released, err := lock.Release(context.Background())
	if err != nil {
		t.Fatalf("Release for lock returned error: %v", err)
	}
	if released {
		t.Errorf("expected released=false, got true")
	}
}

func TestQuorumSucceedsAgainstMinorityContention(t *testing.T) {
	addrs := []string{"localhost:6379", "localhost:6380",
		"localhost:6381", "localhost:6382", "localhost:6383"}
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
		contended.Release(context.Background())
		main.Release(context.Background())
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
		contended.Release(context.Background())
		main.Release(context.Background())
		check.Release(context.Background())
	})
}
