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
