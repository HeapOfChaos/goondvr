package chaturbate

import (
	"context"
	"testing"
	"time"
)

// Pausing a channel must stop the segment watchers at once; sleepCtx is what
// replaced their fixed time.After waits.
func TestSleepCtxReturnsAtOnceWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := sleepCtx(ctx, 10*time.Second)
	if err == nil {
		t.Fatal("sleepCtx returned nil although the context was cancelled")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("sleepCtx kept waiting %v after cancellation", elapsed)
	}
}

func TestSleepCtxWaitsWhenNotCancelled(t *testing.T) {
	start := time.Now()
	if err := sleepCtx(context.Background(), 30*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
		t.Fatalf("returned after %v, before the requested wait", elapsed)
	}
}

func TestSleepCtxWithAlreadyCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepCtx(ctx, time.Hour); err == nil {
		t.Fatal("expected an error for an already cancelled context")
	}
}
