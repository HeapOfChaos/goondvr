package channel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HeapOfChaos/goondvr/internal"
)

// fakeThumbSource hands out a new preview address on every call.
type fakeThumbSource struct {
	calls atomic.Int32
	fail  atomic.Bool
	delay time.Duration
}

func (f *fakeThumbSource) FetchLiveThumbURL(ctx context.Context, _ *internal.Req, user string) (string, error) {
	n := f.calls.Add(1)
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if f.fail.Load() {
		return "", errors.New("site unavailable")
	}
	return fmt.Sprintf("https://cdn.example/thumbs/%d/%s", n, user), nil
}

func newThumbChannel(t *testing.T, minAge, maxBackoff time.Duration) *Channel {
	t.Helper()
	oldA, oldB := liveThumbMinAge, liveThumbMaxBackoff
	liveThumbMinAge, liveThumbMaxBackoff = minAge, maxBackoff
	ch := newTestChannel(t, &fakeUI{pushes: map[string]int{}}, time.Hour)
	ch.setLiveThumbURL("https://cdn.example/thumbs/0/tester")
	t.Cleanup(func() { liveThumbMinAge, liveThumbMaxBackoff = oldA, oldB })
	return ch
}

func TestLiveThumbIsRenewedOnDemandAndRateLimited(t *testing.T) {
	ch := newThumbChannel(t, 80*time.Millisecond, time.Second)
	src := &fakeThumbSource{}
	ctx := context.Background()

	first := ch.freshLiveThumbURL(ctx, src)
	if first == "https://cdn.example/thumbs/0/tester" {
		t.Fatal("the preview address was not renewed on the first request")
	}
	// A burst of requests (several browsers, several cards) asks the site once.
	for i := 0; i < 20; i++ {
		if got := ch.freshLiveThumbURL(ctx, src); got != first {
			t.Fatalf("request %d changed the address within the minimum age: %q", i, got)
		}
	}
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("site asked %d times for 21 requests in a burst, want 1", n)
	}
	time.Sleep(100 * time.Millisecond)
	if got := ch.freshLiveThumbURL(ctx, src); got == first || src.calls.Load() != 2 {
		t.Fatalf("after the minimum age the address must be renewed: %q (%d calls)", got, src.calls.Load())
	}
}

func TestLiveThumbKeepsThePictureWhenTheSiteFails(t *testing.T) {
	ch := newThumbChannel(t, 20*time.Millisecond, 160*time.Millisecond)
	src := &fakeThumbSource{}
	src.fail.Store(true)
	before := ch.liveThumbURL()
	ctx := context.Background()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := ch.freshLiveThumbURL(ctx, src); got != before {
			t.Fatalf("a failing site replaced the preview address: %q", got)
		}
		time.Sleep(2 * time.Millisecond)
	}
	// Backing off: 20, 40, 80, 160, 160... ms between questions, not one per request.
	if n := src.calls.Load(); n < 2 || n > 8 {
		t.Fatalf("expected a handful of backed-off retries, got %d", n)
	}

	src.fail.Store(false)
	if !waitFor(2*time.Second, func() bool { return ch.freshLiveThumbURL(ctx, src) != before }) {
		t.Fatal("the preview address did not recover once the site answered again")
	}
}

func TestLiveThumbConcurrentRequestsShareOneQuestion(t *testing.T) {
	ch := newThumbChannel(t, time.Second, time.Minute)
	src := &fakeThumbSource{delay: 60 * time.Millisecond}
	var wg sync.WaitGroup
	got := make([]string, 8)
	for i := range got {
		wg.Add(1)
		go func(i int) { defer wg.Done(); got[i] = ch.freshLiveThumbURL(context.Background(), src) }(i)
	}
	wg.Wait()
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("8 simultaneous requests asked the site %d times, want 1", n)
	}
	for _, u := range got {
		if u != got[0] {
			t.Fatalf("simultaneous requests got different addresses: %v", got)
		}
	}
}

func TestLiveThumbRequestThatIsAbandonedDoesNotCountAsAFailure(t *testing.T) {
	ch := newThumbChannel(t, time.Second, time.Minute)
	src := &fakeThumbSource{delay: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	before := ch.liveThumbURL()
	if got := ch.freshLiveThumbURL(ctx, src); got != before {
		t.Fatalf("abandoned request changed the address: %q", got)
	}
	src.delay = 0
	if got := ch.freshLiveThumbURL(context.Background(), src); got == before {
		t.Fatal("the next request must be allowed to ask the site at once after an abandoned one")
	}
}
