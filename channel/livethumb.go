package channel

import (
	"context"
	"sync"
	"time"

	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/site"
)

// Live previews of sites whose preview address is a snapshot (Stripchat) go
// out of date: the address stays pointing at the same picture until the site
// is asked for the address of a newer one.
//
// The address is renewed ON DEMAND: only when the web UI asks for the picture
// of a channel (FreshLiveThumbURL, called by the preview proxy), and at most
// once per liveThumbMinAge for each channel. So channels that nobody looks at
// cost nothing, no goroutine runs per channel, and a card that is on screen
// gets a picture that is at most a few seconds old.

var (
	// liveThumbMinAge is the shortest time between two questions to the site
	// about the same channel. (Variables only so that tests can shorten them.)
	liveThumbMinAge = 8 * time.Second
	// liveThumbMaxBackoff is the longest wait after repeated failures.
	liveThumbMaxBackoff = 60 * time.Second
)

// previewReq is the HTTP client shared by all the preview renewals (one
// transport for the whole program instead of one per channel).
var previewReq = sync.OnceValue(internal.NewReq)

// liveThumbURL returns the address of the channel's live preview.
func (ch *Channel) liveThumbURL() string {
	ch.stateMu.RLock()
	defer ch.stateMu.RUnlock()
	return ch.LiveThumbURL
}

// setLiveThumbURL stores a new preview address and reports whether it differs
// from the previous one.
func (ch *Channel) setLiveThumbURL(u string) bool {
	ch.stateMu.Lock()
	defer ch.stateMu.Unlock()
	if ch.LiveThumbURL == u {
		return false
	}
	ch.LiveThumbURL = u
	return true
}

// GetLiveThumbURL is the address of the channel's live preview as last known
// (safe to call from any goroutine).
func (ch *Channel) GetLiveThumbURL() string { return ch.liveThumbURL() }

// FreshLiveThumbURL returns the address of the newest live preview. For sites
// whose address never changes (Chaturbate) it is just the stored one; for the
// others the site is asked for a newer address first, unless it was asked less
// than liveThumbMinAge ago.
func (ch *Channel) FreshLiveThumbURL(ctx context.Context) string {
	src, ok := resolveSite(ch.Config.Site).(site.LiveThumbSource)
	if !ok {
		return ch.liveThumbURL()
	}
	return ch.freshLiveThumbURL(ctx, src)
}

func (ch *Channel) freshLiveThumbURL(ctx context.Context, src site.LiveThumbSource) string {
	// One question at a time per channel: requests that arrive while it is being
	// asked wait for the answer instead of asking again.
	ch.liveThumbMu.Lock()
	defer ch.liveThumbMu.Unlock()

	wait := liveThumbMinAge
	for i := 0; i < ch.liveThumbFails && wait < liveThumbMaxBackoff; i++ {
		wait *= 2
	}
	if wait > liveThumbMaxBackoff {
		wait = liveThumbMaxBackoff
	}
	if !ch.liveThumbAsked.IsZero() && time.Since(ch.liveThumbAsked) < wait {
		return ch.liveThumbURL()
	}
	ch.liveThumbAsked = time.Now()

	u, err := src.FetchLiveThumbURL(ctx, previewReq(), ch.Config.Username)
	if err != nil || u == "" {
		if ctx.Err() != nil {
			ch.liveThumbAsked = time.Time{} // the browser went away; that is not the site's fault
			return ch.liveThumbURL()
		}
		ch.liveThumbFails++ // keep the picture we have and try again later
		return ch.liveThumbURL()
	}
	ch.liveThumbFails = 0
	ch.setLiveThumbURL(u)
	return u
}
