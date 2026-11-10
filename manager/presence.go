package manager

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/HeapOfChaos/goondvr/channel"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/site"
)

// Presence engine: keeps the "on air" mark of every channel that is not being
// recorded up to date, so that the web UI can show (and move to the top) the
// channels that are broadcasting. It runs for the life of the process and does
// not need a browser: whatever opens the page later finds the marks ready.
//
// Two methods, the second one takes over when the first fails:
//
//  1. The list of rooms on the air (site.OnlineListSource), the way siren
//     (github.com/bcmk/siren) does it: one sweep (a few requests) says which of
//     ALL the channels are on the air, plus viewers and tags. It is tried first
//     every presenceInterval. A failure (blocked, refused, broken answer, not
//     configured) backs off (doubling, up to listMaxBackoff) and is not
//     reported as a channel problem.
//  2. The per-channel check (site.AvailabilitySource): one light request per
//     paused channel, a few at a time, paced so that a full round takes about
//     probeCycle (never faster than probeMinSpacing between two requests); the
//     first round is quicker so that the list settles soon. It is used for the
//     sites whose list is not healthy and to double-check a channel that the
//     list stopped reporting.
//
// Both avoid the browser fallback, and stay silent for a while after any channel
// was blocked by Cloudflare (more requests would only make it worse).
//
// (Variables only so that tests can shorten them.)
var (
	presenceDefaultInterval = 60 * time.Second
	presenceMinInterval     = 20 * time.Second
	listTimeout             = 90 * time.Second
	listFirstBackoff        = time.Minute
	listMaxBackoff          = 10 * time.Minute
	listUnconfiguredRecheck = 30 * time.Second

	probeCycle        = 60 * time.Second
	probeMinSpacing   = 1200 * time.Millisecond
	probeMaxSpacing   = 6 * time.Second
	probeFirstSpacing = 600 * time.Millisecond // between channels never probed yet
	probeWorkers      = 3
	probeMaxBlock     = 10 * time.Minute

	presenceIdlePoll = 5 * time.Second // how often it looks for work when idle
)

// presenceCFQuiet is how long after the last Cloudflare block reported by a
// channel the engine stays silent.
const presenceCFQuiet = 5 * time.Minute

// listState is what the engine knows about the list of one site.
type listState struct {
	next         time.Time // when the next sweep is due
	backoff      time.Duration
	lastOK       time.Time
	unconfigured bool
	lastErr      string
}

type presenceEngine struct {
	mu         sync.Mutex
	lists      map[string]*listState
	confirm    map[string]struct{} // ids of paused channels that the list stopped reporting: ask the site before believing it
	probeBlock time.Duration
	lastAnswer time.Time     // last time anything (list or probe) answered
	probing    bool          // some site is being served by the per-channel method
	kick       chan struct{} // wakes the loop up (a channel was added, settings were saved)
}

// kickPresence makes the engine look at the channels right away.
func (m *Manager) kickPresence() {
	select {
	case m.presence.kick <- struct{}{}:
	default: // already pending (or no loop is running)
	}
}

func (e *presenceEngine) listFor(siteName string) *listState {
	if e.lists == nil {
		e.lists = map[string]*listState{}
	}
	ls := e.lists[siteName]
	if ls == nil {
		ls = &listState{}
		e.lists[siteName] = ls
	}
	return ls
}

// presenceInterval is the pause between two sweeps of a list.
func presenceInterval() time.Duration {
	if server.Config != nil && server.Config.PresenceInterval > 0 {
		if d := time.Duration(server.Config.PresenceInterval) * time.Second; d > presenceMinInterval {
			return d
		}
		return presenceMinInterval
	}
	return presenceDefaultInterval
}

// listHealthy reports whether the list of a site answered recently, so that the
// per-channel method is not needed.
func (ls *listState) healthy(interval time.Duration) bool {
	return !ls.unconfigured && !ls.lastOK.IsZero() && time.Since(ls.lastOK) < 3*interval+listTimeout
}

// ResetPresence makes the engine try its lists again right away (the settings
// that control them were saved).
func (m *Manager) ResetPresence() {
	e := &m.presence
	e.mu.Lock()
	for _, ls := range e.lists {
		ls.next = time.Time{}
		ls.backoff = 0
		ls.unconfigured = false
	}
	e.mu.Unlock()
	m.kickPresence()
}

// presenceLoop runs for the life of the process.
func (m *Manager) presenceLoop() {
	req := internal.NewReq()
	for {
		select {
		case <-time.After(m.presenceStep(req)):
		case <-m.presence.kick:
		}
	}
}

// presenceStep does what is due and returns how long to sleep. After the
// detection it lets the channels that record automatically react to it
// (auto.go).
func (m *Manager) presenceStep(req *internal.Req) time.Duration {
	wait := m.detectStep(req)
	if m.autoStep() && wait > autoStartGap {
		wait = autoStartGap // some channels are waiting for their turn to start
	}
	return wait
}

// detectStep does the detection that is due and returns how long to sleep.
func (m *Manager) detectStep(req *internal.Req) time.Duration {
	if m.cloudflareBlockedRecently() {
		return presenceIdlePoll
	}
	interval := presenceInterval()
	e := &m.presence

	// What there is to watch: every channel, grouped by site.
	bySite := map[string][]*channel.Channel{}
	waiting := map[string]bool{} // sites with a channel that is not being recorded
	m.Channels.Range(func(_, v any) bool {
		ch := v.(*channel.Channel)
		s := ch.Config.Site
		bySite[s] = append(bySite[s], ch)
		if !ch.Online() {
			waiting[s] = true
		}
		return true
	})

	wait := presenceIdlePoll
	needProbe := map[string]bool{}
	anyProbe := false
	for s := range waiting {
		src, ok := channel.ResolveSite(s).(site.OnlineListSource)
		e.mu.Lock()
		ls := e.listFor(s)
		due := !time.Now().Before(ls.next)
		e.mu.Unlock()
		if ok && due {
			m.sweepList(src, s, bySite[s], req, interval)
		}
		e.mu.Lock()
		ls = e.listFor(s)
		if !ok || !ls.healthy(interval) {
			needProbe[s] = true
			anyProbe = true
		}
		if ok {
			if d := time.Until(ls.next); d > 0 && d < wait {
				wait = d
			}
		}
		e.mu.Unlock()
	}
	e.mu.Lock()
	e.probing = anyProbe
	e.mu.Unlock()

	if d := m.probeRound(req, needProbe); d > 0 && d < wait {
		wait = d
	} else if d > 0 && anyProbe {
		wait = d
	}
	if wait < 200*time.Millisecond {
		wait = 200 * time.Millisecond
	}
	return wait
}

// sweepList asks the list of one site and applies the answer to its channels.
func (m *Manager) sweepList(src site.OnlineListSource, siteName string, chans []*channel.Channel, req *internal.Req, interval time.Duration) {
	e := &m.presence
	want := make(map[string]struct{}, len(chans))
	byName := make(map[string]*channel.Channel, len(chans))
	for _, ch := range chans {
		n := strings.ToLower(ch.Config.Username)
		want[n] = struct{}{}
		byName[n] = ch
	}

	ctx, cancel := context.WithTimeout(context.Background(), listTimeout)
	found, err := src.ListOnline(ctx, req, want)
	cancel()

	e.mu.Lock()
	ls := e.listFor(siteName)
	switch {
	case err == nil:
		ls.lastOK, ls.lastErr, ls.backoff, ls.unconfigured = time.Now(), "", 0, false
		ls.next = time.Now().Add(interval + time.Duration(rand.Int63n(int64(interval/10)+1)))
		e.lastAnswer = time.Now()
	case errors.Is(err, site.ErrListNotConfigured):
		ls.unconfigured, ls.lastErr = true, ""
		ls.next = time.Now().Add(listUnconfiguredRecheck)
	default:
		ls.unconfigured, ls.lastErr = false, err.Error()
		if ls.backoff == 0 {
			ls.backoff = listFirstBackoff
		} else if ls.backoff *= 2; ls.backoff > listMaxBackoff {
			ls.backoff = listMaxBackoff
		}
		ls.next = time.Now().Add(ls.backoff)
	}
	e.mu.Unlock()
	if err != nil {
		return
	}

	tagsChanged := false
	for name, ch := range byName {
		p, ok := found[name]
		var pp *site.Presence
		if ok {
			pp = &p
			e.mu.Lock()
			delete(e.confirm, siteName+"__"+name)
			e.mu.Unlock()
		} else if ch.OnAir() && ch.Paused() && !ch.Online() {
			// Reported on air before, absent now. A list can miss a room while
			// the pages shift under it: ask the site about this one before
			// believing it (the answer arrives through the per-channel method).
			e.mu.Lock()
			if e.confirm == nil {
				e.confirm = map[string]struct{}{}
			}
			e.confirm[siteName+"__"+name] = struct{}{}
			e.mu.Unlock()
			continue
		}
		if _, t := ch.ApplyPresence(pp); t {
			tagsChanged = true
		}
	}
	if tagsChanged {
		_ = m.SaveConfig()
	}
}

// probeRound runs the per-channel method for one small batch of channels and
// returns how long to wait before the next batch (0 = nothing was due).
func (m *Manager) probeRound(req *internal.Req, needProbe map[string]bool) time.Duration {
	e := &m.presence
	type cand struct {
		ch   *channel.Channel
		last int64
	}
	var due []cand
	n := 0
	firstRound := false
	now := time.Now().UnixNano()

	e.mu.Lock()
	confirm := e.confirm
	e.mu.Unlock()

	var stale []string // ids waiting for a confirmation that can no longer happen
	m.Channels.Range(func(_, v any) bool {
		ch := v.(*channel.Channel)
		id := ch.Config.Site + "__" + strings.ToLower(ch.Config.Username)
		eligible, last := ch.ProbeCandidate()
		if !eligible {
			if _, ok := confirm[id]; ok {
				stale = append(stale, id)
			}
			return true
		}
		_, confirming := confirm[id]
		if !needProbe[ch.Config.Site] && !confirming {
			return true
		}
		n++
		if confirming || last == 0 || now-last >= int64(probeCycle) {
			due = append(due, cand{ch, last})
			if last == 0 {
				firstRound = true
			}
		}
		return true
	})
	if len(stale) > 0 {
		e.mu.Lock()
		for _, id := range stale {
			delete(e.confirm, id)
		}
		e.mu.Unlock()
	}
	if len(due) == 0 {
		return 0
	}
	sort.Slice(due, func(i, j int) bool { return due[i].last < due[j].last })
	if len(due) > probeWorkers {
		due = due[:probeWorkers]
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	blocked := false
	for _, c := range due {
		wg.Add(1)
		go func(ch *channel.Channel) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			err := ch.ProbeAvailability(ctx, req)
			cancel()
			id := ch.Config.Site + "__" + strings.ToLower(ch.Config.Username)
			e.mu.Lock()
			if err == nil {
				delete(e.confirm, id)
				e.lastAnswer = time.Now()
			}
			e.mu.Unlock()
			if errors.Is(err, internal.ErrCloudflareBlocked) {
				mu.Lock()
				blocked = true
				mu.Unlock()
			}
		}(c.ch)
	}
	wg.Wait()

	wait := probeSpacing(n, len(due), firstRound)
	e.mu.Lock()
	if blocked {
		if e.probeBlock == 0 {
			e.probeBlock = time.Minute
		} else if e.probeBlock *= 2; e.probeBlock > probeMaxBlock {
			e.probeBlock = probeMaxBlock
		}
		if e.probeBlock > wait {
			wait = e.probeBlock
		}
	} else {
		e.probeBlock = 0
	}
	e.mu.Unlock()
	return wait + time.Duration(rand.Int63n(int64(wait/5)+1))
}

// probeSpacing is the pause after a batch of `batch` probes when n channels are
// waiting for one.
func probeSpacing(n, batch int, firstRound bool) time.Duration {
	if firstRound {
		return probeFirstSpacing
	}
	if n < 1 {
		n = 1
	}
	if batch < 1 {
		batch = 1
	}
	d := probeCycle * time.Duration(batch) / time.Duration(n)
	if d < probeMinSpacing {
		d = probeMinSpacing
	}
	if d > probeMaxSpacing {
		d = probeMaxSpacing
	}
	return d
}

// cloudflareBlockedRecently reports whether any channel was blocked by
// Cloudflare in the last presenceCFQuiet.
func (m *Manager) cloudflareBlockedRecently() bool {
	m.cfBlocksMu.Lock()
	defer m.cfBlocksMu.Unlock()
	for _, t := range m.cfBlocks {
		if time.Since(t) < presenceCFQuiet {
			return true
		}
	}
	return false
}

// PresenceStatus says how the "on air" marks are being kept up to date.
func (m *Manager) PresenceStatus() server.PresenceStatus {
	e := &m.presence
	e.mu.Lock()
	defer e.mu.Unlock()
	st := server.PresenceStatus{Method: "", AgeSec: -1}
	for name, ls := range e.lists {
		if ls.healthy(presenceInterval()) {
			st.Method = "list"
		} else if st.Method == "" && e.probing {
			st.Method = "probe"
		}
		if ls.unconfigured && name == "chaturbate" {
			st.Detail = "no-list-config"
		} else if ls.lastErr != "" && st.Detail == "" {
			st.Detail = "list-failing"
		}
	}
	if st.Method == "" && e.probing {
		st.Method = "probe"
	}
	if !e.lastAnswer.IsZero() {
		st.AgeSec = int(time.Since(e.lastAnswer).Seconds())
	}
	return st
}
