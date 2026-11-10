package channel

import (
	"sync"
	"testing"
	"time"

	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/server"
)

// fakeUI stands in for the manager: it records what the Publisher pushes and
// can be told to refuse (queue full) or to hang (browser not reading).
type fakeUI struct {
	server.IManager

	mu      sync.Mutex
	pushes  map[string]int
	viewers bool          // answer to HasViewers
	refuse  int           // refuse this many Publish calls (return false)
	hang    chan struct{} // if set, Publish blocks until closed
	lastLog []string
}

func (f *fakeUI) HasViewers() bool { return f.viewers }

func (f *fakeUI) Publish(evt string, info *entity.ChannelInfo) bool {
	if f.hang != nil {
		<-f.hang
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse > 0 {
		f.refuse--
		return false
	}
	f.pushes[evt]++
	if evt == entity.EventLog {
		f.lastLog = info.Logs
	}
	return true
}

func (f *fakeUI) count(evt string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pushes[evt]
}

func newTestChannel(t *testing.T, ui *fakeUI, interval time.Duration) *Channel {
	t.Helper()
	return newTestChannelGap(t, ui, interval, interval)
}

// newTestChannelGap is newTestChannel with a separate (shorter) gap for
// urgent state changes.
func newTestChannelGap(t *testing.T, ui *fakeUI, interval, urgentGap time.Duration) *Channel {
	t.Helper()
	oldInterval, oldUrgent, oldMgr, oldCfg := uiMinInterval, uiUrgentInterval, server.Manager, server.Config
	uiMinInterval, uiUrgentInterval = interval, urgentGap
	server.Manager = ui
	server.Config = &entity.Config{}
	ch := New(&entity.ChannelConfig{Username: "tester", Site: "chaturbate"})
	t.Cleanup(func() {
		ch.stopPublisher()
		<-ch.publisherExited // the goroutine reads the globals restored below
		uiMinInterval, uiUrgentInterval, server.Manager, server.Config = oldInterval, oldUrgent, oldMgr, oldCfg
	})
	return ch
}

func TestPublisherCoalescesBursts(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannel(t, ui, 100*time.Millisecond)

	// 2000 updates and 2000 log lines in ~400 ms, like a busy recording.
	deadline := time.Now().Add(400 * time.Millisecond)
	n := 0
	for time.Now().Before(deadline) {
		ch.Update()
		ch.sendLog("line")
		n++
		time.Sleep(200 * time.Microsecond)
	}
	time.Sleep(250 * time.Millisecond) // let the trailing push happen

	if got := ui.count(entity.EventUpdate); got < 2 || got > 8 {
		t.Fatalf("%d signals produced %d update pushes, want a handful (2-8)", n, got)
	}
	if got := ui.count(entity.EventLog); got < 2 || got > 8 {
		t.Fatalf("%d log lines produced %d log pushes, want a handful (2-8)", n, got)
	}
	// The last push must carry the latest state, capped at maxLogLines.
	ui.mu.Lock()
	defer ui.mu.Unlock()
	if len(ui.lastLog) != maxLogLines {
		t.Fatalf("last pushed log has %d lines, want %d", len(ui.lastLog), maxLogLines)
	}
}

func TestPublisherSendsFirstChangeImmediately(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannel(t, ui, time.Hour) // cool-down far longer than the test

	ch.Update()
	deadline := time.Now().Add(time.Second)
	for ui.count(entity.EventUpdate) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if ui.count(entity.EventUpdate) != 1 {
		t.Fatal("first change after a quiet period was not pushed immediately")
	}
}

func TestProducersNeverBlockOnAStuckBrowser(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}, hang: make(chan struct{})}
	ch := newTestChannel(t, ui, time.Millisecond)
	defer close(ui.hang)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50000; i++ { // what the recording loop does per segment
			ch.sendLog("duration")
			ch.Update()
			ch.UpdateThumb()
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Update/UpdateThumb/sendLog blocked while the web UI was not consuming events")
	}
}

func TestPublishIsRetriedWhenQueueIsFull(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}, refuse: 3}
	ch := newTestChannel(t, ui, 20*time.Millisecond)

	ch.Update()
	deadline := time.Now().Add(2 * time.Second)
	for ui.count(entity.EventUpdate) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if ui.count(entity.EventUpdate) != 1 {
		t.Fatal("update refused by a full queue was never retried")
	}
}

// Pause must cancel the context of the run that is currently active, whatever
// the monitor is doing at that moment (including its very first network call).
func TestPauseCancelsTheRunningMonitor(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannel(t, ui, time.Millisecond)

	ch.monitorMu.Lock()
	ch.startMonitorLocked()
	ch.monitorMu.Unlock()
	ctx := ch.runContext()
	if ctx.Err() != nil {
		t.Fatal("fresh run context is already cancelled")
	}

	ch.Pause()

	select {
	case <-ctx.Done():
	default:
		t.Fatal("Pause did not cancel the context of the running monitor")
	}
}

// A run that follows a Pause must not inherit the cancelled context of the
// previous one (otherwise a resumed channel would stop immediately).
func TestEachRunGetsItsOwnContext(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannel(t, ui, time.Millisecond)

	ch.monitorMu.Lock()
	ch.startMonitorLocked()
	ch.monitorMu.Unlock()
	first := ch.runContext()

	ch.Pause()

	ch.monitorMu.Lock()
	ch.startMonitorLocked()
	ch.monitorMu.Unlock()
	second := ch.runContext()

	if first.Err() == nil {
		t.Fatal("first run was not cancelled by Pause")
	}
	if second.Err() != nil {
		t.Fatal("second run starts with an already cancelled context")
	}
}

// waitFor polls until cond holds or the timeout expires.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// A state change (paused, resumed...) must reach the browser at once even
// while the channel is in the middle of the progress cool-down.
func TestStateChangeBypassesProgressCooldown(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannelGap(t, ui, time.Hour, 10*time.Millisecond)

	ch.Update() // first progress push goes out immediately
	if !waitFor(time.Second, func() bool { return ui.count(entity.EventUpdate) == 1 }) {
		t.Fatal("first progress update was not pushed")
	}
	ch.Update() // second one must wait for the (one hour) cool-down
	time.Sleep(150 * time.Millisecond)
	if got := ui.count(entity.EventUpdate); got != 1 {
		t.Fatalf("progress update was not held back: %d pushes", got)
	}

	ch.UpdateNow() // a state change must not
	if !waitFor(time.Second, func() bool { return ui.count(entity.EventUpdate) == 2 }) {
		t.Fatalf("state change stayed behind the progress cool-down (%d pushes)", ui.count(entity.EventUpdate))
	}
}

// ...but a storm of state changes is still merged into a few pushes.
func TestStateChangesAreStillCoalesced(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannelGap(t, ui, time.Hour, 50*time.Millisecond)

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		ch.UpdateNow()
		ch.UpdateThumb()
		time.Sleep(100 * time.Microsecond)
	}
	time.Sleep(120 * time.Millisecond)

	for _, evt := range []string{entity.EventUpdate, entity.EventThumb} {
		if got := ui.count(evt); got < 2 || got > 10 {
			t.Fatalf("%s: %d pushes for a 300 ms storm with a 50 ms gap, want a handful (2-10)", evt, got)
		}
	}
}

// Info/Error lines ("channel resumed"...) are urgent, per-segment Verbose
// lines are not.
func TestInfoLogIsUrgentVerboseIsNot(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannelGap(t, ui, time.Hour, 10*time.Millisecond)

	ch.Verbose("duration: 1")
	if !waitFor(time.Second, func() bool { return ui.count(entity.EventLog) == 1 }) {
		t.Fatal("first log push missing")
	}
	ch.Verbose("duration: 2")
	time.Sleep(150 * time.Millisecond)
	if got := ui.count(entity.EventLog); got != 1 {
		t.Fatalf("Verbose log line was not held back: %d pushes", got)
	}

	ch.Info("channel resumed")
	if !waitFor(time.Second, func() bool { return ui.count(entity.EventLog) == 2 }) {
		t.Fatal("Info log line stayed behind the progress cool-down")
	}
	ui.mu.Lock()
	defer ui.mu.Unlock()
	if n := len(ui.lastLog); n != 3 {
		t.Fatalf("pushed log has %d lines, want all 3", n)
	}
}

// Resume while the previous run is still stopping is queued, and says so.
func TestResumeWhilePreviousRunIsStopping(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannel(t, ui, time.Millisecond)

	ch.monitorMu.Lock()
	ch.startMonitorLocked() // a run is active
	ch.monitorMu.Unlock()

	if _, started, queued := ch.requestMonitorStart(); started || queued {
		t.Fatalf("resume of a channel that is already running: started=%v queued=%v, want neither", started, queued)
	}

	ch.Pause() // run is cancelled but has not finished yet
	if _, started, queued := ch.requestMonitorStart(); started || !queued {
		t.Fatalf("resume during shutdown: started=%v queued=%v, want queued only", started, queued)
	}
	ch.monitorMu.Lock()
	requested := ch.monitorRestartRequested
	ch.monitorMu.Unlock()
	if !requested {
		t.Fatal("restart was not recorded for when the previous run finishes")
	}
}
