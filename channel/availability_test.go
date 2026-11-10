package channel

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/server"
)

// fakeRoomAPI answers like Chaturbate's chatvideocontext endpoint.
func fakeRoomAPI(t *testing.T, body *atomic.Value, calls *atomic.Int32) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, body.Load().(string))
	}))
	t.Cleanup(srv.Close)
	server.Config.Domain = srv.URL + "/"
}

const (
	roomLive    = `{"room_status":"public","hls_source":"http://x/master.m3u8","num_viewers":7,"room_title":"t"}`
	roomOffline = `{"room_status":"offline","hls_source":""}`
	roomPrivate = `{"room_status":"private","hls_source":""}`
)

func TestProbeMarksAPausedChannelAsAvailableAndBack(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannel(t, ui, time.Millisecond)
	ch.Config.IsPaused = true

	var body atomic.Value
	var calls atomic.Int32
	body.Store(roomLive)
	fakeRoomAPI(t, &body, &calls)
	req := internal.NewReq()
	ctx := t.Context()

	if info := ch.ExportInfo(); info.Rank != 3 || info.IsAvailable {
		t.Fatalf("a new paused channel must be rank 3, got rank %d available %v", info.Rank, info.IsAvailable)
	}
	if err := ch.ProbeAvailability(ctx, req); err != nil {
		t.Fatal(err)
	}
	info := ch.ExportInfo()
	if info.Rank != 1 || !info.IsAvailable || info.IsOnline || !info.Live() || info.NumViewers != 7 {
		t.Fatalf("on air but paused: want rank 1, available, not recording, 7 viewers; got %+v", info)
	}
	if !waitFor(time.Second, func() bool { return ui.count("update") >= 1 }) {
		t.Fatal("the change was not published to the web UI")
	}

	body.Store(roomPrivate) // a private show cannot be recorded: not available
	if err := ch.ProbeAvailability(ctx, req); err != nil {
		t.Fatal(err)
	}
	if info := ch.ExportInfo(); info.Rank != 3 || info.IsAvailable || info.NumViewers != 0 {
		t.Fatalf("private show: want rank 2, got %+v", info)
	}
	body.Store(roomLive)
	_ = ch.ProbeAvailability(ctx, req)
	body.Store(roomOffline)
	_ = ch.ProbeAvailability(ctx, req)
	if ch.IsAvailable() {
		t.Fatal("still available after the site said offline")
	}
}

func TestProbeKeepsTheAnswerWhenTheSiteFails(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannel(t, ui, time.Millisecond)
	ch.Config.IsPaused = true
	ch.setAvailable(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "<html>oops</html>")
	}))
	defer srv.Close()
	server.Config.Domain = srv.URL + "/"
	if err := ch.ProbeAvailability(t.Context(), internal.NewReq()); err == nil {
		t.Fatal("a broken answer must be reported as an error")
	}
	if !ch.IsAvailable() {
		t.Fatal("a failed probe must not change the answer")
	}
}

func TestProbeIgnoresChannelsThatAreNotPausedOrAreRecording(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannel(t, ui, time.Millisecond)
	if ok, _ := ch.ProbeCandidate(); ok {
		t.Fatal("a channel that is being monitored must not be probed")
	}
	ch.Config.IsPaused = true
	if ok, last := ch.ProbeCandidate(); !ok || last != 0 {
		t.Fatalf("paused, never probed: want candidate with last=0, got %v %d", ok, last)
	}
	ch.UpdateOnlineStatus(true)
	if ok, _ := ch.ProbeCandidate(); ok {
		t.Fatal("a recording channel must not be probed")
	}
	if info := ch.ExportInfo(); info.Rank != 0 || !info.IsAvailable {
		t.Fatalf("recording: want rank 0 and available, got rank %d available %v", info.Rank, info.IsAvailable)
	}
}

func TestRankAndSortKeyOrderTheList(t *testing.T) {
	ui := &fakeUI{pushes: map[string]int{}}
	ch := newTestChannel(t, ui, time.Millisecond)
	ch.Config.Username = "Ab_C"
	if got := ch.ExportInfo().SortKey; got != "ab_c chaturbate" {
		t.Fatalf("sort key %q", got)
	}
	// ' ' sorts before every character a username can contain, so "ab" < "ab_c" < "ab-c" < "abc".
	keys := []string{"ab chaturbate", "ab-c chaturbate", "ab_c chaturbate", "abc chaturbate"}
	for i := 1; i < len(keys); i++ {
		if !(keys[i-1] < keys[i]) && keys[i-1] != "ab-c chaturbate" {
			t.Fatalf("%q must sort before %q", keys[i-1], keys[i])
		}
	}
}

func TestRankOfTheFourSections(t *testing.T) {
	cases := []struct {
		online, onAir, paused bool
		want                  int
	}{
		{true, true, false, RankRecording},
		{true, true, true, RankRecording}, // being recorded wins over everything
		{false, true, true, RankOnAir},    // on air, paused: shown as "on air"
		{false, true, false, RankOnAir},
		{false, false, false, RankRetrying}, // off air but the monitor keeps trying
		{false, false, true, RankPaused},
	}
	for _, c := range cases {
		if got := RankOf(c.online, c.onAir, c.paused); got != c.want {
			t.Errorf("RankOf(%v,%v,%v) = %d, want %d", c.online, c.onAir, c.paused, got, c.want)
		}
	}
}
