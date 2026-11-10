package manager

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HeapOfChaos/goondvr/channel"
	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/site"
)

// fakeSite is a Chaturbate that answers the list and the per-channel API.
type fakeSite struct {
	mu        sync.Mutex
	listBody  string // what the list says ("" = HTTP 500)
	rooms     map[string]string
	listCalls atomic.Int32
	roomCalls atomic.Int32
}

func (f *fakeSite) handler(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case strings.HasPrefix(r.URL.Path, "/api/public/affiliates/onlinerooms"):
		f.listCalls.Add(1)
		if f.listBody == "" {
			http.Error(w, "boom", 500)
			return
		}
		fmt.Fprint(w, f.listBody)
	case strings.HasPrefix(r.URL.Path, "/api/chatvideocontext/"):
		f.roomCalls.Add(1)
		name := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/chatvideocontext/"), "/")
		if b, ok := f.rooms[name]; ok {
			fmt.Fprint(w, b)
			return
		}
		fmt.Fprint(w, `{"room_status":"offline","hls_source":""}`)
	default:
		http.NotFound(w, r)
	}
}

const (
	liveRoom = `{"room_status":"public","hls_source":"http://x/m.m3u8","num_viewers":5}`
	offRoom  = `{"room_status":"offline","hls_source":""}`
)

// presenceRig builds a manager with paused channels and a fake site.
func presenceRig(t *testing.T, campaign string, names ...string) (*Manager, *fakeSite, *internal.Req) {
	t.Helper()
	f := &fakeSite{rooms: map[string]string{}}
	ts := httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(ts.Close)

	oldCfg, oldMgr := server.Config, server.Manager
	server.Config = &entity.Config{Domain: ts.URL + "/", PresenceCampaign: campaign, Username: "cli"} // Username set: SaveConfig is a no-op
	m := &Manager{hub: newHub(), cfBlocks: map[string]time.Time{}}
	server.Manager = m
	for _, n := range names {
		ch := channel.New(&entity.ChannelConfig{Username: n, Site: "chaturbate", IsPaused: true})
		m.Channels.Store(entity.ChannelID("chaturbate", n), ch)
	}
	t.Cleanup(func() {
		m.Channels.Range(func(_, v any) bool {
			ch := v.(*channel.Channel)
			ch.Stop()
			ch.WaitPublisherExit()
			ch.WaitBackground()
			return true
		})
		server.Config, server.Manager = oldCfg, oldMgr
	})
	return m, f, internal.NewReq()
}

func onAir(m *Manager, name string) bool {
	v, _ := m.Channels.Load(entity.ChannelID("chaturbate", name))
	return v.(*channel.Channel).IsAvailable()
}

func TestPresenceListMarksEveryChannelAtOnceWithoutABrowser(t *testing.T) {
	m, f, req := presenceRig(t, "camp", "alice", "bob", "carol")
	f.listBody = `{"count":2,"results":[
	  {"username":"Alice","current_show":"public","num_users":9,"tags":["a","b"]},
	  {"username":"bob","current_show":"private","num_users":2}]}`

	if m.HasViewers() {
		t.Fatal("test premise: no browser is connected")
	}
	m.presenceStep(req) // ONE step

	if !onAir(m, "alice") || onAir(m, "bob") || onAir(m, "carol") {
		t.Fatalf("alice on air, bob (private) and carol (absent) not: %v %v %v", onAir(m, "alice"), onAir(m, "bob"), onAir(m, "carol"))
	}
	if f.roomCalls.Load() != 0 {
		t.Fatalf("with a healthy list no per-channel request is needed, got %d", f.roomCalls.Load())
	}
	v, _ := m.Channels.Load(entity.ChannelID("chaturbate", "alice"))
	if got := v.(*channel.Channel).ExportInfo(); got.NumViewers != 9 || strings.Join(got.Tags, ",") != "a,b" {
		t.Fatalf("viewers/tags from the list: %+v", got)
	}
	if st := m.PresenceStatus(); st.Method != "list" || st.AgeSec < 0 {
		t.Fatalf("status = %+v", st)
	}
}

func TestPresenceFallsBackToPerChannelWhenTheListFails(t *testing.T) {
	m, f, req := presenceRig(t, "camp", "alice", "bob")
	f.listBody = "" // HTTP 500
	f.rooms["alice"] = liveRoom

	m.presenceStep(req)
	if f.listCalls.Load() != 1 {
		t.Fatalf("the list is tried first, got %d calls", f.listCalls.Load())
	}
	deadline := time.Now().Add(3 * time.Second)
	for f.roomCalls.Load() < 2 && time.Now().Before(deadline) {
		m.presenceStep(req)
	}
	if !onAir(m, "alice") || onAir(m, "bob") {
		t.Fatal("the fallback must find alice on the air")
	}
	if f.listCalls.Load() != 1 {
		t.Fatalf("a failing list must back off, but it was asked %d times", f.listCalls.Load())
	}
	if st := m.PresenceStatus(); st.Method != "probe" || st.Detail != "list-failing" {
		t.Fatalf("status = %+v", st)
	}

	// The list recovers once its back-off is over.
	f.mu.Lock()
	f.listBody = `{"count":1,"results":[{"username":"bob","current_show":"public","num_users":1}]}`
	f.mu.Unlock()
	m.ResetPresence()
	m.presenceStep(req)
	if st := m.PresenceStatus(); st.Method != "list" {
		t.Fatalf("after recovering: %+v", st)
	}
	if !onAir(m, "bob") {
		t.Fatal("bob is on the list")
	}
}

func TestPresenceWithoutAffiliateIdUsesPerChannelMethod(t *testing.T) {
	m, f, req := presenceRig(t, "", "alice")
	f.rooms["alice"] = liveRoom
	deadline := time.Now().Add(3 * time.Second)
	for !onAir(m, "alice") && time.Now().Before(deadline) {
		m.presenceStep(req)
	}
	if !onAir(m, "alice") {
		t.Fatal("per-channel method did not find alice")
	}
	if f.listCalls.Load() != 0 {
		t.Fatal("an unconfigured list must not be requested")
	}
	if st := m.PresenceStatus(); st.Method != "probe" || st.Detail != "no-list-config" {
		t.Fatalf("status = %+v", st)
	}
}

func TestPresenceDoubleChecksAChannelTheListLost(t *testing.T) {
	m, f, req := presenceRig(t, "camp", "alice")
	f.listBody = `{"count":1,"results":[{"username":"alice","current_show":"public","num_users":3}]}`
	m.presenceStep(req)
	if !onAir(m, "alice") {
		t.Fatal("alice should be on air")
	}

	// The list now misses alice, but the site still says she is live
	// (pages shifting under the sweep): she must stay on air.
	f.mu.Lock()
	f.listBody = `{"count":1,"results":[{"username":"someone","current_show":"public","num_users":3}]}`
	f.rooms["alice"] = liveRoom
	f.mu.Unlock()
	m.ResetPresence()
	m.presenceStep(req)
	if !onAir(m, "alice") {
		t.Fatal("a channel the list lost must not be marked off the air before asking the site")
	}
	for i := 0; i < 20 && f.roomCalls.Load() == 0; i++ {
		m.presenceStep(req)
	}
	if f.roomCalls.Load() == 0 || !onAir(m, "alice") {
		t.Fatalf("expected a confirmation request and alice still on air (calls=%d)", f.roomCalls.Load())
	}

	// Now the site agrees that she left.
	f.mu.Lock()
	f.rooms["alice"] = offRoom
	f.mu.Unlock()
	m.ResetPresence()
	m.presenceStep(req)
	deadline := time.Now().Add(3 * time.Second)
	for onAir(m, "alice") && time.Now().Before(deadline) {
		m.presenceStep(req)
	}
	if onAir(m, "alice") {
		t.Fatal("alice left: she must be off the air once the site confirms it")
	}
}

func TestPresenceStaysSilentWhileCloudflareBlocks(t *testing.T) {
	m, f, req := presenceRig(t, "camp", "alice")
	f.listBody = `{"count":1,"results":[{"username":"alice","current_show":"public"}]}`
	m.cfBlocks["someone"] = time.Now()
	if d := m.presenceStep(req); d != presenceIdlePoll {
		t.Fatalf("wait = %v", d)
	}
	if f.listCalls.Load() != 0 || f.roomCalls.Load() != 0 {
		t.Fatal("no request while Cloudflare is blocking")
	}
}

func TestProbeSpacingStaysLightAndAdapts(t *testing.T) {
	if d := probeSpacing(3, 3, true); d != probeFirstSpacing {
		t.Fatalf("first round: want %v, got %v", probeFirstSpacing, d)
	}
	if d := probeSpacing(1000, probeWorkers, false); d != probeMinSpacing {
		t.Fatalf("1000 paused channels: never faster than %v, got %v", probeMinSpacing, d)
	}
	if d := probeSpacing(2, 2, false); d != probeMaxSpacing {
		t.Fatalf("2 paused channels: never slower than %v, got %v", probeMaxSpacing, d)
	}
	d := probeSpacing(90, probeWorkers, false)
	if round := d * 30; round < probeCycle*2/3 || round > probeCycle*3/2 {
		t.Fatalf("90 channels, 3 at a time: a full round takes %v, want about %v", round, probeCycle)
	}
}

func TestChannelInfoMarksTheStartOfEverySection(t *testing.T) {
	m, _, _ := presenceRig(t, "", "bob", "alice", "carol", "dave")
	set := func(name string, paused, onAir bool) {
		v, _ := m.Channels.Load(entity.ChannelID("chaturbate", name))
		ch := v.(*channel.Channel)
		ch.SetPaused(paused)
		ch.ApplyPresence(&site.Presence{Live: onAir})
	}
	set("alice", true, true)  // on air
	set("bob", true, false)   // paused
	set("carol", false, true) // on air (not paused)
	set("dave", false, false) // retrying

	var order, starts []string
	for _, c := range m.ChannelInfo() {
		order = append(order, c.Username)
		if c.SectionStart {
			starts = append(starts, c.Username)
		}
	}
	if strings.Join(order, ",") != "alice,carol,dave,bob" {
		t.Fatalf("order %v", order)
	}
	if strings.Join(starts, ",") != "alice,dave,bob" {
		t.Fatalf("section starts %v", starts)
	}
}
