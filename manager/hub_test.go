package manager

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// sseReader reads the text/event-stream of a test connection.
type sseReader struct {
	resp *http.Response
	r    *bufio.Reader
}

func connect(t *testing.T, url string, header map[string]string) *sseReader {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return &sseReader{resp: resp, r: bufio.NewReader(resp.Body)}
}

// next returns the next block (up to the blank line), or "" on timeout/EOF.
func (s *sseReader) next(timeout time.Duration) string {
	type res struct{ block string }
	done := make(chan res, 1)
	go func() {
		var b strings.Builder
		for {
			line, err := s.r.ReadString('\n')
			if err != nil {
				done <- res{}
				return
			}
			if line == "\n" {
				done <- res{b.String()}
				return
			}
			b.WriteString(line)
		}
	}()
	select {
	case r := <-done:
		return r.block
	case <-time.After(timeout):
		return ""
	}
}

// nextEvent skips the handshake and comments and returns the next real event.
func (s *sseReader) nextEvent(timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		b := s.next(time.Until(deadline))
		if b == "" {
			return ""
		}
		if strings.Contains(b, "event: ") {
			return b
		}
	}
	return ""
}

func newTestServer(t *testing.T, h *hub, snapshot func() []hubMessage) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.serve(w, r, snapshot) }))
	t.Cleanup(srv.Close)
	return srv
}

func TestFrameFormat(t *testing.T) {
	got := string(frame(7, hubMessage{event: "chaturbate__a-log", data: []byte("one\r\ntwo\nthree")}))
	want := "id: 7\nevent: chaturbate__a-log\ndata: one\ndata: two\ndata: three\n\n"
	if got != want {
		t.Fatalf("frame:\n%q\nwant:\n%q", got, want)
	}
	// An empty payload is still a dispatchable event (one empty data line).
	if got := string(frame(1, hubMessage{event: "x", data: nil})); got != "id: 1\nevent: x\ndata: \n\n" {
		t.Fatalf("empty frame: %q", got)
	}
}

func TestPublishReachesEveryBrowser(t *testing.T) {
	h := newHub()
	srv := newTestServer(t, h, nil)
	a, b := connect(t, srv.URL, nil), connect(t, srv.URL, nil)
	for h.count() < 2 {
		time.Sleep(5 * time.Millisecond)
	}

	h.publish(hubMessage{event: "chaturbate__u-info", data: []byte("<b>hi</b>")})

	for name, c := range map[string]*sseReader{"a": a, "b": b} {
		if ev := c.nextEvent(2 * time.Second); !strings.Contains(ev, "event: chaturbate__u-info") || !strings.Contains(ev, "data: <b>hi</b>") {
			t.Fatalf("browser %s got %q", name, ev)
		}
	}
}

// The 30 s "heartbeat" used to end the response of every browser.
func TestKeepAliveDoesNotEndTheConnection(t *testing.T) {
	h := newHub()
	srv := newTestServer(t, h, nil)
	c := connect(t, srv.URL, nil)
	for h.count() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	for i := 0; i < 5; i++ {
		h.comment(hubKeepAliveText)
		if b := c.next(2 * time.Second); !strings.HasPrefix(b, ": keep-alive") && !strings.Contains(b, "retry:") {
			t.Fatalf("keep-alive %d: got %q (connection ended?)", i, b)
		}
	}
	h.publish(hubMessage{event: "chaturbate__u-info", data: []byte("after")})
	if ev := c.nextEvent(2 * time.Second); !strings.Contains(ev, "data: after") {
		t.Fatalf("event after the keep-alives was not delivered on the same connection: %q", ev)
	}
}

// One browser that stops reading must not block publishing nor starve the others.
func TestSlowBrowserIsDroppedAndOthersKeepReceiving(t *testing.T) {
	h := newHub()
	slow, _ := h.subscribe(nil) // never read
	good, _ := h.subscribe(nil)

	got := make(chan int, 1)
	go func() {
		n := 0
		for f := range good.ch {
			_ = f
			n++
			if n == 3000 {
				got <- n
				return
			}
		}
	}()

	start := time.Now()
	for i := 0; i < 3000; i++ {
		h.publish(hubMessage{event: "e", data: []byte("payload")})
		for len(good.ch) > hubMinQueue/2 { // let the healthy reader keep up
			time.Sleep(time.Millisecond)
		}
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("publishing took %v: it blocked on the slow browser", d)
	}
	select {
	case <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("the healthy browser did not receive every event")
	}
	select {
	case <-slow.dropped:
	default:
		t.Fatal("the slow browser was not dropped")
	}
	if h.count() != 1 {
		t.Fatalf("%d subscribers left, want only the healthy one", h.count())
	}
}

// Reconnections get the state of every channel first; the first connection of a
// page (which was just rendered) does not.
func TestSnapshotOnlyOnReconnect(t *testing.T) {
	h := newHub()
	snap := func() []hubMessage {
		return []hubMessage{{event: "chaturbate__a-info", data: []byte("A")}, {event: "chaturbate__b-info", data: []byte("B")}}
	}
	srv := newTestServer(t, h, snap)

	first := connect(t, srv.URL, nil)
	h.publish(hubMessage{event: "chaturbate__live-info", data: []byte("live")})
	if ev := first.nextEvent(2 * time.Second); !strings.Contains(ev, "data: live") {
		t.Fatalf("first connection must not receive a snapshot, got %q", ev)
	}

	for name, hdr := range map[string]map[string]string{
		"Last-Event-ID": {"Last-Event-ID": "5"},
	} {
		c := connect(t, srv.URL, hdr)
		a, b := c.nextEvent(2*time.Second), c.nextEvent(2*time.Second)
		if !strings.Contains(a, "data: A") || !strings.Contains(b, "data: B") {
			t.Fatalf("%s: reconnection did not start with the snapshot: %q %q", name, a, b)
		}
	}
	c := connect(t, srv.URL+"?resync=1", nil)
	if a := c.nextEvent(2 * time.Second); !strings.Contains(a, "data: A") {
		t.Fatalf("?resync=1 did not send the snapshot: %q", a)
	}
}

// A big dashboard must fit its own snapshot without being dropped at once.
func TestSnapshotLargerThanTheMinimumQueue(t *testing.T) {
	h := newHub()
	var snap []hubMessage
	for i := 0; i < 2000; i++ {
		snap = append(snap, hubMessage{event: "e", data: []byte("x")})
	}
	sub, _ := h.subscribe(snap)
	if len(sub.ch) != 2000 {
		t.Fatalf("queued %d snapshot events, want 2000", len(sub.ch))
	}
	h.publish(hubMessage{event: "live", data: []byte("y")})
	select {
	case <-sub.dropped:
		t.Fatal("a browser with a large snapshot was dropped by the first live event")
	default:
	}
}

// Events published while a browser is being served arrive after its snapshot.
func TestLiveEventsComeAfterTheSnapshot(t *testing.T) {
	h := newHub()
	sub, _ := h.subscribe([]hubMessage{{event: "e", data: []byte("snapshot")}})
	h.publish(hubMessage{event: "e", data: []byte("live")})
	first, second := string(<-sub.ch), string(<-sub.ch)
	if !strings.Contains(first, "snapshot") || !strings.Contains(second, "live") {
		t.Fatalf("order wrong: %q then %q", first, second)
	}
}
