package manager

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// A small Server-Sent Events hub for the dashboard.
//
// It replaces github.com/r3labs/sse, which has two properties that froze the
// web UI of busy installations:
//
//   - its stream loop sends to every browser with a blocking send, so ONE tab
//     that stops reading (frozen, throttled, network stalled) blocks the loop
//     and no other tab receives anything any more;
//   - an event without data ends the response of every connected browser, which
//     is what the 30 s "heartbeat" used to do (events published while the
//     browser was reconnecting were lost).
//
// Here every browser has its own queue. Publishing never blocks: a browser
// whose queue overflows is disconnected (it reconnects by itself, see below)
// and nobody else is affected.
//
// Every event is a complete snapshot of one card, so nothing is lost by
// dropping a slow browser as long as it receives the current state when it
// comes back: a connection that arrives with a Last-Event-ID header (automatic
// EventSource reconnect) or ?resync=1 (the page's own reconnect) is sent the
// state of every channel first. The very first connection of a page does not
// need that, the page was just rendered.

const (
	hubMinQueue      = 512              // events a browser may be behind before it is dropped
	hubWriteTimeout  = 15 * time.Second // a single write to a browser may take this long
	hubRetryMillis   = 2000             // EventSource reconnect delay announced to browsers
	hubKeepAliveText = "keep-alive"
)

// heartbeatInterval is how often a comment is sent to every browser so that
// proxies keep the connection open and a dead connection is noticed.
// (A variable only so tests can shorten it.)
var heartbeatInterval = 15 * time.Second

// hubMessage is one named event, as sent to browsers.
type hubMessage struct {
	event string
	data  []byte
}

type hubSub struct {
	ch      chan []byte   // wire frames waiting to be written
	dropped chan struct{} // closed when the subscriber was removed for being too slow
}

type hub struct {
	mu     sync.Mutex
	subs   map[*hubSub]struct{}
	lastID uint64
}

func newHub() *hub {
	return &hub{subs: make(map[*hubSub]struct{})}
}

// frame renders one event in the text/event-stream format.
func frame(id uint64, m hubMessage) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "id: %d\n", id)
	if m.event != "" {
		name := strings.NewReplacer("\r", "", "\n", "").Replace(m.event)
		fmt.Fprintf(&b, "event: %s\n", name)
	}
	text := strings.NewReplacer("\r\n", "\n", "\r", "\n").Replace(string(m.data))
	for _, line := range strings.Split(text, "\n") {
		b.WriteString("data: ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.Bytes()
}

// publish sends an event to every connected browser. It never blocks.
func (h *hub) publish(m hubMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastID++
	f := frame(h.lastID, m)
	for sub := range h.subs {
		select {
		case sub.ch <- f:
		default:
			h.dropLocked(sub) // too slow: it will reconnect and get a fresh snapshot
		}
	}
}

// comment sends an SSE comment line (ignored by EventSource) to every browser.
func (h *hub) comment(text string) {
	f := []byte(": " + text + "\n\n")

	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range h.subs {
		select {
		case sub.ch <- f:
		default:
			h.dropLocked(sub)
		}
	}
}

func (h *hub) dropLocked(sub *hubSub) {
	if _, ok := h.subs[sub]; ok {
		delete(h.subs, sub)
		close(sub.dropped)
	}
}

// subscribe registers a browser. The snapshot (may be nil) is queued before any
// live event, atomically with the registration, so the browser sees the state
// first and every later change after it.
func (h *hub) subscribe(snapshot []hubMessage) (*hubSub, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	size := hubMinQueue
	if n := 2*len(snapshot) + hubMinQueue/2; n > size {
		size = n // a big dashboard must fit its own snapshot
	}
	sub := &hubSub{ch: make(chan []byte, size), dropped: make(chan struct{})}
	for _, m := range snapshot {
		sub.ch <- frame(h.lastID, m)
	}
	h.subs[sub] = struct{}{}
	return sub, h.lastID
}

func (h *hub) unsubscribe(sub *hubSub) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dropLocked(sub)
}

// count returns the number of connected browsers.
func (h *hub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// serve handles one browser connection until it goes away or is dropped.
// snapshot is called only for reconnections (see the comment at the top).
func (h *hub) serve(w http.ResponseWriter, r *http.Request, snapshot func() []hubMessage) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	hd := w.Header()
	hd.Set("Content-Type", "text/event-stream")
	hd.Set("Cache-Control", "no-cache")
	hd.Set("Connection", "keep-alive")
	hd.Set("X-Accel-Buffering", "no")

	var snap []hubMessage
	if snapshot != nil && (r.Header.Get("Last-Event-ID") != "" || r.URL.Query().Get("resync") == "1") {
		snap = snapshot()
	}
	sub, id := h.subscribe(snap)
	defer h.unsubscribe(sub)

	rc := http.NewResponseController(w)
	write := func(b []byte) error {
		_ = rc.SetWriteDeadline(time.Now().Add(hubWriteTimeout)) // not supported: no deadline, still correct
		_, err := w.Write(b)
		return err
	}

	// Announce the reconnect delay and the current event id (so that even a
	// browser that never receives an event reconnects with a Last-Event-ID).
	w.WriteHeader(http.StatusOK)
	if err := write([]byte(fmt.Sprintf("retry: %d\nid: %d\n\n", hubRetryMillis, id))); err != nil {
		return
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-sub.dropped:
			return
		case b := <-sub.ch:
			if write(b) != nil {
				return
			}
			// Write everything that is already waiting, then flush once.
		drain:
			for {
				select {
				case b = <-sub.ch:
					if write(b) != nil {
						return
					}
				default:
					break drain
				}
			}
			flusher.Flush()
		}
	}
}
