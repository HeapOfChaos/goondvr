package channel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HeapOfChaos/goondvr/detect"
	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/site"
)

// Image detection of a channel (settings: entity.DetectConfig; the engine that drives it is in
// manager/detect.go). Pictures of the channel go to the detector; what it finds
//   - is kept as stretches of the recording (<file>.detect.json), from which clips are cut when the file is finished;
//   - with "gate", holds the recording back: the channel is recorded only while pictures match (it uses the
//     automatic-recording machinery of settings.go).
// A channel that does not use detection costs nothing: the engine skips it after one read-locked check.

// detectState is guarded by its own mutex, the innermost lock of the channel: nothing else is locked
// while it is held (it may be taken while fileMu or stateMu is).
type detectState struct {
	mu       sync.Mutex
	wantSegs atomic.Bool // keep the newest segments of the stream (source "stream"): read per segment without a lock

	busy      bool // a picture of this channel is being checked
	nextAt    time.Time
	fails     int
	err       string
	matching  bool // the last picture matched
	label     string
	lastMatch time.Time

	spans    []detect.Span // matches in the file being written
	open     bool          // the last span is still growing
	segs     [][]byte      // newest segments of the stream, never modified once stored
	segBytes int
}

const (
	detectMaxSegs     = 8
	detectMaxSegBytes = 6 << 20
	detectMaxBackoff  = 5 * time.Minute
	maxPicture        = 4 << 20
)

var (
	// ErrNoFrameYet: the recording has not produced anything to look at yet (not an error to report).
	ErrNoFrameYet = errors.New("no recorded segment to look at yet")
	mediaReq      = sync.OnceValue(internal.NewMediaReq)
	mediaReqSC    = sync.OnceValue(func() *internal.Req { return internal.NewMediaReqWithReferer("https://stripchat.com/") })
)

// detectCfgLocked is the detection setting of the channel with the defaults filled in. Caller holds stateMu.
func (ch *Channel) detectCfgLocked() entity.DetectConfig {
	var d entity.DetectConfig
	if ch.Config.Detect != nil {
		d = *ch.Config.Detect
	}
	return d.WithDefaults()
}

// DetectConfig returns the detection settings of the channel (Enabled says whether it is used).
func (ch *Channel) DetectConfig() entity.DetectConfig {
	ch.stateMu.RLock()
	defer ch.stateMu.RUnlock()
	return ch.detectCfgLocked()
}

// DetectActive reports whether the channel uses detection (the engine skips the others).
func (ch *Channel) DetectActive() bool {
	ch.stateMu.RLock()
	defer ch.stateMu.RUnlock()
	return ch.Config.Detect != nil && ch.Config.Detect.Enabled
}

// detectGateLocked: detection holds the recording back. Caller holds stateMu.
func (ch *Channel) detectGateLocked() bool {
	return ch.Config.Detect != nil && ch.Config.Detect.Enabled && ch.Config.Detect.Gate
}

// gateOpen reports whether a match was seen in the last idle seconds.
func (ch *Channel) gateOpen(now time.Time, idle int) bool {
	ch.det.mu.Lock()
	defer ch.det.mu.Unlock()
	return !ch.det.lastMatch.IsZero() && now.Sub(ch.det.lastMatch) < time.Duration(idle)*time.Second
}

// DetectDue says whether the channel needs a picture checked now, and marks it busy if so (the caller then
// must call DetectDone or DetectRelease). It is not due when it is off the air, when it is not being recorded
// but the source is the recording, or when its time has not come.
func (ch *Channel) DetectDue(now time.Time) (entity.DetectConfig, bool) {
	ch.stateMu.RLock()
	cfg := ch.detectCfgLocked()
	enabled := ch.Config.Detect != nil && ch.Config.Detect.Enabled
	onAir, recording := ch.Available || ch.IsOnline, ch.IsOnline
	ch.stateMu.RUnlock()
	if !enabled || cfg.Model == "" {
		return cfg, false
	}
	if cfg.Source == "stream" && !recording || cfg.Source != "stream" && !onAir {
		return cfg, false
	}
	ch.det.mu.Lock()
	defer ch.det.mu.Unlock()
	if ch.det.busy || now.Before(ch.det.nextAt) {
		return cfg, false
	}
	ch.det.busy = true
	return cfg, true
}

// DetectRelease gives back a channel that DetectDue returned but that was not checked.
func (ch *Channel) DetectRelease() {
	ch.det.mu.Lock()
	ch.det.busy = false
	ch.det.nextAt = time.Now().Add(time.Second)
	ch.det.mu.Unlock()
}

// DetectFrame gets the picture to check: the preview picture of the channel, or a frame of what is being recorded.
func (ch *Channel) DetectFrame(ctx context.Context, cfg entity.DetectConfig) ([]byte, error) {
	if cfg.Source == "stream" {
		ch.fileMu.RLock()
		ext, init := ch.FileExt, append([]byte(nil), ch.mp4InitSegment...)
		ch.fileMu.RUnlock()
		parts := ch.detectSegments()
		if len(parts) == 0 {
			return nil, ErrNoFrameYet
		}
		if ext == ".mp4" {
			if len(init) == 0 {
				return nil, ErrNoFrameYet
			}
			parts = append([][]byte{init}, parts...)
		}
		return detect.FrameFromSegments(ctx, parts...)
	}

	url := "https://thumb.live.mmcdn.com/riw/" + ch.Config.Username + ".jpg"
	req := mediaReq()
	if _, ok := resolveSite(ch.Config.Site).(site.LiveThumbSource); ok {
		if url = ch.FreshLiveThumbURL(ctx); url == "" {
			return nil, errors.New("the site has not given a preview picture yet")
		}
		req = mediaReqSC()
	}
	b, err := req.GetBytes(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("preview picture: %w", err)
	}
	if len(b) > maxPicture || !strings.HasPrefix(http.DetectContentType(b), "image/") {
		return nil, errors.New("the preview address did not give a picture")
	}
	return b, nil
}

func backoff(interval, fails int) time.Duration {
	d := time.Duration(interval) * time.Second
	for i := 0; i < fails && d < detectMaxBackoff; i++ {
		d *= 2
	}
	if d > detectMaxBackoff {
		d = detectMaxBackoff
	}
	return d
}

// DetectDone records what the detector said about a picture (err != nil: it could not be checked). It
// reports whether what the card shows changed.
func (ch *Channel) DetectDone(cfg entity.DetectConfig, res *detect.Result, err error) (changed bool) {
	ch.fileMu.RLock()
	dur, hasFile := ch.Duration, ch.File != nil
	ch.fileMu.RUnlock()
	now := time.Now()

	ch.det.mu.Lock()
	defer ch.det.mu.Unlock()
	d := &ch.det
	d.busy = false
	if err != nil {
		d.fails++
		d.nextAt = now.Add(backoff(cfg.Interval, d.fails))
		changed = d.err != err.Error()
		d.err = err.Error()
		return changed
	}
	d.fails = 0
	// a little jitter keeps channels that were switched on together from asking in the same second for ever
	d.nextAt = now.Add(time.Duration(cfg.Interval)*time.Second + time.Duration(rand.Int63n(int64(cfg.Interval)*int64(time.Second)/5+1)))
	label, score, ok := res.Best(cfg.Labels, cfg.MinScore)
	changed = d.err != "" || d.matching != ok || (ok && d.label != label)
	d.err, d.matching, d.label = "", ok, label
	if ok {
		d.lastMatch = now
		if hasFile {
			if n := len(d.spans); d.open && n > 0 {
				s := &d.spans[n-1]
				s.End = dur
				if score > s.Score {
					s.Score, s.Label = score, label
				}
			} else {
				d.spans = append(d.spans, detect.Span{Start: dur, End: dur, Label: label, Score: score})
				d.open = true
			}
		}
	} else {
		d.open = false
	}
	return changed
}

// detectInfo is what the card shows about the detection.
func (ch *Channel) detectInfo(cfg entity.DetectConfig) entity.DetectInfo {
	if !cfg.Enabled {
		return entity.DetectInfo{}
	}
	ch.det.mu.Lock()
	defer ch.det.mu.Unlock()
	return entity.DetectInfo{On: true, Matching: ch.det.matching, Label: ch.det.label, Error: ch.det.err}
}

// detectReset forgets what was found (the settings changed: it no longer means anything).
func (ch *Channel) detectReset(wantSegs bool) {
	ch.det.mu.Lock()
	d := &ch.det
	d.nextAt, d.fails, d.err, d.matching, d.label, d.lastMatch, d.open = time.Time{}, 0, "", false, "", time.Time{}, false
	if !wantSegs {
		d.segs, d.segBytes = nil, 0
	}
	ch.det.mu.Unlock()
	ch.det.wantSegs.Store(wantSegs)
}

// detectKeep remembers a segment that was just recorded, if the channel's detection looks at the recording.
func (ch *Channel) detectKeep(b []byte) {
	if !ch.det.wantSegs.Load() {
		return
	}
	cp := append([]byte(nil), b...)
	ch.det.mu.Lock()
	d := &ch.det
	d.segs = append(d.segs, cp)
	d.segBytes += len(cp)
	for len(d.segs) > detectMaxSegs || (d.segBytes > detectMaxSegBytes && len(d.segs) > 1) {
		d.segBytes -= len(d.segs[0])
		d.segs = d.segs[1:]
	}
	ch.det.mu.Unlock()
}

func (ch *Channel) detectSegments() [][]byte {
	ch.det.mu.Lock()
	defer ch.det.mu.Unlock()
	return append([][]byte(nil), ch.det.segs...)
}

func (ch *Channel) detectClearSegments() {
	ch.det.mu.Lock()
	ch.det.segs, ch.det.segBytes = nil, 0
	ch.det.mu.Unlock()
}

// detectFileOpened starts the list of matches of a new recording file (fileMu is held).
func (ch *Channel) detectFileOpened() {
	ch.det.mu.Lock()
	d := &ch.det
	d.spans, d.open = nil, false
	if d.matching { // the match goes on into the new file
		d.spans, d.open = []detect.Span{{Label: d.label}}, true
	}
	ch.det.mu.Unlock()
}

// detectFileClosed saves the matches of the file that was just closed next to it (fileMu is held).
func (ch *Channel) detectFileClosed(filename string) {
	ch.det.mu.Lock()
	spans := ch.det.spans
	ch.det.spans, ch.det.open = nil, false
	ch.det.mu.Unlock()
	if len(spans) == 0 {
		return
	}
	cfg := ch.DetectConfig()
	if err := detect.WriteSidecar(filename, detect.Sidecar{Pre: cfg.Pre, Post: cfg.Post, Clips: cfg.Clips, Spans: spans}); err != nil {
		log.Printf("WARN  detection marks of `%s`: %v", filename, err)
	}
}

// cutClips makes the clips that the matches of a finished recording ask for, and keeps the list of matches
// next to the recording. original is the file as it was recorded (the matches are saved next to it), final is
// where the finished recording is now.
func (ch *Channel) cutClips(ctx context.Context, original, final string) {
	side := detect.SidecarPath(original)
	sc, err := detect.ReadSidecar(side)
	if err != nil {
		return // no matches in this recording
	}
	if sc.Clips {
		made, err := detect.CutClips(ctx, final, detect.Windows(sc.Spans, sc.Pre, sc.Post))
		if err != nil {
			ch.Error("clips of `%s`: %s", final, err)
		}
		if len(made) > 0 {
			ch.Info("%d clip(s) cut from `%s`", len(made), final)
		}
	}
	if dst := detect.SidecarPath(final); dst != side {
		if err := os.Rename(side, dst); err != nil { // the list of matches follows the recording
			if err = copyFile(side, dst); err == nil {
				_ = os.Remove(side)
			} else {
				ch.Error("move the detection marks of `%s`: %s", final, err)
			}
		}
	}
}
