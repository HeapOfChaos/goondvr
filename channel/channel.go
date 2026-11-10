package channel

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/server"
)

// uiMinInterval is the minimum time between two pushes of the same kind
// (info / thumb / log) of one channel to the web UI. Recording fires a signal
// on every segment (several per second with LL-HLS), and every push makes the
// browser rebuild part of the page, so bursts are coalesced and only the
// latest state is sent. (A variable only so tests can shorten it.)
var uiMinInterval = 2 * time.Second

// uiUrgentInterval is the minimum time between two pushes when the change is a
// state change the user is waiting to see (paused, resumed, online/offline,
// "channel resumed" in the log...). Those must not sit behind the progress
// cool-down above; this small gap only keeps a burst of them from flooding the
// browser. (A variable only so tests can shorten it.)
var uiUrgentInterval = 200 * time.Millisecond

// maxLogLines is the number of log lines kept per channel for the web UI.
const maxLogLines = 100

// Channel represents a channel instance.
type Channel struct {
	// LogCh, UpdateCh and ThumbCh are "something changed" signals for the
	// Publisher. They hold at most one pending signal and are written with a
	// non-blocking send (see signal), so neither the recording loop nor the
	// loggers can ever be stalled by a slow or frozen browser.
	LogCh    chan bool
	UpdateCh chan bool
	ThumbCh  chan bool
	done     chan struct{}
	// publisherExited is closed when the Publisher goroutine has returned.
	publisherExited chan struct{}
	// urgent[i] is set by a producer whose change to event kind i (index in
	// Publisher's events: info, thumb, log) is a state change rather than
	// progress; the Publisher then uses uiUrgentInterval instead of uiMinInterval.
	urgent [3]atomic.Bool

	IsOnline bool
	// Available is true while the channel is broadcasting in public, recorded
	// or not (see availability.go). Guarded by stateMu like IsOnline.
	Available bool
	// lastProbe is when the availability of a paused channel was last asked
	// for (unix nanoseconds, 0 = never).
	lastProbe           atomic.Int64
	StreamedAt          int64
	Duration            float64 // Seconds
	Filesize            int     // Bytes
	TotalDiskUsageBytes int64   // Total bytes across all recordings for this channel
	Sequence            int
	FileExt             string // ".ts" or ".mp4", set per-stream
	RoomTitle           string
	Gender              string
	NumViewers          int
	EdgeRegion          string
	SummaryCardImage    string
	LiveThumbURL        string
	// Renewal of LiveThumbURL on demand (see livethumb.go), guarded by liveThumbMu.
	liveThumbMu    sync.Mutex
	liveThumbAsked time.Time
	liveThumbFails int
	CFBlockCount   int
	// Automatic recording (see settings.go), guarded by stateMu.
	autoStarted  bool           // the presence engine resumed the channel; it is paused again once it is off the air
	autoOffSince time.Time      // since when it has been off the air after being auto-started
	autoWaiting  bool           // wants to start but is held back (low free disk space)
	autoHold     bool           // the user paused it while it was on the air: not started again until that broadcast is over
	scanWG       sync.WaitGroup // background scans of the recordings started by Apply
	det          detectState    // image detection (see detect.go)

	logsMu sync.RWMutex
	Logs   []string

	fileMu        sync.RWMutex
	finalizeMu    sync.Mutex
	finalizeWG    sync.WaitGroup
	finalizeCount int
	// stateMu guards the state that the recording goroutine updates while the
	// web UI and the config writer read it: IsOnline, StreamedAt, RoomTitle,
	// Gender, NumViewers, EdgeRegion, SummaryCardImage, LiveThumbURL and the
	// copies of RoomTitle/Gender/SummaryCardImage/StreamedAt kept in Config.
	// Config.IsPaused is guarded by monitorMu instead.
	//
	// Lock order: fileMu -> monitorMu -> stateMu. Nothing is called while
	// stateMu is held.
	stateMu                 sync.RWMutex
	monitorMu               sync.Mutex
	monitorCtx              context.Context    // context of the current monitor run (created by startMonitorLocked)
	monitorCancel           context.CancelFunc // cancels monitorCtx; both guarded by monitorMu
	monitorRunning          bool
	monitorRestartRequested bool
	monitorRunID            uint64
	monitorDone             chan struct{}
	doneOnce                sync.Once

	File           *os.File
	mp4InitSegment []byte
	Config         *entity.ChannelConfig
}

// New creates a new channel instance with the given manager and configuration.
func New(conf *entity.ChannelConfig) *Channel {
	ch := &Channel{
		LogCh:            make(chan bool, 1),
		UpdateCh:         make(chan bool, 1),
		ThumbCh:          make(chan bool, 1),
		done:             make(chan struct{}),
		publisherExited:  make(chan struct{}),
		Config:           conf,
		monitorCancel:    func() {},
		RoomTitle:        conf.RoomTitle,
		Gender:           conf.Gender,
		SummaryCardImage: conf.SummaryCardImage,
		StreamedAt:       conf.StreamedAt,
	}
	go ch.Publisher()

	return ch
}

// signal raises a coalescing "something changed" flag. It never blocks: if a
// signal is already pending the new one is merged into it.
func signal(c chan bool) {
	select {
	case c <- true:
	default:
	}
}

// Publisher turns the change signals of the channel into SSE events for the
// web UI.
//
// Each kind of event (info, thumb, log) is sent at most once per
// uiMinInterval. The first change after a quiet period is sent immediately;
// changes that arrive during the cool-down are merged and sent once it ends,
// always carrying the latest state. If the SSE queue is full (browser too slow
// to keep up) the push is retried later instead of blocking.
func (ch *Channel) Publisher() {
	defer close(ch.publisherExited)

	events := [...]entity.Event{entity.EventUpdate, entity.EventThumb, entity.EventLog}
	var (
		pending [len(events)]bool
		last    [len(events)]time.Time
		wake    <-chan time.Time
	)

	for {
		select {
		case <-ch.done:
			return
		case <-ch.UpdateCh:
			pending[0] = true
		case <-ch.ThumbCh:
			pending[1] = true
		case <-ch.LogCh:
			pending[2] = true
		case <-wake:
		}

		var (
			due  [len(events)]bool
			info *entity.ChannelInfo
			wait time.Duration = -1
			now                = time.Now()
		)
		for i := range events {
			if !pending[i] {
				continue
			}
			gap := uiMinInterval
			if ch.urgent[i].Load() {
				gap = uiUrgentInterval
			}
			if remaining := gap - now.Sub(last[i]); remaining > 0 {
				if wait < 0 || remaining < wait {
					wait = remaining
				}
				continue
			}
			// Clear the flag *before* the snapshot below is taken: a state change
			// made after this point raises it again together with its own signal,
			// and one made before it is already part of the snapshot.
			ch.urgent[i].Store(false)
			due[i] = true
		}
		for i, evt := range events {
			if !due[i] {
				continue
			}
			if info == nil {
				info = ch.ExportInfo()
			}
			// Even when the queue is full we restart the cool-down, so a stuck
			// browser is retried every uiMinInterval rather than in a hot loop.
			last[i] = now
			if server.Manager.Publish(evt, info) {
				pending[i] = false
			} else if wait < 0 || uiMinInterval < wait {
				wait = uiMinInterval
			}
		}

		wake = nil
		if wait >= 0 {
			wake = time.After(wait)
		}
	}
}

// Info logs an informational message to both the browser log and stdout.
func (ch *Channel) Info(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	ch.sendLogUrgent(fmt.Sprintf("%s [INFO] %s", time.Now().Format("15:04"), msg))
	log.Printf(" INFO [%s] %s", ch.Config.Username, msg)
}

// Verbose logs a message to the browser log always, and to stdout only when --debug is enabled.
// Use this for high-frequency events (e.g. per-segment updates) that would clutter the console.
func (ch *Channel) Verbose(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	ch.sendLog(fmt.Sprintf("%s [INFO] %s", time.Now().Format("15:04"), msg))
	if server.Config.Debug {
		log.Printf(" INFO [%s] %s", ch.Config.Username, msg)
	}
}

// Error logs an error message to both the browser log and stdout.
func (ch *Channel) Error(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	ch.sendLogUrgent(fmt.Sprintf("%s [ERROR] %s", time.Now().Format("15:04"), msg))
	log.Printf("ERROR [%s] %s", ch.Config.Username, msg)
}

// Sections of the list (ChannelInfo.Rank).
const (
	RankRecording = 0 // being recorded
	RankOnAir     = 1 // broadcasting, not being recorded
	RankRetrying  = 2 // off the air, not paused: the monitor keeps trying to record it
	RankPaused    = 3 // off the air and paused
)

// RankOf is the section of the list a channel belongs to.
func RankOf(online, onAir, paused bool) int {
	switch {
	case online:
		return RankRecording
	case onAir:
		return RankOnAir
	case !paused:
		return RankRetrying
	}
	return RankPaused
}

// ExportInfo exports the channel information as a ChannelInfo struct.
func (ch *Channel) ExportInfo() *entity.ChannelInfo {
	var filename string
	ch.fileMu.RLock()
	if ch.File != nil {
		filename = ch.File.Name()
	}
	duration := ch.Duration
	filesize := ch.Filesize
	totalDiskUsageBytes := ch.TotalDiskUsageBytes
	ch.fileMu.RUnlock()
	ch.stateMu.RLock()
	isOnline, roomTitle, gender, numViewers := ch.IsOnline, ch.RoomTitle, ch.Gender, ch.NumViewers
	available := ch.Available || ch.IsOnline
	edgeRegion, summaryCard, liveThumb := ch.EdgeRegion, ch.SummaryCardImage, ch.LiveThumbURL
	tags := ch.Config.Tags // replaced as a whole under stateMu, never edited in place
	set := ch.settingsLocked()
	autoWaiting := ch.autoWaiting && set.AutoRecord
	streamedAtUnix := ch.StreamedAt
	ch.stateMu.RUnlock()
	var streamedAt string
	if streamedAtUnix != 0 {
		streamedAt = time.Unix(streamedAtUnix, 0).Format("2006-01-02 15:04 AM")
	}
	ch.logsMu.RLock()
	logs := make([]string, len(ch.Logs))
	copy(logs, ch.Logs)
	ch.logsMu.RUnlock()

	siteDomain := server.Config.Domain // default to Chaturbate domain
	if ch.Config.Site == "stripchat" {
		siteDomain = "https://stripchat.com/"
	}

	site := ch.Config.Site
	if site == "" {
		site = "chaturbate"
	}

	paused := ch.isPaused() // Pause/Resume write it under monitorMu
	rank := RankOf(isOnline, available, paused)

	return &entity.ChannelInfo{
		ChannelID:        entity.ChannelID(ch.Config.Site, ch.Config.Username),
		IsOnline:         isOnline,
		IsAvailable:      available,
		Rank:             rank,
		SortKey:          strings.ToLower(ch.Config.Username) + " " + strings.ToLower(site),
		IsPaused:         paused,
		Username:         ch.Config.Username,
		MaxDuration:      internal.FormatDuration(float64(set.MaxDuration * 60)), // MaxDuration from config is in minutes
		MaxFilesize:      internal.FormatFilesize(set.MaxFilesize * 1024 * 1024), // MaxFilesize from config is in MB
		StreamedAt:       streamedAt,
		CreatedAt:        ch.Config.CreatedAt,
		Duration:         internal.FormatDuration(duration),
		Filesize:         internal.FormatFilesize(filesize),
		TotalDiskUsage:   internal.FormatFilesize(int(totalDiskUsageBytes)),
		Filename:         filename,
		Logs:             logs,
		GlobalConfig:     server.Config,
		RoomTitle:        roomTitle,
		Gender:           gender,
		NumViewers:       numViewers,
		EdgeRegion:       edgeRegion,
		SummaryCardImage: summaryCard,
		LiveThumbURL:     liveThumb,
		Tags:             tags,
		Settings:         set,
		Detect:           ch.detectInfo(set.Detect),
		AutoWaiting:      autoWaiting,
		Site:             site,
		SiteDomain:       siteDomain,
	}
}

// Pause pauses the channel and cancels the context.
func (ch *Channel) Pause() {
	// Stop the monitoring loop, this also updates `ch.IsOnline` to false
	// `context.Canceled` → `ch.Monitor()` → `onRetry` → `ch.UpdateOnlineStatus(false)`.
	ch.monitorMu.Lock()
	ch.monitorRestartRequested = false
	ch.Config.IsPaused = true
	cancel := ch.monitorCancel
	ch.monitorMu.Unlock()
	cancel()

	ch.UpdateNow()
	ch.Info("channel paused")
}

// Stop stops the channel and cancels the context.
func (ch *Channel) Stop() {
	// Stop the monitoring loop
	ch.monitorMu.Lock()
	ch.monitorRestartRequested = false
	ch.Config.IsPaused = true
	cancel := ch.monitorCancel
	ch.monitorMu.Unlock()
	cancel()
	ch.waitForMonitorStop()
	ch.waitForFinalizations()

	ch.Info("channel stopped")
	ch.stopPublisher()
}

// WaitPublisherExit blocks until the goroutine that pushes the channel to the
// web UI has returned (after Stop). Tests use it before restoring the globals
// that goroutine reads.
func (ch *Channel) WaitPublisherExit() { <-ch.publisherExited }

// Resume resumes the channel monitoring.
//
// `startSeq` is used to prevent all channels from starting at the same time, preventing TooManyRequests errors.
// It's only be used when program starting and trying to resume all channels at once.
func (ch *Channel) Resume(startSeq int) {
	go func() {
		if shuttingDown() {
			return // the program is closing: nothing new starts
		}
		if startSeq > 0 {
			select {
			case <-time.After(time.Duration(startSeq) * time.Second):
			case <-ch.done: // channel was removed while waiting for its turn
				return
			}
			// The user may have paused the channel while it was waiting for its
			// turn; starting it now would silently undo that.
			if ch.isPaused() {
				return
			}
		}
		runID, started, queued := ch.requestMonitorStart()
		if queued {
			// The previous run is still shutting down; the new one starts the
			// moment it is done. Say so instead of looking like the click was lost.
			ch.Info("resume requested, starting as soon as the previous run has stopped")
			return
		}
		if !started {
			return
		}
		ch.UpdateNow()
		ch.Info("channel resumed")
		ch.Monitor(runID)
	}()
}

// UpdateOnlineStatus updates the online status of the channel.
func (ch *Channel) UpdateOnlineStatus(isOnline bool) {
	ch.stateMu.Lock()
	ch.IsOnline = isOnline
	if isOnline {
		ch.Available = true // it is being recorded, so it is on air
	} else {
		ch.NumViewers = 0
	}
	ch.stateMu.Unlock()
	ch.UpdateThumb()
	ch.UpdateNow()
}

// Update tells the publisher that the progress of the channel changed (a new
// segment was written). Bursts are merged and sent at most once per
// uiMinInterval. Use UpdateNow for state changes.
func (ch *Channel) Update() {
	signal(ch.UpdateCh)
}

// UpdateNow tells the publisher that the state of the channel changed (paused,
// resumed, online/offline, new metadata). It is sent without waiting for the
// progress cool-down.
func (ch *Channel) UpdateNow() {
	ch.urgent[0].Store(true)
	signal(ch.UpdateCh)
}

// UpdateThumb sends a thumbnail-only update signal to the publisher. Thumbnails
// only change together with the state of the channel, so it is never held back.
func (ch *Channel) UpdateThumb() {
	ch.urgent[1].Store(true)
	signal(ch.ThumbCh)
}

// requestMonitorStart starts a monitor immediately when possible. If a
// previous monitor of a paused channel is still shutting down it records a
// pending restart instead and reports queued=true.
func (ch *Channel) requestMonitorStart() (runID uint64, started, queued bool) {
	ch.monitorMu.Lock()
	defer ch.monitorMu.Unlock()

	if ch.monitorRunning {
		if ch.Config.IsPaused {
			ch.monitorRestartRequested = true
			return 0, false, true
		}
		return 0, false, false
	}

	return ch.startMonitorLocked(), true, false
}

// finishMonitor clears the running flag when a monitor loop exits.
func (ch *Channel) finishMonitor() {
	ch.monitorMu.Lock()
	done := ch.monitorDone
	shouldRestart := ch.monitorRestartRequested
	ch.monitorRunning = false
	ch.monitorRestartRequested = false
	var runID uint64
	if shouldRestart {
		runID = ch.startMonitorLocked()
	} else {
		ch.monitorDone = nil
	}
	ch.monitorMu.Unlock()

	if done != nil {
		close(done)
	}

	if shouldRestart {
		ch.UpdateNow()
		ch.Info("channel resumed")
		go ch.Monitor(runID)
	}
}

// startMonitorLocked marks a monitor as active and allocates a new run ID.
// monitorMu must already be held by the caller.
func (ch *Channel) startMonitorLocked() uint64 {
	// A fresh context for every run, created here (not later inside Monitor) so
	// that a Pause arriving at any moment after the start cancels *this* run.
	ch.monitorCancel() // release the previous run's context
	ch.monitorCtx, ch.monitorCancel = context.WithCancel(context.Background())
	ch.Config.IsPaused = false
	ch.monitorRunning = true
	ch.monitorRestartRequested = false
	ch.monitorDone = make(chan struct{})
	ch.monitorRunID++
	return ch.monitorRunID
}

// isPaused reports whether the channel is currently paused.
func (ch *Channel) isPaused() bool {
	ch.monitorMu.Lock()
	defer ch.monitorMu.Unlock()
	return ch.Config.IsPaused
}

// runContext returns the context of the current monitor run.
func (ch *Channel) runContext() context.Context {
	ch.monitorMu.Lock()
	defer ch.monitorMu.Unlock()
	return ch.monitorCtx
}

// stopPublisher signals the publisher goroutine to exit. It is only used when
// a channel is being permanently stopped and removed, not for pause/resume.
func (ch *Channel) stopPublisher() {
	ch.doneOnce.Do(func() {
		close(ch.done)
	})
}

// sendLog stores a log line (keeping the last maxLogLines) and tells the
// publisher that the log changed, unless the channel has been permanently shut
// down. It never blocks on the web UI. Used for high-frequency progress lines.
func (ch *Channel) sendLog(msg string) {
	if ch.storeLog(msg) {
		signal(ch.LogCh)
	}
}

// sendLogUrgent is sendLog for lines the user is waiting to read ("channel
// resumed", errors...): they are not held back by the progress cool-down.
func (ch *Channel) sendLogUrgent(msg string) {
	if ch.storeLog(msg) {
		ch.urgent[2].Store(true)
		signal(ch.LogCh)
	}
}

// storeLog appends msg to the log kept for the web UI; false if the channel
// has been permanently shut down.
func (ch *Channel) storeLog(msg string) bool {
	select {
	case <-ch.done:
		return false
	default:
	}

	ch.logsMu.Lock()
	ch.Logs = append(ch.Logs, msg)
	if len(ch.Logs) > maxLogLines {
		ch.Logs = ch.Logs[len(ch.Logs)-maxLogLines:]
	}
	ch.logsMu.Unlock()
	return true
}

// waitForMonitorStop blocks until the current monitor run has finished cleanup.
func (ch *Channel) waitForMonitorStop() {
	ch.monitorMu.Lock()
	done := ch.monitorDone
	ch.monitorMu.Unlock()

	if done != nil {
		<-done
	}
}

func (ch *Channel) startFinalization() {
	ch.finalizeMu.Lock()
	ch.finalizeCount++
	ch.finalizeWG.Add(1)
	ch.finalizeMu.Unlock()
}

func (ch *Channel) finishFinalization() {
	ch.finalizeMu.Lock()
	ch.finalizeCount--
	ch.finalizeMu.Unlock()
	ch.finalizeWG.Done()
}

func (ch *Channel) waitForFinalizations() int {
	ch.finalizeMu.Lock()
	pending := ch.finalizeCount
	ch.finalizeMu.Unlock()

	if pending > 0 {
		ch.Info("waiting for %d recording finalization task(s)", pending)
		ch.finalizeWG.Wait()
	}
	return pending
}

// Online reports whether the channel is online (recording).
func (ch *Channel) Online() bool {
	ch.stateMu.RLock()
	defer ch.stateMu.RUnlock()
	return ch.IsOnline
}

// GetSummaryCardImage returns the address of the channel's profile picture.
func (ch *Channel) GetSummaryCardImage() string {
	ch.stateMu.RLock()
	defer ch.stateMu.RUnlock()
	return ch.SummaryCardImage
}

// Paused reports whether the channel is paused.
func (ch *Channel) Paused() bool { return ch.isPaused() }

// SetPaused sets the paused flag without starting or stopping anything. Shutdown
// uses it to save the state the user had chosen, after Stop marked every channel paused.
func (ch *Channel) SetPaused(v bool) {
	ch.monitorMu.Lock()
	ch.Config.IsPaused = v
	ch.monitorMu.Unlock()
}

// ConfigSnapshot returns a consistent copy of the channel's configuration, for
// saving it while the recording goroutine keeps updating it.
func (ch *Channel) ConfigSnapshot() entity.ChannelConfig {
	ch.monitorMu.Lock()
	defer ch.monitorMu.Unlock()
	ch.stateMu.RLock()
	defer ch.stateMu.RUnlock()
	snap := *ch.Config
	if ch.autoStarted {
		snap.IsPaused = true // what the user chose: armed and waiting (see settings.go)
	}
	return snap
}
