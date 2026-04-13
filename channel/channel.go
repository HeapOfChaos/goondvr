package channel

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/server"
)

// Channel represents a channel instance.
type Channel struct {
	CancelFunc context.CancelFunc
	LogCh      chan string
	UpdateCh   chan bool
	ThumbCh    chan bool
	done       chan struct{}

	IsOnline            bool
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
	CFBlockCount        int

	logsMu sync.RWMutex
	Logs   []string

	fileMu                  sync.RWMutex
	finalizeMu              sync.Mutex
	finalizeWG              sync.WaitGroup
	finalizeCount           int
	monitorMu               sync.Mutex
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
		LogCh:            make(chan string),
		UpdateCh:         make(chan bool),
		ThumbCh:          make(chan bool),
		done:             make(chan struct{}),
		Config:           conf,
		CancelFunc:       func() {},
		RoomTitle:        conf.RoomTitle,
		Gender:           conf.Gender,
		SummaryCardImage: conf.SummaryCardImage,
		StreamedAt:       conf.StreamedAt,
	}
	go ch.Publisher()

	return ch
}

// Publisher listens for log messages and updates from the channel
// and publishes once received.
func (ch *Channel) Publisher() {
	for {
		select {
		case <-ch.done:
			return
		case v := <-ch.LogCh:
			// Append the log message to ch.Logs and keep only the last 100 rows
			ch.logsMu.Lock()
			ch.Logs = append(ch.Logs, v)
			if len(ch.Logs) > 100 {
				ch.Logs = ch.Logs[len(ch.Logs)-100:]
			}
			ch.logsMu.Unlock()
			server.Manager.Publish(entity.EventLog, ch.ExportInfo())

		case <-ch.UpdateCh:
			server.Manager.Publish(entity.EventUpdate, ch.ExportInfo())
		case <-ch.ThumbCh:
			server.Manager.Publish(entity.EventThumb, ch.ExportInfo())
		}
	}
}

// WithCancel creates a new context with a cancel function,
// then stores the cancel function in the channel's CancelFunc field.
//
// This is used to cancel the context when the channel is stopped or paused.
func (ch *Channel) WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, ch.CancelFunc = context.WithCancel(ctx)
	return ctx, ch.CancelFunc
}

// Info logs an informational message to both the browser log and stdout.
func (ch *Channel) Info(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	ch.sendLog(fmt.Sprintf("%s [INFO] %s", time.Now().Format("15:04"), msg))
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
	ch.sendLog(fmt.Sprintf("%s [ERROR] %s", time.Now().Format("15:04"), msg))
	log.Printf("ERROR [%s] %s", ch.Config.Username, msg)
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
	var streamedAt string
	if ch.StreamedAt != 0 {
		streamedAt = time.Unix(ch.StreamedAt, 0).Format("2006-01-02 15:04 AM")
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

	return &entity.ChannelInfo{
		ChannelID:        entity.ChannelID(ch.Config.Site, ch.Config.Username),
		IsOnline:         ch.IsOnline,
		IsPaused:         ch.Config.IsPaused,
		Username:         ch.Config.Username,
		MaxDuration:      internal.FormatDuration(float64(ch.Config.MaxDuration * 60)), // MaxDuration from config is in minutes
		MaxFilesize:      internal.FormatFilesize(ch.Config.MaxFilesize * 1024 * 1024), // MaxFilesize from config is in MB
		StreamedAt:       streamedAt,
		CreatedAt:        ch.Config.CreatedAt,
		Duration:         internal.FormatDuration(duration),
		Filesize:         internal.FormatFilesize(filesize),
		TotalDiskUsage:   internal.FormatFilesize(int(totalDiskUsageBytes)),
		Filename:         filename,
		Logs:             logs,
		GlobalConfig:     server.Config,
		RoomTitle:        ch.RoomTitle,
		Gender:           ch.Gender,
		NumViewers:       ch.NumViewers,
		EdgeRegion:       ch.EdgeRegion,
		SummaryCardImage: ch.SummaryCardImage,
		LiveThumbURL:     ch.LiveThumbURL,
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
	ch.monitorMu.Unlock()
	ch.CancelFunc()

	ch.Update()
	ch.Info("channel paused")
}

// Stop stops the channel and cancels the context.
func (ch *Channel) Stop() {
	// Stop the monitoring loop
	ch.monitorMu.Lock()
	ch.monitorRestartRequested = false
	ch.Config.IsPaused = true
	ch.monitorMu.Unlock()
	ch.CancelFunc()
	ch.waitForMonitorStop()
	ch.waitForFinalizations()

	ch.Info("channel stopped")
	ch.stopPublisher()
}

// Resume resumes the channel monitoring.
//
// `startSeq` is used to prevent all channels from starting at the same time, preventing TooManyRequests errors.
// It's only be used when program starting and trying to resume all channels at once.
func (ch *Channel) Resume(startSeq int) {
	go func() {
		<-time.After(time.Duration(startSeq) * time.Second)
		runID, ok := ch.requestMonitorStart()
		if !ok {
			return
		}
		ch.Update()
		ch.Info("channel resumed")
		ch.Monitor(runID)
	}()
}

// UpdateOnlineStatus updates the online status of the channel.
func (ch *Channel) UpdateOnlineStatus(isOnline bool) {
	ch.IsOnline = isOnline
	if !isOnline {
		ch.NumViewers = 0
	}
	ch.UpdateThumb()
	ch.Update()
}

// UpdateThumb sends a thumbnail-only update signal to the publisher.
func (ch *Channel) UpdateThumb() {
	select {
	case <-ch.done:
		return
	case ch.ThumbCh <- true:
	}
}

// requestMonitorStart starts a monitor immediately when possible, or records
// a pending restart if a previous monitor is still shutting down.
func (ch *Channel) requestMonitorStart() (uint64, bool) {
	ch.monitorMu.Lock()
	defer ch.monitorMu.Unlock()

	if ch.monitorRunning {
		if ch.Config.IsPaused {
			ch.monitorRestartRequested = true
		}
		return 0, false
	}

	return ch.startMonitorLocked(), true
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
		ch.Update()
		ch.Info("channel resumed")
		go ch.Monitor(runID)
	}
}

// startMonitorLocked marks a monitor as active and allocates a new run ID.
// monitorMu must already be held by the caller.
func (ch *Channel) startMonitorLocked() uint64 {
	ch.Config.IsPaused = false
	ch.monitorRunning = true
	ch.monitorRestartRequested = false
	ch.monitorDone = make(chan struct{})
	ch.monitorRunID++
	return ch.monitorRunID
}

// stopPublisher signals the publisher goroutine to exit. It is only used when
// a channel is being permanently stopped and removed, not for pause/resume.
func (ch *Channel) stopPublisher() {
	ch.doneOnce.Do(func() {
		close(ch.done)
	})
}

// sendLog forwards a log line to the publisher unless the channel has been
// permanently shut down.
func (ch *Channel) sendLog(msg string) {
	select {
	case <-ch.done:
		return
	case ch.LogCh <- msg:
	}
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
