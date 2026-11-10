package channel

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/HeapOfChaos/goondvr/chaturbate"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/notifier"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/site"
	"github.com/HeapOfChaos/goondvr/stripchat"
	"github.com/avast/retry-go/v4"
)

// resolveSite returns the site.Site implementation for the given site name.
// An empty or unrecognised name defaults to Chaturbate.
func resolveSite(siteName string) site.Site {
	switch siteName {
	case "stripchat":
		return stripchat.New()
	default:
		return chaturbate.New()
	}
}

// ResolveSite returns the site.Site implementation for the given site name.
func ResolveSite(siteName string) site.Site { return resolveSite(siteName) }

// Monitor starts monitoring the channel for live streams and records them.
func (ch *Channel) Monitor(runID uint64) {
	defer ch.finishMonitor()

	// Context of this run, created when the run was started: Pause/Stop cancel
	// it, and every network call below honours it.
	ctx := ch.runContext()

	s := resolveSite(ch.Config.Site)
	req := internal.NewReq()
	ch.Info("starting to record `%s`", ch.Config.Username)

	// Seed total disk usage in the background so the UI shows it immediately.
	go ch.ScanTotalDiskUsage()

	// Seed StreamedAt from the site API if we haven't seen this channel stream yet.
	ch.stateMu.RLock()
	needStreamedAt := ch.StreamedAt == 0
	ch.stateMu.RUnlock()
	if needStreamedAt {
		if ts, err := s.FetchLastBroadcast(ctx, req, ch.Config.Username); err == nil && ts > 0 {
			ch.stateMu.Lock()
			ch.StreamedAt = ts
			ch.Config.StreamedAt = ts
			ch.stateMu.Unlock()
			_ = server.Manager.SaveConfig()
			ch.UpdateNow()
		}
	}

	var err error
	for {
		if err = ctx.Err(); err != nil {
			break
		}

		pipeline := func() error {
			return ch.RecordStream(ctx, runID, s, req)
		}
		// isExpectedOffline returns true for errors where the full interval delay is appropriate.
		// Transient errors (502, decode errors, network hiccups) should retry quickly.
		isExpectedOffline := func(err error) bool {
			return errors.Is(err, internal.ErrChannelOffline) ||
				errors.Is(err, internal.ErrPrivateStream) ||
				errors.Is(err, internal.ErrHiddenStream) ||
				errors.Is(err, internal.ErrAgeVerification) ||
				errors.Is(err, internal.ErrCloudflareBlocked) ||
				errors.Is(err, internal.ErrRoomPasswordRequired)
		}
		onRetry := func(_ uint, err error) {
			// The site said the channel is not broadcasting in public (a hidden or
			// private show cannot be recorded either): it is no longer "available".
			// Cancellation (pause) and transient errors say nothing about it.
			if errors.Is(err, internal.ErrChannelOffline) ||
				errors.Is(err, internal.ErrPrivateStream) ||
				errors.Is(err, internal.ErrHiddenStream) {
				ch.setAvailable(false)
			}
			ch.UpdateOnlineStatus(false)

			// Reset CF block count whenever a non-CF response is received.
			if !errors.Is(err, internal.ErrCloudflareBlocked) && ch.CFBlockCount > 0 {
				ch.CFBlockCount = 0
				server.Manager.ResetCFBlock(ch.Config.Username)
				notifier.Default.ResetCooldown(fmt.Sprintf(notifier.KeyCFChannel, ch.Config.Username))
			}

			if errors.Is(err, internal.ErrChannelOffline) {
				ch.Info("channel is offline, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrPrivateStream) {
				ch.Info("channel is in a private show, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrHiddenStream) {
				ch.Info("channel is hidden, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrCloudflareBlocked) {
				ch.CFBlockCount++
				cfThresh := server.Config.CFChannelThreshold
				if cfThresh <= 0 {
					cfThresh = 5
				}
				if ch.CFBlockCount >= cfThresh {
					notifier.Notify(
						fmt.Sprintf(notifier.KeyCFChannel, ch.Config.Username),
						"⚠️ Cloudflare Block",
						fmt.Sprintf("`%s` has been blocked by Cloudflare %d times consecutively", ch.Config.Username, ch.CFBlockCount),
					)
				}
				server.Manager.ReportCFBlock(ch.Config.Username)
				ch.Info("channel was blocked by Cloudflare; try with `-cookies` and `-user-agent`? try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrAgeVerification) {
				ch.Info("age verification required; pass cookies with `-cookies` to authenticate, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrRoomPasswordRequired) {
				ch.Info("room requires a password, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, context.Canceled) {
				// ...
			} else {
				ch.Error("on retry: %s: retrying in 10s", err.Error())
			}
		}
		delayFn := func(_ uint, err error, _ *retry.Config) time.Duration {
			if isExpectedOffline(err) {
				base := time.Duration(server.Config.Interval) * time.Minute
				jitter := time.Duration(rand.Int63n(int64(base/5))) - base/10 // ±10% of interval
				return base + jitter
			}
			// Transient error (502, decode failure, network hiccup) - recover quickly
			return 10 * time.Second
		}
		if err = retry.Do(
			pipeline,
			retry.Context(ctx),
			retry.Attempts(0),
			retry.DelayType(delayFn),
			retry.OnRetry(onRetry),
		); err != nil {
			break
		}
	}

	// Always cleanup when monitor exits, regardless of error
	if err := ch.Cleanup(); err != nil {
		ch.Error("cleanup on monitor exit: %s", err.Error())
	}

	// Log error if it's not a context cancellation
	if err != nil && !errors.Is(err, context.Canceled) {
		ch.Error("record stream: %s", err.Error())
	}
}

// RecordStream records the stream of the channel using the provided site and HTTP client.
// It retrieves the stream information and starts watching the segments.
func (ch *Channel) RecordStream(ctx context.Context, runID uint64, s site.Site, req *internal.Req) error {
	ch.fileMu.Lock()
	ch.mp4InitSegment = nil
	ch.fileMu.Unlock()
	ch.detectClearSegments() // segments of an earlier stream do not belong to the new init segment

	streamInfo, err := s.FetchStream(ctx, req, ch.Config.Username)

	// Update static metadata whenever the site API returns it, even if the room
	// is currently offline/private/hidden.
	changed := false
	thumbChanged := false
	if streamInfo != nil {
		ch.stateMu.Lock()
		if streamInfo.RoomTitle != "" && streamInfo.RoomTitle != ch.RoomTitle {
			ch.RoomTitle = streamInfo.RoomTitle
			ch.Config.RoomTitle = streamInfo.RoomTitle
			changed = true
		}
		if streamInfo.Gender != "" && streamInfo.Gender != ch.Gender {
			ch.Gender = streamInfo.Gender
			ch.Config.Gender = streamInfo.Gender
			changed = true
		}
		if streamInfo.SummaryCardImage != "" && streamInfo.SummaryCardImage != ch.SummaryCardImage {
			ch.SummaryCardImage = streamInfo.SummaryCardImage
			ch.Config.SummaryCardImage = streamInfo.SummaryCardImage
			changed = true
			thumbChanged = true
		}
		ch.stateMu.Unlock()
		if changed {
			_ = server.Manager.SaveConfig()
			if thumbChanged {
				ch.UpdateThumb()
			}
			ch.UpdateNow()
		}
	}

	if err != nil {
		return fmt.Errorf("get stream: %w", err)
	}
	if streamInfo == nil {
		// Site returned nil, nil — channel is offline.
		return fmt.Errorf("get stream: %w", internal.ErrChannelOffline)
	}

	ch.stateMu.Lock()
	ch.StreamedAt = time.Now().Unix()
	ch.Config.StreamedAt = ch.StreamedAt
	ch.NumViewers = streamInfo.NumViewers
	ch.stateMu.Unlock()
	_ = server.Manager.SaveConfig()
	ch.Sequence = 0
	if ch.setLiveThumbURL(streamInfo.LiveThumbURL) {
		ch.UpdateThumb()
	}

	set := ch.Settings()
	playlist, err := chaturbate.FetchPlaylist(ctx, streamInfo.HLSURL, set.Resolution, set.Framerate, streamInfo.CDNReferer, streamInfo.MouflonPDKey)
	if err != nil {
		return fmt.Errorf("get playlist: %w", err)
	}

	ch.FileExt = playlist.FileExt
	if err := ch.NextFile(playlist.FileExt); err != nil {
		return fmt.Errorf("next file: %w", err)
	}

	// Ensure file is cleaned up when this function exits in any case
	defer func() {
		if err := ch.Cleanup(); err != nil {
			ch.Error("cleanup on record stream exit: %s", err.Error())
		}
	}()

	ch.UpdateOnlineStatus(true) // Update online status after playlist is OK

	// Reset CF state on successful stream start.
	ch.CFBlockCount = 0
	notifier.Default.ResetCooldown(fmt.Sprintf(notifier.KeyCFChannel, ch.Config.Username))
	server.Manager.ResetCFBlock(ch.Config.Username)
	// Notify stream online if enabled.
	if server.Config.NotifyStreamOnline {
		title := fmt.Sprintf("📡 %s is live!", ch.Config.Username)
		ch.stateMu.RLock()
		body := ch.RoomTitle
		ch.stateMu.RUnlock()
		if body == "" {
			body = ch.Config.Username
		}
		notifier.Notify(fmt.Sprintf(notifier.KeyStreamOnline, ch.Config.Username), title, body)
	}

	streamType := "HLS"
	if playlist.FileExt == ".mp4" {
		if playlist.AudioPlaylistURL != "" {
			streamType = "LL-HLS (video+audio)"
		} else if playlist.MouflonPDKey != "" {
			streamType = "HLS (fMP4)"
		} else {
			streamType = "LL-HLS (video only)"
		}
	}
	ch.Info("stream type: %s, resolution %dp (target: %dp), framerate %dfps (target: %dfps)", streamType, playlist.Resolution, set.Resolution, playlist.Framerate, set.Framerate)

	return playlist.WatchSegments(ctx, func(b []byte, duration float64) error {
		return ch.handleSegmentForMonitor(runID, b, duration)
	})
}

// handleSegmentForMonitor processes and writes segment data for a specific
// monitor run, ignoring stale late-arriving segments from older runs.
func (ch *Channel) handleSegmentForMonitor(runID uint64, b []byte, duration float64) error {
	ch.fileMu.Lock()
	ch.monitorMu.Lock()
	isPaused := ch.Config.IsPaused
	isCurrentRun := ch.monitorRunID == runID
	ch.monitorMu.Unlock()

	if isPaused || !isCurrentRun {
		ch.fileMu.Unlock()
		return retry.Unrecoverable(internal.ErrPaused)
	}

	if ch.File == nil {
		ch.fileMu.Unlock()
		return fmt.Errorf("write file: no active file")
	}

	if isMP4InitSegment(b) {
		if normalized, err := chaturbate.NormalizeFragmentedInitForRecording(b); err == nil {
			b = normalized
		} else if server.Config.Debug {
			ch.Verbose("mp4 init normalization skipped: %v", err)
		}
		ch.mp4InitSegment = append(ch.mp4InitSegment[:0], b...)
		ch.fileMu.Unlock()
		return nil
	}
	if ch.FileExt == ".mp4" && ch.Filesize == 0 && !isMP4InitSegment(b) && len(ch.mp4InitSegment) > 0 {
		n, err := ch.File.Write(ch.mp4InitSegment)
		if err != nil {
			ch.fileMu.Unlock()
			return fmt.Errorf("write mp4 init segment: %w", err)
		}
		ch.Filesize += n
	}

	n, err := ch.File.Write(b)
	if err != nil {
		ch.fileMu.Unlock()
		return fmt.Errorf("write file: %w", err)
	}

	ch.Filesize += n
	ch.Duration += duration
	ch.detectKeep(b)
	formattedDuration := internal.FormatDuration(ch.Duration)
	formattedFilesize := internal.FormatFilesize(ch.Filesize)
	shouldSwitch := ch.shouldSwitchFileLocked()

	var newFilename string
	if shouldSwitch {
		if err := ch.cleanupLocked(); err != nil {
			ch.fileMu.Unlock()
			return fmt.Errorf("next file: %w", err)
		}
		filename, err := ch.generateFilenameLocked()
		if err != nil {
			ch.fileMu.Unlock()
			return err
		}
		if err := ch.createNewFileLocked(filename, ch.FileExt); err != nil {
			ch.fileMu.Unlock()
			return fmt.Errorf("next file: %w", err)
		}
		ch.Sequence++
		newFilename = ch.File.Name()
	}
	ch.fileMu.Unlock()

	ch.Verbose("duration: %s, filesize: %s", formattedDuration, formattedFilesize)

	// Send an SSE update to update the view
	ch.Update()

	if newFilename != "" {
		ch.Info("max filesize or duration exceeded, new file created: %s", newFilename)
		return nil
	}
	return nil
}
