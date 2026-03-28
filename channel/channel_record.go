package channel

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/teacat/chaturbate-dvr/chaturbate"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
)

// Monitor starts monitoring the channel for live streams and records them.
func (ch *Channel) Monitor() {
	client := chaturbate.NewClient()
	ch.Info("starting to record `%s`", ch.Config.Username)

	// Seed StreamedAt from biocontext if we haven't seen this channel stream yet.
	if ch.StreamedAt == 0 {
		if ts, err := chaturbate.FetchLastBroadcast(context.Background(), client.Req, ch.Config.Username); err == nil && ts > 0 {
			ch.StreamedAt = ts
			ch.Config.StreamedAt = ts
			_ = server.Manager.SaveConfig()
			ch.Update()
		}
	}


	// Create a new context with a cancel function,
	// the CancelFunc will be stored in the channel's CancelFunc field
	// and will be called by `Pause` or `Stop` functions
	ctx, _ := ch.WithCancel(context.Background())

	var err error
	for {
		if err = ctx.Err(); err != nil {
			break
		}

		pipeline := func() error {
			return ch.RecordStream(ctx, client)
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
			ch.UpdateOnlineStatus(false)

			if errors.Is(err, internal.ErrChannelOffline) {
				ch.Info("channel is offline, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrPrivateStream) {
				ch.Info("channel is in a private show, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrHiddenStream) {
				ch.Info("channel is hidden, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrCloudflareBlocked) {
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

// Update sends an update signal to the channel's update channel.
// This notifies the Server-sent Event to boradcast the channel information to the client.
func (ch *Channel) Update() {
	ch.UpdateCh <- true
}

// RecordStream records the stream of the channel using the provided client.
// It retrieves the stream information and starts watching the segments.
func (ch *Channel) RecordStream(ctx context.Context, client *chaturbate.Client) error {
	stream, err := client.GetStream(ctx, ch.Config.Username)
	// Update static metadata whenever the API responds, even if the channel is offline.
	// This ensures thumbnails and room info show for channels not currently recording.
	// Mirror changes back to Config so they survive restarts via SaveConfig.
	if stream != nil {
		changed := false
		if stream.RoomTitle != "" && stream.RoomTitle != ch.RoomTitle {
			ch.RoomTitle = stream.RoomTitle
			ch.Config.RoomTitle = stream.RoomTitle
			changed = true
		}
		if stream.Gender != "" && stream.Gender != ch.Gender {
			ch.Gender = stream.Gender
			ch.Config.Gender = stream.Gender
			changed = true
		}
		if stream.EdgeRegion != "" {
			ch.EdgeRegion = stream.EdgeRegion
		}
		if stream.SummaryCardImage != "" && stream.SummaryCardImage != ch.SummaryCardImage {
			ch.SummaryCardImage = stream.SummaryCardImage
			ch.Config.SummaryCardImage = stream.SummaryCardImage
			changed = true
		}
		if changed {
			_ = server.Manager.SaveConfig()
		}
	}
	if err != nil {
		return fmt.Errorf("get stream: %w", err)
	}
	ch.StreamedAt = time.Now().Unix()
	ch.Config.StreamedAt = ch.StreamedAt
	_ = server.Manager.SaveConfig()
	ch.Sequence = 0
	ch.NumViewers = stream.NumViewers

	playlist, err := stream.GetPlaylist(ctx, ch.Config.Resolution, ch.Config.Framerate)
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

	ch.UpdateOnlineStatus(true) // Update online status after `GetPlaylist` is OK

	streamType := "HLS"
	if playlist.FileExt == ".mp4" {
		if playlist.AudioPlaylistURL != "" {
			streamType = "LL-HLS (video+audio)"
		} else {
			streamType = "LL-HLS (video only)"
		}
	}
	ch.Info("stream type: %s, resolution %dp (target: %dp), framerate %dfps (target: %dfps)", streamType, playlist.Resolution, ch.Config.Resolution, playlist.Framerate, ch.Config.Framerate)

	return playlist.WatchSegments(ctx, ch.HandleSegment)
}

// HandleSegment processes and writes segment data to a file.
func (ch *Channel) HandleSegment(b []byte, duration float64) error {
	if ch.Config.IsPaused {
		return retry.Unrecoverable(internal.ErrPaused)
	}

	n, err := ch.File.Write(b)
	if err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	ch.Filesize += n
	ch.Duration += duration
	ch.Info("duration: %s, filesize: %s", internal.FormatDuration(ch.Duration), internal.FormatFilesize(ch.Filesize))

	// Send an SSE update to update the view
	ch.Update()

	if ch.ShouldSwitchFile() {
		if err := ch.NextFile(ch.FileExt); err != nil {
			return fmt.Errorf("next file: %w", err)
		}
		ch.Info("max filesize or duration exceeded, new file created: %s", ch.File.Name())
		return nil
	}
	return nil
}
