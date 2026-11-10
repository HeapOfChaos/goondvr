package channel

import (
	"time"

	"github.com/HeapOfChaos/goondvr/entity"
)

// The settings that the settings dialog of a card edits (resolution, framerate,
// filename pattern, splitting limits, automatic recording) can change while the
// channel is being recorded, so they are only read through Settings and only
// written through Apply: both go through stateMu.

// Settings returns a consistent copy of the editable settings of the channel.
func (ch *Channel) Settings() entity.ChannelSettings {
	ch.stateMu.RLock()
	defer ch.stateMu.RUnlock()
	return ch.settingsLocked()
}

func (ch *Channel) settingsLocked() entity.ChannelSettings {
	c := ch.Config
	return entity.ChannelSettings{
		Resolution:      c.Resolution,
		Framerate:       c.Framerate,
		Pattern:         c.Pattern,
		MaxDuration:     c.MaxDuration,
		MaxFilesize:     c.MaxFilesize,
		AutoRecord:      c.AutoRecord,
		AutoNoDiskCheck: c.AutoNoDiskCheck,
		Detect:          ch.detectCfgLocked(),
	}
}

// Apply changes the settings the patch sets and reports what changed. The
// caller has validated the values. A recording in progress keeps going: the
// new resolution/framerate apply to the next stream, the new pattern to the
// next file, the new limits at the next segment.
func (ch *Channel) Apply(p entity.ChannelPatch) (patternChanged, autoChanged bool) {
	ch.stateMu.Lock()
	c := ch.Config
	if p.Resolution != nil {
		c.Resolution = *p.Resolution
	}
	if p.Framerate != nil {
		c.Framerate = *p.Framerate
	}
	if p.Pattern != nil && *p.Pattern != c.Pattern {
		c.Pattern = *p.Pattern
		patternChanged = true
	}
	if p.MaxDuration != nil {
		c.MaxDuration = *p.MaxDuration
	}
	if p.MaxFilesize != nil {
		c.MaxFilesize = *p.MaxFilesize
	}
	if p.AutoRecord != nil && *p.AutoRecord != c.AutoRecord {
		c.AutoRecord = *p.AutoRecord
		autoChanged = true
		ch.autoHold = false
	}
	if p.AutoNoDiskCheck != nil && *p.AutoNoDiskCheck != c.AutoNoDiskCheck {
		c.AutoNoDiskCheck = *p.AutoNoDiskCheck
		autoChanged = true
	}
	if p.Detect != nil {
		d := *p.Detect
		if d.Enabled || d.Model != "" { // a channel that never used detection keeps no block in its file
			c.Detect = &d
		} else {
			c.Detect = nil
		}
		autoChanged = true // the gate arms or disarms the channel
		ch.autoHold = false
		ch.detectReset(d.Enabled && d.WithDefaults().Source == "stream")
	}
	if !ch.armedLocked() { // nothing is armed any more
		ch.autoStarted, ch.autoOffSince, ch.autoWaiting = false, time.Time{}, false
	}
	ch.stateMu.Unlock()

	if patternChanged {
		ch.scanWG.Add(1)
		go func() { // the recordings are looked for where the new pattern says
			defer ch.scanWG.Done()
			ch.ScanTotalDiskUsage()
		}()
	}
	ch.UpdateNow()
	return patternChanged, autoChanged
}

// Automatic recording
//
// A channel with AutoRecord is kept paused ("armed") while it is off the air.
// When the presence engine sees it on the air it starts it (AutoStart); once
// it has been off the air for a while it is paused again (AutoRelax), so that
// it costs nothing but the shared list sweep until the next time. A channel
// that the user paused or resumed by hand is left alone (ClearAuto).

// AutoActive reports whether auto-record has anything to do with the channel
// (it is armed, or it started the channel, or it is holding it back). The
// engine skips every other channel without taking a write lock.
func (ch *Channel) AutoActive() bool {
	ch.stateMu.RLock()
	defer ch.stateMu.RUnlock()
	return ch.armedLocked() || ch.autoStarted || ch.autoWaiting
}

// armedLocked: the channel is kept paused until it is on the air, because it asked for auto-record or
// because detection holds its recording back. Caller holds stateMu.
func (ch *Channel) armedLocked() bool { return ch.Config.AutoRecord || ch.detectGateLocked() }

// gateAllowsLocked: detection does not hold the channel back (it has no gate, or it matched lately).
// Caller holds stateMu.
func (ch *Channel) gateAllowsLocked(now time.Time) bool {
	return !ch.detectGateLocked() || ch.gateOpen(now, ch.detectCfgLocked().Idle)
}

// AutoWant reports whether the engine should start the channel now: auto-record
// is on, it is paused, not recording, on the air, and the user did not pause it
// on purpose during this broadcast. noDiskCheck is the channel's choice to
// ignore the free-space condition; pattern says which disk the condition is about.
func (ch *Channel) AutoWant() (want, noDiskCheck bool, pattern string) {
	ch.stateMu.Lock()
	if !ch.Available && !ch.IsOnline {
		ch.autoHold = false // the broadcast is over: the next one starts by itself again
	}
	armed := ch.armedLocked() && !ch.autoHold && ch.gateAllowsLocked(time.Now())
	noDiskCheck = ch.Config.AutoNoDiskCheck
	pattern = ch.Config.Pattern
	live := ch.Available && !ch.IsOnline
	clearWaiting := ch.autoWaiting && !(armed && live)
	if clearWaiting {
		ch.autoWaiting = false
	}
	ch.stateMu.Unlock()
	if clearWaiting {
		ch.UpdateNow()
	}
	if !armed || !live || !ch.Paused() {
		return false, false, ""
	}
	return true, noDiskCheck, pattern
}

// SetAutoWaiting says that the channel wants to be started but is held back
// (low free disk space); the card shows it. It reports whether that changed.
func (ch *Channel) SetAutoWaiting(v bool) bool {
	ch.stateMu.Lock()
	changed := ch.autoWaiting != v
	ch.autoWaiting = v
	ch.stateMu.Unlock()
	if changed {
		ch.UpdateNow()
	}
	return changed
}

// AutoStart starts a paused, armed channel. It reports whether it did.
func (ch *Channel) AutoStart(why string) bool {
	if !ch.Paused() {
		return false // resumed by hand in the meantime
	}
	ch.stateMu.Lock()
	if !ch.armedLocked() || ch.IsOnline {
		ch.stateMu.Unlock()
		return false
	}
	ch.autoStarted, ch.autoOffSince, ch.autoWaiting = true, time.Time{}, false
	ch.stateMu.Unlock()
	ch.Info("auto-record: %s", why)
	ch.Resume(0)
	return true
}

// AutoRelax pauses a channel that auto-record started once it has been off the
// air (and not recording) for `after`. It reports whether it paused it.
func (ch *Channel) AutoRelax(after time.Duration) bool {
	ch.stateMu.Lock()
	if !ch.autoStarted {
		ch.stateMu.Unlock()
		return false
	}
	if ch.IsOnline || ch.Available {
		ch.autoOffSince = time.Time{}
		gateClosed := !ch.gateAllowsLocked(time.Now())
		if gateClosed {
			ch.autoStarted = false
		}
		ch.stateMu.Unlock()
		if gateClosed { // detection has not matched for a while: stop recording, wait for the next match
			ch.Info("detection: nothing matched for %d s, recording paused until it does again", ch.DetectConfig().Idle)
			ch.Pause()
		}
		return gateClosed
	}
	now := time.Now()
	if ch.autoOffSince.IsZero() {
		ch.autoOffSince = now
	}
	due := now.Sub(ch.autoOffSince) >= after
	if due {
		ch.autoStarted, ch.autoOffSince = false, time.Time{}
	}
	ch.stateMu.Unlock()
	if !due {
		return false
	}
	ch.Info("auto-record: off the air, waiting paused until it is back")
	ch.Pause()
	return true
}

// UserResumed forgets that auto-record started the channel: the user took over
// (they resumed it by hand), so it is not paused again by itself.
func (ch *Channel) UserResumed() {
	ch.stateMu.Lock()
	ch.autoStarted, ch.autoOffSince, ch.autoHold = false, time.Time{}, false
	ch.stateMu.Unlock()
}

// UserPaused is UserResumed for a pause by hand. If the channel is on the air
// it is not started again for this broadcast (that would undo the click).
func (ch *Channel) UserPaused() {
	ch.stateMu.Lock()
	ch.autoStarted, ch.autoOffSince = false, time.Time{}
	ch.autoHold = ch.armedLocked() && (ch.Available || ch.IsOnline)
	ch.stateMu.Unlock()
}

// AutoStarted reports whether auto-record started the channel and it has not
// been paused again (it is saved as paused: that is what the user chose).
func (ch *Channel) AutoStarted() bool {
	ch.stateMu.RLock()
	defer ch.stateMu.RUnlock()
	return ch.autoStarted
}

// WaitBackground blocks until the scans of the recordings that Apply started
// have finished (tests use it before restoring the globals they read).
func (ch *Channel) WaitBackground() { ch.scanWG.Wait() }
