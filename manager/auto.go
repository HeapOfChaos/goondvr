package manager

import (
	"fmt"
	"time"

	"github.com/HeapOfChaos/goondvr/channel"
	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/server"
)

// Automatic recording of the channels that ask for it (ChannelConfig.AutoRecord).
//
// An armed channel is kept paused while it is off the air, so it costs nothing
// but its share of the list sweep. The presence engine calls autoStep after every
// round: a paused, armed channel that is now on the air is started (unless the
// free space of its disk is below the global threshold, Config.AutoMinFreeGB,
// and the channel did not turn that check off); one that auto-record started
// and that has been off the air for autoRelaxAfter is paused again.
//
// (Variables only so that tests can shorten them.)
var (
	autoRelaxAfter = 3 * time.Minute
	autoStartGap   = time.Second // at most one automatic start per gap: a burst of channels does not hit the site at once
)

const gib = 1 << 30

// autoStep does what automatic recording needs now. It reports whether some
// channel is still waiting for its turn (the caller comes back soon).
func (m *Manager) autoStep() (pending bool) {
	m.autoMu.Lock()
	defer m.autoMu.Unlock()
	type diskAnswer struct {
		free uint64
		ok   bool
	}
	disks := map[string]diskAnswer{} // by recording folder: one question per disk per step
	var minFree uint64
	if server.Config != nil && server.Config.AutoMinFreeGB > 0 {
		minFree = uint64(server.Config.AutoMinFreeGB) * gib
	}

	m.Channels.Range(func(_, v any) bool {
		ch := v.(*channel.Channel)
		if !ch.AutoActive() {
			return true // auto-record is off (the default): nothing to do, and no write lock taken
		}
		ch.AutoRelax(autoRelaxAfter)

		want, noDiskCheck, pattern := ch.AutoWant()
		if !want {
			return true
		}
		if !noDiskCheck && minFree > 0 {
			dir := recordingDir(pattern)
			a, seen := disks[dir]
			if !seen {
				a.free, a.ok = freeSpaceFor(pattern)
				disks[dir] = a
			}
			// When the disk cannot be asked the channel is started: a check that cannot
			// be made must not lose a recording.
			if a.ok && a.free < minFree {
				if ch.SetAutoWaiting(true) {
					ch.Info("auto-record: waiting, only %.1f GB free on the recording disk (the limit is %d GB)",
						float64(a.free)/gib, server.Config.AutoMinFreeGB)
				}
				return true
			}
		}
		if time.Since(m.autoLastStart) < autoStartGap {
			pending = true
			return true
		}
		if ch.AutoStart("on the air, starting") {
			m.autoLastStart = time.Now()
		}
		return true
	})
	return pending
}

// validatePatch checks the values of a change to the settings of a channel.
func validatePatch(p *entity.ChannelPatch) error {
	if p.Resolution != nil && (*p.Resolution < 144 || *p.Resolution > 4320) {
		return fmt.Errorf("resolution %d is not valid", *p.Resolution)
	}
	if p.Framerate != nil && *p.Framerate != 30 && *p.Framerate != 60 {
		return fmt.Errorf("framerate must be 30 or 60")
	}
	if p.MaxDuration != nil && (*p.MaxDuration < 0 || *p.MaxDuration > 1_000_000) {
		return fmt.Errorf("max duration must be 0 or more minutes")
	}
	if p.MaxFilesize != nil && (*p.MaxFilesize < 0 || *p.MaxFilesize > 100_000_000) {
		return fmt.Errorf("max filesize must be 0 or more MB")
	}
	if p.Detect != nil {
		if err := validateDetect(p.Detect); err != nil {
			return err
		}
	}
	if p.Pattern != nil {
		pat := *p.Pattern
		if pat == "" || len(pat) > 512 {
			return fmt.Errorf("the filename pattern is empty or too long")
		}
		if err := ValidatePattern(pat); err != nil {
			return err
		}
	}
	return nil
}

// UpdateChannel changes the settings of one channel (the dialog of its card, the
// auto-record button) and saves them.
func (m *Manager) UpdateChannel(channelID string, p entity.ChannelPatch) error {
	v, ok := m.Channels.Load(channelID)
	if !ok {
		return fmt.Errorf("channel %s not found", channelID)
	}
	ch := v.(*channel.Channel)
	if err := validatePatch(&p); err != nil {
		return err
	}

	m.cfgMu.Lock()
	defer m.cfgMu.Unlock()
	if p.Pattern != nil {
		cand := ch.ConfigSnapshot()
		cand.Pattern = *p.Pattern
		if err := detectPatternConflict(&cand, m.channelConfigs()); err != nil {
			return err
		}
	}
	if _, autoChanged := ch.Apply(p); autoChanged {
		m.kickPresence() // it may be on the air already
	}
	if err := m.SaveConfig(); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	return nil
}

// channelConfigs returns a consistent copy of the configuration of every channel.
func (m *Manager) channelConfigs() []*entity.ChannelConfig {
	var out []*entity.ChannelConfig
	m.Channels.Range(func(_, v any) bool {
		snap := v.(*channel.Channel).ConfigSnapshot()
		out = append(out, &snap)
		return true
	})
	return out
}
