package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/HeapOfChaos/goondvr/channel"
	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/notifier"
	"github.com/HeapOfChaos/goondvr/router/view"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/r3labs/sse/v2"
)

// Manager is responsible for managing channels and their states.
type Manager struct {
	Channels sync.Map
	SSE      *sse.Server

	startTime  time.Time
	cfBlocksMu sync.Mutex
	cfBlocks   map[string]time.Time // username -> last CF block time
}

type filenamePatternData struct {
	Username string
	Site     string
	Year     string
	Month    string
	Day      string
	Hour     string
	Minute   string
	Second   string
	Sequence int
}

// New initializes a new Manager instance with an SSE server.
func New() (*Manager, error) {

	server := sse.New()
	server.SplitData = true

	updateStream := server.CreateStream("updates")
	updateStream.AutoReplay = false

	m := &Manager{SSE: server}
	m.startTime = time.Now()
	m.cfBlocks = make(map[string]time.Time)
	go m.diskMonitor()

	// Send a heartbeat event every 30s so browsers can detect a stale connection
	// and the SSE extension will reconnect automatically.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			server.Publish("updates", &sse.Event{
				Event: []byte("heartbeat"),
				Data:  []byte(""),
			})
		}
	}()

	return m, nil
}

// settingsFile is the path to the persisted global settings file.
const settingsFile = "./conf/settings.json"
const channelsFile = "./conf/channels.json"
const legacyDefaultPattern = "videos/{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}"
const siteAwareDefaultPattern = "videos/{{if ne .Site \"chaturbate\"}}{{.Site}}/{{end}}{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}"

// settings holds the subset of global config that can be updated via the web UI.
type settings struct {
	Cookies             string `json:"cookies"`
	UserAgent           string `json:"user_agent"`
	CompletedDir        string `json:"completed_dir,omitempty"`
	FinalizeMode        string `json:"finalize_mode,omitempty"`
	FFmpegEncoder       string `json:"ffmpeg_encoder,omitempty"`
	FFmpegContainer     string `json:"ffmpeg_container,omitempty"`
	FFmpegQuality       int    `json:"ffmpeg_quality,omitempty"`
	FFmpegPreset        string `json:"ffmpeg_preset,omitempty"`
	NtfyURL             string `json:"ntfy_url,omitempty"`
	NtfyTopic           string `json:"ntfy_topic,omitempty"`
	NtfyToken           string `json:"ntfy_token,omitempty"`
	DiscordWebhookURL   string `json:"discord_webhook_url,omitempty"`
	DiskWarningPercent  int    `json:"disk_warning_percent,omitempty"`
	DiskCriticalPercent int    `json:"disk_critical_percent,omitempty"`
	CFChannelThreshold  int    `json:"cf_channel_threshold,omitempty"`
	CFGlobalThreshold   int    `json:"cf_global_threshold,omitempty"`
	NotifyCooldownHours int    `json:"notify_cooldown_hours,omitempty"`
	NotifyStreamOnline  bool   `json:"notify_stream_online,omitempty"`
	StripchatPDKey      string `json:"stripchat_pdkey,omitempty"`
}

// SaveSettings persists the current cookies and user-agent to disk.
func SaveSettings() error {
	s := settings{
		Cookies:             server.Config.Cookies,
		UserAgent:           server.Config.UserAgent,
		CompletedDir:        server.Config.CompletedDir,
		FinalizeMode:        server.Config.FinalizeMode,
		FFmpegEncoder:       server.Config.FFmpegEncoder,
		FFmpegContainer:     server.Config.FFmpegContainer,
		FFmpegQuality:       server.Config.FFmpegQuality,
		FFmpegPreset:        server.Config.FFmpegPreset,
		NtfyURL:             server.Config.NtfyURL,
		NtfyTopic:           server.Config.NtfyTopic,
		NtfyToken:           server.Config.NtfyToken,
		DiscordWebhookURL:   server.Config.DiscordWebhookURL,
		DiskWarningPercent:  server.Config.DiskWarningPercent,
		DiskCriticalPercent: server.Config.DiskCriticalPercent,
		CFChannelThreshold:  server.Config.CFChannelThreshold,
		CFGlobalThreshold:   server.Config.CFGlobalThreshold,
		NotifyCooldownHours: server.Config.NotifyCooldownHours,
		NotifyStreamOnline:  server.Config.NotifyStreamOnline,
		StripchatPDKey:      server.Config.StripchatPDKey,
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}
	if err := os.MkdirAll("./conf", 0700); err != nil {
		return fmt.Errorf("mkdir conf: %w", err)
	}
	if err := os.WriteFile(settingsFile, b, 0600); err != nil {
		return fmt.Errorf("write settings: %w", err)
	}
	return nil
}

// LoadSettings reads persisted cookies and user-agent from disk and applies
// them to server.Config, overriding any CLI-provided values.
func LoadSettings() error {
	b, err := os.ReadFile(settingsFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read settings: %w", err)
	}
	var s settings
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("unmarshal settings: %w", err)
	}
	if s.Cookies != "" {
		server.Config.Cookies = s.Cookies
	}
	if s.UserAgent != "" {
		server.Config.UserAgent = s.UserAgent
	}
	server.Config.NtfyURL = s.NtfyURL
	server.Config.NtfyTopic = s.NtfyTopic
	server.Config.NtfyToken = s.NtfyToken
	server.Config.CompletedDir = s.CompletedDir
	server.Config.FinalizeMode = entity.NormalizeFinalizeMode(s.FinalizeMode)
	server.Config.FFmpegEncoder = s.FFmpegEncoder
	server.Config.FFmpegContainer = s.FFmpegContainer
	server.Config.FFmpegQuality = s.FFmpegQuality
	server.Config.FFmpegPreset = s.FFmpegPreset
	server.Config.DiscordWebhookURL = s.DiscordWebhookURL
	server.Config.NotifyStreamOnline = s.NotifyStreamOnline

	server.Config.DiskWarningPercent = s.DiskWarningPercent
	if server.Config.DiskWarningPercent <= 0 {
		server.Config.DiskWarningPercent = 80
	}
	server.Config.DiskCriticalPercent = s.DiskCriticalPercent
	if server.Config.DiskCriticalPercent <= 0 {
		server.Config.DiskCriticalPercent = 90
	}
	server.Config.CFChannelThreshold = s.CFChannelThreshold
	if server.Config.CFChannelThreshold <= 0 {
		server.Config.CFChannelThreshold = 5
	}
	server.Config.CFGlobalThreshold = s.CFGlobalThreshold
	if server.Config.CFGlobalThreshold <= 0 {
		server.Config.CFGlobalThreshold = 3
	}
	server.Config.NotifyCooldownHours = s.NotifyCooldownHours
	if server.Config.NotifyCooldownHours <= 0 {
		server.Config.NotifyCooldownHours = 4
	}
	if s.StripchatPDKey != "" {
		server.Config.StripchatPDKey = s.StripchatPDKey
	}
	if server.Config.FFmpegEncoder == "" {
		server.Config.FFmpegEncoder = "libx264"
	}
	if server.Config.FFmpegContainer != "mkv" {
		server.Config.FFmpegContainer = "mp4"
	}
	if server.Config.FFmpegQuality <= 0 {
		server.Config.FFmpegQuality = 23
	}
	if server.Config.FFmpegPreset == "" {
		server.Config.FFmpegPreset = "medium"
	}
	return nil
}

func renderPatternSample(conf *entity.ChannelConfig) (string, error) {
	tpl, err := template.New("filename").Parse(conf.Pattern)
	if err != nil {
		return "", fmt.Errorf("filename pattern error for %s (%s): %w", conf.Username, conf.Site, err)
	}

	sampleTime := time.Date(2026, time.January, 2, 15, 4, 5, 0, time.UTC)
	data := filenamePatternData{
		Username: conf.Username,
		Site:     entity.NormalizeSite(conf.Site),
		Year:     sampleTime.Format("2006"),
		Month:    sampleTime.Format("01"),
		Day:      sampleTime.Format("02"),
		Hour:     sampleTime.Format("15"),
		Minute:   sampleTime.Format("04"),
		Second:   sampleTime.Format("05"),
		Sequence: 0,
	}

	var buf bytes.Buffer
	if err := tpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("filename pattern error for %s (%s): %w", conf.Username, conf.Site, err)
	}
	return filepath.Clean(buf.String()), nil
}

func detectPatternConflict(conf *entity.ChannelConfig, existing []*entity.ChannelConfig) error {
	candidatePath, err := renderPatternSample(conf)
	if err != nil {
		return err
	}

	for _, other := range existing {
		if other == nil {
			continue
		}
		if entity.ChannelID(other.Site, other.Username) == entity.ChannelID(conf.Site, conf.Username) {
			continue
		}

		otherPath, err := renderPatternSample(other)
		if err != nil {
			return err
		}
		if candidatePath == otherPath {
			return fmt.Errorf(
				"channel %s (%s) would write to the same output path as %s (%s); update one of the filename patterns to produce distinct paths",
				conf.Username, conf.Site, other.Username, other.Site,
			)
		}
	}

	return nil
}

func migrateLegacyPatternConflicts(config []*entity.ChannelConfig) (bool, error) {
	changed := false

	for {
		conflictFound := false
		for i, conf := range config {
			candidatePath, err := renderPatternSample(conf)
			if err != nil {
				return false, err
			}

			for j := 0; j < i; j++ {
				other := config[j]
				if other == nil {
					continue
				}

				otherPath, err := renderPatternSample(other)
				if err != nil {
					return false, err
				}
				if candidatePath != otherPath {
					continue
				}

				updated := false
				if conf.Pattern == legacyDefaultPattern {
					conf.Pattern = siteAwareDefaultPattern
					changed = true
					updated = true
				}
				if other.Pattern == legacyDefaultPattern {
					other.Pattern = siteAwareDefaultPattern
					changed = true
					updated = true
				}
				if !updated {
					return changed, nil
				}

				conflictFound = true
				break
			}
			if conflictFound {
				break
			}
		}

		if !conflictFound {
			return changed, nil
		}
	}
}

// SaveConfig saves the current channels and state to a JSON file.
func (m *Manager) SaveConfig() error {
	var config []*entity.ChannelConfig

	m.Channels.Range(func(key, value any) bool {
		config = append(config, value.(*channel.Channel).Config)
		return true
	})

	b, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.MkdirAll("./conf", 0700); err != nil {
		return fmt.Errorf("mkdir all conf: %w", err)
	}
	if err := os.WriteFile(channelsFile, b, 0600); err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	return nil
}

func saveChannelConfig(config []*entity.ChannelConfig) error {
	b, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.MkdirAll("./conf", 0700); err != nil {
		return fmt.Errorf("mkdir all conf: %w", err)
	}
	if err := os.WriteFile(channelsFile, b, 0600); err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	return nil
}

// LoadConfig loads the channels from JSON and starts them.
func (m *Manager) LoadConfig() error {
	b, err := os.ReadFile(channelsFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	var config []*entity.ChannelConfig
	if err := json.Unmarshal(b, &config); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	migrated, err := migrateLegacyPatternConflicts(config)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	seen := make(map[string]struct{}, len(config))
	for i, conf := range config {
		conf.Sanitize()
		if conf.Username == "" {
			return fmt.Errorf("channel at index %d has empty username", i)
		}
		channelID := entity.ChannelID(conf.Site, conf.Username)
		if _, ok := seen[channelID]; ok {
			return fmt.Errorf("load config: duplicate channel %s (%s)", conf.Username, conf.Site)
		}
		seen[channelID] = struct{}{}
		if err := detectPatternConflict(conf, config[:i]); err != nil {
			return fmt.Errorf("load config: %w", err)
		}
	}

	if migrated {
		if err := saveChannelConfig(config); err != nil {
			return fmt.Errorf("persist migrated config: %w", err)
		}
	}

	for i, conf := range config {
		ch := channel.New(conf)
		m.Channels.Store(entity.ChannelID(conf.Site, conf.Username), ch)

		if ch.Config.IsPaused {
			ch.Info("channel was paused, waiting for resume")
			continue
		}
		go ch.Resume(i)
	}
	return nil
}

// CreateChannel starts monitoring an M3U8 stream
func (m *Manager) CreateChannel(conf *entity.ChannelConfig, shouldSave bool) error {
	conf.Sanitize()

	if conf.Username == "" {
		return fmt.Errorf("username is empty")
	}

	// prevent duplicate channels
	channelID := entity.ChannelID(conf.Site, conf.Username)
	_, ok := m.Channels.Load(channelID)
	if ok {
		return fmt.Errorf("channel %s (%s) already exists", conf.Username, conf.Site)
	}

	var existing []*entity.ChannelConfig
	m.Channels.Range(func(_, value any) bool {
		existing = append(existing, value.(*channel.Channel).Config)
		return true
	})
	if err := detectPatternConflict(conf, existing); err != nil {
		return err
	}

	ch := channel.New(conf)
	m.Channels.Store(channelID, ch)

	go ch.Resume(0)

	if shouldSave {
		if err := m.SaveConfig(); err != nil {
			return fmt.Errorf("save config: %w", err)
		}
	}
	return nil
}

// StopChannel stops the channel.
func (m *Manager) StopChannel(channelID string) error {
	thing, ok := m.Channels.Load(channelID)
	if !ok {
		return nil
	}
	thing.(*channel.Channel).Stop()
	m.Channels.Delete(channelID)

	if err := m.SaveConfig(); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	return nil
}

// PauseChannel pauses the channel.
func (m *Manager) PauseChannel(channelID string) error {
	thing, ok := m.Channels.Load(channelID)
	if !ok {
		return nil
	}
	thing.(*channel.Channel).Pause()

	if err := m.SaveConfig(); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	return nil
}

// ResumeChannel resumes the channel.
func (m *Manager) ResumeChannel(channelID string) error {
	thing, ok := m.Channels.Load(channelID)
	if !ok {
		return nil
	}
	thing.(*channel.Channel).Resume(0)

	if err := m.SaveConfig(); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	return nil
}

// ChannelInfo returns a list of channel information for the web UI.
func (m *Manager) ChannelInfo() []*entity.ChannelInfo {
	var channels []*entity.ChannelInfo

	// Iterate over the channels and append their information to the slice
	m.Channels.Range(func(key, value any) bool {
		channels = append(channels, value.(*channel.Channel).ExportInfo())
		return true
	})

	sort.Slice(channels, func(i, j int) bool {
		// First priority: Online channels
		if channels[i].IsOnline != channels[j].IsOnline {
			return channels[i].IsOnline
		}
		// Second priority: Alphabetical order by username, then site.
		if strings.ToLower(channels[i].Username) != strings.ToLower(channels[j].Username) {
			return strings.ToLower(channels[i].Username) < strings.ToLower(channels[j].Username)
		}
		return strings.ToLower(channels[i].Site) < strings.ToLower(channels[j].Site)
	})

	return channels
}

// Publish sends an SSE event to the specified channel.
func (m *Manager) Publish(evt entity.Event, info *entity.ChannelInfo) {
	switch evt {
	case entity.EventUpdate:
		var b bytes.Buffer
		for _, mode := range []string{"expanded", "grid", "collapsed"} {
			if err := view.InfoTpl.ExecuteTemplate(&b, "channel_details", withViewMode(info, mode)); err != nil {
				fmt.Println("Error executing template:", err)
				return
			}
		}
		m.SSE.Publish("updates", &sse.Event{
			Event: []byte(info.ChannelID + "-info"),
			Data:  b.Bytes(),
		})
	case entity.EventThumb:
		var b bytes.Buffer
		for _, mode := range []string{"expanded", "grid", "collapsed"} {
			if err := view.InfoTpl.ExecuteTemplate(&b, "channel_thumb", withViewMode(info, mode)); err != nil {
				fmt.Println("Error executing template:", err)
				return
			}
		}
		m.SSE.Publish("updates", &sse.Event{
			Event: []byte(info.ChannelID + "-thumb"),
			Data:  b.Bytes(),
		})
	case entity.EventLog:
		m.SSE.Publish("updates", &sse.Event{
			Event: []byte(info.ChannelID + "-log"),
			Data:  []byte(strings.Join(info.Logs, "\n")),
		})
	}
}

func withViewMode(info *entity.ChannelInfo, mode string) *entity.ChannelInfo {
	cp := *info
	cp.ViewMode = mode
	return &cp
}

// Subscriber handles SSE subscriptions for the specified channel.
func (m *Manager) Subscriber(w http.ResponseWriter, r *http.Request) {
	m.SSE.ServeHTTP(w, r)
}

// GetChannelThumb returns the current summary card image URL for the given username.
// Returns an empty string if the channel is not found or has no image.
func (m *Manager) GetChannelThumb(channelID string) string {
	val, ok := m.Channels.Load(channelID)
	if !ok {
		return ""
	}
	return val.(*channel.Channel).SummaryCardImage
}

// GetChannelLiveThumb returns the live-updating thumbnail URL for the given username.
// For Stripchat this is the doppiocdn snapshot URL; for Chaturbate it returns empty
// (the JS handles Chaturbate live thumbs via mmcdn directly).
func (m *Manager) GetChannelLiveThumb(channelID string) string {
	val, ok := m.Channels.Load(channelID)
	if !ok {
		return ""
	}
	return val.(*channel.Channel).LiveThumbURL
}

// Shutdown gracefully stops all active channels, saves config, and waits for
// any recording finalization tasks to finish before returning.
func (m *Manager) Shutdown() {
	m.Channels.Range(func(key, value any) bool {
		ch := value.(*channel.Channel)
		wasPaused := ch.Config.IsPaused
		ch.Stop()
		ch.Config.IsPaused = wasPaused
		return true
	})
	// Persist channel list so the web UI restores them on next start.
	_ = m.SaveConfig()
}

// ReportCFBlock records a CF block for username and fires a global alert if
// enough channels have been blocked within the current poll window.
func (m *Manager) ReportCFBlock(username string) {
	m.cfBlocksMu.Lock()
	defer m.cfBlocksMu.Unlock()
	m.cfBlocks[username] = time.Now()

	window := time.Duration(server.Config.Interval)*time.Minute*2 + 30*time.Second
	count := 0
	for _, t := range m.cfBlocks {
		if time.Since(t) < window {
			count++
		}
	}
	threshold := server.Config.CFGlobalThreshold
	if threshold <= 0 {
		threshold = 3
	}
	if count >= threshold {
		notifier.Notify(
			notifier.KeyCFGlobal,
			"⚠️ Cloudflare Rate Limit",
			fmt.Sprintf("%d channels are being blocked by Cloudflare simultaneously", count),
		)
	}
}

// ResetCFBlock clears the CF block record for a channel that has recovered.
func (m *Manager) ResetCFBlock(username string) {
	m.cfBlocksMu.Lock()
	defer m.cfBlocksMu.Unlock()
	delete(m.cfBlocks, username)
}

// GetStats returns current system stats for the /api/stats endpoint.
func (m *Manager) GetStats() server.StatsResponse {
	recPath := recordingDir(server.Config.Pattern)
	disk, _ := getDiskStats(recPath)

	count := 0
	m.Channels.Range(func(_, v any) bool {
		if v.(*channel.Channel).IsOnline {
			count++
		}
		return true
	})

	return server.StatsResponse{
		DiskPath:       disk.Path,
		DiskUsedBytes:  disk.Used,
		DiskTotalBytes: disk.Total,
		DiskPercent:    disk.Percent,
		UptimeSeconds:  int64(time.Since(m.startTime).Seconds()),
		RecordingCount: count,
	}
}

// diskMonitor runs every 5 minutes and fires notifications when disk usage
// crosses the configured warning or critical thresholds.
func (m *Manager) diskMonitor() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		recPath := recordingDir(server.Config.Pattern)
		disk, err := getDiskStats(recPath)
		if err != nil {
			continue
		}
		pct := disk.Percent
		critThresh := float64(server.Config.DiskCriticalPercent)
		warnThresh := float64(server.Config.DiskWarningPercent)
		if critThresh <= 0 {
			critThresh = 90
		}
		if warnThresh <= 0 {
			warnThresh = 80
		}
		usedGB := float64(disk.Used) / 1e9
		totalGB := float64(disk.Total) / 1e9
		msg := fmt.Sprintf("%.1f GB used of %.1f GB (%.0f%%)", usedGB, totalGB, pct)
		if pct >= critThresh {
			notifier.Notify(
				fmt.Sprintf(notifier.KeyDiskCritical, recPath),
				"🚨 Disk Space Critical",
				msg,
			)
		} else if pct >= warnThresh {
			notifier.Notify(
				fmt.Sprintf(notifier.KeyDiskWarning, recPath),
				"⚠️ Disk Space Warning",
				msg,
			)
		}
	}
}
