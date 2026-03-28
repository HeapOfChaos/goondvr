package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/r3labs/sse/v2"
	"github.com/teacat/chaturbate-dvr/channel"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/notifier"
	"github.com/teacat/chaturbate-dvr/router/view"
	"github.com/teacat/chaturbate-dvr/server"
)

// Manager is responsible for managing channels and their states.
type Manager struct {
	Channels sync.Map
	SSE      *sse.Server

	startTime  time.Time
	cfBlocksMu sync.Mutex
	cfBlocks   map[string]time.Time // username -> last CF block time
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

// settings holds the subset of global config that can be updated via the web UI.
type settings struct {
	Cookies             string `json:"cookies"`
	UserAgent           string `json:"user_agent"`
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
}

// SaveSettings persists the current cookies and user-agent to disk.
func SaveSettings() error {
	s := settings{
		Cookies:             server.Config.Cookies,
		UserAgent:           server.Config.UserAgent,
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
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}
	if err := os.MkdirAll("./conf", 0777); err != nil {
		return fmt.Errorf("mkdir conf: %w", err)
	}
	if err := os.WriteFile(settingsFile, b, 0777); err != nil {
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
	return nil
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
	if err := os.MkdirAll("./conf", 0777); err != nil {
		return fmt.Errorf("mkdir all conf: %w", err)
	}
	if err := os.WriteFile("./conf/channels.json", b, 0777); err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	return nil
}

// LoadConfig loads the channels from JSON and starts them.
func (m *Manager) LoadConfig() error {
	b, err := os.ReadFile("./conf/channels.json")
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

	for i, conf := range config {
		ch := channel.New(conf)
		m.Channels.Store(conf.Username, ch)

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
	_, ok := m.Channels.Load(conf.Username)
	if ok {
		return fmt.Errorf("channel %s already exists", conf.Username)
	}

	ch := channel.New(conf)
	m.Channels.Store(conf.Username, ch)

	go ch.Resume(0)

	if shouldSave {
		if err := m.SaveConfig(); err != nil {
			return fmt.Errorf("save config: %w", err)
		}
	}
	return nil
}

// StopChannel stops the channel.
func (m *Manager) StopChannel(username string) error {
	thing, ok := m.Channels.Load(username)
	if !ok {
		return nil
	}
	thing.(*channel.Channel).Stop()
	m.Channels.Delete(username)

	if err := m.SaveConfig(); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	return nil
}

// PauseChannel pauses the channel.
func (m *Manager) PauseChannel(username string) error {
	thing, ok := m.Channels.Load(username)
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
func (m *Manager) ResumeChannel(username string) error {
	thing, ok := m.Channels.Load(username)
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
		// Second priority: Alphabetical order by username
		return strings.ToLower(channels[i].Username) < strings.ToLower(channels[j].Username)
	})

	return channels
}

// Publish sends an SSE event to the specified channel.
func (m *Manager) Publish(evt entity.Event, info *entity.ChannelInfo) {
	switch evt {
	case entity.EventUpdate:
		var b bytes.Buffer
		if err := view.InfoTpl.ExecuteTemplate(&b, "channel_info", info); err != nil {
			fmt.Println("Error executing template:", err)
			return
		}
		m.SSE.Publish("updates", &sse.Event{
			Event: []byte(info.Username + "-info"),
			Data:  b.Bytes(),
		})
	case entity.EventLog:
		m.SSE.Publish("updates", &sse.Event{
			Event: []byte(info.Username + "-log"),
			Data:  []byte(strings.Join(info.Logs, "\n")),
		})
	}
}

// Subscriber handles SSE subscriptions for the specified channel.
func (m *Manager) Subscriber(w http.ResponseWriter, r *http.Request) {
	m.SSE.ServeHTTP(w, r)
}

// GetChannelThumb returns the current summary card image URL for the given username.
// Returns an empty string if the channel is not found or has no image.
func (m *Manager) GetChannelThumb(username string) string {
	val, ok := m.Channels.Load(username)
	if !ok {
		return ""
	}
	return val.(*channel.Channel).SummaryCardImage
}

// Shutdown gracefully stops all active channels, saves config, and waits for
// in-progress file cleanup and seek-index goroutines to finish.
// Call this before process exit to ensure recorded files are properly closed
// and indexed for seeking.
func (m *Manager) Shutdown() {
	m.Channels.Range(func(key, value any) bool {
		value.(*channel.Channel).Stop()
		return true
	})
	// Persist channel list so the web UI restores them on next start.
	_ = m.SaveConfig()
	// Give cleanup and BuildSeekIndex goroutines time to complete.
	time.Sleep(5 * time.Second)
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
