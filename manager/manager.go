package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/HeapOfChaos/goondvr/channel"
	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/notifier"
	"github.com/HeapOfChaos/goondvr/persist"
	"github.com/HeapOfChaos/goondvr/router/view"
	"github.com/HeapOfChaos/goondvr/server"
)

// Manager is responsible for managing channels and their states.
type Manager struct {
	Channels sync.Map
	hub      *hub // Server-Sent Events to the web UI (see hub.go)

	presence presenceEngine // keeps the "on air" mark of the channels up to date (presence.go)

	cfgMu         sync.Mutex         // one change to the channel list or to the filename patterns at a time (pattern conflicts)
	autoMu        sync.Mutex         // autoStep runs from the presence loop and from the detection workers
	autoLastStart time.Time          // last automatic start (auto.go), guarded by autoMu
	detectStop    context.CancelFunc // stops the image detection engine (detect.go)

	closing atomic.Bool // Shutdown has saved the channel list: later saves must not overwrite it (Stop marks every channel paused)

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

// New initializes a new Manager instance with an SSE hub for the web UI.
func New() (*Manager, error) {
	m := &Manager{hub: newHub()}
	m.startTime = time.Now()
	m.cfBlocks = make(map[string]time.Time)
	m.presence.kick = make(chan struct{}, 1)
	go m.diskMonitor()
	go m.presenceLoop()
	var detectCtx context.Context
	detectCtx, m.detectStop = context.WithCancel(context.Background())
	go m.detectLoop(detectCtx)

	// Keep-alive for the SSE connections. It is an SSE *comment*: browsers ignore
	// it, but dead connections are noticed and proxies keep the stream open. (It
	// used to be an event without data, which made the SSE library end every
	// browser's response every 30 s.)
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for range ticker.C {
			m.hub.comment(hubKeepAliveText)
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
	BrowserMode         string `json:"browser_mode,omitempty"`
	BrowserPath         string `json:"browser_path,omitempty"`
	BrowserProfileDir   string `json:"browser_profile_dir,omitempty"`
	BrowserHelperURL    string `json:"browser_helper_url,omitempty"`
	BrowserHelperToken  string `json:"browser_helper_token,omitempty"`
	BrowserDebugPort    int    `json:"browser_debug_port,omitempty"`
	BrowserBootstrap    bool   `json:"browser_bootstrap,omitempty"`
	BrowserBootstrapURL string `json:"browser_bootstrap_url,omitempty"`
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

	// Detection of the channels on the air (see presence.go).
	PresenceCampaign string `json:"presence_campaign,omitempty"`
	PresenceListURL  string `json:"presence_list_url,omitempty"`
	PresenceInterval int    `json:"presence_interval,omitempty"`
	// Global threshold of the "record automatically" option of the channels.
	AutoMinFreeGB int `json:"auto_min_free_gb,omitempty"`
	// Image detection: where the detector runs (the per-channel options are in channels.json).
	DetectBackend  string `json:"detect_backend,omitempty"`
	DetectLocalURL string `json:"detect_local_url,omitempty"`
	DetectColabURL string `json:"detect_colab_url,omitempty"`
	DetectToken    string `json:"detect_token,omitempty"`
	// Defaults for the channels that are added from now on (nil = not saved: the
	// command-line / built-in value is used). A channel can change them afterwards.
	DefaultFramerate   *int    `json:"default_framerate,omitempty"`
	DefaultResolution  *int    `json:"default_resolution,omitempty"`
	DefaultPattern     *string `json:"default_pattern,omitempty"`
	DefaultMaxDuration *int    `json:"default_max_duration,omitempty"`
	DefaultMaxFilesize *int    `json:"default_max_filesize,omitempty"`
}

func ensureConfDir() error {
	if err := os.MkdirAll("./conf", 0700); err != nil {
		return fmt.Errorf("mkdir conf: %w", err)
	}
	return nil
}

// writeConf saves a file of conf/ so that a kill never leaves it half written
// (see persist.WriteFile) and keeps a rolling backup of the version it replaces.
func writeConf(path string, b []byte) error {
	persist.Backup(path, false)
	return persist.WriteFile(path, b, 0600)
}

func ensureJSONFile(path string, defaultValue any) error {
	if err := ensureConfDir(); err != nil {
		return err
	}
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat %s: %w", path, err)
	}

	b, err := json.MarshalIndent(defaultValue, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal default %s: %w", path, err)
	}
	b = append(b, '\n')

	if err := persist.WriteFile(path, b, 0600); err != nil {
		return fmt.Errorf("write default %s: %w", path, err)
	}
	return nil
}

func EnsureConfigPlaceholders() error {
	if err := ensureJSONFile(settingsFile, settings{}); err != nil {
		return err
	}
	if err := ensureJSONFile(channelsFile, []*entity.ChannelConfig{}); err != nil {
		return err
	}
	return nil
}

// SaveSettings persists the current cookies and user-agent to disk.
func SaveSettings() error {
	s := settings{
		Cookies:             server.Config.Cookies,
		UserAgent:           server.Config.UserAgent,
		BrowserMode:         server.Config.BrowserMode,
		BrowserPath:         server.Config.BrowserPath,
		BrowserProfileDir:   server.Config.BrowserProfileDir,
		BrowserHelperURL:    server.Config.BrowserHelperURL,
		BrowserHelperToken:  server.Config.BrowserHelperToken,
		BrowserDebugPort:    server.Config.BrowserDebugPort,
		BrowserBootstrap:    server.Config.BrowserBootstrap,
		BrowserBootstrapURL: server.Config.BrowserBootstrapURL,
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
		PresenceCampaign:    server.Config.PresenceCampaign,
		PresenceListURL:     server.Config.PresenceListURL,
		PresenceInterval:    server.Config.PresenceInterval,
		AutoMinFreeGB:       server.Config.AutoMinFreeGB,
		DetectBackend:       server.Config.DetectBackend,
		DetectLocalURL:      server.Config.DetectLocalURL,
		DetectColabURL:      server.Config.DetectColabURL,
		DetectToken:         server.Config.DetectToken,
	}
	// The defaults of new channels are saved once the person has chosen them in
	// Settings (that is the only place that calls SaveSettings).
	fr, res, pat, md, mf := server.Config.Framerate, server.Config.Resolution, server.Config.Pattern, server.Config.MaxDuration, server.Config.MaxFilesize
	s.DefaultFramerate, s.DefaultResolution, s.DefaultPattern, s.DefaultMaxDuration, s.DefaultMaxFilesize = &fr, &res, &pat, &md, &mf
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}
	if err := ensureConfDir(); err != nil {
		return err
	}
	if err := writeConf(settingsFile, b); err != nil {
		return fmt.Errorf("write settings: %w", err)
	}
	return nil
}

// LoadSettings reads persisted cookies and user-agent from disk and applies
// them to server.Config, overriding any CLI-provided values.
func LoadSettings() error {
	if err := EnsureConfigPlaceholders(); err != nil {
		return fmt.Errorf("ensure config placeholders: %w", err)
	}
	var s settings
	note, err := persist.ReadJSON(settingsFile, &s)
	if err != nil {
		return fmt.Errorf("read settings: %w", err)
	}
	if note != "" {
		log.Printf("WARN  %s", note)
	}
	persist.Backup(settingsFile, true) // a snapshot at every start
	if s.Cookies != "" {
		server.Config.Cookies = s.Cookies
	}
	if s.UserAgent != "" {
		server.Config.UserAgent = s.UserAgent
	}
	if s.BrowserMode != "" {
		server.Config.BrowserMode = entity.NormalizeBrowserMode(s.BrowserMode)
	}
	if s.BrowserPath != "" {
		server.Config.BrowserPath = s.BrowserPath
	}
	if s.BrowserProfileDir != "" {
		server.Config.BrowserProfileDir = s.BrowserProfileDir
	}
	if s.BrowserHelperURL != "" {
		server.Config.BrowserHelperURL = s.BrowserHelperURL
	}
	if s.BrowserHelperToken != "" {
		server.Config.BrowserHelperToken = s.BrowserHelperToken
	}
	if s.BrowserDebugPort > 0 {
		server.Config.BrowserDebugPort = s.BrowserDebugPort
	}
	server.Config.BrowserBootstrap = s.BrowserBootstrap
	if s.BrowserBootstrapURL != "" {
		server.Config.BrowserBootstrapURL = s.BrowserBootstrapURL
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
	server.Config.PresenceCampaign = strings.TrimSpace(s.PresenceCampaign)
	server.Config.PresenceListURL = strings.TrimSpace(s.PresenceListURL)
	server.Config.PresenceInterval = s.PresenceInterval
	server.Config.DetectBackend = entity.NormalizeDetectBackend(s.DetectBackend)
	server.Config.DetectLocalURL = s.DetectLocalURL
	server.Config.DetectColabURL = s.DetectColabURL
	server.Config.DetectToken = s.DetectToken
	server.Config.AutoMinFreeGB = s.AutoMinFreeGB
	if server.Config.AutoMinFreeGB <= 0 {
		server.Config.AutoMinFreeGB = DefaultAutoMinFreeGB
	}
	if s.DefaultFramerate != nil && (*s.DefaultFramerate == 30 || *s.DefaultFramerate == 60) {
		server.Config.Framerate = *s.DefaultFramerate
	}
	if s.DefaultResolution != nil && *s.DefaultResolution > 0 {
		server.Config.Resolution = *s.DefaultResolution
	}
	if s.DefaultPattern != nil && strings.TrimSpace(*s.DefaultPattern) != "" {
		server.Config.Pattern = *s.DefaultPattern
	}
	if s.DefaultMaxDuration != nil && *s.DefaultMaxDuration >= 0 {
		server.Config.MaxDuration = *s.DefaultMaxDuration
	}
	if s.DefaultMaxFilesize != nil && *s.DefaultMaxFilesize >= 0 {
		server.Config.MaxFilesize = *s.DefaultMaxFilesize
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
	server.Config.BrowserMode = entity.NormalizeBrowserMode(server.Config.BrowserMode)
	if server.Config.BrowserPath == "" {
		server.Config.BrowserPath = "chromium"
	}
	if server.Config.BrowserProfileDir == "" {
		server.Config.BrowserProfileDir = "./conf/browser-profile"
	}
	if server.Config.BrowserDebugPort <= 0 {
		server.Config.BrowserDebugPort = 9222
	}
	if server.Config.BrowserBootstrapURL == "" {
		server.Config.BrowserBootstrapURL = server.Config.Domain
	}
	return nil
}

// DefaultAutoMinFreeGB is the free disk space below which "record automatically" waits.
const DefaultAutoMinFreeGB = 10

// ValidatePattern reports whether a filename pattern can be used.
func ValidatePattern(pattern string) error {
	_, err := renderPatternSample(&entity.ChannelConfig{Username: "user", Site: "chaturbate", Pattern: pattern})
	return err
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
	if m.closing.Load() {
		return nil // Shutdown already saved the list as the person left it
	}
	if server.Config != nil && server.Config.Username != "" {
		// Single-channel CLI runs are ephemeral and should not rewrite the
		// persisted web UI channel list.
		return nil
	}

	var config []*entity.ChannelConfig

	m.Channels.Range(func(key, value any) bool {
		snap := value.(*channel.Channel).ConfigSnapshot() // a copy: recording goroutines keep updating the live one
		config = append(config, &snap)
		return true
	})

	b, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := ensureConfDir(); err != nil {
		return err
	}
	if err := writeConf(channelsFile, b); err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	return nil
}

func saveChannelConfig(config []*entity.ChannelConfig) error {
	b, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := ensureConfDir(); err != nil {
		return err
	}
	if err := writeConf(channelsFile, b); err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	return nil
}

// LoadConfig loads the channels from JSON and starts them.
func (m *Manager) LoadConfig() error {
	if err := EnsureConfigPlaceholders(); err != nil {
		return fmt.Errorf("ensure config placeholders: %w", err)
	}
	var config []*entity.ChannelConfig
	note, err := persist.ReadJSON(channelsFile, &config)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}
	if note != "" {
		log.Printf("WARN  %s", note)
	}
	persist.Backup(channelsFile, true) // a snapshot at every start

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
	m.kickPresence() // find out right away which of them are on the air
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

	m.cfgMu.Lock()
	if err := detectPatternConflict(conf, m.channelConfigs()); err != nil {
		m.cfgMu.Unlock()
		return err
	}
	if _, ok := m.Channels.Load(channelID); ok { // another request added it while the patterns were checked
		m.cfgMu.Unlock()
		return fmt.Errorf("channel %s (%s) already exists", conf.Username, conf.Site)
	}

	ch := channel.New(conf)
	m.Channels.Store(channelID, ch)
	m.cfgMu.Unlock()

	go ch.Resume(0)
	m.kickPresence()

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
	ch := thing.(*channel.Channel)
	ch.UserPaused()
	ch.Pause()

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
	ch := thing.(*channel.Channel)
	ch.UserResumed()
	ch.Resume(0)

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
		// First priority: the section (recording, on air, retrying, paused).
		if channels[i].Rank != channels[j].Rank {
			return channels[i].Rank < channels[j].Rank
		}
		// Second priority: Alphabetical order by username, then site (SortKey is
		// "username site" in lower case; ' ' sorts before every character a username
		// can contain, so a shorter username always comes first).
		return channels[i].SortKey < channels[j].SortKey
	})
	for i, c := range channels {
		c.SectionStart = i == 0 || channels[i-1].Rank != c.Rank
	}

	return channels
}

// Publish sends an SSE event for the given channel to every connected browser.
//
// It never blocks: each browser has its own queue and one that cannot keep up
// is disconnected (it reconnects and gets the current state), see hub.go.
// Blocking here used to freeze the recording loop of every channel as soon as
// one browser tab got stuck. The result is always true (there is nothing to
// retry); it is kept for the server.IManager interface.
func (m *Manager) Publish(evt entity.Event, info *entity.ChannelInfo) bool {
	msg, err := renderEvent(evt, info)
	if err != nil {
		fmt.Println("Error executing template:", err)
		return true // retrying would fail the same way
	}
	m.hub.publish(msg)
	return true
}

// renderEvent builds the SSE message of one kind of update for one channel.
func renderEvent(evt entity.Event, info *entity.ChannelInfo) (hubMessage, error) {
	switch evt {
	case entity.EventUpdate:
		var b bytes.Buffer
		for _, mode := range []string{"expanded", "grid", "collapsed"} {
			if err := view.InfoTpl.ExecuteTemplate(&b, "channel_details", withViewMode(info, mode)); err != nil {
				return hubMessage{}, err
			}
		}
		return hubMessage{event: info.ChannelID + "-info", data: b.Bytes()}, nil
	case entity.EventThumb:
		var b bytes.Buffer
		for _, mode := range []string{"expanded", "grid", "collapsed"} {
			if err := view.InfoTpl.ExecuteTemplate(&b, "channel_thumb", withViewMode(info, mode)); err != nil {
				return hubMessage{}, err
			}
		}
		return hubMessage{event: info.ChannelID + "-thumb", data: b.Bytes()}, nil
	case entity.EventLog:
		return hubMessage{event: info.ChannelID + "-log", data: []byte(strings.Join(info.Logs, "\n"))}, nil
	}
	return hubMessage{}, fmt.Errorf("unknown event %q", evt)
}

// snapshot renders the current card, thumbnail and log of every channel. A
// browser that reconnects receives it first, so whatever it missed while it was
// disconnected (or too slow) is replaced by the current state.
func (m *Manager) snapshot() []hubMessage {
	var msgs []hubMessage
	m.Channels.Range(func(_, value any) bool {
		info := value.(*channel.Channel).ExportInfo()
		for _, evt := range []entity.Event{entity.EventUpdate, entity.EventThumb, entity.EventLog} {
			msg, err := renderEvent(evt, info)
			if err != nil {
				fmt.Println("Error executing template:", err)
				break
			}
			msgs = append(msgs, msg)
		}
		return true
	})
	return msgs
}

func withViewMode(info *entity.ChannelInfo, mode string) *entity.ChannelInfo {
	cp := *info
	cp.ViewMode = mode
	return &cp
}

// HasViewers reports whether any browser is connected to the web UI.
func (m *Manager) HasViewers() bool {
	return m.hub.count() > 0
}

// Subscriber serves one browser's SSE connection until it goes away.
func (m *Manager) Subscriber(w http.ResponseWriter, r *http.Request) {
	m.hub.serve(w, r, m.snapshot)
}

// GetChannelThumb returns the current summary card image URL for the given username.
// Returns an empty string if the channel is not found or has no image.
func (m *Manager) GetChannelThumb(channelID string) string {
	val, ok := m.Channels.Load(channelID)
	if !ok {
		return ""
	}
	return val.(*channel.Channel).GetSummaryCardImage()
}

// GetChannelLiveThumb returns the live-updating thumbnail URL for the given username.
// For Stripchat this is the doppiocdn snapshot URL; for Chaturbate it returns empty
// (the JS handles Chaturbate live thumbs via mmcdn directly).
func (m *Manager) GetChannelLiveThumb(ctx context.Context, channelID string) string {
	val, ok := m.Channels.Load(channelID)
	if !ok {
		return ""
	}
	return val.(*channel.Channel).FreshLiveThumbURL(ctx)
}

// shutdownBudget is how long Shutdown waits for the channels to close their
// files. Windows ends the process about five seconds after the console window is
// closed (or the session ends), so everything has to fit in less than that.
// (A variable only so that tests can shorten it.)
var shutdownBudget = 4 * time.Second

// Shutdown closes the program in an orderly way within shutdownBudget: the
// channel list is saved first (it is tiny and cannot be left half written), then
// every channel stops at once, in parallel, and closes its recording. The slow
// work that follows a recording (seek index, ffmpeg) is not started: the files
// stay on the journal of open recordings and are finished at the next start.
func (m *Manager) Shutdown() {
	channel.BeginShutdown()
	if m.detectStop != nil {
		m.detectStop()
	}
	_ = m.SaveConfig()
	m.closing.Store(true)
	persist.Backup(channelsFile, true)
	persist.Backup(settingsFile, true)

	var wg sync.WaitGroup
	m.Channels.Range(func(_, v any) bool {
		wg.Add(1)
		go func(ch *channel.Channel) {
			defer wg.Done()
			ch.Stop()
		}(v.(*channel.Channel))
		return true
	})
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(shutdownBudget):
		log.Printf("WARN  some channels did not close within %s; exiting anyway (their recordings are repaired at the next start)", shutdownBudget)
	}
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
		if v.(*channel.Channel).Online() {
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
		Presence:       m.PresenceStatus(),
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
