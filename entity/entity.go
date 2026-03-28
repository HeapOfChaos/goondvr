package entity

import (
	"regexp"
	"strings"
)

// Event represents the type of event for the channel.
type Event = string

const (
	EventUpdate Event = "update"
	EventLog    Event = "log"
)

// ChannelConfig represents the configuration for a channel.
type ChannelConfig struct {
	IsPaused    bool   `json:"is_paused"`
	Username    string `json:"username"`
	Framerate   int    `json:"framerate"`
	Resolution  int    `json:"resolution"`
	Pattern     string `json:"pattern"`
	MaxDuration int    `json:"max_duration"`
	MaxFilesize int    `json:"max_filesize"`
	CreatedAt   int64  `json:"created_at"`

	// Persisted metadata — populated at runtime and saved so restarts don't lose them.
	RoomTitle        string `json:"room_title,omitempty"`
	Gender           string `json:"gender,omitempty"`
	SummaryCardImage string `json:"summary_card_image,omitempty"`
	StreamedAt       int64  `json:"streamed_at,omitempty"`
}

func (c *ChannelConfig) Sanitize() {
	c.Username = regexp.MustCompile(`[^a-zA-Z0-9_-]`).ReplaceAllString(c.Username, "")
	c.Username = strings.TrimSpace(c.Username)
}

// ChannelInfo represents the information about a channel,
// mostly used for the template rendering.
type ChannelInfo struct {
	IsOnline         bool
	IsPaused         bool
	Username         string
	Duration         string
	Filesize         string
	Filename         string
	StreamedAt       string
	MaxDuration      string
	MaxFilesize      string
	CreatedAt        int64
	Logs             []string
	GlobalConfig     *Config // for nested template to access $.Config
	RoomTitle        string
	Gender           string
	NumViewers       int
	EdgeRegion       string
	SummaryCardImage string
}

// Config holds the configuration for the application.
type Config struct {
	Version       string
	Username      string
	AdminUsername string
	AdminPassword string
	Framerate     int
	Resolution    int
	Pattern       string
	MaxDuration   int
	MaxFilesize   int
	Port          string
	Interval      int
	Cookies       string
	UserAgent     string
	Domain        string
	Debug         bool

	// Notification settings — persisted in settings.json, configured via web UI.
	NtfyURL             string
	NtfyTopic           string
	NtfyToken           string
	DiscordWebhookURL   string
	DiskWarningPercent  int // notify when disk usage exceeds this %; default 80
	DiskCriticalPercent int // notify when disk usage exceeds this %; default 90
	CFChannelThreshold  int // consecutive CF blocks before per-channel alert; default 5
	CFGlobalThreshold   int // channels hitting CF in same window for global alert; default 3
	NotifyCooldownHours int // hours between repeated alerts of the same type; default 4
	NotifyStreamOnline  bool
}
