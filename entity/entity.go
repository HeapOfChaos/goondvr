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
	EventThumb  Event = "thumb"
)

// ChannelConfig represents the configuration for a channel.
type ChannelConfig struct {
	IsPaused    bool   `json:"is_paused"`
	Username    string `json:"username"`
	Site        string `json:"site,omitempty"` // "chaturbate" (default) or "stripchat"
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
	c.Site = NormalizeSite(c.Site)
}

// NormalizeSite returns a supported site name, defaulting to Chaturbate.
func NormalizeSite(site string) string {
	if site == "stripchat" {
		return "stripchat"
	}
	return "chaturbate"
}

// NormalizeFinalizeMode returns a supported finalization mode.
func NormalizeFinalizeMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "remux":
		return "remux"
	case "transcode":
		return "transcode"
	default:
		return "none"
	}
}

// ChannelID returns the stable internal identifier for a site+username pair.
func ChannelID(site, username string) string {
	return NormalizeSite(site) + "__" + username
}

// ChannelInfo represents the information about a channel,
// mostly used for the template rendering.
type ChannelInfo struct {
	ChannelID        string
	ViewMode         string
	IsOnline         bool
	IsPaused         bool
	Username         string
	Duration         string
	Filesize         string
	TotalDiskUsage   string
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
	LiveThumbURL     string // live-updating thumbnail; empty = use platform default
	Site             string // "chaturbate" (default) or "stripchat"
	SiteDomain       string // pre-computed site URL, e.g. "https://chaturbate.com/" or "https://stripchat.com/"
}

// Config holds the configuration for the application.
type Config struct {
	Version         string
	Username        string
	Site            string
	AdminUsername   string
	AdminPassword   string
	Framerate       int
	Resolution      int
	Pattern         string
	MaxDuration     int
	MaxFilesize     int
	Port            string
	Interval        int
	Cookies         string
	UserAgent       string
	Domain          string
	CompletedDir    string
	FinalizeMode    string
	FFmpegEncoder   string
	FFmpegContainer string
	FFmpegQuality   int
	FFmpegPreset    string
	Debug           bool

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
	StripchatPDKey      string // MOUFLON v2 decryption key; auto-extracted or manual override
}
