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

	// Record automatically as soon as the channel is seen on the air: a paused channel with this
	// on is started by the presence engine. Off by default. Unless AutoNoDiskCheck is set it is not
	// started while the free disk space is below the global threshold (Config.AutoMinFreeGB).
	AutoRecord      bool `json:"auto_record,omitempty"`
	AutoNoDiskCheck bool `json:"auto_no_disk_check,omitempty"`

	// Detection of images in the channel (see detect/ and manager/detect.go). nil = off, the default.
	Detect *DetectConfig `json:"detect,omitempty"`

	// Persisted metadata — populated at runtime and saved so restarts don't lose them.
	RoomTitle        string `json:"room_title,omitempty"`
	Gender           string `json:"gender,omitempty"`
	SummaryCardImage string `json:"summary_card_image,omitempty"`
	StreamedAt       int64  `json:"streamed_at,omitempty"`
	// Tags the site gives the room (lower case); filled in from the list of rooms on the air.
	Tags []string `json:"tags,omitempty"`
}

// DetectConfig is the image detection of one channel: pictures of the channel go to a detector (a
// classifier or an object detector, running on this computer or on Google Colab) and what it finds
// can hold back the recording and/or cut clips. Zero values mean the defaults (see WithDefaults).
type DetectConfig struct {
	Enabled  bool     `json:"enabled"`
	Model    string   `json:"model"`
	Source   string   `json:"source,omitempty"`    // "thumb" (the preview picture, the default) or "stream" (a frame of what is being recorded)
	Labels   []string `json:"labels,omitempty"`    // what counts as a match; empty = anything the model finds
	MinScore float64  `json:"min_score,omitempty"` // 0..1, default 0.6
	Interval int      `json:"interval,omitempty"`  // seconds between two pictures, default 10
	Gate     bool     `json:"gate,omitempty"`      // record only while it matches (needs source "thumb")
	Idle     int      `json:"idle,omitempty"`      // gate: seconds without a match before the recording stops, default 60
	Clips    bool     `json:"clips,omitempty"`     // cut the parts that match out of each recording
	Pre      int      `json:"pre,omitempty"`       // clips: seconds kept before the first match, default 5
	Post     int      `json:"post,omitempty"`      // clips: seconds kept after the last match, default 10
}

// WithDefaults fills in what was left at zero.
func (d DetectConfig) WithDefaults() DetectConfig {
	if d.Source != "stream" {
		d.Source = "thumb"
	}
	if d.MinScore <= 0 {
		d.MinScore = 0.6
	}
	if d.Interval <= 0 {
		d.Interval = 10
	}
	if d.Idle <= 0 {
		d.Idle = 60
	}
	if d.Pre <= 0 {
		d.Pre = 5
	}
	if d.Post <= 0 {
		d.Post = 10
	}
	return d
}

// DetectInfo is what a card shows about the detection of its channel.
type DetectInfo struct {
	On       bool
	Matching bool   // the last picture matched
	Label    string // best label of the last picture
	Error    string // why the last picture could not be checked
}

// ChannelSettings are the settings of a channel that its settings dialog edits.
type ChannelSettings struct {
	Resolution      int
	Framerate       int
	Pattern         string
	MaxDuration     int // minutes, 0 = no limit
	MaxFilesize     int // MB, 0 = no limit
	AutoRecord      bool
	AutoNoDiskCheck bool
	Detect          DetectConfig // with the defaults filled in
}

// ChannelPatch is a change to ChannelSettings: only the fields that are set change.
type ChannelPatch struct {
	Resolution      *int
	Framerate       *int
	Pattern         *string
	MaxDuration     *int
	MaxFilesize     *int
	AutoRecord      *bool
	AutoNoDiskCheck *bool
	Detect          *DetectConfig // replaces the whole detection block
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

// NormalizeDetectBackend returns "colab" or "local" (the default).
func NormalizeDetectBackend(b string) string {
	if strings.ToLower(strings.TrimSpace(b)) == "colab" {
		return "colab"
	}
	return "local"
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

// NormalizeBrowserMode returns a supported browser fetch mode.
func NormalizeBrowserMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "local":
		return "local"
	case "remote":
		return "remote"
	default:
		return "off"
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
	IsOnline         bool // recording (connected to the stream)
	IsAvailable      bool // broadcasting right now, whether or not it is being recorded
	IsPaused         bool
	Rank             int    // section of the list: 0 recording, 1 on air but not recording, 2 off air but still trying (not paused), 3 off air and paused
	SectionStart     bool   // first card of its section in the list being rendered (the page puts the section header before it)
	SortKey          string // lower-case "username site": the order inside a rank
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
	Tags             []string
	Settings         ChannelSettings // what the settings dialog of the card edits
	Detect           DetectInfo      // image detection of the channel (On = false: not used)
	AutoWaiting      bool            // auto-record is on and the channel is on the air, but it is not started: low free disk space
	Site             string          // "chaturbate" (default) or "stripchat"
	SiteDomain       string          // pre-computed site URL, e.g. "https://chaturbate.com/" or "https://stripchat.com/"
}

// Live reports whether the channel is broadcasting (recorded or not).
func (c *ChannelInfo) Live() bool { return c.IsOnline || c.IsAvailable }

// Config holds the configuration for the application.
type Config struct {
	Version             string
	Username            string
	Site                string
	AdminUsername       string
	AdminPassword       string
	Framerate           int
	Resolution          int
	Pattern             string
	MaxDuration         int
	MaxFilesize         int
	Port                string
	Interval            int
	Cookies             string
	UserAgent           string
	Domain              string
	CompletedDir        string
	FinalizeMode        string
	FFmpegEncoder       string
	FFmpegContainer     string
	FFmpegQuality       int
	FFmpegPreset        string
	Debug               bool
	BrowserMode         string
	BrowserPath         string
	BrowserProfileDir   string
	BrowserHelperURL    string
	BrowserHelperToken  string
	BrowserHelperServer bool
	BrowserHelperBind   string
	BrowserDebugPort    int
	BrowserBootstrap    bool
	BrowserBootstrapURL string

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

	// Detection of the channels on the air (see manager/presence.go).
	PresenceCampaign string // Chaturbate affiliate (campaign) id for the list of rooms on the air; empty = list not used
	PresenceListURL  string // advanced: full address of a list of rooms on the air (siren "users_online_endpoint")
	PresenceInterval int    // seconds between two sweeps of the list; default 60

	// AutoMinFreeGB is the global estimate used by the "record automatically" option of a
	// channel: it is not started while the recording disk has less than this much free space.
	AutoMinFreeGB int // default 10

	// Image detection (see detect/): where the detector runs. The per-channel options are in ChannelConfig.Detect.
	DetectBackend  string // "local" (default) or "colab"
	DetectLocalURL string // default http://127.0.0.1:8099
	DetectColabURL string // the https address the Colab notebook prints
	DetectToken    string // sent to the detector; the Colab one needs it
}
