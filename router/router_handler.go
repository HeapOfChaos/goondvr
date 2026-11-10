package router

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/manager"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/gin-gonic/gin"
)

// IndexData represents the data structure for the index page.
type IndexData struct {
	Config   *entity.Config
	Channels []*entity.ChannelInfo
}

// Index renders the index page with channel information.
func Index(c *gin.Context) {
	c.HTML(200, "index.html", &IndexData{
		Config:   server.Config,
		Channels: server.Manager.ChannelInfo(),
	})
}

// CreateChannelRequest represents the request body for creating a channel.
type CreateChannelRequest struct {
	Username    string `form:"username" binding:"required"`
	Site        string `form:"site"`
	Framerate   int    `form:"framerate" binding:"required"`
	Resolution  int    `form:"resolution" binding:"required"`
	Pattern     string `form:"pattern" binding:"required"`
	MaxDuration int    `form:"max_duration"`
	MaxFilesize int    `form:"max_filesize"`
}

// CreateChannel creates a new channel.
func CreateChannel(c *gin.Context) {
	var req *CreateChannelRequest
	if err := c.Bind(&req); err != nil {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bind: %w", err))
		return
	}

	siteName := entity.NormalizeSite(req.Site)

	var errs []string
	for _, username := range strings.Split(req.Username, ",") {
		if err := server.Manager.CreateChannel(&entity.ChannelConfig{
			IsPaused:    false,
			Username:    username,
			Site:        siteName,
			Framerate:   req.Framerate,
			Resolution:  req.Resolution,
			Pattern:     req.Pattern,
			MaxDuration: req.MaxDuration,
			MaxFilesize: req.MaxFilesize,
			CreatedAt:   time.Now().Unix(),
		}, true); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("%s", strings.Join(errs, "; ")))
		return
	}
	c.Redirect(http.StatusFound, "/")
}

// UpdateChannelRequest is the body of a change to the settings of one channel.
// Only the fields that are sent change. The checkboxes are sent as "1" / "0".
type UpdateChannelRequest struct {
	Resolution    *int    `form:"resolution"`
	Framerate     *int    `form:"framerate"`
	Pattern       *string `form:"pattern"`
	MaxDuration   *int    `form:"max_duration"`
	MaxFilesize   *int    `form:"max_filesize"`
	AutoRecord    *bool   `form:"auto_record"`
	AutoDiskCheck *bool   `form:"auto_disk_check"` // "1" = respect the free-space limit (the default)

	// Image detection (sent together by the settings dialog; DetectEnabled absent = not touched).
	DetectEnabled  *bool    `form:"detect_enabled"`
	DetectModel    string   `form:"detect_model"`
	DetectSource   string   `form:"detect_source"`
	DetectLabels   string   `form:"detect_labels"`    // comma separated
	DetectMinScore *float64 `form:"detect_min_score"` // percent, 1..100
	DetectInterval int      `form:"detect_interval"`
	DetectGate     bool     `form:"detect_gate"`
	DetectIdle     int      `form:"detect_idle"`
	DetectClips    bool     `form:"detect_clips"`
	DetectPre      int      `form:"detect_pre"`
	DetectPost     int      `form:"detect_post"`
}

// detectPatch builds the detection block of a channel from the dialog fields.
func (r *UpdateChannelRequest) detectPatch() *entity.DetectConfig {
	if r.DetectEnabled == nil {
		return nil
	}
	d := &entity.DetectConfig{
		Enabled:  *r.DetectEnabled,
		Model:    strings.TrimSpace(r.DetectModel),
		Source:   strings.TrimSpace(r.DetectSource),
		Interval: r.DetectInterval,
		Gate:     r.DetectGate,
		Idle:     r.DetectIdle,
		Clips:    r.DetectClips,
		Pre:      r.DetectPre,
		Post:     r.DetectPost,
	}
	if r.DetectMinScore != nil {
		d.MinScore = *r.DetectMinScore / 100
	}
	seen := map[string]bool{}
	for _, l := range strings.Split(r.DetectLabels, ",") {
		if l = strings.TrimSpace(l); l != "" && !seen[strings.ToLower(l)] {
			seen[strings.ToLower(l)] = true
			d.Labels = append(d.Labels, l)
		}
	}
	return d
}

// UpdateChannel changes the settings of one channel. The card is redrawn by the
// event stream, so the answer is empty (204) or the reason it was refused (400).
func UpdateChannel(c *gin.Context) {
	var req UpdateChannelRequest
	if err := c.ShouldBind(&req); err != nil {
		c.String(http.StatusBadRequest, "bind: %v", err)
		return
	}
	patch := entity.ChannelPatch{
		Resolution:  req.Resolution,
		Framerate:   req.Framerate,
		MaxDuration: req.MaxDuration,
		MaxFilesize: req.MaxFilesize,
		AutoRecord:  req.AutoRecord,
		Detect:      req.detectPatch(),
	}
	if req.Pattern != nil {
		p := strings.TrimSpace(*req.Pattern)
		patch.Pattern = &p
	}
	if req.AutoDiskCheck != nil {
		noCheck := !*req.AutoDiskCheck
		patch.AutoNoDiskCheck = &noCheck
	}
	if err := server.Manager.UpdateChannel(c.Param("channelID"), patch); err != nil {
		c.String(http.StatusBadRequest, "%v", err)
		return
	}
	c.Status(http.StatusNoContent)
	c.Writer.WriteHeaderNow()
}

// StopChannel stops a channel.
func StopChannel(c *gin.Context) {
	server.Manager.StopChannel(c.Param("channelID"))

	c.Redirect(http.StatusFound, "/")
}

// PauseChannel pauses a channel.
func PauseChannel(c *gin.Context) {
	server.Manager.PauseChannel(c.Param("channelID"))

	respondDone(c)
}

// ResumeChannel resumes a paused channel.
func ResumeChannel(c *gin.Context) {
	server.Manager.ResumeChannel(c.Param("channelID"))

	respondDone(c)
}

// respondDone ends a request that changed the state of a channel.
//
// The Pause/Resume buttons are htmx requests that do not use the response (the
// card is redrawn by the event stream), so they get an empty 204. Following a
// redirect made the browser download and throw away the whole dashboard page
// (three views of every channel) on every click. Plain form posts are still
// redirected to the dashboard.
func respondDone(c *gin.Context) {
	if c.GetHeader("HX-Request") == "true" {
		c.Status(http.StatusNoContent)
		c.Writer.WriteHeaderNow()
		return
	}
	c.Redirect(http.StatusFound, "/")
}

// ThumbProxy proxies the channel's summary card image from the CDN through the server.
// This avoids hotlink-protection issues when the browser requests the image directly.
// Images are cached in memory and the browser may cache them too (see thumb_cache.go).
func ThumbProxy(c *gin.Context) {
	imgURL := server.Manager.GetChannelThumb(c.Param("channelID"))
	if imgURL == "" {
		c.Status(http.StatusNotFound)
		return
	}

	if e, ok := thumbs.get(imgURL); ok {
		writeThumb(c.Writer, e.data, e.contentType, cardThumbTTL)
		return
	}

	req := internal.NewMediaReq()
	imgBytes, err := req.GetBytes(c.Request.Context(), imgURL)
	if err != nil {
		c.Status(http.StatusBadGateway)
		return
	}

	contentType := http.DetectContentType(imgBytes)
	thumbs.put(imgURL, imgBytes, contentType, cardThumbTTL)
	writeThumb(c.Writer, imgBytes, contentType, cardThumbTTL)
}

// LiveThumbProxy proxies the channel's live-updating thumbnail from the CDN.
// For Stripchat this uses img.doppiocdn.net; for Chaturbate it falls back to
// the summary card image (the JS handles Chaturbate live thumbs directly).
func LiveThumbProxy(c *gin.Context) {
	imgURL := server.Manager.GetChannelLiveThumb(c.Request.Context(), c.Param("channelID"))
	if imgURL == "" {
		c.Status(http.StatusNotFound)
		return
	}

	if e, ok := thumbs.get(imgURL); ok {
		writeThumb(c.Writer, e.data, e.contentType, liveThumbTTL)
		return
	}

	req := internal.NewMediaReqWithReferer("https://stripchat.com/")
	imgBytes, err := req.GetBytes(c.Request.Context(), imgURL)
	if err != nil {
		c.Status(http.StatusBadGateway)
		return
	}

	contentType := http.DetectContentType(imgBytes)
	thumbs.put(imgURL, imgBytes, contentType, liveThumbTTL)
	writeThumb(c.Writer, imgBytes, contentType, liveThumbTTL)
}

// Updates handles the SSE connection for updates.
func Updates(c *gin.Context) {
	server.Manager.Subscriber(c.Writer, c.Request)
}

// Stats returns system stats as JSON for the header stats bar.
func Stats(c *gin.Context) {
	c.JSON(http.StatusOK, server.Manager.GetStats())
}

// DetectModels asks the image detector which models it has. The settings form sends what is typed in it
// (not saved yet) so that "Test" tests that; without them the saved settings are used.
func DetectModels(c *gin.Context) {
	var o *server.DetectSettings
	if c.PostForm("detect_backend") != "" {
		o = &server.DetectSettings{
			Backend:  c.PostForm("detect_backend"),
			LocalURL: strings.TrimSpace(c.PostForm("detect_local_url")),
			ColabURL: strings.TrimSpace(c.PostForm("detect_colab_url")),
			Token:    strings.TrimSpace(c.PostForm("detect_token")),
		}
	}
	c.JSON(http.StatusOK, server.Manager.DetectModels(c.Request.Context(), o))
}

// UpdateConfigRequest represents the request body for updating configuration.
type UpdateConfigRequest struct {
	BrowserExport       string `form:"browser_export"`
	Cookies             string `form:"cookies"`
	UserAgent           string `form:"user_agent"`
	BrowserMode         string `form:"browser_mode"`
	BrowserPath         string `form:"browser_path"`
	BrowserProfileDir   string `form:"browser_profile_dir"`
	BrowserHelperURL    string `form:"browser_helper_url"`
	BrowserHelperToken  string `form:"browser_helper_token"`
	BrowserDebugPort    int    `form:"browser_debug_port"`
	BrowserBootstrap    bool   `form:"browser_bootstrap"`
	BrowserBootstrapURL string `form:"browser_bootstrap_url"`
	CompletedDir        string `form:"completed_dir"`
	FinalizeMode        string `form:"finalize_mode"`
	FFmpegEncoder       string `form:"ffmpeg_encoder"`
	FFmpegContainer     string `form:"ffmpeg_container"`
	FFmpegQuality       int    `form:"ffmpeg_quality"`
	FFmpegPreset        string `form:"ffmpeg_preset"`
	NtfyURL             string `form:"ntfy_url"`
	NtfyTopic           string `form:"ntfy_topic"`
	NtfyToken           string `form:"ntfy_token"`
	DiscordWebhookURL   string `form:"discord_webhook_url"`
	DiskWarningPercent  int    `form:"disk_warning_percent"`
	DiskCriticalPercent int    `form:"disk_critical_percent"`
	CFChannelThreshold  int    `form:"cf_channel_threshold"`
	CFGlobalThreshold   int    `form:"cf_global_threshold"`
	NotifyCooldownHours int    `form:"notify_cooldown_hours"`
	NotifyStreamOnline  bool   `form:"notify_stream_online"`

	// Detection of the channels on the air
	PresenceCampaign string `form:"presence_campaign"`
	PresenceListURL  string `form:"presence_list_url"`
	PresenceInterval *int   `form:"presence_interval"`
	// Automatic recording: free disk space (GB) below which it waits
	AutoMinFreeGB *int `form:"auto_min_free_gb"`
	// Image detection: where the detector runs
	DetectBackend  string `form:"detect_backend"`
	DetectLocalURL string `form:"detect_local_url"`
	DetectColabURL string `form:"detect_colab_url"`
	DetectToken    string `form:"detect_token"`
	// Defaults of the channels added from now on
	DefaultFramerate   int    `form:"default_framerate"`
	DefaultResolution  int    `form:"default_resolution"`
	DefaultPattern     string `form:"default_pattern"`
	DefaultMaxDuration *int   `form:"default_max_duration"`
	DefaultMaxFilesize *int   `form:"default_max_filesize"`
}

// UpdateConfig updates the server configuration.
func UpdateConfig(c *gin.Context) {
	var req *UpdateConfigRequest
	if err := c.Bind(&req); err != nil {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bind: %w", err))
		return
	}

	cookieInput := req.Cookies
	userAgentInput := req.UserAgent
	if req.BrowserExport != "" {
		importedCookies, importedUserAgent := internal.ExtractBrowserImport(req.BrowserExport)
		if importedCookies != "" {
			cookieInput = importedCookies
		}
		if importedUserAgent != "" {
			userAgentInput = importedUserAgent
		}
	}

	server.Config.Cookies = internal.MergeCookieUpdate(server.Config.Cookies, cookieInput)
	server.Config.UserAgent = userAgentInput
	server.Config.BrowserMode = entity.NormalizeBrowserMode(req.BrowserMode)
	server.Config.BrowserPath = strings.TrimSpace(req.BrowserPath)
	server.Config.BrowserProfileDir = strings.TrimSpace(req.BrowserProfileDir)
	server.Config.BrowserHelperURL = strings.TrimSpace(req.BrowserHelperURL)
	server.Config.BrowserHelperToken = strings.TrimSpace(req.BrowserHelperToken)
	if req.BrowserDebugPort > 0 {
		server.Config.BrowserDebugPort = req.BrowserDebugPort
	}
	server.Config.BrowserBootstrap = req.BrowserBootstrap
	server.Config.BrowserBootstrapURL = strings.TrimSpace(req.BrowserBootstrapURL)
	server.Config.CompletedDir = req.CompletedDir
	server.Config.FinalizeMode = entity.NormalizeFinalizeMode(req.FinalizeMode)
	server.Config.FFmpegEncoder = req.FFmpegEncoder
	if req.FFmpegContainer == "mkv" {
		server.Config.FFmpegContainer = "mkv"
	} else {
		server.Config.FFmpegContainer = "mp4"
	}
	if req.FFmpegQuality > 0 {
		server.Config.FFmpegQuality = req.FFmpegQuality
	} else if server.Config.FFmpegQuality <= 0 {
		server.Config.FFmpegQuality = 23
	}
	server.Config.FFmpegPreset = req.FFmpegPreset
	if server.Config.FFmpegEncoder == "" {
		server.Config.FFmpegEncoder = "libx264"
	}
	if server.Config.FFmpegPreset == "" {
		server.Config.FFmpegPreset = "medium"
	}
	server.Config.NtfyURL = req.NtfyURL
	server.Config.NtfyTopic = req.NtfyTopic
	server.Config.NtfyToken = req.NtfyToken
	server.Config.DiscordWebhookURL = req.DiscordWebhookURL
	server.Config.DiskWarningPercent = req.DiskWarningPercent
	server.Config.DiskCriticalPercent = req.DiskCriticalPercent
	server.Config.CFChannelThreshold = req.CFChannelThreshold
	server.Config.CFGlobalThreshold = req.CFGlobalThreshold
	server.Config.NotifyCooldownHours = req.NotifyCooldownHours
	server.Config.NotifyStreamOnline = req.NotifyStreamOnline

	server.Config.PresenceCampaign = strings.TrimSpace(req.PresenceCampaign)
	server.Config.PresenceListURL = strings.TrimSpace(req.PresenceListURL)
	if v := req.PresenceInterval; v != nil {
		server.Config.PresenceInterval = 0 // 0 = the default (60 s)
		if *v >= 20 && *v <= 3600 {
			server.Config.PresenceInterval = *v
		}
	}
	if v := req.AutoMinFreeGB; v != nil && *v > 0 {
		server.Config.AutoMinFreeGB = *v
	}
	if req.DefaultFramerate == 30 || req.DefaultFramerate == 60 {
		server.Config.Framerate = req.DefaultFramerate
	}
	if req.DefaultResolution > 0 {
		server.Config.Resolution = req.DefaultResolution
	}
	if pat := strings.TrimSpace(req.DefaultPattern); pat != "" {
		if err := manager.ValidatePattern(pat); err != nil {
			c.AbortWithError(http.StatusBadRequest, fmt.Errorf("default filename pattern: %w", err))
			return
		}
		server.Config.Pattern = pat
	}
	if v := req.DefaultMaxDuration; v != nil && *v >= 0 {
		server.Config.MaxDuration = *v
	}
	if v := req.DefaultMaxFilesize; v != nil && *v >= 0 {
		server.Config.MaxFilesize = *v
	}
	if err := manager.ValidateDetectURL(req.DetectLocalURL, false); err != nil {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("detector on this computer: %w", err))
		return
	}
	if err := manager.ValidateDetectURL(req.DetectColabURL, true); err != nil {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("detector on Colab: %w", err))
		return
	}
	server.Config.DetectBackend = entity.NormalizeDetectBackend(req.DetectBackend)
	server.Config.DetectLocalURL = strings.TrimSpace(req.DetectLocalURL)
	server.Config.DetectColabURL = strings.TrimSpace(req.DetectColabURL)
	server.Config.DetectToken = strings.TrimSpace(req.DetectToken)
	server.Manager.ResetPresence() // the list may have just been configured

	if err := manager.SaveSettings(); err != nil {
		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("save settings: %w", err))
		return
	}
	c.Redirect(http.StatusFound, "/")
}
