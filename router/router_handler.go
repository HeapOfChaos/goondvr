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

// StopChannel stops a channel.
func StopChannel(c *gin.Context) {
	server.Manager.StopChannel(c.Param("channelID"))

	c.Redirect(http.StatusFound, "/")
}

// PauseChannel pauses a channel.
func PauseChannel(c *gin.Context) {
	server.Manager.PauseChannel(c.Param("channelID"))

	c.Redirect(http.StatusFound, "/")
}

// ResumeChannel resumes a paused channel.
func ResumeChannel(c *gin.Context) {
	server.Manager.ResumeChannel(c.Param("channelID"))

	c.Redirect(http.StatusFound, "/")
}

// ThumbProxy proxies the channel's summary card image from the CDN through the server.
// This avoids hotlink-protection issues when the browser requests the image directly.
func ThumbProxy(c *gin.Context) {
	imgURL := server.Manager.GetChannelThumb(c.Param("channelID"))
	if imgURL == "" {
		c.Status(http.StatusNotFound)
		return
	}

	req := internal.NewMediaReq()
	imgBytes, err := req.GetBytes(c.Request.Context(), imgURL)
	if err != nil {
		c.Status(http.StatusBadGateway)
		return
	}

	contentType := http.DetectContentType(imgBytes)
	c.Data(http.StatusOK, contentType, imgBytes)
}

// LiveThumbProxy proxies the channel's live-updating thumbnail from the CDN.
// For Stripchat this uses img.doppiocdn.net; for Chaturbate it falls back to
// the summary card image (the JS handles Chaturbate live thumbs directly).
func LiveThumbProxy(c *gin.Context) {
	imgURL := server.Manager.GetChannelLiveThumb(c.Param("channelID"))
	if imgURL == "" {
		c.Status(http.StatusNotFound)
		return
	}

	referer := "https://stripchat.com/"
	if strings.Contains(imgURL, "mfcimg.com") {
		referer = "https://app.myfreecams.com/"
	}
	req := internal.NewMediaReqWithReferer(referer)
	imgBytes, err := req.GetBytes(c.Request.Context(), imgURL)
	if err != nil {
		c.Status(http.StatusBadGateway)
		return
	}

	contentType := http.DetectContentType(imgBytes)
	c.Data(http.StatusOK, contentType, imgBytes)
}

// Updates handles the SSE connection for updates.
func Updates(c *gin.Context) {
	server.Manager.Subscriber(c.Writer, c.Request)
}

// Stats returns system stats as JSON for the header stats bar.
func Stats(c *gin.Context) {
	c.JSON(http.StatusOK, server.Manager.GetStats())
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

	if err := manager.SaveSettings(); err != nil {
		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("save settings: %w", err))
		return
	}
	c.Redirect(http.StatusFound, "/")
}
