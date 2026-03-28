package router

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/manager"
	"github.com/teacat/chaturbate-dvr/server"
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

	var errs []string
	for _, username := range strings.Split(req.Username, ",") {
		if err := server.Manager.CreateChannel(&entity.ChannelConfig{
			IsPaused:    false,
			Username:    username,
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
	server.Manager.StopChannel(c.Param("username"))

	c.Redirect(http.StatusFound, "/")
}

// PauseChannel pauses a channel.
func PauseChannel(c *gin.Context) {
	server.Manager.PauseChannel(c.Param("username"))

	c.Redirect(http.StatusFound, "/")
}

// ResumeChannel resumes a paused channel.
func ResumeChannel(c *gin.Context) {
	server.Manager.ResumeChannel(c.Param("username"))

	c.Redirect(http.StatusFound, "/")
}

// ThumbProxy proxies the channel's summary card image from the CDN through the server.
// This avoids hotlink-protection issues when the browser requests the image directly.
func ThumbProxy(c *gin.Context) {
	imgURL := server.Manager.GetChannelThumb(c.Param("username"))
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
	Cookies             string `form:"cookies"`
	UserAgent           string `form:"user_agent"`
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

	server.Config.Cookies = req.Cookies
	server.Config.UserAgent = req.UserAgent
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
