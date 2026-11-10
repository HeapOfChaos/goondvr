package server

import (
	"context"
	"net/http"

	"github.com/HeapOfChaos/goondvr/detect"
	"github.com/HeapOfChaos/goondvr/entity"
)

var Manager IManager

type IManager interface {
	CreateChannel(conf *entity.ChannelConfig, shouldSave bool) error
	StopChannel(channelID string) error
	PauseChannel(channelID string) error
	ResumeChannel(channelID string) error
	// UpdateChannel changes the settings of one channel (see entity.ChannelPatch). The
	// error says what is wrong with a value; it is meant to be shown to the user.
	UpdateChannel(channelID string, p entity.ChannelPatch) error
	ChannelInfo() []*entity.ChannelInfo
	// Publish pushes the given kind of update to the web UI. It never blocks and
	// reports false when the event could not be queued (slow browser), in which
	// case the caller should retry later.
	Publish(name string, ch *entity.ChannelInfo) bool
	Subscriber(w http.ResponseWriter, r *http.Request)
	LoadConfig() error
	SaveConfig() error
	Shutdown()
	GetChannelThumb(channelID string) string
	// GetChannelLiveThumb returns the address of the newest live preview of the
	// channel; for sites whose previews are snapshots it asks the site for a newer
	// one when the last one is a few seconds old.
	GetChannelLiveThumb(ctx context.Context, channelID string) string
	ReportCFBlock(username string)
	ResetCFBlock(username string)
	GetStats() StatsResponse
	// ResetPresence makes the detection of the channels on the air try its main method
	// again right away (the settings that control it were saved).
	ResetPresence()
	// DetectModels asks the image detector (the one in the settings, or the one described by override
	// when it is not nil) which models it has; it also proves that the address and token work.
	DetectModels(ctx context.Context, override *DetectSettings) DetectModelsResponse
	// HasViewers reports whether any browser has the web UI open, so that work
	// done only for the sake of the UI (renewing previews) can be skipped.
	HasViewers() bool
}

// DetectSettings are the detector settings of the settings form, before they are saved.
type DetectSettings struct{ Backend, LocalURL, ColabURL, Token string }

// DetectModelsResponse is the answer of /api/detect/models.
type DetectModelsResponse struct {
	OK      bool               `json:"ok"`
	Backend string             `json:"backend"`
	Error   string             `json:"error,omitempty"`
	Models  []detect.ModelInfo `json:"models"`
}

// StatsResponse holds system stats returned by the /api/stats endpoint.
type StatsResponse struct {
	DiskPath       string  `json:"disk_path"`
	DiskUsedBytes  uint64  `json:"disk_used_bytes"`
	DiskTotalBytes uint64  `json:"disk_total_bytes"`
	DiskPercent    float64 `json:"disk_percent"`
	UptimeSeconds  int64   `json:"uptime_seconds"`
	RecordingCount int     `json:"recording_count"`
	// Presence says how the "on air" marks of the channels are being kept up to date.
	Presence PresenceStatus `json:"presence"`
}

// PresenceStatus is the state of the detection of the channels that are on the air.
type PresenceStatus struct {
	Method string `json:"method"`           // "list" (main), "probe" (per-channel fallback) or "" (nothing to watch)
	AgeSec int    `json:"age_sec"`          // seconds since the last answer, -1 = none yet
	Detail string `json:"detail,omitempty"` // "no-list-config": the list needs an affiliate id; "list-failing": the list failed and the fallback is in use
}
