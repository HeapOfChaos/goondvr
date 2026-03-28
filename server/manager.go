package server

import (
	"net/http"

	"github.com/teacat/chaturbate-dvr/entity"
)

var Manager IManager

type IManager interface {
	CreateChannel(conf *entity.ChannelConfig, shouldSave bool) error
	StopChannel(username string) error
	PauseChannel(username string) error
	ResumeChannel(username string) error
	ChannelInfo() []*entity.ChannelInfo
	Publish(name string, ch *entity.ChannelInfo)
	Subscriber(w http.ResponseWriter, r *http.Request)
	LoadConfig() error
	SaveConfig() error
	Shutdown()
	GetChannelThumb(username string) string
	ReportCFBlock(username string)
	ResetCFBlock(username string)
	GetStats() StatsResponse
}

// StatsResponse holds system stats returned by the /api/stats endpoint.
type StatsResponse struct {
	DiskPath       string  `json:"disk_path"`
	DiskUsedBytes  uint64  `json:"disk_used_bytes"`
	DiskTotalBytes uint64  `json:"disk_total_bytes"`
	DiskPercent    float64 `json:"disk_percent"`
	UptimeSeconds  int64   `json:"uptime_seconds"`
	RecordingCount int     `json:"recording_count"`
}
