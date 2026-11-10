package manager

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/HeapOfChaos/goondvr/channel"
	"github.com/HeapOfChaos/goondvr/detect"
	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/server"
)

// Image detection engine. Once a second it looks at the channels that use detection (a read-locked check
// each; the others cost nothing) and hands the ones whose time has come to a few workers, which fetch a
// picture, ask the detector, and record the answer in the channel (channel/detect.go).
//
// The detector is a separate program (detector/server.py) that runs on this computer or on Google Colab,
// whichever the settings say. If it is down, each channel backs off by itself and the problem is shown on
// its card; the log gets one line per minute, not one per channel.

// DefaultDetectLocalURL is where detector/server.py listens when it is started without options.
const DefaultDetectLocalURL = "http://127.0.0.1:8099"

var (
	detectTick    = time.Second
	detectWorkers = 3
)

var modelNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

func detectTarget(c *entity.Config) detect.Target {
	if c.DetectBackend == "colab" {
		return detect.Target{URL: c.DetectColabURL, Token: c.DetectToken}
	}
	u := c.DetectLocalURL
	if strings.TrimSpace(u) == "" {
		u = DefaultDetectLocalURL
	}
	return detect.Target{URL: u, Token: c.DetectToken}
}

type detectJob struct {
	ch  *channel.Channel
	cfg entity.DetectConfig
}

func (m *Manager) detectLoop(ctx context.Context) {
	jobs := make(chan detectJob, 64)
	var logMu sync.Mutex
	var lastLog time.Time
	var lastMsg string
	report := func(err error) { // one line a minute (or when the problem changes), not one per channel
		logMu.Lock()
		defer logMu.Unlock()
		if err.Error() != lastMsg || time.Since(lastLog) > time.Minute {
			lastMsg, lastLog = err.Error(), time.Now()
			log.Printf("WARN  image detection: %v", err)
		}
	}
	for i := 0; i < detectWorkers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case j := <-jobs:
					m.detectOne(ctx, j, report)
				}
			}
		}()
	}
	tick := time.NewTicker(detectTick)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			m.Channels.Range(func(_, v any) bool {
				ch := v.(*channel.Channel)
				if !ch.DetectActive() {
					return true
				}
				if cfg, due := ch.DetectDue(now); due {
					select {
					case jobs <- detectJob{ch, cfg}:
					default: // the workers are behind: try again next second
						ch.DetectRelease()
					}
				}
				return true
			})
		}
	}
}

func (m *Manager) detectOne(ctx context.Context, j detectJob, report func(error)) {
	frame, err := j.ch.DetectFrame(ctx, j.cfg)
	if errors.Is(err, channel.ErrNoFrameYet) || ctx.Err() != nil {
		j.ch.DetectRelease() // nothing to look at yet / closing: not a failure
		return
	}
	var res *detect.Result
	if err == nil {
		res, err = detect.Detect(ctx, detectTarget(server.Config), j.cfg.Model, frame)
		if err != nil {
			report(err)
		}
	}
	if j.ch.DetectDone(j.cfg, res, err) {
		j.ch.UpdateNow()
	}
	if j.cfg.Gate {
		m.autoStep() // a match may start the recording; a long silence may stop it
	}
}

// DetectModels asks the detector for its models.
func (m *Manager) DetectModels(ctx context.Context, o *server.DetectSettings) server.DetectModelsResponse {
	cfg := *server.Config
	if o != nil {
		cfg.DetectBackend = entity.NormalizeDetectBackend(o.Backend)
		cfg.DetectLocalURL, cfg.DetectColabURL, cfg.DetectToken = o.LocalURL, o.ColabURL, o.Token
	}
	resp := server.DetectModelsResponse{Backend: cfg.DetectBackend, Models: []detect.ModelInfo{}}
	models, err := detect.Models(ctx, detectTarget(&cfg))
	if err != nil {
		resp.Error = err.Error()
		return resp
	}
	resp.OK = true
	if models != nil {
		resp.Models = models
	}
	return resp
}

// ValidateDetectURL checks an address of the detector from the settings form. The Colab one carries the
// token over the internet, so it must be https.
func ValidateDetectURL(raw string, needHTTPS bool) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("%q is not a valid address (it must start with http:// or https://)", raw)
	}
	if needHTTPS && u.Scheme != "https" {
		return fmt.Errorf("the Colab address must start with https:// (the token travels with it)")
	}
	return nil
}

// validateDetect checks the detection settings of a channel.
func validateDetect(d *entity.DetectConfig) error {
	if d.Enabled && !modelNameRe.MatchString(d.Model) {
		return fmt.Errorf("detection: choose a model (letters, numbers, _ . - only)")
	}
	if d.Source != "" && d.Source != "thumb" && d.Source != "stream" {
		return fmt.Errorf("detection: the picture comes from the preview or from the recording")
	}
	if d.Gate && d.Source == "stream" {
		return fmt.Errorf("detection: recording only while it matches needs the preview picture (a channel that is not recorded has no recording to look at)")
	}
	if len(d.Labels) > 30 {
		return fmt.Errorf("detection: too many labels (30 at most)")
	}
	for _, l := range d.Labels {
		if l == "" || len(l) > 64 {
			return fmt.Errorf("detection: a label is empty or too long")
		}
	}
	if d.MinScore < 0 || d.MinScore > 1 {
		return fmt.Errorf("detection: the minimum score is between 1 and 100 %%")
	}
	if d.Interval != 0 && (d.Interval < 2 || d.Interval > 3600) {
		return fmt.Errorf("detection: check every 2 to 3600 seconds")
	}
	if d.Idle != 0 && (d.Idle < 5 || d.Idle > 86400) {
		return fmt.Errorf("detection: stop after 5 to 86400 seconds without a match")
	}
	if d.Pre < 0 || d.Pre > 300 || d.Post < 0 || d.Post > 300 {
		return fmt.Errorf("detection: seconds before/after a clip are 0 to 300")
	}
	return nil
}
