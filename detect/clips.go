package detect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/HeapOfChaos/goondvr/persist"
)

// Span is a stretch of a recording in which the detector kept finding a match, in seconds from its start.
type Span struct {
	Start float64 `json:"start"`
	End   float64 `json:"end"`
	Label string  `json:"label"`
	Score float64 `json:"score"`
}

// Sidecar is the list of matches of one recording, saved next to it as <recording>.detect.json.
type Sidecar struct {
	Pre   int    `json:"pre"`
	Post  int    `json:"post"`
	Clips bool   `json:"clips"` // the channel asked for clips
	Spans []Span `json:"spans"`
}

// SidecarPath is where the matches of a recording are kept.
func SidecarPath(recording string) string { return recording + ".detect.json" }

// WriteSidecar saves the matches of a recording.
func WriteSidecar(recording string, sc Sidecar) error {
	b, err := json.MarshalIndent(sc, "", " ")
	if err != nil {
		return err
	}
	return persist.WriteFile(SidecarPath(recording), b, 0644)
}

// ReadSidecar reads what WriteSidecar saved.
func ReadSidecar(path string) (*Sidecar, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var sc Sidecar
	if err := json.Unmarshal(b, &sc); err != nil {
		return nil, err
	}
	return &sc, nil
}

// Window is a part of a recording to keep as a clip, in seconds.
type Window struct{ From, To float64 }

const (
	minClip  = 1.0
	maxClips = 50
)

// Windows turns the spans into the parts to cut: each one gets pre seconds before and post after, and the
// ones that touch are joined.
func Windows(spans []Span, pre, post int) []Window {
	w := make([]Window, 0, len(spans))
	for _, s := range spans {
		from := s.Start - float64(pre)
		if from < 0 {
			from = 0
		}
		w = append(w, Window{from, s.End + float64(post)})
	}
	sort.Slice(w, func(i, j int) bool { return w[i].From < w[j].From })
	var out []Window
	for _, x := range w {
		if n := len(out); n > 0 && x.From <= out[n-1].To {
			if x.To > out[n-1].To {
				out[n-1].To = x.To
			}
			continue
		}
		out = append(out, x)
	}
	return out
}

// CutClips copies each window of the recording into its own file next to the recording
// (<name>_clip01.mp4, ...) without re-encoding, so the cut points fall on the nearest key frame before.
// It returns the files it made.
func CutClips(ctx context.Context, recording string, wins []Window) ([]string, error) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		return nil, errors.New("ffmpeg is needed to cut clips and was not found in PATH")
	}
	ext := filepath.Ext(recording)
	base := strings.TrimSuffix(recording, ext)
	var made []string
	var firstErr error
	n := 0
	for _, w := range wins {
		if w.To-w.From < minClip {
			continue
		}
		if n++; n > maxClips {
			if firstErr == nil {
				firstErr = fmt.Errorf("more than %d clips: the rest were not cut", maxClips)
			}
			break
		}
		final := fmt.Sprintf("%s_clip%02d%s", base, n, ext)
		tmp := fmt.Sprintf("%s_clip%02d.part%s", base, n, ext)
		args := []string{"-nostdin", "-y", "-v", "error", "-ss", fmt.Sprintf("%.2f", w.From), "-i", recording,
			"-t", fmt.Sprintf("%.2f", w.To-w.From), "-c", "copy", "-avoid_negative_ts", "make_zero"}
		if strings.EqualFold(ext, ".mp4") {
			args = append(args, "-movflags", "+faststart")
		}
		out, err := exec.CommandContext(ctx, ffmpeg, append(args, tmp)...).CombinedOutput()
		if err == nil {
			err = os.Rename(tmp, final)
		}
		if err != nil {
			_ = os.Remove(tmp)
			if firstErr == nil {
				firstErr = fmt.Errorf("clip %d: %s", n, strings.TrimSpace(string(out)+" "+err.Error()))
			}
			continue
		}
		made = append(made, final)
	}
	return made, firstErr
}
