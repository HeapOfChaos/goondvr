// Package detect talks to the image detector (detector/server.py), takes frames out of a stream, and
// cuts clips. It knows nothing about channels.
package detect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Target says where the detector runs.
type Target struct {
	URL   string // http://127.0.0.1:8099, or the https address of the Colab tunnel
	Token string // sent as "Authorization: Bearer"; may be empty for a local detector without one
}

// Box is one object found by a detector of boxes; Box is x1,y1,x2,y2 as 0..1 of the picture.
type Box struct {
	Label string     `json:"label"`
	Score float64    `json:"score"`
	Box   [4]float64 `json:"box"`
}

// Result is what the detector says about one picture.
type Result struct {
	Scores   map[string]float64 `json:"scores"` // best score of each label
	Boxes    []Box              `json:"boxes,omitempty"`
	Model    string             `json:"model"`
	Kind     string             `json:"kind"` // "embed" (classifier) or "yolox" (boxes)
	Provider string             `json:"provider"`
	MS       float64            `json:"ms"`
}

// ModelInfo describes one model the detector has.
type ModelInfo struct {
	Name     string   `json:"name"`
	Kind     string   `json:"kind"`
	Labels   []string `json:"labels"`
	Provider string   `json:"provider"`
	Error    string   `json:"error,omitempty"`
}

// Best returns the label with the highest score among labels (any label when labels is empty), if that
// score reaches min.
func (r *Result) Best(labels []string, min float64) (label string, score float64, ok bool) {
	if r == nil {
		return "", 0, false
	}
	want := map[string]bool{}
	for _, l := range labels {
		want[strings.ToLower(strings.TrimSpace(l))] = true
	}
	for l, s := range r.Scores {
		if len(want) > 0 && !want[strings.ToLower(l)] {
			continue
		}
		if s > score || label == "" {
			label, score = l, s
		}
	}
	return label, score, label != "" && score >= min
}

var client = &http.Client{}

const maxReply = 1 << 20

func (t Target) do(ctx context.Context, method, path string, body []byte, ctype string, timeout time.Duration) ([]byte, error) {
	base := strings.TrimRight(strings.TrimSpace(t.URL), "/")
	if base == "" {
		return nil, errors.New("the detector address is empty (Settings → Image detection)")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("the detector address %q is not valid", t.URL)
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if t.Token != "" {
		req.Header.Set("Authorization", "Bearer "+t.Token)
	}
	resp, err := client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err // the address is in the message already
		}
		return nil, fmt.Errorf("the detector does not answer at %s (is it running?): %w", base, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxReply))
	if err != nil {
		return nil, fmt.Errorf("reading the answer of the detector: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(b, &e)
		switch {
		case resp.StatusCode == http.StatusUnauthorized:
			return nil, errors.New("the detector refused the token (Settings → Image detection)")
		case e.Error != "":
			return nil, fmt.Errorf("detector: %s", e.Error)
		}
		return nil, fmt.Errorf("detector: HTTP %d", resp.StatusCode)
	}
	return b, nil
}

// Detect sends a picture (JPEG or PNG) to the model and returns what it found.
func Detect(ctx context.Context, t Target, model string, img []byte) (*Result, error) {
	b, err := t.do(ctx, http.MethodPost, "/detect?model="+url.QueryEscape(model), img, "image/jpeg", 20*time.Second)
	if err != nil {
		return nil, err
	}
	var r Result
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, errors.New("the detector gave an answer that is not understood")
	}
	return &r, nil
}

// Models lists the models of the detector (it also proves that the address and the token are right).
func Models(ctx context.Context, t Target) ([]ModelInfo, error) {
	b, err := t.do(ctx, http.MethodGet, "/models", nil, "", 20*time.Second)
	if err != nil {
		return nil, err
	}
	var r struct {
		Models []ModelInfo `json:"models"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, errors.New("the detector gave an answer that is not understood")
	}
	return r.Models, nil
}
