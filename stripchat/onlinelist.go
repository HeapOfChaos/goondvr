package stripchat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/site"
)

// Like siren, Stripchat is asked for the status of many models in one request:
// each channel's numeric id is looked up once (and remembered), then
// models/get-statuses answers for a whole batch of ids. Models that are not in
// the answer are off the air.

var (
	apiBase     = "https://stripchat.com" // a variable so that tests can point it at a fake site
	statusBatch = 100                     // ids per request
	idLookupGap = 300 * time.Millisecond  // pause between two id lookups (only the first time a channel is seen)
	idCache     sync.Map                  // lower-case username -> int64 (0 = no such model)
)

type userIDAnswer struct {
	ID          int64  `json:"id"`
	NewUsername string `json:"newUsername"`
}

type statusesAnswer struct {
	Statuses []struct {
		ID     int64  `json:"id"`
		Status string `json:"status"`
	} `json:"statuses"`
}

// resolveID returns the numeric id of a model (0 when it does not exist).
func resolveID(ctx context.Context, req *internal.Req, name string) (int64, error) {
	if v, ok := idCache.Load(name); ok {
		return v.(int64), nil
	}
	apiURL := fmt.Sprintf("%s/api/front/users/user-ids/%s", apiBase, url.PathEscape(name))
	body, err := req.GetBytes(ctx, apiURL)
	if errors.Is(err, internal.ErrNotFound) { // HTTP 404: there is no such model
		idCache.Store(name, int64(0))
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("stripchat: look up id of %s: %w", name, err)
	}
	var a userIDAnswer
	if err := json.Unmarshal(body, &a); err != nil {
		return 0, fmt.Errorf("stripchat: id of %s is not JSON: %w", name, err)
	}
	if a.ID <= 0 || a.NewUsername != "" { // an old name of a renamed model is not the model
		a.ID = 0
	}
	idCache.Store(name, a.ID)
	return a.ID, nil
}

// ListOnline implements site.OnlineListSource.
func (s *Stripchat) ListOnline(ctx context.Context, req *internal.Req, want map[string]struct{}) (map[string]site.Presence, error) {
	names := make([]string, 0, len(want))
	for n := range want {
		names = append(names, n)
	}
	sort.Strings(names)

	byID := make(map[int64]string, len(names))
	for _, n := range names {
		_, cached := idCache.Load(n)
		id, err := resolveID(ctx, req, n)
		if err != nil {
			return nil, err
		}
		if id > 0 {
			byID[id] = n
		}
		if !cached {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(idLookupGap):
			}
		}
	}
	found := make(map[string]site.Presence, len(byID))
	if len(byID) == 0 {
		return found, nil
	}

	ids := make([]int64, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for start := 0; start < len(ids); start += statusBatch {
		end := start + statusBatch
		if end > len(ids) {
			end = len(ids)
		}
		payload, _ := json.Marshal(map[string][]int64{"ids": ids[start:end]})
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		httpReq, err := http.NewRequestWithContext(cctx, http.MethodPost, apiBase+"/api/front/models/get-statuses", bytes.NewReader(payload))
		if err != nil {
			cancel()
			return nil, err
		}
		req.SetRequestHeaders(httpReq)
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Accept", "application/json, text/plain, */*")
		httpReq.Header.Set("Origin", apiBase)
		httpReq.Header.Set("Referer", apiBase+"/")
		body, err := req.DoRequest(httpReq)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("stripchat: get statuses: %w", err)
		}
		var a statusesAnswer
		if err := json.Unmarshal([]byte(body), &a); err != nil {
			return nil, fmt.Errorf("stripchat: statuses are not JSON: %w", err)
		}
		for _, st := range a.Statuses {
			if name, ok := byID[st.ID]; ok {
				status := strings.ToLower(st.Status)
				found[name] = site.Presence{Live: status == "public", Show: status}
			}
		}
		if end < len(ids) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(idLookupGap):
			}
		}
	}
	return found, nil
}
