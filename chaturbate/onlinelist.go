package chaturbate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/site"
)

// The list of rooms on the air, as used by siren (github.com/bcmk/siren): one
// request to an affiliate endpoint returns the rooms that are broadcasting
// (username, current_show, num_users, room_subject, tags...) so a single sweep
// answers "who is on the air?" for every channel at once, however many there
// are, instead of asking the site about each channel.
//
// The affiliate endpoint needs the affiliate (campaign) id of the person
// running the program (Settings), or a complete address of one's own
// (Settings, advanced; any siren "users_online_endpoint" works).

const (
	onlineListPageLimit = 500
	onlineListMaxPages  = 60 // 30000 rooms; far more than the site ever has on air
	onlineListPageGap   = 250 * time.Millisecond
)

// onlineRoom is the subset of a room of the list that is used.
type onlineRoom struct {
	Username    string   `json:"username"`
	CurrentShow string   `json:"current_show"`
	NumUsers    int      `json:"num_users"`
	RoomSubject string   `json:"room_subject"`
	Subject     string   `json:"subject"` // the site's own front-end list calls it so
	Tags        []string `json:"tags"`
}

// onlinePage is one answer of the list. The affiliate API answers
// {"count": N, "results": [...]}; siren's endpoint answers a plain array.
type onlinePage struct {
	Count      int          `json:"count"`
	TotalCount int          `json:"total_count"`
	Results    []onlineRoom `json:"results"`
	Rooms      []onlineRoom `json:"rooms"`
}

// parseOnlinePage reads one answer; total is the number of rooms the list says
// it has (0 = not said, i.e. the answer is the whole list).
func parseOnlinePage(body []byte) (rooms []onlineRoom, total int, err error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, 0, errors.New("empty answer")
	}
	if body[0] == '[' {
		if err := json.Unmarshal(body, &rooms); err != nil {
			return nil, 0, err
		}
		return rooms, 0, nil
	}
	var p onlinePage
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, 0, err
	}
	rooms = p.Results
	if rooms == nil {
		rooms = p.Rooms
	}
	total = p.Count
	if total == 0 {
		total = p.TotalCount
	}
	return rooms, total, nil
}

// onlineListURL is the address of the list, or ErrListNotConfigured.
func onlineListURL() (*url.URL, error) {
	raw := strings.TrimSpace(server.Config.PresenceListURL)
	if raw == "" {
		campaign := strings.TrimSpace(server.Config.PresenceCampaign)
		if campaign == "" {
			return nil, site.ErrListNotConfigured
		}
		q := url.Values{}
		q.Set("wm", campaign)
		q.Set("client_ip", "request_ip")
		q.Set("format", "json")
		raw = server.Config.Domain + "api/public/affiliates/onlinerooms/?" + q.Encode()
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("chaturbate: the online list address is not valid")
	}
	return u, nil
}

// ListOnline implements site.OnlineListSource.
func (cb *Chaturbate) ListOnline(ctx context.Context, req *internal.Req, want map[string]struct{}) (map[string]site.Presence, error) {
	base, err := onlineListURL()
	if err != nil {
		return nil, err
	}
	found := make(map[string]site.Presence, len(want))
	seen, total := 0, 0
	for page := 0; page < onlineListMaxPages; page++ {
		u := *base
		q := u.Query()
		if q.Get("limit") == "" {
			q.Set("limit", strconv.Itoa(onlineListPageLimit))
		}
		if page > 0 {
			q.Set("offset", strconv.Itoa(seen))
		}
		u.RawQuery = q.Encode()

		body, err := req.GetBytes(ctx, u.String())
		if err != nil {
			if errors.Is(err, internal.ErrPrivateStream) { // HTTP 403
				return nil, errors.New("chaturbate: the online list refused the request (is the affiliate id right?)")
			}
			return nil, err
		}
		rooms, count, err := parseOnlinePage(body)
		if err != nil {
			return nil, fmt.Errorf("chaturbate: online list is not valid JSON: %w", err)
		}
		for i := range rooms {
			r := &rooms[i]
			name := strings.ToLower(r.Username)
			if _, ok := want[name]; !ok {
				continue
			}
			subject := r.RoomSubject
			if subject == "" {
				subject = r.Subject
			}
			show := strings.ToLower(r.CurrentShow)
			found[name] = site.Presence{
				Live:       show == "public",
				Show:       show,
				NumViewers: r.NumUsers,
				Subject:    subject,
				Tags:       lowerTags(r.Tags),
			}
		}
		seen += len(rooms)
		total = count
		if total == 0 || len(rooms) == 0 || seen >= total {
			if seen == 0 {
				return nil, errors.New("chaturbate: the online list has no rooms") // siren: zero models is an error
			}
			return found, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(onlineListPageGap):
		}
	}
	return nil, errors.New("chaturbate: the online list is too long")
}

func lowerTags(tags []string) []string {
	if len(tags) == 0 {
		return nil
	}
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		t = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(t, "#")))
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}
