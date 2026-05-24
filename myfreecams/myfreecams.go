package myfreecams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/site"
)

const (
	baseURL        = "https://app.myfreecams.com/"
	roomURLFormat  = baseURL + "room/%s"
	cdnReferer     = baseURL
	publicIDOffset = int64(100000000)
)

// MyFreeCams implements site.Site for the MyFreeCams platform.
type MyFreeCams struct{}

// New returns a new MyFreeCams site implementation.
func New() *MyFreeCams {
	return &MyFreeCams{}
}

type pageState struct {
	ServerState []serverStateEntry
	Servers     videoServers
}

type serverStateEntry struct {
	Type string `json:"type"`
	Data struct {
		User userState `json:"user"`
		FCS  fcsState  `json:"_fcs"`
	} `json:"data"`
}

type userState struct {
	ID              int64  `json:"id"`
	Name            string `json:"name"`
	Slug            string `json:"slug"`
	Server          string `json:"server"`
	Channel         int64  `json:"channel"`
	Phase           string `json:"phase"`
	IsOnline        bool   `json:"isOnline"`
	IsBroadcasting  bool   `json:"isBroadcasting"`
	IsPublicShow    bool   `json:"isPublicShow"`
	IsPrivateShow   bool   `json:"isPrivateShow"`
	IsGroupShow     bool   `json:"isGroupShow"`
	IsClubShow      bool   `json:"isClubShow"`
	IsWowzaOBSVideo bool   `json:"isWowzaOBSVideo"`
	IsOBSExtVideo   bool   `json:"isOBSExtVideo"`
	Viewers         int    `json:"viewers"`
	Topic           string `json:"topic"`
	Blurb           string `json:"blurb"`
	Avatar          int    `json:"avatar"`
}

type fcsState struct {
	U struct {
		CamServ int    `json:"camserv"`
		Phase   string `json:"phase"`
		Blurb   string `json:"blurb"`
		Avatar  int    `json:"avatar"`
	} `json:"u"`
	M struct {
		Topic string `json:"topic"`
		RC    int    `json:"rc"`
	} `json:"m"`
}

type videoServers struct {
	H5VideoServers   map[string]string `json:"h5video_servers"`
	NGVideoServers   map[string]string `json:"ngvideo_servers"`
	WZOBSServers     map[string]string `json:"wzobs_servers"`
	WZExtServers     map[string]string `json:"wzext_servers"`
	VideoServers     []string          `json:"video_servers"`
	AjaxServers      []string          `json:"ajax_servers"`
	ChatServers      []string          `json:"chat_servers"`
	WebsocketServers map[string]string `json:"websocket_servers"`
}

// FetchStream implements site.Site. Returns StreamInfo when online, nil when offline.
func (m *MyFreeCams) FetchStream(ctx context.Context, req *internal.Req, username string) (*site.StreamInfo, error) {
	roomURL := fmt.Sprintf(roomURLFormat, url.PathEscape(username))
	httpReq, cancel, err := req.CreateRequest(ctx, roomURL)
	if err != nil {
		return nil, fmt.Errorf("myfreecams: create request: %w", err)
	}
	defer cancel()

	httpReq.Header.Set("Referer", baseURL)
	httpReq.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	body, err := req.DoRequest(httpReq)
	if err != nil {
		return nil, fmt.Errorf("myfreecams: fetch room: %w", err)
	}

	state, err := parsePageState(body)
	if err != nil {
		return nil, fmt.Errorf("myfreecams: parse room state: %w", err)
	}

	entry := state.findUser(username)
	if entry == nil || entry.Data.User.ID == 0 {
		return nil, internal.ErrChannelOffline
	}

	u := entry.Data.User
	fcs := entry.Data.FCS
	info := &site.StreamInfo{
		RoomTitle:        firstNonEmpty(u.Topic, fcs.M.Topic, u.Blurb, fcs.U.Blurb),
		NumViewers:       firstPositive(u.Viewers, fcs.M.RC),
		SummaryCardImage: avatarURL(u.ID, firstPositive(u.Avatar, fcs.U.Avatar)),
		CDNReferer:       cdnReferer,
	}

	if !u.IsOnline || !u.IsBroadcasting {
		return info, internal.ErrChannelOffline
	}
	if !u.IsPublicShow {
		if u.IsPrivateShow || u.IsGroupShow || u.IsClubShow {
			return info, internal.ErrPrivateStream
		}
		return info, internal.ErrChannelOffline
	}

	serverNum := resolveServerNum(u, fcs, state.Servers)
	channel := u.Channel
	if channel == 0 && u.ID > 0 {
		channel = u.ID + publicIDOffset
	}
	if serverNum == "" || channel == 0 {
		return info, internal.ErrChannelOffline
	}

	if liveThumb := liveThumbURL(serverNum, u.ID); liveThumb != "" {
		info.LiveThumbURL = liveThumb
	}

	hlsURL, err := resolveHLSURL(ctx, serverNum, channel, candidatePhases(u, fcs))
	if err != nil {
		return info, err
	}
	info.HLSURL = hlsURL
	return info, nil
}

// FetchLastBroadcast implements site.Site. MyFreeCams exposes this as display
// text in the room page, but not as a stable timestamp in the embedded state.
func (m *MyFreeCams) FetchLastBroadcast(_ context.Context, _ *internal.Req, _ string) (int64, error) {
	return 0, nil
}

func (p pageState) findUser(username string) *serverStateEntry {
	slug := strings.ToLower(username)
	for i := range p.ServerState {
		entry := &p.ServerState[i]
		if entry.Type != "SERVER_SIDE_USER" {
			continue
		}
		u := entry.Data.User
		if strings.EqualFold(u.Slug, slug) || strings.EqualFold(u.Name, username) {
			return entry
		}
	}
	return nil
}

func parsePageState(page string) (*pageState, error) {
	serverStateJSON, err := extractJSValue(page, "serverState:", '[', ']')
	if err != nil {
		return nil, err
	}
	serversJSON, err := extractJSValue(page, "servers:", '{', '}')
	if err != nil {
		return nil, err
	}

	var state pageState
	if err := json.Unmarshal([]byte(serverStateJSON), &state.ServerState); err != nil {
		return nil, fmt.Errorf("decode serverState: %w", err)
	}
	if err := json.Unmarshal([]byte(serversJSON), &state.Servers); err != nil {
		return nil, fmt.Errorf("decode servers: %w", err)
	}
	return &state, nil
}

func extractJSValue(page, marker string, open, close byte) (string, error) {
	markerIdx := strings.Index(page, marker)
	if markerIdx == -1 {
		return "", fmt.Errorf("could not find %s", strings.TrimSuffix(marker, ":"))
	}
	startRel := strings.IndexByte(page[markerIdx:], open)
	if startRel == -1 {
		return "", fmt.Errorf("could not find %q after %s", open, strings.TrimSuffix(marker, ":"))
	}
	start := markerIdx + startRel

	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(page); i++ {
		c := page[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch c {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case open:
			depth++
		case close:
			depth--
			if depth == 0 {
				return page[start : i+1], nil
			}
		}
	}
	return "", fmt.Errorf("unterminated %s value", strings.TrimSuffix(marker, ":"))
}

func resolveServerNum(u userState, fcs fcsState, servers videoServers) string {
	if num := digitsOnly(u.Server); num != "" {
		return num
	}
	camserv := fmt.Sprintf("%d", fcs.U.CamServ)
	for _, group := range []map[string]string{
		servers.WZExtServers,
		servers.WZOBSServers,
		servers.NGVideoServers,
		servers.H5VideoServers,
	} {
		if name, ok := group[camserv]; ok {
			return digitsOnly(name)
		}
	}
	return ""
}

func candidatePhases(u userState, fcs fcsState) []string {
	var phases []string
	add := func(phase string) {
		for _, existing := range phases {
			if existing == phase {
				return
			}
		}
		phases = append(phases, phase)
	}

	phase := firstNonEmpty(u.Phase, fcs.U.Phase)
	if phase != "" && phase != "z" {
		add(phase)
	}
	if u.IsWowzaOBSVideo || u.IsOBSExtVideo {
		add("a")
	}
	add("")
	if phase == "z" {
		add("z")
	}
	return phases
}

func resolveHLSURL(ctx context.Context, serverNum string, channel int64, phases []string) (string, error) {
	client := internal.NewMediaReqWithReferer(cdnReferer)
	var lastErr error
	for _, phase := range phases {
		room := fmt.Sprintf("mfc_%d", channel)
		if phase != "" {
			room = fmt.Sprintf("mfc_%s_%d", phase, channel)
		}
		hlsURL := fmt.Sprintf(
			"https://edgevideo.myfreecams.com/hls/NxServer/%s/ngrp:%s.f4v_mobile/playlist.m3u8",
			serverNum,
			room,
		)
		body, err := client.Get(ctx, hlsURL)
		if err != nil {
			lastErr = err
			continue
		}
		if strings.HasPrefix(body, "#EXTM3U") {
			return hlsURL, nil
		}
		lastErr = fmt.Errorf("not an m3u8 playlist")
	}
	if lastErr != nil {
		return "", fmt.Errorf("myfreecams: resolve hls playlist: %w", lastErr)
	}
	return "", fmt.Errorf("myfreecams: no hls playlist candidates")
}

func avatarURL(modelID int64, avatar int) string {
	if modelID <= 0 || avatar <= 0 {
		return ""
	}
	return fmt.Sprintf("https://img.mfcimg.com/photos2/%d/%d/avatar.300x300.jpg", modelID/100000, modelID)
}

func liveThumbURL(serverNum string, modelID int64) string {
	if serverNum == "" || modelID <= 0 {
		return ""
	}
	return fmt.Sprintf("https://snap.mfcimg.com/snapimg/%s/427x240/mfc_%d?no-cache=0", serverNum, modelID)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func firstPositive(values ...int) int {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}

var nonDigitRe = regexp.MustCompile(`\D+`)

func digitsOnly(value string) string {
	return nonDigitRe.ReplaceAllString(value, "")
}

// ensure MyFreeCams implements site.Site at compile time.
var _ site.Site = (*MyFreeCams)(nil)
