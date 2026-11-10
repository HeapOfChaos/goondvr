package stripchat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/site"
)

// Stripchat implements site.Site for the Stripchat platform.
type Stripchat struct{}

// New returns a new Stripchat site implementation.
func New() *Stripchat {
	return &Stripchat{}
}

// camResponse is the relevant subset of the Stripchat cam API response.
type camResponse struct {
	Cam struct {
		StreamName        string            `json:"streamName"`
		IsCamActive       bool              `json:"isCamActive"`
		ViewServers       map[string]string `json:"viewServers"`
		BroadcastSettings struct {
			BroadcastType string `json:"broadcastType"`
		} `json:"broadcastSettings"`
		Topic string `json:"topic"`
	} `json:"cam"`
	User struct {
		User struct {
			ID                 int64  `json:"id"`
			Username           string `json:"username"`
			IsOnline           bool   `json:"isOnline"`
			IsLive             bool   `json:"isLive"`
			Status             string `json:"status"`
			BroadcastGender    string `json:"broadcastGender"`
			PreviewUrlThumbBig string `json:"previewUrlThumbBig"`
			BroadcastServer    string `json:"broadcastServer"`
			SnapshotTimestamp  int64  `json:"snapshotTimestamp"`
		} `json:"user"`
	} `json:"user"`
}

var (
	reStripchatMetaImage = regexp.MustCompile(`property="og:image"\s+content="([^"]+)"`)
	reStripchatMetaDesc  = regexp.MustCompile(`property="og:description"\s+content="([^"]+)"`)
	reStripchatTitle     = regexp.MustCompile(`<title>([^<]+)</title>`)
	reStripchatRootM3U8  = regexp.MustCompile(`^\d+\.m3u8$`)
	reStripchatModelID   = regexp.MustCompile(`/(\d{6,})/`)
)

// mapGender converts Stripchat gender strings to the single-letter codes used
// throughout the app ("f", "m", "c", "t").
func mapGender(g string) string {
	switch g {
	case "female":
		return "f"
	case "male":
		return "m"
	case "couple", "malefemale", "group":
		return "c"
	case "trans", "tranny":
		return "t"
	default:
		return g
	}
}

// FetchStream implements site.Site. Returns StreamInfo when online, nil when offline.
func (s *Stripchat) FetchStream(ctx context.Context, req *internal.Req, username string) (*site.StreamInfo, error) {
	info, err := fetchStreamViaLegacyAPI(ctx, req, username)
	if err == nil ||
		errors.Is(err, internal.ErrChannelOffline) ||
		errors.Is(err, internal.ErrPrivateStream) {
		return info, err
	}

	if server.Config.BrowserMode == "off" {
		return nil, err
	}

	browserInfo, browserErr := fetchStreamFromBrowserResources(ctx, username)
	if browserErr == nil || errors.Is(browserErr, internal.ErrChannelOffline) {
		return browserInfo, browserErr
	}
	if server.Config.Debug {
		fmt.Printf("[DEBUG] stripchat browser fallback failed for %s: %v\n", username, browserErr)
	}
	return nil, err
}

// FetchLiveThumbURL implements site.LiveThumbSource. It asks only the light
// cam API (never the browser fallback of FetchStream) for the address of the
// newest preview snapshot.
func (s *Stripchat) FetchLiveThumbURL(ctx context.Context, req *internal.Req, username string) (string, error) {
	info, err := fetchStreamViaLegacyAPI(ctx, req, username)
	if info != nil && info.LiveThumbURL != "" &&
		(err == nil || errors.Is(err, internal.ErrChannelOffline) || errors.Is(err, internal.ErrPrivateStream)) {
		return info.LiveThumbURL, nil
	}
	if err == nil {
		err = fmt.Errorf("stripchat: no live preview address")
	}
	return "", err
}

// FetchAvailability implements site.AvailabilitySource using only the light
// cam API (never the browser fallback).
func (s *Stripchat) FetchAvailability(ctx context.Context, req *internal.Req, username string) (site.Availability, error) {
	info, err := fetchStreamViaLegacyAPI(ctx, req, username)
	return availabilityFrom(info, err)
}

// availabilityFrom turns the answer of the cam API into an Availability:
// offline and private rooms are "not available", anything else that failed is
// "could not tell".
func availabilityFrom(info *site.StreamInfo, err error) (site.Availability, error) {
	switch {
	case err == nil && info != nil:
		return site.Availability{Live: true, NumViewers: info.NumViewers, LiveThumbURL: info.LiveThumbURL}, nil
	case errors.Is(err, internal.ErrChannelOffline), errors.Is(err, internal.ErrPrivateStream):
		return site.Availability{}, nil
	}
	if err == nil {
		err = fmt.Errorf("stripchat: empty availability answer")
	}
	return site.Availability{}, err
}

func fetchStreamViaLegacyAPI(ctx context.Context, req *internal.Req, username string) (*site.StreamInfo, error) {
	apiURL := fmt.Sprintf("https://stripchat.com/api/front/v2/models/username/%s/cam", username)

	httpReq, cancel, err := req.CreateRequest(ctx, apiURL)
	if err != nil {
		return nil, fmt.Errorf("stripchat: create request: %w", err)
	}
	defer cancel()

	httpReq.Header.Set("Accept", "application/json, text/plain, */*")
	httpReq.Header.Set("Origin", "https://stripchat.com")
	httpReq.Header.Set("Referer", fmt.Sprintf("https://stripchat.com/%s", username))
	httpReq.Header.Set("Sec-Fetch-Dest", "empty")
	httpReq.Header.Set("Sec-Fetch-Mode", "cors")
	httpReq.Header.Set("Sec-Fetch-Site", "same-origin")
	httpReq.Header.Set("X-Requested-With", "XMLHttpRequest")

	body, err := req.DoRequest(httpReq)
	if err != nil {
		return nil, fmt.Errorf("stripchat: fetch cam: %w", err)
	}
	if body == "" {
		internal.DumpParseFailure("stripchat cam response", body)
		return nil, fmt.Errorf("stripchat: empty cam response")
	}

	var resp camResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		internal.DumpParseFailure("stripchat cam response", body)
		return nil, fmt.Errorf("stripchat: parse cam response: %w", err)
	}
	return streamInfoFromCamResponse(resp)
}

func streamInfoFromCamResponse(resp camResponse) (*site.StreamInfo, error) {
	u := resp.User.User
	info := &site.StreamInfo{
		RoomTitle:        resp.Cam.Topic,
		Gender:           mapGender(u.BroadcastGender),
		NumViewers:       0,
		SummaryCardImage: u.PreviewUrlThumbBig,
		CDNReferer:       "https://stripchat.com/",
		MouflonPDKey:     "auto",
	}

	if u.SnapshotTimestamp > 0 && resp.Cam.StreamName != "" {
		info.LiveThumbURL = fmt.Sprintf(
			"https://img.doppiocdn.net/thumbs/%d/%s",
			u.SnapshotTimestamp, resp.Cam.StreamName,
		)
	}

	// Stripchat can report isOnline=false for rooms that are still publicly live.
	// Treat an active public cam as online when either flag indicates liveness.
	if (!u.IsOnline && !u.IsLive) || u.Status != "public" {
		return info, internal.ErrChannelOffline
	}
	if !resp.Cam.IsCamActive {
		return info, internal.ErrChannelOffline
	}

	streamName := resp.Cam.StreamName
	if streamName == "" {
		return info, fmt.Errorf("stripchat: empty stream name in cam response")
	}

	if server, ok := resp.Cam.ViewServers["flashphoner-hls"]; ok && server != "" {
		info.HLSURL = fmt.Sprintf(
			"https://b-%s.doppiocdn.com/hls/%s/master_%s.m3u8",
			server, streamName, streamName,
		)
	} else {
		info.HLSURL = fmt.Sprintf(
			"https://edge-hls.doppiocdn.net/hls/%s/master/%s_auto.m3u8?playlistType=lowLatency",
			streamName, streamName,
		)
	}
	return info, nil
}

func fetchStreamFromBrowserResources(ctx context.Context, username string) (*site.StreamInfo, error) {
	pageURL := fmt.Sprintf("https://stripchat.com/%s", username)
	resourceURLs, err := internal.BrowserResourceURLs(ctx, pageURL)
	if err != nil {
		return nil, fmt.Errorf("stripchat: browser resource scan: %w", err)
	}

	info := &site.StreamInfo{
		CDNReferer:   "https://stripchat.com/",
		MouflonPDKey: "auto",
	}
	populateBrowserMetadataFromResources(info, resourceURLs)
	if body, err := internal.BrowserFetch(ctx, pageURL); err == nil {
		populateBrowserMetadata(info, body)
	} else if server.Config.Debug {
		fmt.Printf("[DEBUG] stripchat browser metadata fetch failed for %s: %v\n", username, err)
	}

	playlistURL := selectBrowserPlaylistURL(resourceURLs)
	if playlistURL == "" {
		return info, internal.ErrChannelOffline
	}

	info.HLSURL = playlistURL

	if playlistBody, err := internal.NewMediaReqWithReferer("https://stripchat.com/").Get(ctx, playlistURL); err == nil {
		if learnBrowserDecryptMask(playlistBody, resourceURLs) {
			info.MouflonPDKey = "derived"
		}
	} else if server.Config.Debug {
		fmt.Printf("[DEBUG] stripchat browser playlist prefetch failed for %s: %v\n", username, err)
	}

	if server.Config.Debug {
		fmt.Printf("[DEBUG] stripchat browser playlist for %s: %s\n", username, playlistURL)
	}
	return info, nil
}

func selectBrowserPlaylistURL(resourceURLs []string) string {
	bestScore := -1
	bestURL := ""
	for _, rawURL := range resourceURLs {
		score := scoreBrowserPlaylistURL(rawURL)
		if score > bestScore {
			bestScore = score
			bestURL = rawURL
		}
	}
	return bestURL
}

func scoreBrowserPlaylistURL(rawURL string) int {
	u, err := url.Parse(rawURL)
	if err != nil {
		return -1
	}
	host := strings.ToLower(u.Hostname())
	if !strings.Contains(host, "doppiocdn") {
		return -1
	}
	if !strings.HasSuffix(strings.ToLower(u.Path), ".m3u8") {
		return -1
	}
	if !strings.Contains(strings.ToLower(u.RawQuery), "playlisttype=lowlatency") {
		return -1
	}

	base := path.Base(u.Path)
	score := 10
	if strings.Contains(host, "media-hls.") {
		score += 200
	}
	if strings.Contains(host, "edge-hls.") {
		score -= 25
	}
	if reStripchatRootM3U8.MatchString(base) {
		score += 100
	}
	if strings.Contains(base, "_init_") {
		return -1
	}
	if strings.Contains(base, "_240p.") || strings.Contains(base, "_480p.") || strings.Contains(base, "_720p.") || strings.Contains(base, "_1080p.") {
		score += 25
	}
	if strings.Contains(strings.ToLower(u.RawQuery), "psch=v2") {
		score += 10
	}
	if strings.Contains(strings.ToLower(u.RawQuery), "pkey=") {
		score += 100
	}
	if strings.Contains(strings.ToLower(u.RawQuery), "_hls_msn=") {
		score += 10
	}
	return score
}

func learnBrowserDecryptMask(playlistBody string, resourceURLs []string) bool {
	for _, line := range strings.Split(playlistBody, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "#EXT-X-MOUFLON:URI:") {
			continue
		}
		encryptedURI := strings.TrimPrefix(line, "#EXT-X-MOUFLON:URI:")
		resolvedURI := findMatchingBrowserSegmentURL(encryptedURI, resourceURLs)
		if resolvedURI == "" {
			continue
		}
		return LearnDecryptMaskFromPair(encryptedURI, resolvedURI)
	}
	return false
}

func findMatchingBrowserSegmentURL(encryptedURI string, resourceURLs []string) string {
	idx := reToken.FindStringSubmatchIndex(encryptedURI)
	if idx == nil || len(idx) < 6 {
		return ""
	}

	tokenStart := idx[4]
	tokenEnd := idx[5]
	prefix := encryptedURI[:tokenStart]
	suffix := encryptedURI[tokenEnd:]

	for _, candidate := range resourceURLs {
		if candidate == encryptedURI {
			continue
		}
		if strings.HasPrefix(candidate, prefix) && strings.HasSuffix(candidate, suffix) {
			return candidate
		}
	}
	return ""
}

func populateBrowserMetadata(info *site.StreamInfo, body string) {
	if info == nil || body == "" {
		return
	}

	if info.SummaryCardImage == "" {
		if match := reStripchatMetaImage.FindStringSubmatch(body); len(match) > 1 {
			info.SummaryCardImage = htmlUnescape(match[1])
		}
	}
	if info.RoomTitle == "" {
		if match := reStripchatMetaDesc.FindStringSubmatch(body); len(match) > 1 {
			info.RoomTitle = strings.TrimSpace(htmlUnescape(match[1]))
		} else if match := reStripchatTitle.FindStringSubmatch(body); len(match) > 1 {
			info.RoomTitle = strings.TrimSpace(htmlUnescape(match[1]))
		}
	}
}

func populateBrowserMetadataFromResources(info *site.StreamInfo, resourceURLs []string) {
	if info == nil {
		return
	}

	if info.SummaryCardImage == "" {
		if card := selectSummaryCardURL(resourceURLs); card != "" {
			info.SummaryCardImage = card
		}
	}
	if info.LiveThumbURL == "" {
		if live := selectLiveThumbURL(resourceURLs); live != "" {
			info.LiveThumbURL = live
		}
	}
}

func selectSummaryCardURL(resourceURLs []string) string {
	bestScore := -1
	bestURL := ""
	for _, rawURL := range resourceURLs {
		score := scoreSummaryCardURL(rawURL)
		if score > bestScore {
			bestScore = score
			bestURL = rawURL
		}
	}
	return bestURL
}

func scoreSummaryCardURL(rawURL string) int {
	u, err := url.Parse(rawURL)
	if err != nil {
		return -1
	}
	host := strings.ToLower(u.Hostname())
	pathLower := strings.ToLower(u.Path)
	if !strings.Contains(host, "strpst.com") && !strings.Contains(host, "highwebmedia.com") {
		return -1
	}

	score := -1
	switch {
	case strings.Contains(pathLower, "/previews/"):
		score = 100
	case strings.Contains(pathLower, "/avatars/"):
		score = 80
	case strings.Contains(pathLower, "/photos/"):
		score = 60
	default:
		return -1
	}

	if strings.Contains(pathLower, "-thumb-big") {
		score += 20
	}
	if strings.Contains(pathLower, "-full") {
		score += 10
	}
	if strings.HasSuffix(pathLower, ".jpg") || strings.HasSuffix(pathLower, ".jpeg") || strings.HasSuffix(pathLower, ".webp") || strings.HasSuffix(pathLower, ".png") {
		score += 5
	}
	return score
}

func selectLiveThumbURL(resourceURLs []string) string {
	bestScore := -1
	bestURL := ""
	for _, rawURL := range resourceURLs {
		score := scoreLiveThumbURL(rawURL)
		if score > bestScore {
			bestScore = score
			bestURL = rawURL
		}
	}
	return bestURL
}

func scoreLiveThumbURL(rawURL string) int {
	u, err := url.Parse(rawURL)
	if err != nil {
		return -1
	}
	host := strings.ToLower(u.Hostname())
	pathLower := strings.ToLower(u.Path)
	if !strings.Contains(host, "doppiocdn") && !strings.Contains(host, "strpst.com") {
		return -1
	}

	switch {
	case strings.Contains(host, "img.doppiocdn.net") && strings.Contains(pathLower, "/thumbs/"):
		score := 100
		if reStripchatModelID.MatchString(pathLower) {
			score += 10
		}
		return score
	case strings.Contains(pathLower, "/previews/") && strings.Contains(pathLower, "-thumb-big"):
		return 40
	case strings.Contains(pathLower, "/previews/"):
		return 30
	default:
		return -1
	}
}

func htmlUnescape(s string) string {
	replacer := strings.NewReplacer(
		"&amp;", "&",
		"&#39;", "'",
		"&quot;", `"`,
		"&lt;", "<",
		"&gt;", ">",
	)
	return replacer.Replace(s)
}

// FetchLastBroadcast implements site.Site. Stripchat has no equivalent endpoint.
func (s *Stripchat) FetchLastBroadcast(_ context.Context, _ *internal.Req, _ string) (int64, error) {
	return 0, nil
}

// ensure Stripchat implements site.Site at compile time.
var _ site.Site = (*Stripchat)(nil)
