package chaturbate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/site"
	"github.com/HeapOfChaos/goondvr/stripchat"
	"github.com/avast/retry-go/v4"
	"github.com/grafov/m3u8"
	"github.com/samber/lo"
)

// Chaturbate implements site.Site for the Chaturbate platform.
type Chaturbate struct{}

// New returns a new Chaturbate site implementation.
func New() *Chaturbate {
	return &Chaturbate{}
}

// FetchStream implements site.Site. It returns *site.StreamInfo if online, nil if offline.
func (cb *Chaturbate) FetchStream(ctx context.Context, req *internal.Req, username string) (*site.StreamInfo, error) {
	stream, err := FetchStream(ctx, req, username)
	if err != nil {
		info := &site.StreamInfo{}
		if stream != nil {
			info.RoomTitle = stream.RoomTitle
			info.Gender = stream.Gender
			info.NumViewers = stream.NumViewers
			info.SummaryCardImage = stream.SummaryCardImage
		}

		// Preserve metadata on offline/private/hidden responses so the UI can
		// still show room title/profile imagery for channels that aren't live.
		if errors.Is(err, internal.ErrChannelOffline) ||
			errors.Is(err, internal.ErrPrivateStream) ||
			errors.Is(err, internal.ErrHiddenStream) {
			return info, err
		}
		return info, err
	}
	if stream == nil || stream.HLSSource == "" {
		return nil, nil
	}
	return &site.StreamInfo{
		HLSURL:           stream.HLSSource,
		RoomTitle:        stream.RoomTitle,
		Gender:           stream.Gender,
		NumViewers:       stream.NumViewers,
		SummaryCardImage: stream.SummaryCardImage,
	}, nil
}

// FetchLastBroadcast implements site.Site.
func (cb *Chaturbate) FetchLastBroadcast(ctx context.Context, req *internal.Req, username string) (int64, error) {
	return FetchLastBroadcast(ctx, req, username)
}

type Client struct {
	Req *internal.Req
}

func NewClient() *Client {
	return &Client{Req: internal.NewReq()}
}

func (c *Client) GetStream(ctx context.Context, username string) (*Stream, error) {
	return FetchStream(ctx, c.Req, username)
}

type apiResponse struct {
	RoomStatus       string `json:"room_status"`
	HLSSource        string `json:"hls_source"`
	Code             string `json:"code"`
	RoomTitle        string `json:"room_title"`
	Gender           string `json:"broadcaster_gender"`
	NumViewers       int    `json:"num_viewers"`
	EdgeRegion       string `json:"edge_region"`
	SummaryCardImage string `json:"summary_card_image"`
}

func FetchStream(ctx context.Context, client *internal.Req, username string) (*Stream, error) {
	apiURL := fmt.Sprintf("%sapi/chatvideocontext/%s/", server.Config.Domain, username)
	body, err := client.Get(ctx, apiURL)
	if err != nil {
		if errors.Is(err, internal.ErrCloudflareBlocked) {
			if body, berr := internal.BrowserAPIGet(ctx, server.Config.Domain, apiURL); berr == nil {
				if stream, perr := parseAPIStream(username, body); perr == nil ||
					errors.Is(perr, internal.ErrChannelOffline) ||
					errors.Is(perr, internal.ErrPrivateStream) ||
					errors.Is(perr, internal.ErrHiddenStream) ||
					errors.Is(perr, internal.ErrRoomPasswordRequired) {
					return stream, perr
				} else if server.Config.Debug {
					fmt.Printf("[DEBUG] browser API parse failed for %s stream fetch: %v\n", username, perr)
				}
			} else if server.Config.Debug {
				fmt.Printf("[DEBUG] browser API fallback failed for %s stream fetch: %v\n", username, berr)
			}
			if stream, berr := fetchStreamFromBrowserPage(ctx, username); berr == nil || errors.Is(berr, internal.ErrChannelOffline) || errors.Is(berr, internal.ErrPrivateStream) || errors.Is(berr, internal.ErrHiddenStream) || errors.Is(berr, internal.ErrRoomPasswordRequired) {
				return stream, berr
			} else if server.Config.Debug {
				fmt.Printf("[DEBUG] browser fallback failed for %s stream fetch: %v\n", username, berr)
			}
			return nil, fmt.Errorf("failed to get stream info: %w (browser fallback failed)", err)
		}
		return nil, fmt.Errorf("failed to get stream info: %w", err)
	}

	return parseAPIStream(username, body)
}

// bioResponse is the subset of fields we care about from the biocontext API.
type bioResponse struct {
	LastBroadcast string `json:"last_broadcast"`
}

// FetchLastBroadcast calls the biocontext API and returns the last_broadcast
// time as a Unix timestamp, or 0 if unavailable.
func FetchLastBroadcast(ctx context.Context, req *internal.Req, username string) (int64, error) {
	apiURL := fmt.Sprintf("%sapi/biocontext/%s/", server.Config.Domain, username)
	body, err := req.Get(ctx, apiURL)
	if err != nil {
		if errors.Is(err, internal.ErrCloudflareBlocked) {
			if body, berr := internal.BrowserAPIGet(ctx, server.Config.Domain, apiURL); berr == nil {
				if ts, perr := parseAPILastBroadcast(body); perr == nil {
					return ts, nil
				} else if server.Config.Debug {
					fmt.Printf("[DEBUG] browser API parse failed for %s biocontext fetch: %v\n", username, perr)
				}
			} else if server.Config.Debug {
				fmt.Printf("[DEBUG] browser API fallback failed for %s biocontext fetch: %v\n", username, berr)
			}
			if ts, berr := fetchLastBroadcastFromBrowserPage(ctx, username); berr == nil {
				return ts, nil
			} else if server.Config.Debug {
				fmt.Printf("[DEBUG] browser fallback failed for %s biocontext fetch: %v\n", username, berr)
			}
			return 0, fmt.Errorf("fetch biocontext: %w (browser fallback failed)", err)
		}
		return 0, fmt.Errorf("fetch biocontext: %w", err)
	}
	return parseAPILastBroadcast(body)
}

func parseAPIStream(username, body string) (*Stream, error) {
	var resp apiResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		if server.Config.Debug {
			fmt.Printf("[DEBUG] API parse fallback for %s: %v\n", username, err)
		}
		return parseLooseAPIStream(body)
	}

	if resp.Code == "unauthorized" {
		return nil, internal.ErrRoomPasswordRequired
	}

	if server.Config.Debug {
		fmt.Printf("[DEBUG] API response for %s: room_status=%s hls_source=%v\n", username, resp.RoomStatus, resp.HLSSource != "")
	}

	meta := &Stream{
		RoomTitle:        resp.RoomTitle,
		Gender:           resp.Gender,
		EdgeRegion:       resp.EdgeRegion,
		SummaryCardImage: resp.SummaryCardImage,
	}

	if resp.HLSSource != "" {
		meta.HLSSource = resp.HLSSource
		meta.NumViewers = resp.NumViewers
		return meta, nil
	}

	switch resp.RoomStatus {
	case "private":
		return meta, internal.ErrPrivateStream
	case "hidden":
		return meta, internal.ErrHiddenStream
	default:
		return meta, internal.ErrChannelOffline
	}
}

func parseAPILastBroadcast(body string) (int64, error) {
	var bio bioResponse
	if err := json.Unmarshal([]byte(body), &bio); err != nil {
		if ts := extractJSONString(body, "last_broadcast"); ts != "" {
			t, terr := time.Parse("2006-01-02T15:04:05.999", ts)
			if terr == nil {
				return t.Unix(), nil
			}
		}
		if looksLikeHTMLDocument(body) {
			return 0, nil
		}
		return 0, fmt.Errorf("parse biocontext: %w", err)
	}
	if bio.LastBroadcast == "" {
		return 0, nil
	}
	t, err := time.Parse("2006-01-02T15:04:05.999", bio.LastBroadcast)
	if err != nil {
		return 0, fmt.Errorf("parse last_broadcast: %w", err)
	}
	return t.Unix(), nil
}

func parseLooseAPIStream(body string) (*Stream, error) {
	stream := &Stream{
		HLSSource:        extractJSONString(body, "hls_source"),
		RoomTitle:        extractJSONString(body, "room_title"),
		Gender:           extractJSONString(body, "broadcaster_gender"),
		SummaryCardImage: extractJSONString(body, "summary_card_image"),
		NumViewers:       extractJSONInt(body, "num_viewers"),
		EdgeRegion:       extractJSONString(body, "edge_region"),
	}

	roomStatus := extractJSONString(body, "room_status")
	code := extractJSONString(body, "code")
	if code == "unauthorized" {
		return nil, internal.ErrRoomPasswordRequired
	}
	if stream.HLSSource != "" {
		return stream, nil
	}
	switch roomStatus {
	case "private":
		return stream, internal.ErrPrivateStream
	case "hidden":
		return stream, internal.ErrHiddenStream
	case "offline":
		return stream, internal.ErrChannelOffline
	}
	if isGenericLobbyPage(body) || looksLikeHTMLDocument(body) || isRoomPageHTML(body) {
		return stream, internal.ErrChannelOffline
	}
	return nil, fmt.Errorf("stream info body was not parseable")
}

func fetchStreamFromBrowserPage(ctx context.Context, username string) (*Stream, error) {
	pageURL := fmt.Sprintf("%s%s/", server.Config.Domain, username)
	body, err := internal.BrowserFetch(ctx, pageURL)
	if err != nil {
		return nil, fmt.Errorf("browser fetch room page: %w", err)
	}

	stream := &Stream{
		HLSSource:        extractJSONString(body, "hls_source"),
		RoomTitle:        extractJSONString(body, "room_title"),
		Gender:           extractJSONString(body, "broadcaster_gender"),
		SummaryCardImage: extractJSONString(body, "summary_card_image"),
		NumViewers:       extractJSONInt(body, "num_viewers"),
		EdgeRegion:       extractJSONString(body, "edge_region"),
	}

	roomStatus := extractJSONString(body, "room_status")
	code := extractJSONString(body, "code")
	if server.Config.Debug {
		fmt.Printf("[DEBUG] browser page parse for %s: room_status=%q code=%q hls_source=%v\n", username, roomStatus, code, stream.HLSSource != "")
	}
	if code == "unauthorized" {
		return nil, internal.ErrRoomPasswordRequired
	}
	if stream.HLSSource != "" {
		return stream, nil
	}
	switch roomStatus {
	case "private":
		return stream, internal.ErrPrivateStream
	case "hidden":
		return stream, internal.ErrHiddenStream
	case "offline":
		return stream, internal.ErrChannelOffline
	}
	if isGenericLobbyPage(body) {
		return stream, internal.ErrChannelOffline
	}
	return nil, fmt.Errorf("browser page did not contain stream info")
}

func fetchLastBroadcastFromBrowserPage(ctx context.Context, username string) (int64, error) {
	pageURL := fmt.Sprintf("%s%s/", server.Config.Domain, username)
	body, err := internal.BrowserFetch(ctx, pageURL)
	if err != nil {
		return 0, fmt.Errorf("browser fetch room page: %w", err)
	}

	lastBroadcast := extractJSONString(body, "last_broadcast")
	if lastBroadcast == "" {
		return 0, nil
	}
	t, err := time.Parse("2006-01-02T15:04:05.999", lastBroadcast)
	if err != nil {
		return 0, fmt.Errorf("parse last_broadcast: %w", err)
	}
	return t.Unix(), nil
}

var (
	jsonStringFieldPattern = regexp.MustCompile(`"([a-zA-Z0-9_]+)"\s*:\s*"((?:\\.|[^"])*)"`)
	jsonNumberFieldPattern = regexp.MustCompile(`"([a-zA-Z0-9_]+)"\s*:\s*([0-9]+)`)
)

func extractJSONString(body, field string) string {
	matches := jsonStringFieldPattern.FindAllStringSubmatch(body, -1)
	for _, match := range matches {
		if len(match) != 3 || match[1] != field {
			continue
		}
		value, err := strconv.Unquote(`"` + match[2] + `"`)
		if err != nil {
			return match[2]
		}
		return value
	}
	return ""
}

func extractJSONInt(body, field string) int {
	matches := jsonNumberFieldPattern.FindAllStringSubmatch(body, -1)
	for _, match := range matches {
		if len(match) != 3 || match[1] != field {
			continue
		}
		v, err := strconv.Atoi(match[2])
		if err != nil {
			return 0
		}
		return v
	}
	return 0
}

func isGenericLobbyPage(body string) bool {
	return strings.Contains(body, "<title>Chaturbate - 100% Free Chat &amp; Webcams</title>") ||
		strings.Contains(body, "<title>Chaturbate - 100% Free Chat & Webcams</title>")
}

func looksLikeHTMLDocument(body string) bool {
	trimmed := strings.TrimSpace(body)
	return strings.HasPrefix(trimmed, "<!DOCTYPE html") ||
		strings.HasPrefix(trimmed, "<html") ||
		strings.Contains(trimmed, "<head") ||
		strings.Contains(trimmed, "<body")
}

func isRoomPageHTML(body string) bool {
	return strings.Contains(body, "'s Room @ Chaturbate") ||
		strings.Contains(body, "Room @ Chaturbate - Chat in a Live Adult Video Chat Room Now")
}

type Stream struct {
	HLSSource        string
	RoomTitle        string
	Gender           string
	NumViewers       int
	EdgeRegion       string
	SummaryCardImage string
}

func (s *Stream) GetPlaylist(ctx context.Context, resolution, framerate int) (*Playlist, error) {
	return FetchPlaylist(ctx, s.HLSSource, resolution, framerate, "", "")
}

func FetchPlaylist(ctx context.Context, hlsSource string, resolution, framerate int, cdnReferer, mouflonPDKey string) (*Playlist, error) {
	if hlsSource == "" {
		// The page loaded but the stream is not active — treat as offline.
		return nil, internal.ErrChannelOffline
	}

	var client *internal.Req
	if cdnReferer != "" {
		client = internal.NewMediaReqWithReferer(cdnReferer)
	} else {
		client = internal.NewMediaReq()
	}
	resp, err := client.Get(ctx, hlsSource)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch HLS source: %w", err)
	}

	if server.Config.Debug {
		fmt.Printf("[DEBUG] master playlist response for %s:\n%s\n", hlsSource, resp)
	}

	// Extract Stripchat's custom MOUFLON tag which carries the CDN pkey.
	// Format: #EXT-X-MOUFLON:PSCH:v2:{pkey}
	// The variant URLs in the master omit the pkey; it must be appended when fetching.
	var mouflonSuffix string
	pkey := stripchat.ParsePKeyFromURL(hlsSource)
	if pkey == "" {
		pkey = stripchat.ParsePKeyFromMaster(resp)
	}
	if pkey != "" {
		resolvedPKey := stripchat.ResolveMediaPKey(ctx, pkey)
		if resolvedPKey != "" {
			pkey = resolvedPKey
		}

		// Build the query suffix needed for variant playlist URLs.
		mouflonSuffix = fmt.Sprintf("&psch=v2&pkey=%s", pkey)

		// Resolve the actual decryption key (pdkey) from the pkey.
		if mouflonPDKey == "auto" {
			mouflonPDKey = stripchat.ResolvePDKey(ctx, pkey)
			if mouflonPDKey == "pending" {
				if server.Config.Debug {
					fmt.Println("[DEBUG] mouflon: candidate keys extracted; will test against first encrypted segment")
				}
			} else if mouflonPDKey != "" {
				if server.Config.Debug {
					fmt.Printf("[DEBUG] mouflon: pdkey resolved for pkey=%s (%d chars)\n", pkey, len(mouflonPDKey))
				}
			} else {
				fmt.Printf("[WARN] mouflon: no pdkey for pkey=%s; segments will 404. Use --stripchat-pdkey to set manually.\n", pkey)
			}
		}
	}

	playlist, err := ParsePlaylist(resp, hlsSource, resolution, framerate)
	if err != nil {
		return nil, err
	}
	if mouflonSuffix != "" {
		if stripchat.ParsePKeyFromURL(playlist.PlaylistURL) == "" {
			playlist.PlaylistURL += mouflonSuffix
		}
		if playlist.AudioPlaylistURL != "" && stripchat.ParsePKeyFromURL(playlist.AudioPlaylistURL) == "" {
			playlist.AudioPlaylistURL += mouflonSuffix
		}
	}
	playlist.Client = client
	playlist.MouflonPDKey = mouflonPDKey
	return playlist, nil
}

func ParsePlaylist(resp, hlsSource string, resolution, framerate int) (*Playlist, error) {
	normalized := normalizeM3U8(resp)
	p, _, err := safeDecodeFrom(strings.NewReader(normalized))
	if err != nil {
		if server.Config.Debug {
			fmt.Printf("[DEBUG] master playlist parse failed: %v\n", err)
			fmt.Printf("[DEBUG]   HLS source URL: %s\n", hlsSource)
			end := len(resp)
			if end > 2000 {
				end = 2000
			}
			fmt.Printf("[DEBUG]   Response (first 2000 chars):\n%s\n", resp[:end])
		}
		return nil, fmt.Errorf("failed to decode m3u8 playlist: %w", err)
	}

	if masterPlaylist, ok := p.(*m3u8.MasterPlaylist); ok {
		return PickPlaylist(masterPlaylist, hlsSource, resolution, framerate)
	}

	if mediaPlaylist, ok := p.(*m3u8.MediaPlaylist); ok {
		return directMediaPlaylist(mediaPlaylist, hlsSource, resolution, framerate), nil
	}

	return nil, errors.New("invalid playlist format")
}

func directMediaPlaylist(pl *m3u8.MediaPlaylist, hlsSource string, resolution, framerate int) *Playlist {
	fileExt := ".ts"
	if strings.Contains(hlsSource, "doppiocdn") || pl.Map != nil {
		fileExt = ".mp4"
	}

	rootURL := strings.SplitN(hlsSource, "?", 2)[0]
	return &Playlist{
		PlaylistURL: hlsSource,
		RootURL:     rootURL,
		Resolution:  resolution,
		Framerate:   framerate,
		FileExt:     fileExt,
	}
}

// Playlist represents an HLS playlist containing variant streams.
type Playlist struct {
	PlaylistURL      string
	AudioPlaylistURL string // LL-HLS audio rendition URL; empty for legacy streams
	RootURL          string // base for resolving video segment URIs
	Resolution       int
	Framerate        int
	FileExt          string        // ".ts" for legacy HLS, ".mp4" for LL-HLS fMP4
	Client           *internal.Req // reuse the same client that fetched the master playlist
	MouflonPDKey     string        // Stripchat MOUFLON v2 decryption key; empty for Chaturbate
}

// VideoResolution represents a video resolution and its corresponding framerate URLs.
type VideoResolution struct {
	Framerate map[int]string // [framerate]url
	Width     int
}

// Resolution is a type alias kept for compatibility.
type Resolution = VideoResolution

func resolveHLSURL(base, ref string) string {
	baseClean := strings.SplitN(base, "?", 2)[0]
	baseURL, err := url.Parse(baseClean)
	if err != nil {
		return base + ref
	}
	refURL, err := url.Parse(ref)
	if err != nil {
		return base + ref
	}
	return baseURL.ResolveReference(refURL).String()
}

func PickPlaylist(masterPlaylist *m3u8.MasterPlaylist, baseURL string, resolution, framerate int) (*Playlist, error) {
	resolutions := map[int]*VideoResolution{}

	for _, v := range masterPlaylist.Variants {
		parts := strings.Split(v.Resolution, "x")
		if len(parts) != 2 {
			continue
		}
		width, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("parse resolution: %w", err)
		}
		framerateVal := 30
		if strings.Contains(v.Name, "FPS:60.0") {
			framerateVal = 60
		}
		if _, exists := resolutions[width]; !exists {
			resolutions[width] = &VideoResolution{Framerate: map[int]string{}, Width: width}
		}
		resolutions[width].Framerate[framerateVal] = v.URI
	}

	variant, exists := resolutions[resolution]
	if !exists {
		candidates := lo.Filter(lo.Values(resolutions), func(r *VideoResolution, _ int) bool {
			return r.Width < resolution
		})
		variant = lo.MaxBy(candidates, func(a, b *VideoResolution) bool {
			return a.Width > b.Width
		})
	}
	if variant == nil {
		return nil, fmt.Errorf("resolution not found")
	}

	var (
		finalResolution = variant.Width
		finalFramerate  = framerate
	)
	playlistURL, exists := variant.Framerate[framerate]
	if !exists {
		for fr, u := range variant.Framerate {
			playlistURL = u
			finalFramerate = fr
			break
		}
	}

	fileExt := ".ts"
	if strings.Contains(playlistURL, "llhls") || strings.HasSuffix(strings.SplitN(playlistURL, "?", 2)[0], ".m4s") {
		fileExt = ".mp4"
	}

	// Stripchat uses fMP4 segments (.mp4) even though the playlist URL
	// doesn't contain "llhls" or end in ".m4s". Detect by checking the
	// master playlist for EXT-X-MAP (init segment indicator) in any variant.
	if fileExt == ".ts" && strings.Contains(baseURL, "doppiocdn") {
		fileExt = ".mp4"
	}

	// For LL-HLS streams, find the audio rendition from the selected variant's EXT-X-MEDIA alternatives.
	var audioPlaylistURL string
	if fileExt == ".mp4" {
		for _, v := range masterPlaylist.Variants {
			if v.URI == playlistURL {
				for _, alt := range v.Alternatives {
					if alt != nil && alt.Type == "AUDIO" && alt.URI != "" {
						audioPlaylistURL = resolveHLSURL(baseURL, alt.URI)
						break
					}
				}
				break
			}
		}
		if server.Config.Debug {
			if audioPlaylistURL != "" {
				fmt.Printf("[DEBUG] LL-HLS audio rendition: %s\n", audioPlaylistURL)
			} else {
				fmt.Printf("[DEBUG] LL-HLS stream has no separate audio rendition\n")
			}
		}
	}

	resolvedPlaylist := resolveHLSURL(baseURL, playlistURL)
	return &Playlist{
		PlaylistURL:      resolvedPlaylist,
		AudioPlaylistURL: audioPlaylistURL,
		RootURL:          strings.SplitN(resolvedPlaylist, "?", 2)[0],
		Resolution:       finalResolution,
		Framerate:        finalFramerate,
		FileExt:          fileExt,
	}, nil
}

// WatchHandler is a function type that processes video segments.
type WatchHandler func(b []byte, duration float64) error

// WatchSegments continuously fetches and processes video segments.
// For LL-HLS streams with a separate audio rendition it automatically muxes
// audio and video into a single fragmented MP4 output stream.
func (p *Playlist) WatchSegments(ctx context.Context, handler WatchHandler) error {
	if p.AudioPlaylistURL != "" {
		return p.watchMuxedSegments(ctx, handler)
	}
	return p.watchVideoOnlySegments(ctx, handler)
}

// safeDecodeFrom wraps m3u8.DecodeFrom with a recover so that library panics
// (e.g. nil-pointer on unknown LL-HLS tags) are returned as errors instead of
// crashing the process.
func safeDecodeFrom(r io.Reader) (pl m3u8.Playlist, listType m3u8.ListType, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("m3u8 decode panic: %v", rec)
		}
	}()
	return m3u8.DecodeFrom(r, true)
}

// decodeMouflon rewrites a Stripchat media playlist that uses the proprietary
// #EXT-X-MOUFLON:URI: tag to hide real segment URLs behind a generic placeholder
// (e.g. https://.../media.mp4). Each MOUFLON URI tag is consumed and its real
// URL replaces the following non-comment placeholder line.
//
// When pdkey is non-empty, the encrypted token in each URI is decrypted using
// the MOUFLON v2 algorithm (reverse -> base64-decode -> XOR SHA256(pdkey)).
// If pdkey is "pending", the first encrypted URI is used to brute-force the
// correct key from candidate strings extracted from the player JS.
func decodeMouflon(resp, pdkey string) string {
	if !strings.Contains(resp, "#EXT-X-MOUFLON:URI:") {
		return resp
	}

	// If pdkey is "pending", try to find the working key from candidates
	// using the first MOUFLON URI as a test sample.
	if pdkey == "pending" {
		for _, line := range strings.Split(resp, "\n") {
			trimmed := strings.TrimRight(line, "\r")
			if strings.HasPrefix(trimmed, "#EXT-X-MOUFLON:URI:") {
				sampleURI := strings.TrimPrefix(trimmed, "#EXT-X-MOUFLON:URI:")
				found := stripchat.TryFindWorkingKey(sampleURI)
				if found != "" {
					pdkey = found
				} else {
					pdkey = "" // no working key found
				}
				break
			}
		}
	}

	lines := strings.Split(resp, "\n")
	out := make([]string, 0, len(lines))
	var pendingURI string
	for _, line := range lines {
		trimmed := strings.TrimRight(line, "\r")
		if strings.HasPrefix(trimmed, "#EXT-X-MOUFLON:URI:") {
			uri := strings.TrimPrefix(trimmed, "#EXT-X-MOUFLON:URI:")
			if pdkey != "" {
				decrypted, err := stripchat.DecryptMouflonURI(uri, pdkey)
				if err != nil {
					if server.Config.Debug {
						fmt.Printf("[DEBUG] mouflon decrypt failed for URI: %v\n", err)
					}
				} else {
					uri = decrypted
				}
			}
			pendingURI = uri
			continue // drop the MOUFLON tag line entirely
		}
		if pendingURI != "" && !strings.HasPrefix(trimmed, "#") && trimmed != "" {
			out = append(out, pendingURI) // real URI replaces placeholder
			pendingURI = ""
			continue // drop the placeholder line
		}
		out = append(out, trimmed)
	}
	return strings.Join(out, "\n")
}

// normalizeM3U8 fixes non-standard #EXTINF lines that lack a trailing comma,
// and strips LL-HLS extension tags that cause the m3u8 library to panic.
// Some CDNs (e.g. Stripchat) emit "#EXTINF:2.000" instead of "#EXTINF:2.000,".
func normalizeM3U8(resp string) string {
	// LL-HLS tags the grafov/m3u8 library cannot handle without panicking.
	stripPrefixes := []string{
		"#EXT-X-PART:",
		"#EXT-X-PART-INF:",
		"#EXT-X-PRELOAD-HINT:",
		"#EXT-X-SERVER-CONTROL:",
		"#EXT-X-RENDITION-REPORT:",
		"#EXT-X-MOUFLON:",
	}
	lines := strings.Split(resp, "\n")
	out := lines[:0]
	for _, line := range lines {
		trimmed := strings.TrimRight(line, "\r")
		skip := false
		for _, pfx := range stripPrefixes {
			if strings.HasPrefix(trimmed, pfx) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		if strings.HasPrefix(trimmed, "#EXTINF:") && !strings.Contains(trimmed, ",") {
			trimmed = trimmed + ","
		}
		out = append(out, trimmed)
	}
	return strings.Join(out, "\n")
}

// watchVideoOnlySegments is the original single-track segment loop.
func (p *Playlist) watchVideoOnlySegments(ctx context.Context, handler WatchHandler) error {
	client := p.Client
	if client == nil {
		client = internal.NewMediaReq()
	}
	lastSeq := -1
	lastSegURI := ""
	lastMapURI := ""
	consecutiveErrors := 0

	// For fMP4 streams, normalise tfdt timestamps so the recording starts
	// at 0:00 instead of the CDN's absolute stream uptime. Always attempt
	// this — extractAllTrackBaseTimes returns nil on non-fMP4 (.ts) data.
	var trackBaseTimes map[uint32]uint64

	for {
		resp, err := client.Get(ctx, p.PlaylistURL)
		if err != nil {
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("get playlist: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}
		pl, _, err := safeDecodeFrom(strings.NewReader(normalizeM3U8(decodeMouflon(resp, p.MouflonPDKey))))
		if err != nil {
			if server.Config.Debug {
				fmt.Printf("[DEBUG] variant playlist parse failed: %v\n", err)
				fmt.Printf("[DEBUG]   Playlist URL: %s\n", p.PlaylistURL)
				end := len(resp)
				if end > 2000 {
					end = 2000
				}
				fmt.Printf("[DEBUG]   Response (first 2000 chars):\n%s\n", resp[:end])
			}
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("decode from: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}
		playlist, ok := pl.(*m3u8.MediaPlaylist)
		if !ok {
			return fmt.Errorf("cast to media playlist")
		}
		consecutiveErrors = 0

		if server.Config.Debug {
			var count int
			for _, v := range playlist.Segments {
				if v != nil {
					count++
				}
			}
			fmt.Printf("[DEBUG] playlist poll: mediaSeq=%d segments=%d lastSeq=%d url=%s\n",
				playlist.SeqNo, count, lastSeq, p.PlaylistURL)
		}

		for _, v := range playlist.Segments {
			if v == nil {
				continue
			}
			seq := internal.SegmentSeq(v.URI)
			// Fall back to the HLS media sequence number (v.SeqId) when the URI
			// doesn't contain a parseable sequence (e.g. Stripchat .mp4 segments).
			if seq == -1 && v.SeqId > 0 {
				seq = int(v.SeqId)
			}
			if server.Config.Debug && lastSeq == -1 && lastSegURI == "" {
				fmt.Printf("[DEBUG] first segment URI: %s (seq=%d)\n", v.URI, seq)
			}
			if seq != -1 {
				if seq <= lastSeq {
					continue
				}
				lastSeq = seq
			} else {
				if v.URI == lastSegURI {
					continue
				}
			}
			lastSegURI = v.URI
			if v.Map != nil && v.Map.URI != lastMapURI {
				mapURL := resolveHLSURL(p.RootURL, v.Map.URI)
				initBytes, err := client.GetBytes(ctx, mapURL)
				if err != nil {
					return fmt.Errorf("get init segment: %w", err)
				}
				if err := handler(initBytes, 0); err != nil {
					return fmt.Errorf("handler init segment: %w", err)
				}
				lastMapURI = v.Map.URI
			}

			pipeline := func() ([]byte, error) {
				return client.GetBytes(ctx, resolveHLSURL(p.RootURL, v.URI))
			}
			resp, err := retry.DoWithData(
				pipeline,
				retry.Context(ctx),
				retry.Attempts(3),
				retry.Delay(600*time.Millisecond),
				retry.DelayType(retry.FixedDelay),
				retry.RetryIf(func(err error) bool {
					return !errors.Is(err, internal.ErrNotFound)
				}),
			)
			if err != nil {
				if errors.Is(err, internal.ErrNotFound) {
					if server.Config.Debug {
						fmt.Printf("[DEBUG] segment 404 (skipping): seq=%d %s\n", seq, resolveHLSURL(p.RootURL, v.URI))
					}
					continue // segment expired on CDN; move on to next
				}
				if server.Config.Debug {
					fmt.Printf("[DEBUG] segment error (breaking inner loop): seq=%d err=%v\n", seq, err)
				}
				break
			}
			// Normalise fMP4 tfdt so playback starts at 0:00 (all tracks).
			if trackBaseTimes == nil {
				trackBaseTimes = extractAllTrackBaseTimes(resp)
			}
			if trackBaseTimes != nil {
				resp = shiftSegmentAllTracks(resp, trackBaseTimes)
			}
			if err := handler(resp, v.Duration); err != nil {
				return fmt.Errorf("handler: %w", err)
			}
		}

		<-time.After(1 * time.Second)
	}
}

// watchMuxedSegments downloads video/audio independently and orders fragments
// by a common tfdt/timescale presentation timeline. Sequence state is advanced
// only after a fragment has been successfully downloaded and parsed.
func (p *Playlist) watchMuxedSegments(ctx context.Context, handler WatchHandler) error {
	client := p.Client
	if client == nil {
		client = internal.NewMediaReq()
	}

	lastVideoSeq := -1
	lastAudioSeq := -1
	lastVideoURI := ""
	lastAudioURI := ""
	lastVideoMapURI := ""
	lastAudioMapURI := ""
	var videoInitBytes []byte
	var audioInitBytes []byte
	initWritten := false
	consecutiveErrors := 0
	videoTimescale := uint32(0)
	audioTimescale := uint32(0)

	// Per-track tfdt base times and timescales captured from init/first segments.
	// videoShift/audioShift align both tracks to the same presentation origin.
	var videoTimeBase uint64
	var audioTimeBase uint64
	videoBaseSet := false
	audioBaseSet := false
	syncBaseComputed := false
	var videoShift uint64
	var audioShift uint64

	type pendingSeg struct {
		track       string
		presentTime float64
		duration    float64
		data        []byte
	}
	var pending []pendingSeg

	computeSyncShifts := func() {
		if videoTimescale > 0 && audioTimescale > 0 {
			videoStart := float64(videoTimeBase) / float64(videoTimescale)
			audioStart := float64(audioTimeBase) / float64(audioTimescale)
			minStart := videoStart
			if audioStart < minStart {
				minStart = audioStart
			}
			videoShift = uint64(math.Round(minStart * float64(videoTimescale)))
			audioShift = uint64(math.Round(minStart * float64(audioTimescale)))
		} else {
			videoShift = videoTimeBase
			audioShift = audioTimeBase
		}
		syncBaseComputed = true
	}

	for {
		// Fetch video playlist
		videoResp, err := client.Get(ctx, p.PlaylistURL)
		if err != nil {
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("get video playlist: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}
		vpl, _, err := safeDecodeFrom(strings.NewReader(normalizeM3U8(decodeMouflon(videoResp, p.MouflonPDKey))))
		if err != nil {
			if server.Config.Debug {
				fmt.Printf("[DEBUG] muxed: video playlist parse failed: %v\n", err)
			}
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("decode video playlist: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}
		videoPlaylist, ok := vpl.(*m3u8.MediaPlaylist)
		if !ok {
			return fmt.Errorf("cast video playlist to media playlist")
		}

		// Fetch audio playlist
		audioResp, err := client.Get(ctx, p.AudioPlaylistURL)
		if err != nil {
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("get audio playlist: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}
		apl, _, err := safeDecodeFrom(strings.NewReader(normalizeM3U8(decodeMouflon(audioResp, p.MouflonPDKey))))
		if err != nil {
			if server.Config.Debug {
				fmt.Printf("[DEBUG] muxed: audio playlist parse failed: %v\n", err)
			}
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("decode audio playlist: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}
		audioPlaylist, ok := apl.(*m3u8.MediaPlaylist)
		if !ok {
			return fmt.Errorf("cast audio playlist to media playlist")
		}
		consecutiveErrors = 0

		// Collect video init segment (EXT-X-MAP)
		for _, v := range videoPlaylist.Segments {
			if v == nil {
				continue
			}
			if v.Map != nil && v.Map.URI != lastVideoMapURI {
				mapURL := resolveHLSURL(p.RootURL, v.Map.URI)
				b, err := client.GetBytes(ctx, mapURL)
				if err != nil {
					return fmt.Errorf("get video init segment: %w", err)
				}
				videoInitBytes = b
				videoTimescale = 0
				if ts, ok := extractInitTrackTimescale(b); ok && ts > 0 {
					videoTimescale = ts
				}
				lastVideoMapURI = v.Map.URI
				initWritten = false
				syncBaseComputed = false
				videoBaseSet = false
				audioBaseSet = false
				pending = nil
			}
			break
		}

		// Collect audio init segment (EXT-X-MAP)
		for _, v := range audioPlaylist.Segments {
			if v == nil {
				continue
			}
			if v.Map != nil && v.Map.URI != lastAudioMapURI {
				mapURL := resolveHLSURL(p.AudioPlaylistURL, v.Map.URI)
				b, err := client.GetBytes(ctx, mapURL)
				if err != nil {
					return fmt.Errorf("get audio init segment: %w", err)
				}
				audioInitBytes = b
				audioTimescale = 0
				if ts, ok := extractInitTrackTimescale(b); ok && ts > 0 {
					audioTimescale = ts
				}
				lastAudioMapURI = v.Map.URI
				initWritten = false
				syncBaseComputed = false
				videoBaseSet = false
				audioBaseSet = false
				pending = nil
			}
			break
		}

		// Write combined init once we have both init segments
		if !initWritten && videoInitBytes != nil && audioInitBytes != nil {
			combined, err := buildCombinedInit(videoInitBytes, audioInitBytes)
			if err != nil {
				return fmt.Errorf("build combined init: %w", err)
			}
			if err := handler(combined, 0); err != nil {
				return fmt.Errorf("handler combined init: %w", err)
			}
			initWritten = true
		}
		if !initWritten {
			<-time.After(1 * time.Second)
			continue
		}

		// Download video fragments
		for _, v := range videoPlaylist.Segments {
			if v == nil {
				continue
			}
			seq := internal.SegmentSeq(v.URI)
			if seq == -1 && v.SeqId > 0 {
				seq = int(v.SeqId)
			}
			if server.Config.Debug && lastVideoSeq == -1 && lastVideoURI == "" {
				fmt.Printf("[DEBUG] muxed: first video segment URI: %s (seq=%d)\n", v.URI, seq)
			}
			if seq != -1 && seq <= lastVideoSeq {
				continue
			}
			if seq == -1 && v.URI == lastVideoURI {
				continue
			}

			segURL := resolveHLSURL(p.RootURL, v.URI)
			segBytes, err := retry.DoWithData(
				func() ([]byte, error) { return client.GetBytes(ctx, segURL) },
				retry.Context(ctx),
				retry.Attempts(3),
				retry.Delay(600*time.Millisecond),
				retry.DelayType(retry.FixedDelay),
				retry.RetryIf(func(err error) bool { return !errors.Is(err, internal.ErrNotFound) }),
			)
			if err != nil {
				if server.Config.Debug {
					fmt.Printf("[DEBUG] muxed video segment skipped: seq=%d err=%v\n", seq, err)
				}
				continue
			}

			rawTfdt, ok := extractMoofFirstTfdt(segBytes)
			if !ok {
				if server.Config.Debug {
					fmt.Printf("[DEBUG] muxed video segment has no tfdt: seq=%d\n", seq)
				}
				continue
			}

			if videoTimescale == 0 {
				return fmt.Errorf("video timescale is zero")
			}

			if !videoBaseSet {
				videoTimeBase = rawTfdt
				videoBaseSet = true
				if server.Config.Debug {
					fmt.Printf("[sync] muxed: video first tfdt=%d (%.6fs)\n", rawTfdt, float64(rawTfdt)/float64(videoTimescale))
				}
			}

			if seq != -1 {
				lastVideoSeq = seq
			}
			lastVideoURI = v.URI

			pending = append(pending, pendingSeg{
				track:       "video",
				presentTime: float64(rawTfdt) / float64(videoTimescale),
				duration:    v.Duration,
				data:        segBytes,
			})
		}

		// Download audio fragments
		for _, v := range audioPlaylist.Segments {
			if v == nil {
				continue
			}
			seq := internal.SegmentSeq(v.URI)
			if seq == -1 && v.SeqId > 0 {
				seq = int(v.SeqId)
			}

			if seq != -1 && seq <= lastAudioSeq {
				continue
			}
			if seq == -1 && v.URI == lastAudioURI {
				continue
			}

			segURL := resolveHLSURL(p.AudioPlaylistURL, v.URI)
			segBytes, err := retry.DoWithData(
				func() ([]byte, error) { return client.GetBytes(ctx, segURL) },
				retry.Context(ctx),
				retry.Attempts(3),
				retry.Delay(600*time.Millisecond),
				retry.DelayType(retry.FixedDelay),
				retry.RetryIf(func(err error) bool { return !errors.Is(err, internal.ErrNotFound) }),
			)
			if err != nil {
				if server.Config.Debug {
					fmt.Printf("[DEBUG] muxed audio segment skipped: seq=%d err=%v\n", seq, err)
				}
				continue
			}

			rawTfdt, ok := extractMoofFirstTfdt(segBytes)
			if !ok {
				if server.Config.Debug {
					fmt.Printf("[DEBUG] muxed audio segment has no tfdt: seq=%d\n", seq)
				}
				continue
			}

			if audioTimescale == 0 {
				return fmt.Errorf("audio timescale is zero")
			}

			if !audioBaseSet {
				audioTimeBase = rawTfdt
				audioBaseSet = true
				if server.Config.Debug {
					fmt.Printf("[sync] muxed: audio first tfdt=%d (%.6fs)\n", rawTfdt, float64(rawTfdt)/float64(audioTimescale))
				}
			}

			if seq != -1 {
				lastAudioSeq = seq
			}
			lastAudioURI = v.URI

			segBytes = rewriteAudioMoofTrackID(segBytes)

			pending = append(pending, pendingSeg{
				track:       "audio",
				presentTime: float64(rawTfdt) / float64(audioTimescale),
				duration:    0,
				data:        segBytes,
			})
		}

		if !syncBaseComputed && videoBaseSet && audioBaseSet {
			computeSyncShifts()
		}
		if !syncBaseComputed {
			<-time.After(1 * time.Second)
			continue
		}

		// Normalise timestamps and sort on the common timeline
		for i := range pending {
			seg := &pending[i]
			if seg.track == "video" {
				seg.presentTime -= float64(videoShift) / float64(videoTimescale)
				seg.data = shiftSegmentTfdt(seg.data, 1, videoShift)
			} else {
				seg.presentTime -= float64(audioShift) / float64(audioTimescale)
				seg.data = shiftSegmentTfdt(seg.data, 2, audioShift)
			}
		}

		sort.SliceStable(pending, func(i, j int) bool {
			if pending[i].presentTime != pending[j].presentTime {
				return pending[i].presentTime < pending[j].presentTime
			}
			return pending[i].track < pending[j].track
		})

		for _, seg := range pending {
			if err := handler(seg.data, seg.duration); err != nil {
				return fmt.Errorf("handler muxed segment: %w", err)
			}
		}
		pending = nil

		<-time.After(1 * time.Second)
	}
}
