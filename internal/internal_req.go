package internal

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/HeapOfChaos/goondvr/server"
)

// Req represents an HTTP client with customized settings.
type Req struct {
	client  *http.Client
	isMedia bool   // when true, omits browser-spoofing headers not needed for CDN media requests
	referer string // CDN Referer/Origin override; only used when isMedia is true
}

// NewReq creates a new HTTP client for Chaturbate page requests.
func NewReq() *Req {
	return &Req{
		client: &http.Client{
			Transport: CreateTransport(),
		},
	}
}

// NewMediaReq creates a new HTTP client for CDN media requests (playlists, segments).
// It omits headers like X-Requested-With that are only needed for Chaturbate page fetches.
func NewMediaReq() *Req {
	return &Req{
		client: &http.Client{
			Transport: CreateTransport(),
		},
		isMedia: true,
	}
}

// NewMediaReqWithReferer creates a media HTTP client that sends the given URL as
// Referer and Origin instead of the Chaturbate defaults. Use this for non-Chaturbate CDNs.
func NewMediaReqWithReferer(referer string) *Req {
	return &Req{
		client: &http.Client{
			Transport: CreateTransport(),
		},
		isMedia: true,
		referer: referer,
	}
}

// CreateTransport initializes a custom HTTP transport.
func CreateTransport() *http.Transport {
	// The DefaultTransport allows user changes the proxy settings via environment variables
	// such as HTTP_PROXY, HTTPS_PROXY.
	defaultTransport := http.DefaultTransport.(*http.Transport)

	newTransport := defaultTransport.Clone()
	newTransport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	return newTransport
}

// Get sends an HTTP GET request and returns the response as a string.
func (h *Req) Get(ctx context.Context, url string) (string, error) {
	resp, err := h.GetBytes(ctx, url)
	if err != nil {
		return "", fmt.Errorf("get bytes: %w", err)
	}
	return string(resp), nil
}

// GetBytes sends an HTTP GET request and returns the response as a byte slice.
func (h *Req) GetBytes(ctx context.Context, url string) ([]byte, error) {
	req, cancel, err := h.CreateRequest(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	defer cancel()

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("client do: %w", err)
	}
	defer resp.Body.Close()

	if server.Config.Debug && resp.StatusCode >= 400 {
		fmt.Printf("[DEBUG] HTTP %d: %s\n", resp.StatusCode, req.URL)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	if server.Config.Debug && shouldDumpDebugResponse(resp, b) {
		dumpDebugResponse(req.URL.String(), resp, b)
	}

	// Check for Cloudflare protection
	if isCloudflareBlockedResponse(resp, b) {
		return nil, ErrCloudflareBlocked
	}
	// Check for Age Verification
	if bytes.Contains(b, []byte("Verify your age")) {
		return nil, ErrAgeVerification
	}

	if resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("forbidden: %w", ErrPrivateStream)
	}

	return b, err
}

// CreateRequest constructs an HTTP GET request with necessary headers.
func (h *Req) CreateRequest(ctx context.Context, url string) (*http.Request, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second) // timed out after 10 seconds

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, cancel, err
	}
	h.SetRequestHeaders(req)
	if server.Config.Debug {
		dumpDebugRequest(req)
	}
	return req, cancel, nil
}

// DoRequest executes an already-constructed *http.Request and returns the
// response body as a string. This allows callers to set extra headers on the
// request before executing it (e.g. site-specific Referer or X-Requested-With).
func (h *Req) DoRequest(req *http.Request) (string, error) {
	if server.Config.Debug {
		dumpDebugRequest(req)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("client do: %w", err)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}

	if server.Config.Debug && shouldDumpDebugResponse(resp, b) {
		dumpDebugResponse(req.URL.String(), resp, b)
	}

	// Check for Cloudflare protection
	if isCloudflareBlockedResponse(resp, b) {
		return "", ErrCloudflareBlocked
	}
	// Check for Age Verification
	if bytes.Contains(b, []byte("Verify your age")) {
		return "", ErrAgeVerification
	}

	if resp.StatusCode == http.StatusForbidden {
		return "", fmt.Errorf("forbidden: %w", ErrPrivateStream)
	}

	return string(b), nil
}

// SetRequestHeaders applies necessary headers to the request.
func (h *Req) SetRequestHeaders(req *http.Request) {
	if h.isMedia {
		ref := h.referer
		if ref == "" {
			ref = "https://chaturbate.com/"
		}
		req.Header.Set("Referer", ref)
		req.Header.Set("Origin", strings.TrimRight(ref, "/"))
	} else {
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Pragma", "no-cache")
		req.Header.Set("Priority", "u=0")
		req.Header.Set("Sec-Fetch-Dest", "empty")
		req.Header.Set("Sec-Fetch-Mode", "cors")
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		req.Header.Set("Referer", deriveBrowserReferer(req.URL))

		// X-Requested-With helps bypass Cloudflare on chaturbate.com page fetches.
		// Do NOT send it to CDN media hosts (mmcdn.com) as it may cause rejection.
		req.Header.Set("X-Requested-With", "XMLHttpRequest")
	}
	if server.Config.UserAgent != "" {
		req.Header.Set("User-Agent", server.Config.UserAgent)
	}
	if shouldSendConfiguredCookies(req.URL) {
		if cookieHeader := NormalizeCookieString(server.Config.Cookies); cookieHeader != "" {
			req.Header.Set("Cookie", cookieHeader)
		}
	}
}

func shouldSendConfiguredCookies(target *url.URL) bool {
	if target == nil {
		return false
	}

	host := strings.ToLower(target.Hostname())
	if host == "" {
		return false
	}

	if configuredHost := configuredCookieHost(); configuredHost != "" {
		return host == configuredHost || strings.HasSuffix(host, "."+configuredHost)
	}

	return host == "chaturbate.com" || strings.HasSuffix(host, ".chaturbate.com")
}

func configuredCookieHost() string {
	domain := strings.TrimSpace(server.Config.Domain)
	if domain == "" {
		return ""
	}
	u, err := url.Parse(domain)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

func DumpParseFailure(label, body string) {
	if !server.Config.Debug {
		return
	}
	body = strings.TrimSpace(body)
	end := len(body)
	if end > 1200 {
		end = 1200
	}
	fmt.Printf("[DEBUG] %s parse failure: body_len=%d\n", label, len(body))
	if end > 0 {
		fmt.Printf("[DEBUG] %s body preview:\n%s\n", label, body[:end])
	}
	if body == "" {
		fmt.Printf("[DEBUG] %s body preview: <empty>\n", label)
	}
}

func NormalizeCookieString(cookieStr string) string {
	cookieStr = strings.TrimSpace(cookieStr)
	if cookieStr == "" {
		return ""
	}

	lines := strings.FieldsFunc(cookieStr, func(r rune) bool {
		return r == '\n' || r == '\r'
	})
	parts := make([]string, 0, len(lines))
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if i == 0 && strings.HasPrefix(strings.ToLower(line), "cookie:") {
			line = strings.TrimSpace(line[len("cookie:"):])
		}
		if line != "" {
			parts = append(parts, line)
		}
	}
	return strings.Join(parts, "; ")
}

func SanitizeCookieString(cookieStr string) string {
	return serializeCookieEntries(filterImportedCookieEntries(parseCookieEntries(cookieStr)))
}

type cookieEntry struct {
	name  string
	value string
}

func parseCookieEntries(cookieStr string) []cookieEntry {
	cookieStr = NormalizeCookieString(cookieStr)
	if cookieStr == "" {
		return nil
	}

	pairs := strings.Split(cookieStr, ";")
	entries := make([]cookieEntry, 0, len(pairs))
	for _, pair := range pairs {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) != 2 {
			continue
		}
		name := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if name == "" {
			continue
		}
		entries = append(entries, cookieEntry{name: name, value: value})
	}
	return entries
}

func filterImportedCookieEntries(entries []cookieEntry) []cookieEntry {
	if len(entries) == 0 {
		return nil
	}

	filtered := make([]cookieEntry, 0, len(entries))
	for _, entry := range entries {
		if shouldIgnoreImportedCookie(entry.name) {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func shouldIgnoreImportedCookie(name string) bool {
	name = strings.TrimSpace(strings.ToLower(name))
	return strings.HasPrefix(name, "tbu_")
}

func serializeCookieEntries(entries []cookieEntry) string {
	if len(entries) == 0 {
		return ""
	}

	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		parts = append(parts, fmt.Sprintf("%s=%s", entry.name, entry.value))
	}
	return strings.Join(parts, "; ")
}

// MergeCookieUpdate preserves the existing cookie jar when the user only pastes
// a replacement cf_clearance value from the browser. Any fuller cookie input
// replaces the stored cookie string as-is.
func MergeCookieUpdate(existing, incoming string) string {
	incoming = SanitizeCookieString(incoming)
	if incoming == "" {
		return ""
	}

	incomingEntries := parseCookieEntries(incoming)
	if len(incomingEntries) != 1 || incomingEntries[0].name != "cf_clearance" {
		return incoming
	}

	existingEntries := parseCookieEntries(SanitizeCookieString(existing))
	if len(existingEntries) == 0 {
		return incoming
	}

	updated := false
	for i := range existingEntries {
		if existingEntries[i].name == "cf_clearance" {
			existingEntries[i].value = incomingEntries[0].value
			updated = true
			break
		}
	}
	if !updated {
		existingEntries = append(existingEntries, incomingEntries[0])
	}

	return serializeCookieEntries(filterImportedCookieEntries(existingEntries))
}

// ExtractBrowserImport parses a browser-exported request blob, such as Firefox
// "Copy as cURL", and extracts the Cookie and User-Agent headers when present.
func ExtractBrowserImport(raw string) (cookieHeader, userAgent string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}

	headerLines := extractHeaderLines(raw)
	for _, headerLine := range headerLines {
		name, value, ok := strings.Cut(headerLine, ":")
		if !ok {
			continue
		}

		switch strings.ToLower(strings.TrimSpace(name)) {
		case "cookie":
			cookieHeader = serializeCookieEntries(filterImportedCookieEntries(parseCookieEntries(value)))
		case "user-agent":
			userAgent = strings.TrimSpace(value)
		}
	}

	return cookieHeader, userAgent
}

func extractHeaderLines(raw string) []string {
	normalized := strings.ReplaceAll(raw, "\\\r\n", " ")
	normalized = strings.ReplaceAll(normalized, "\\\n", " ")

	var headers []string
	segments := strings.Split(normalized, "-H ")
	if len(segments) > 1 {
		for _, segment := range segments[1:] {
			segment = strings.TrimSpace(segment)
			if segment == "" {
				continue
			}

			headerLine, ok := parseQuotedSegment(segment)
			if ok {
				headers = append(headers, headerLine)
			}
		}
		if len(headers) > 0 {
			return headers
		}
	}

	for _, line := range strings.FieldsFunc(raw, func(r rune) bool { return r == '\n' || r == '\r' }) {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(line), "cookie:") || strings.HasPrefix(strings.ToLower(line), "user-agent:") {
			headers = append(headers, line)
		}
	}
	return headers
}

func parseQuotedSegment(segment string) (string, bool) {
	if segment == "" {
		return "", false
	}

	quote := segment[0]
	if quote != '\'' && quote != '"' {
		return "", false
	}

	end := strings.IndexByte(segment[1:], quote)
	if end < 0 {
		return "", false
	}

	return segment[1 : 1+end], true
}

func cookieDebugInfo(cookieStr string) []string {
	cookieStr = NormalizeCookieString(cookieStr)
	if cookieStr == "" {
		return nil
	}

	pairs := strings.Split(cookieStr, ";")
	info := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) != 2 {
			continue
		}
		name := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if name == "" {
			continue
		}
		info = append(info, fmt.Sprintf("%s(len=%d)", name, len(value)))
	}
	sort.Strings(info)
	return info
}

func shouldDumpDebugResponse(resp *http.Response, body []byte) bool {
	return resp.StatusCode >= 400 || looksLikeHTMLResponse(resp, body) || isCloudflareBlockPage(resp, body)
}

func looksLikeHTMLResponse(resp *http.Response, body []byte) bool {
	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	if strings.Contains(contentType, "text/html") {
		return true
	}

	bodyStart := strings.ToLower(strings.TrimSpace(string(body[:min(len(body), 512)])))
	return strings.HasPrefix(bodyStart, "<!doctype html") ||
		strings.HasPrefix(bodyStart, "<html") ||
		strings.Contains(bodyStart, "<head") ||
		strings.Contains(bodyStart, "<body")
}

func isCloudflareBlockPage(resp *http.Response, body []byte) bool {
	content := strings.ToLower(string(body))
	return strings.Contains(content, "<title>just a moment...</title>") ||
		strings.Contains(content, "cf-browser-verification") ||
		strings.Contains(content, "cf_chl_") ||
		strings.Contains(content, "challenge-platform") ||
		(strings.Contains(content, "cloudflare") && looksLikeHTMLResponse(resp, body))
}

func isCloudflareBlockedResponse(resp *http.Response, body []byte) bool {
	if resp == nil {
		return false
	}
	if isCloudflareBlockPage(resp, body) {
		return true
	}
	if resp.StatusCode == http.StatusTeapot {
		serverHeader := strings.ToLower(resp.Header.Get("Server"))
		if strings.Contains(serverHeader, "cloudflare") {
			return true
		}
		for _, cookie := range resp.Header.Values("Set-Cookie") {
			lower := strings.ToLower(cookie)
			if strings.Contains(lower, "__cf_bm=") || strings.Contains(lower, "_cfuvid=") {
				return true
			}
		}
	}
	return false
}

func dumpDebugResponse(url string, resp *http.Response, body []byte) {
	fmt.Printf("[DEBUG] HTTP response for %s (status %d)\n", url, resp.StatusCode)
	tmpFile, err := os.CreateTemp("", "chaturbate-debug-response-*.http")
	if err != nil {
		fmt.Printf("[DEBUG]   Failed to create temp file: %v\n", err)
		return
	}
	defer tmpFile.Close()

	if _, err := fmt.Fprintf(tmpFile, "URL: %s\nStatus: %s\n", url, resp.Status); err != nil {
		fmt.Printf("[DEBUG]   Failed to write temp file: %v\n", err)
		return
	}
	if err := resp.Header.Write(tmpFile); err != nil {
		fmt.Printf("[DEBUG]   Failed to write headers: %v\n", err)
		return
	}
	if _, err := tmpFile.WriteString("\n"); err != nil {
		fmt.Printf("[DEBUG]   Failed to write separator: %v\n", err)
		return
	}
	if _, err := tmpFile.Write(body); err != nil {
		fmt.Printf("[DEBUG]   Failed to write body: %v\n", err)
		return
	}

	fmt.Printf("[DEBUG]   Full response written to: %s\n", tmpFile.Name())
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func deriveBrowserReferer(target *url.URL) string {
	base := strings.TrimRight(server.Config.Domain, "/")
	if base == "" {
		base = "https://chaturbate.com"
	}
	if target == nil {
		return base + "/"
	}

	trimmedPath := strings.Trim(target.Path, "/")
	parts := strings.Split(trimmedPath, "/")
	if len(parts) >= 3 && parts[0] == "api" {
		username := parts[2]
		if username != "" {
			return fmt.Sprintf("%s/%s/", base, username)
		}
	}

	return base + "/"
}

func dumpDebugRequest(req *http.Request) {
	fmt.Printf("[DEBUG] HTTP request %s %s\n", req.Method, req.URL.String())

	headerNames := make([]string, 0, len(req.Header))
	for name := range req.Header {
		headerNames = append(headerNames, name)
	}
	sort.Strings(headerNames)

	for _, name := range headerNames {
		if strings.EqualFold(name, "Cookie") {
			continue
		}
		fmt.Printf("[DEBUG]   %s: %s\n", name, strings.Join(req.Header.Values(name), ", "))
	}

	cookieInfo := cookieDebugInfo(req.Header.Get("Cookie"))
	if len(cookieInfo) == 0 {
		fmt.Println("[DEBUG]   Cookies: none")
		return
	}
	fmt.Printf("[DEBUG]   Cookies: %s\n", strings.Join(cookieInfo, ", "))
}
