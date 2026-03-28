package internal

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/server"
)

// Req represents an HTTP client with customized settings.
type Req struct {
	client  *http.Client
	isMedia bool // when true, omits browser-spoofing headers not needed for CDN media requests
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

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	// Check for Cloudflare protection
	if strings.Contains(string(b), "<title>Just a moment...</title>") {
		if server.Config.Debug {
			fmt.Printf("[DEBUG] CF response for %s (status %d)\n", req.URL, resp.StatusCode)
			tmpFile, ferr := os.CreateTemp("", "chaturbate-debug-cf-*.html")
			if ferr == nil {
				if _, werr := tmpFile.Write(b); werr == nil {
					fmt.Printf("[DEBUG]   Full body written to: %s\n", tmpFile.Name())
				}
				tmpFile.Close()
			}
		}
		return nil, ErrCloudflareBlocked
	}
	// Check for Age Verification
	if strings.Contains(string(b), "Verify your age") {
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
	return req, cancel, nil
}

// SetRequestHeaders applies necessary headers to the request.
func (h *Req) SetRequestHeaders(req *http.Request) {
	if h.isMedia {
		// Referer satisfies CDN hotlink protection on mmcdn.com media requests.
		req.Header.Set("Referer", "https://chaturbate.com/")
		req.Header.Set("Origin", "https://chaturbate.com")
	} else {
		// X-Requested-With helps bypass Cloudflare on chaturbate.com page fetches.
		// Do NOT send it to CDN media hosts (mmcdn.com) as it may cause rejection.
		req.Header.Set("X-Requested-With", "XMLHttpRequest")
	}
	if server.Config.UserAgent != "" {
		req.Header.Set("User-Agent", server.Config.UserAgent)
	}
	if server.Config.Cookies != "" {
		cookies := ParseCookies(server.Config.Cookies)
		for name, value := range cookies {
			req.AddCookie(&http.Cookie{Name: name, Value: value})
		}
	}
}

// ParseCookies converts a cookie string into a map.
func ParseCookies(cookieStr string) map[string]string {
	cookies := make(map[string]string)
	pairs := strings.Split(cookieStr, ";")

	// Iterate over each cookie pair and extract key-value pairs
	for _, pair := range pairs {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) == 2 {
			// Trim spaces around key and value
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			// Store cookie name and value in the map
			cookies[key] = value
		}
	}
	return cookies
}
