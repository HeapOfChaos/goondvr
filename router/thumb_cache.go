package router

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Thumbnail proxy cache.
//
// The dashboard asks for channel thumbnails all the time (every redraw, every
// minute, every view switch). Without a cache each of those requests was
// forwarded to the CDN, so a handful of channels were enough to flood both the
// browser's connection pool and the CDN. Images are now kept in memory for a
// short time and the browser is told it may reuse them as well.

const (
	cardThumbTTL = 5 * time.Minute // profile / summary-card image
	liveThumbTTL = 8 * time.Second // live preview snapshot: about the shortest age at which the site is asked for a newer one

	thumbCacheMaxEntries = 256
	thumbCacheMaxBytes   = 32 << 20 // 32 MiB
)

type thumbEntry struct {
	data        []byte
	contentType string
	expires     time.Time
}

type thumbCache struct {
	mu      sync.Mutex
	entries map[string]*thumbEntry
	bytes   int
}

var thumbs = &thumbCache{entries: make(map[string]*thumbEntry)}

// get returns a cached, still valid image for key.
func (c *thumbCache) get(key string) (*thumbEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expires) {
		c.removeLocked(key)
		return nil, false
	}
	return e, true
}

// put stores an image, evicting expired (then oldest) entries to stay within
// the entry and memory limits.
func (c *thumbCache) put(key string, data []byte, contentType string, ttl time.Duration) {
	if len(data) == 0 || len(data) > thumbCacheMaxBytes/4 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeLocked(key)
	now := time.Now()
	for len(c.entries) >= thumbCacheMaxEntries || c.bytes+len(data) > thumbCacheMaxBytes {
		var victim string
		var victimExpiry time.Time
		for k, e := range c.entries {
			if now.After(e.expires) {
				victim = k
				break
			}
			if victim == "" || e.expires.Before(victimExpiry) {
				victim, victimExpiry = k, e.expires
			}
		}
		if victim == "" {
			break
		}
		c.removeLocked(victim)
	}

	c.entries[key] = &thumbEntry{data: data, contentType: contentType, expires: now.Add(ttl)}
	c.bytes += len(data)
}

func (c *thumbCache) removeLocked(key string) {
	if e, ok := c.entries[key]; ok {
		c.bytes -= len(e.data)
		delete(c.entries, key)
	}
}

// writeThumb sends an image and lets the browser keep it for ttl.
func writeThumb(w http.ResponseWriter, data []byte, contentType string, ttl time.Duration) {
	w.Header().Set("Cache-Control", "private, max-age="+strconv.Itoa(int(ttl/time.Second)))
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
