package notifier

import (
	"fmt"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/server"
)

// Cooldown key helpers — use these to build consistent keys.
const (
	KeyStreamOnline = "stream_online:%s"  // % username
	KeyCFChannel    = "cf_channel:%s"     // % username
	KeyCFGlobal     = "cf_global"
	KeyDiskWarning  = "disk_warning:%s"   // % path
	KeyDiskCritical = "disk_critical:%s"  // % path
)

// Default is the package-level notifier singleton.
var Default = &Notifier{
	cooldowns: make(map[string]time.Time),
}

// Notifier manages per-key cooldowns and dispatches to configured backends.
type Notifier struct {
	mu        sync.Mutex
	cooldowns map[string]time.Time
}

func (n *Notifier) canFire(key string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	cooldown := time.Duration(server.Config.NotifyCooldownHours) * time.Hour
	if cooldown <= 0 {
		cooldown = 4 * time.Hour
	}
	last, ok := n.cooldowns[key]
	if !ok || time.Since(last) >= cooldown {
		n.cooldowns[key] = time.Now()
		return true
	}
	return false
}

// ResetCooldown clears the cooldown for a key so the next event fires immediately.
// Call when a condition resolves (e.g. CF blocks stop for a channel).
func (n *Notifier) ResetCooldown(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.cooldowns, key)
}

// Notify sends title+message to all configured backends if the cooldown for key
// has expired. Reads backend config from server.Config at call time.
func Notify(key, title, message string) {
	if !Default.canFire(key) {
		return
	}
	cfg := server.Config
	if cfg.NtfyURL != "" && cfg.NtfyTopic != "" {
		if err := sendNtfy(cfg.NtfyURL, cfg.NtfyTopic, cfg.NtfyToken, title, message); err != nil {
			fmt.Printf("[WARN] ntfy: %v\n", err)
		}
	}
	if cfg.DiscordWebhookURL != "" {
		if err := sendDiscord(cfg.DiscordWebhookURL, title, message); err != nil {
			fmt.Printf("[WARN] discord: %v\n", err)
		}
	}
}
