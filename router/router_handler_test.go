package router

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/gin-gonic/gin"
)

type fakeManager struct {
	server.IManager
	paused, resumed []string
	resets          int
	updatedID       string
	updated         entity.ChannelPatch
	updateErr       error
}

func (f *fakeManager) UpdateChannel(id string, p entity.ChannelPatch) error {
	f.updatedID, f.updated = id, p
	return f.updateErr
}

func (f *fakeManager) ResetPresence() { f.resets++ }

func (f *fakeManager) PauseChannel(id string) error  { f.paused = append(f.paused, id); return nil }
func (f *fakeManager) ResumeChannel(id string) error { f.resumed = append(f.resumed, id); return nil }

func newTestRouter(t *testing.T) (*gin.Engine, *fakeManager) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	old := server.Manager
	fm := &fakeManager{}
	server.Manager = fm
	t.Cleanup(func() { server.Manager = old })

	r := gin.New()
	r.POST("/pause_channel/:channelID", PauseChannel)
	r.POST("/resume_channel/:channelID", ResumeChannel)
	return r, fm
}

func do(r *gin.Engine, path string, htmx bool) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, nil)
	if htmx {
		req.Header.Set("HX-Request", "true")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// The htmx buttons must not trigger a download of the whole dashboard.
func TestPauseResumeFromHtmxGetNoContent(t *testing.T) {
	r, fm := newTestRouter(t)

	for _, path := range []string{"/pause_channel/chaturbate__a", "/resume_channel/chaturbate__a"} {
		w := do(r, path, true)
		if w.Code != http.StatusNoContent {
			t.Fatalf("%s: status %d, want 204", path, w.Code)
		}
		if loc := w.Header().Get("Location"); loc != "" {
			t.Fatalf("%s: unexpected redirect to %q", path, loc)
		}
		if w.Body.Len() != 0 {
			t.Fatalf("%s: body should be empty, got %d bytes", path, w.Body.Len())
		}
	}
	if len(fm.paused) != 1 || len(fm.resumed) != 1 {
		t.Fatalf("handlers did not reach the manager: paused=%v resumed=%v", fm.paused, fm.resumed)
	}
}

// ...while ordinary form posts keep redirecting to the dashboard.
func TestPauseResumeFromPlainPostStillRedirect(t *testing.T) {
	r, _ := newTestRouter(t)

	for _, path := range []string{"/pause_channel/chaturbate__a", "/resume_channel/chaturbate__a"} {
		w := do(r, path, false)
		if w.Code != http.StatusFound || w.Header().Get("Location") != "/" {
			t.Fatalf("%s: status %d Location %q, want 302 to /", path, w.Code, w.Header().Get("Location"))
		}
	}
}

func postForm(r *gin.Engine, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestUpdateConfigSavesPresenceAndNewChannelDefaults(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Chdir(t.TempDir())
	oldCfg, oldMgr := server.Config, server.Manager
	server.Config = &entity.Config{Framerate: 60, Resolution: 1080, Pattern: "videos/{{.Username}}", MaxDuration: 0, MaxFilesize: 0, AutoMinFreeGB: 10}
	fm := &fakeManager{}
	server.Manager = fm
	t.Cleanup(func() { server.Config, server.Manager = oldCfg, oldMgr })
	r := gin.New()
	r.POST("/update_config", UpdateConfig)

	w := postForm(r, "/update_config", "presence_campaign=+camp9+&presence_interval=90&auto_min_free_gb=25"+
		"&default_framerate=30&default_resolution=720&default_pattern=videos%2F%7B%7B.Username%7D%7D_x&default_max_duration=60&default_max_filesize=2048")
	if w.Code != http.StatusFound {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	c := server.Config
	if c.PresenceCampaign != "camp9" || c.PresenceInterval != 90 || c.AutoMinFreeGB != 25 ||
		c.Framerate != 30 || c.Resolution != 720 || c.Pattern != "videos/{{.Username}}_x" || c.MaxDuration != 60 || c.MaxFilesize != 2048 {
		t.Fatalf("settings not applied: %+v", c)
	}
	if fm.resets != 1 {
		t.Fatal("the presence engine must be told to retry its list")
	}
	saved, err := os.ReadFile("conf/settings.json")
	if err != nil || !strings.Contains(string(saved), `"presence_campaign": "camp9"`) || !strings.Contains(string(saved), `"default_resolution": 720`) {
		t.Fatalf("settings.json: %v\n%s", err, saved)
	}

	// A form that does not carry a field leaves its value alone; a bad pattern is refused.
	w = postForm(r, "/update_config", "presence_campaign=camp9")
	if w.Code != http.StatusFound || server.Config.MaxFilesize != 2048 || server.Config.AutoMinFreeGB != 25 || server.Config.Resolution != 720 {
		t.Fatalf("absent fields must stay: %d %+v", w.Code, server.Config)
	}
	w = postForm(r, "/update_config", "default_pattern=%7B%7B.Nope")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("a broken pattern must be refused, got %d", w.Code)
	}
}

func TestUpdateChannelSendsOnlyWhatTheFormHas(t *testing.T) {
	r, fm := newTestRouter(t)
	r.POST("/update_channel/:channelID", UpdateChannel)

	// the auto-record button sends just its own field
	w := postForm(r, "/update_channel/chaturbate__a", "auto_record=1")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status %d, want 204", w.Code)
	}
	p := fm.updated
	if fm.updatedID != "chaturbate__a" || p.AutoRecord == nil || !*p.AutoRecord {
		t.Fatalf("auto_record=1 must turn it on: %+v", p)
	}
	if p.Resolution != nil || p.Framerate != nil || p.Pattern != nil || p.MaxDuration != nil || p.MaxFilesize != nil || p.AutoNoDiskCheck != nil {
		t.Fatalf("nothing else may change: %+v", p)
	}

	// the dialog sends everything; the disk check is the default, so "1" means NOT skipping it
	w = postForm(r, "/update_channel/chaturbate__a",
		"resolution=720&framerate=30&pattern=+videos%2F%7B%7B.Username%7D%7D+&max_duration=15&max_filesize=700&auto_record=0&auto_disk_check=0")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status %d", w.Code)
	}
	p = fm.updated
	if *p.Resolution != 720 || *p.Framerate != 30 || *p.Pattern != "videos/{{.Username}}" || *p.MaxDuration != 15 || *p.MaxFilesize != 700 {
		t.Fatalf("values: %+v", p)
	}
	if *p.AutoRecord || !*p.AutoNoDiskCheck {
		t.Fatalf("auto_record=0, auto_disk_check=0 => off, and the check is skipped: %+v", p)
	}
	postForm(r, "/update_channel/chaturbate__a", "auto_disk_check=1")
	if *fm.updated.AutoNoDiskCheck {
		t.Fatal("auto_disk_check=1 means the check is made")
	}

	// a refusal reaches the user as text
	fm.updateErr = errors.New("framerate must be 30 or 60")
	w = postForm(r, "/update_channel/chaturbate__a", "framerate=45")
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "framerate must be 30 or 60") {
		t.Fatalf("refusal: %d %q", w.Code, w.Body.String())
	}
	if w = postForm(r, "/update_channel/chaturbate__a", "framerate=abc"); w.Code != http.StatusBadRequest {
		t.Fatalf("a value that is not a number must be a 400, got %d", w.Code)
	}
}
