package detect

import (
	"bytes"
	"context"
	"encoding/binary"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestBest(t *testing.T) {
	r := &Result{Scores: map[string]float64{"person": 0.7, "cup": 0.9, "Laptop": 0.4}}
	for _, c := range []struct {
		labels []string
		min    float64
		want   string
		ok     bool
	}{
		{nil, 0.6, "cup", true},                         // any label
		{[]string{"person"}, 0.6, "person", true},       // only the ones asked for
		{[]string{"PERSON", " cup "}, 0.8, "cup", true}, // case and spaces do not matter
		{[]string{"laptop"}, 0.6, "Laptop", false},      // there, but below the minimum
		{[]string{"dog"}, 0.1, "", false},               // not there at all
	} {
		label, _, ok := r.Best(c.labels, c.min)
		if label != c.want || ok != c.ok {
			t.Errorf("Best(%v, %v) = %q, %v; want %q, %v", c.labels, c.min, label, ok, c.want, c.ok)
		}
	}
	if _, _, ok := (*Result)(nil).Best(nil, 0); ok {
		t.Error("a missing result matched")
	}
}

func TestDetectSendsTheModelTheTokenAndThePicture(t *testing.T) {
	var gotModel, gotAuth string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotModel, gotAuth = r.URL.Query().Get("model"), r.Header.Get("Authorization")
		gotBody = make([]byte, r.ContentLength)
		_, _ = r.Body.Read(gotBody)
		_, _ = w.Write([]byte(`{"scores":{"a":0.9},"model":"m","kind":"embed","provider":"CPUExecutionProvider","ms":3}`))
	}))
	defer srv.Close()
	res, err := Detect(context.Background(), Target{srv.URL + "/", "tok"}, "mi modelo", []byte("IMG"))
	if err != nil {
		t.Fatal(err)
	}
	if gotModel != "mi modelo" || gotAuth != "Bearer tok" || string(gotBody) != "IMG" {
		t.Fatalf("sent model=%q auth=%q body=%q", gotModel, gotAuth, gotBody)
	}
	if res.Scores["a"] != 0.9 || res.Provider != "CPUExecutionProvider" {
		t.Fatalf("result %+v", res)
	}
}

func TestDetectErrorsAreUnderstandable(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("model") {
		case "401":
			w.WriteHeader(401)
		case "404":
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":"model \"x\" not found"}`))
		case "junk":
			_, _ = w.Write([]byte(`<html>`))
		default:
			w.WriteHeader(502)
		}
	}))
	defer bad.Close()
	for model, want := range map[string]string{"401": "refused the token", "404": "not found", "junk": "not understood", "x": "HTTP 502"} {
		_, err := Detect(context.Background(), Target{URL: bad.URL}, model, []byte("i"))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("model %s: error %v, want it to say %q", model, err, want)
		}
	}
	_, err := Detect(context.Background(), Target{URL: "http://127.0.0.1:1"}, "m", []byte("i"))
	if err == nil || !strings.Contains(err.Error(), "does not answer") {
		t.Errorf("a detector that is not running: %v", err)
	}
	if _, err := Detect(context.Background(), Target{}, "m", nil); err == nil {
		t.Error("an empty address was accepted")
	}
}

func TestWindowsAddPaddingAndJoinWhatTouches(t *testing.T) {
	got := Windows([]Span{{Start: 100, End: 110}, {Start: 3, End: 4}, {Start: 118, End: 130}, {Start: 300, End: 301}}, 5, 10)
	want := []Window{{0, 14}, {95, 140}, {295, 311}}
	if len(got) != len(want) {
		t.Fatalf("%v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("window %d = %v, want %v (%v)", i, got[i], want[i], got)
		}
	}
	if Windows(nil, 5, 10) != nil {
		t.Fatal("no spans, no windows")
	}
}

func TestSidecarRoundTrip(t *testing.T) {
	rec := filepath.Join(t.TempDir(), "a.ts")
	sc := Sidecar{Pre: 5, Post: 10, Clips: true, Spans: []Span{{1, 2, "x", 0.9}}}
	if err := WriteSidecar(rec, sc); err != nil {
		t.Fatal(err)
	}
	got, err := ReadSidecar(SidecarPath(rec))
	if err != nil || got.Pre != 5 || len(got.Spans) != 1 || got.Spans[0].Label != "x" {
		t.Fatalf("%+v %v", got, err)
	}
}

// ---- the ones that run ffmpeg (skipped when it is not installed)

func needFFmpeg(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
}

// makeVideo writes a 12 s test video with a key frame every second, in the container of the extension.
func makeVideo(t *testing.T, name string, extra ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	args := append([]string{"-y", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=320x240:rate=10:duration=12",
		"-c:v", "libx264", "-g", "10", "-pix_fmt", "yuv420p"}, extra...)
	if out, err := exec.Command("ffmpeg", append(args, p)...).CombinedOutput(); err != nil {
		t.Fatalf("ffmpeg: %v %s", err, out)
	}
	return p
}

func isJPEG(t *testing.T, b []byte) (w int) {
	t.Helper()
	cfg, err := jpeg.DecodeConfig(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("not a JPEG: %v", err)
	}
	return cfg.Width
}

func TestFrameFromTSSegments(t *testing.T) {
	needFFmpeg(t)
	b, _ := os.ReadFile(makeVideo(t, "a.ts"))
	// the last 3 packets-worth of a stream cut at an arbitrary packet boundary: the decoder must find a key frame
	tail := b[len(b)/3/188*188:]
	img, err := FrameFromSegments(context.Background(), tail)
	if err != nil {
		t.Fatal(err)
	}
	if w := isJPEG(t, img); w != 320 {
		t.Fatalf("width %d", w)
	}
}

// splitFMP4 returns the init segment and the fragments (moof+mdat) of a fragmented MP4.
func splitFMP4(t *testing.T, b []byte) (init []byte, frags [][]byte) {
	t.Helper()
	start := -1
	for pos := 0; pos+8 <= len(b); {
		size := int(binary.BigEndian.Uint32(b[pos:]))
		typ := string(b[pos+4 : pos+8])
		switch typ {
		case "moof":
			if start >= 0 {
				frags = append(frags, b[start:pos])
			} else {
				init = b[:pos]
			}
			start = pos
		case "mfra":
			if start >= 0 {
				frags = append(frags, b[start:pos])
				start = -1
			}
		}
		pos += size
	}
	if start >= 0 {
		frags = append(frags, b[start:])
	}
	return
}

func TestFrameFromFragmentedMP4(t *testing.T) {
	needFFmpeg(t)
	b, _ := os.ReadFile(makeVideo(t, "a.mp4", "-movflags", "frag_keyframe+empty_moov+default_base_moof"))
	init, frags := splitFMP4(t, b)
	if len(frags) < 4 {
		t.Fatalf("test video has %d fragments", len(frags))
	}
	last := append(append([]byte{}, frags[len(frags)-2]...), frags[len(frags)-1]...)
	img, err := FrameFromSegments(context.Background(), init, last)
	if err != nil {
		t.Fatal(err)
	}
	isJPEG(t, img)
	if _, err := FrameFromSegments(context.Background(), last); err == nil { // without the init segment nothing can be decoded
		t.Fatal("fragments without the init segment gave a picture")
	}
}

func duration(t *testing.T, p string) float64 {
	t.Helper()
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	out, err := exec.Command("ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", p).Output()
	if err != nil {
		t.Fatal(err)
	}
	d, _ := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	return d
}

func TestCutClips(t *testing.T) {
	needFFmpeg(t)
	for _, name := range []string{"a.mp4", "a.ts"} {
		rec := makeVideo(t, name)
		made, err := CutClips(context.Background(), rec, []Window{{2, 5}, {0.2, 0.5} /* too short */, {8, 11}})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(made) != 2 || !strings.HasSuffix(made[0], "_clip01"+filepath.Ext(name)) {
			t.Fatalf("%s: made %v", name, made)
		}
		if d := duration(t, made[0]); d < 2.5 || d > 4 {
			t.Errorf("%s: first clip lasts %.1f s, want about 3", name, d)
		}
		if _, err := os.Stat(strings.TrimSuffix(made[0], filepath.Ext(name)) + ".part" + filepath.Ext(name)); !os.IsNotExist(err) {
			t.Errorf("%s: a temporary file was left", name)
		}
	}
	if _, err := CutClips(context.Background(), filepath.Join(t.TempDir(), "missing.mp4"), []Window{{0, 5}}); err == nil {
		t.Error("a missing recording did not fail")
	}
}
