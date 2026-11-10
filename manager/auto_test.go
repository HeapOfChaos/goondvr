package manager

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/HeapOfChaos/goondvr/channel"
	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/site"
)

// autoRig is a presence rig with channels the test configures itself.
func autoRig(t *testing.T, cfgs ...*entity.ChannelConfig) (*Manager, *fakeSite, map[string]*channel.Channel) {
	t.Helper()
	m, f, _ := presenceRig(t, "camp")
	server.Config.Interval = 1 // minutes between two looks at an offline channel (a monitor that starts must not spin)
	oldGap, oldRelax := autoStartGap, autoRelaxAfter
	autoStartGap, autoRelaxAfter = time.Millisecond, 40*time.Millisecond
	t.Cleanup(func() { autoStartGap, autoRelaxAfter = oldGap, oldRelax })

	chans := map[string]*channel.Channel{}
	for _, c := range cfgs {
		if c.Site == "" {
			c.Site = "chaturbate"
		}
		ch := channel.New(c)
		m.Channels.Store(entity.ChannelID(c.Site, c.Username), ch)
		chans[c.Username] = ch
	}
	return m, f, chans
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func live() *site.Presence { return &site.Presence{Live: true, NumViewers: 3} }

func TestAutoRecordStartsOnlyTheArmedChannelsThatAreOnAir(t *testing.T) {
	m, _, ch := autoRig(t,
		&entity.ChannelConfig{Username: "armed", IsPaused: true, AutoRecord: true},
		&entity.ChannelConfig{Username: "plain", IsPaused: true}, // auto-record is off by default
		&entity.ChannelConfig{Username: "away", IsPaused: true, AutoRecord: true},
	)
	for _, name := range []string{"armed", "plain"} {
		ch[name].ApplyPresence(live())
	}
	m.autoStep()

	waitFor(t, "the armed channel to start", func() bool { return !ch["armed"].Paused() })
	if !ch["plain"].Paused() {
		t.Fatal("a channel without auto-record must stay paused")
	}
	if !ch["away"].Paused() {
		t.Fatal("an armed channel that is not on the air must stay paused")
	}
	if !ch["armed"].AutoStarted() {
		t.Fatal("the start must be remembered as automatic")
	}
	if snap := ch["armed"].ConfigSnapshot(); !snap.IsPaused || !snap.AutoRecord {
		t.Fatalf("what is saved is what the user chose (armed, paused): %+v", snap)
	}
}

func TestAutoRecordWaitsWhenTheDiskIsTooFullAndTheCheckCanBeTurnedOff(t *testing.T) {
	dir := t.TempDir()
	pat := filepath.ToSlash(filepath.Join(dir, "not-created-yet", "{{.Username}}")) // the folder does not exist yet
	m, _, ch := autoRig(t,
		&entity.ChannelConfig{Username: "checked", IsPaused: true, AutoRecord: true, Pattern: pat},
		&entity.ChannelConfig{Username: "unchecked", IsPaused: true, AutoRecord: true, AutoNoDiskCheck: true, Pattern: pat},
	)
	server.Config.AutoMinFreeGB = 1_000_000_000 // 1e9 GB: no disk has that much free
	ch["checked"].ApplyPresence(live())
	ch["unchecked"].ApplyPresence(live())

	m.autoStep()

	waitFor(t, "the channel without the check to start", func() bool { return !ch["unchecked"].Paused() })
	time.Sleep(50 * time.Millisecond)
	if !ch["checked"].Paused() {
		t.Fatal("the channel with the disk check must wait while the disk is below the limit")
	}
	if !ch["checked"].ExportInfo().AutoWaiting {
		t.Fatal("the card must say that auto-record is waiting")
	}

	server.Config.AutoMinFreeGB = 1 // the limit is lowered (global setting): it starts
	m.autoStep()
	waitFor(t, "the checked channel to start once the limit is reachable", func() bool { return !ch["checked"].Paused() })
	if ch["checked"].ExportInfo().AutoWaiting {
		t.Fatal("it is not waiting any more")
	}
}

func TestAutoRecordPausesAgainOnceTheBroadcastIsOver(t *testing.T) {
	m, _, ch := autoRig(t, &entity.ChannelConfig{Username: "armed", IsPaused: true, AutoRecord: true})
	a := ch["armed"]
	a.ApplyPresence(live())
	m.autoStep()
	waitFor(t, "start", func() bool { return !a.Paused() })

	a.ApplyPresence(nil) // the list no longer has it
	m.autoStep()         // starts counting
	if a.Paused() {
		t.Fatal("it must not be paused the moment it goes off the air")
	}
	time.Sleep(60 * time.Millisecond)
	m.autoStep()
	waitFor(t, "to be paused again", func() bool { return a.Paused() })
	if a.AutoStarted() {
		t.Fatal("it is armed again, not 'started'")
	}

	// and it starts by itself with the next broadcast
	a.ApplyPresence(live())
	m.autoStep()
	waitFor(t, "second start", func() bool { return !a.Paused() })
}

func TestPausingABroadcastByHandIsNotUndoneByAutoRecord(t *testing.T) {
	m, _, ch := autoRig(t, &entity.ChannelConfig{Username: "armed", IsPaused: false, AutoRecord: true})
	a := ch["armed"]
	a.ApplyPresence(live())
	if err := m.PauseChannel(entity.ChannelID("chaturbate", "armed")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		m.autoStep()
		time.Sleep(10 * time.Millisecond)
	}
	if !a.Paused() {
		t.Fatal("auto-record restarted a channel that the user had just paused")
	}

	a.ApplyPresence(nil) // the broadcast is over
	m.autoStep()
	a.ApplyPresence(live()) // a new one
	m.autoStep()
	waitFor(t, "start with the next broadcast", func() bool { return !a.Paused() })
}

func TestAutoRecordIsOffByDefaultForNewChannels(t *testing.T) {
	m, _, _ := autoRig(t)
	conf := &entity.ChannelConfig{Username: "fresh", Site: "chaturbate", IsPaused: true}
	if err := m.CreateChannel(conf, false); err != nil {
		t.Fatal(err)
	}
	v, _ := m.Channels.Load(entity.ChannelID("chaturbate", "fresh"))
	fresh := v.(*channel.Channel)
	if fresh.Settings().AutoRecord {
		t.Fatal("auto-record must be off for a new channel")
	}
	// CreateChannel starts the monitor: let its first look at the site finish before the test
	// restores the globals that it reads
	waitFor(t, "the first look at the site", func() bool {
		return strings.Contains(strings.Join(fresh.ExportInfo().Logs, "\n"), "try again in")
	})
}

func intp(v int) *int       { return &v }
func strp(v string) *string { return &v }
func boolp(v bool) *bool    { return &v }

func TestUpdateChannelValidatesAndApplies(t *testing.T) {
	m, _, ch := autoRig(t,
		&entity.ChannelConfig{Username: "alice", IsPaused: true, Pattern: "videos/{{.Username}}", Resolution: 1080, Framerate: 60},
		&entity.ChannelConfig{Username: "bob", IsPaused: true, Pattern: "videos/{{.Username}}", Resolution: 1080, Framerate: 60},
	)
	id := entity.ChannelID("chaturbate", "alice")

	for name, p := range map[string]entity.ChannelPatch{
		"framerate":  {Framerate: intp(45)},
		"resolution": {Resolution: intp(5)},
		"negative":   {MaxFilesize: intp(-1)},
		"template":   {Pattern: strp("videos/{{.Nope")},
		"empty":      {Pattern: strp("")},
		"conflict":   {Pattern: strp("videos/bob")}, // bob writes there
	} {
		if err := m.UpdateChannel(id, p); err == nil {
			t.Errorf("%s: a wrong value was accepted", name)
		}
	}
	if got := ch["alice"].Settings(); got.Pattern != "videos/{{.Username}}" || got.Framerate != 60 || got.MaxFilesize != 0 {
		t.Fatalf("a refused change must change nothing: %+v", got)
	}

	err := m.UpdateChannel(id, entity.ChannelPatch{
		Resolution: intp(720), Framerate: intp(30), Pattern: strp("other/{{.Username}}_{{.Year}}"),
		MaxDuration: intp(30), MaxFilesize: intp(500), AutoRecord: boolp(true), AutoNoDiskCheck: boolp(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := entity.ChannelSettings{Resolution: 720, Framerate: 30, Pattern: "other/{{.Username}}_{{.Year}}", MaxDuration: 30, MaxFilesize: 500, AutoRecord: true, AutoNoDiskCheck: true,
		Detect: entity.DetectConfig{}.WithDefaults()} // detection is off, with its defaults ready for the dialog
	if got := ch["alice"].Settings(); !reflect.DeepEqual(got, want) {
		t.Fatalf("settings = %+v, want %+v", got, want)
	}
	// only what is sent changes
	if err := m.UpdateChannel(id, entity.ChannelPatch{AutoRecord: boolp(false)}); err != nil {
		t.Fatal(err)
	}
	if got := ch["alice"].Settings(); got.AutoRecord || got.Resolution != 720 || got.Pattern != want.Pattern {
		t.Fatalf("partial change: %+v", got)
	}
	if err := m.UpdateChannel("chaturbate__nobody", entity.ChannelPatch{}); err == nil {
		t.Fatal("an unknown channel must be an error")
	}
	if info := ch["alice"].ExportInfo(); info.Settings.MaxFilesize != 500 || info.MaxDuration == "" {
		t.Fatalf("the card gets the settings: %+v", info.Settings)
	}

	// the saved form keeps the two choices; the defaults stay out of the file
	b, _ := json.Marshal(ch["bob"].ConfigSnapshot())
	if strings.Contains(string(b), "auto_record") || strings.Contains(string(b), "auto_no_disk_check") {
		t.Fatalf("a channel that never used auto-record must not carry it: %s", b)
	}
	_ = m.UpdateChannel(id, entity.ChannelPatch{AutoRecord: boolp(true)})
	b, _ = json.Marshal(ch["alice"].ConfigSnapshot())
	if !strings.Contains(string(b), `"auto_record":true`) || !strings.Contains(string(b), `"auto_no_disk_check":true`) {
		t.Fatalf("saved form: %s", b)
	}
}

// The settings can change while the channel records: this is for `go test -race`.
func TestSettingsCanChangeWhileTheChannelRuns(t *testing.T) {
	_, _, ch := autoRig(t, &entity.ChannelConfig{Username: "alice", IsPaused: true, Pattern: "videos/{{.Username}}", Resolution: 1080, Framerate: 60})
	a := ch["alice"]
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				a.ExportInfo()
				a.ConfigSnapshot()
				a.ShouldSwitchFile()
				_, _ = a.GenerateFilename()
				a.AutoWant()
			}
		}()
	}
	for i := 0; i < 200; i++ {
		a.Apply(entity.ChannelPatch{MaxDuration: intp(i), Pattern: strp("videos/x" + string(rune('a'+i%20)) + "/{{.Username}}"), AutoRecord: boolp(i%2 == 0)})
	}
	close(stop)
	wg.Wait()
	a.WaitBackground()
}

func TestRecordingDiskLookup(t *testing.T) {
	for in, want := range map[string]string{
		"videos/{{.Username}}_x": "videos",
		"{{.Username}}":          ".",
		"a/b/c/{{.Year}}":        "a/b/c",
		"plain":                  ".",
	} {
		if got := filepath.ToSlash(recordingDir(in)); got != want {
			t.Errorf("recordingDir(%q) = %q, want %q", in, got, want)
		}
	}
	dir := t.TempDir()
	if got := existingDir(filepath.Join(dir, "a", "b")); got != filepath.Clean(dir) {
		t.Errorf("existingDir climbs to the closest existing folder: %q", got)
	}
	if free, ok := freeSpaceFor(filepath.ToSlash(filepath.Join(dir, "new", "{{.Username}}"))); !ok || free == 0 {
		t.Errorf("the free space of a folder that does not exist yet must be known: %d %v", free, ok)
	}
}

func TestCardsCarryTheCopyAutoAndSettingsButtons(t *testing.T) {
	_, _, ch := autoRig(t, &entity.ChannelConfig{Username: "alice", IsPaused: true, Pattern: `videos/"q"/{{.Username}}`, Resolution: 720, Framerate: 30, MaxFilesize: 500, MaxDuration: 20})
	a := ch["alice"]
	render := func() string {
		msg, err := renderEvent(entity.EventUpdate, a.ExportInfo())
		if err != nil {
			t.Fatal(err)
		}
		return string(msg.data)
	}

	off := render()
	if n := strings.Count(off, `data-act="copy"`); n != 3 { // expanded, grid, collapsed
		t.Fatalf("copy buttons = %d, want 3 (one per view)", n)
	}
	for _, want := range []string{
		`data-link="` + server.Config.Domain + `alice"`,
		`hx-post="/update_channel/chaturbate__alice"`,
		`hx-vals='{"auto_record":"1"}'`, // off now: the button turns it on
		`data-channel-id="chaturbate__alice"`, `data-res="720"`, `data-fps="30"`, `data-max-size="500"`, `data-max-dur="20"`,
		`data-auto="0"`, `data-disk-check="1"`, // the disk check is on unless the channel turned it off
		`Auto: OFF`,
		`data-pattern="videos/&#34;q&#34;/{{.Username}}"`, // quotes cannot break out of the attribute
	} {
		if !strings.Contains(off, want) {
			t.Fatalf("card lacks %s\n%s", want, off)
		}
	}

	a.Apply(entity.ChannelPatch{AutoRecord: boolp(true), AutoNoDiskCheck: boolp(true)})
	on := render()
	for _, want := range []string{`hx-vals='{"auto_record":"0"}'`, `Auto: ON`, `ct-btn is-on`, `data-auto="1"`, `data-disk-check="0"`} {
		if !strings.Contains(on, want) {
			t.Fatalf("armed card lacks %s", want)
		}
	}
}
