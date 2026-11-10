package channel

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/HeapOfChaos/goondvr/server"
)

func TestMain(m *testing.M) {
	journalPath = "" // the tests must not write a conf/ folder into the source tree
	os.Exit(m.Run())
}

// useJournal points the journal at a temporary file and restores the globals.
func useJournal(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "open-recordings.json")
	journalPath = p
	resetJournal()
	t.Cleanup(func() {
		journalPath = ""
		resetJournal()
		shutdown.Store(false)
		shutdownCtx, shutdownCancelFinal = context.WithCancel(context.Background())
	})
	return p
}

// resetJournal forgets the in-memory state, as a new start of the program would.
func resetJournal() {
	journal.Lock()
	journal.loaded, journal.pending, journal.open = false, nil, nil
	journal.Unlock()
}

func pendingCount() int {
	journal.Lock()
	defer journal.Unlock()
	loadJournalLocked()
	return len(journal.pending)
}

func box(typ string, content []byte) []byte {
	b := make([]byte, 8+len(content))
	binary.BigEndian.PutUint32(b, uint32(len(b)))
	copy(b[4:], typ)
	copy(b[8:], content)
	return b
}

func be(v uint64, n int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b[8-n:]
}

// mp4Fixture is an init segment plus one moof+mdat fragment per decode time.
func mp4Fixture(times ...uint64) []byte {
	b := append(box("ftyp", []byte("isom\x00\x00\x02\x00")), box("moov", box("mvhd", make([]byte, 100)))...)
	for _, t := range times {
		tfhd := box("tfhd", append(be(0, 4), be(1, 4)...))
		tfdt := box("tfdt", append([]byte{1, 0, 0, 0}, be(t, 8)...))
		moof := box("moof", append(box("mfhd", make([]byte, 8)), box("traf", append(tfhd, tfdt...))...))
		b = append(append(b, moof...), box("mdat", bytes.Repeat([]byte{7}, 40))...)
	}
	return b
}

func hasIndex(b []byte) bool { return len(b) > 16 && string(b[len(b)-12:len(b)-8]) == "mfro" }

// firstTfdt returns the decode time of the first fragment.
func firstTfdt(b []byte) uint64 {
	i := bytes.Index(b, []byte("tfdt"))
	return binary.BigEndian.Uint64(b[i+8:])
}

// allTfdts returns the decode time of every fragment.
func allTfdts(b []byte) []uint64 {
	var out []uint64
	for off := 0; ; {
		i := bytes.Index(b[off:], []byte("tfdt"))
		if i < 0 {
			return out
		}
		out = append(out, binary.BigEndian.Uint64(b[off+i+8:]))
		off += i + 4
	}
}

func TestFinishedRecordingLeavesTheJournal(t *testing.T) {
	useJournal(t)
	dir := t.TempDir()
	ch := newTestChannel(t, &fakeUI{pushes: map[string]int{}}, time.Second)
	server.Config.FinalizeMode = "none"
	ch.Config.Pattern = filepath.Join(dir, "{{.Username}}")
	ch.FileExt = ".mp4"

	if err := ch.NextFile(".mp4"); err != nil {
		t.Fatal(err)
	}
	if n := pendingCount(); n != 1 {
		t.Fatalf("an open recording is not on the journal (%d)", n)
	}
	if _, err := ch.File.Write(mp4Fixture(5_000_000, 5_090_000)); err != nil {
		t.Fatal(err)
	}
	if err := ch.Cleanup(); err != nil {
		t.Fatal(err)
	}
	ch.waitForFinalizations()
	ch.WaitBackground()

	if n := pendingCount(); n != 0 {
		t.Fatalf("a finished recording is still on the journal (%d)", n)
	}
	done, err := os.ReadFile(filepath.Join(dir, "completed", "tester.mp4"))
	if err != nil {
		t.Fatal(err)
	}
	if !hasIndex(done) || firstTfdt(done) != 0 {
		t.Fatal("the finished recording has no seek index / was not normalized")
	}
}

// The console window is closed: no finalization may start, the file stays where
// it is and on the journal, and the next start finishes it.
func TestClosingTheProgramSkipsFinalizationAndTheNextStartFinishesIt(t *testing.T) {
	jp := useJournal(t)
	dir := t.TempDir()
	ch := newTestChannel(t, &fakeUI{pushes: map[string]int{}}, time.Second)
	server.Config.FinalizeMode = "none"
	ch.Config.Pattern = filepath.Join(dir, "{{.Username}}")
	ch.FileExt = ".mp4"

	if err := ch.NextFile(".mp4"); err != nil {
		t.Fatal(err)
	}
	torn := mp4Fixture(5_000_000, 5_090_000, 5_180_000)
	torn = torn[:len(torn)-20] // the last fragment was being written when the window was closed
	if _, err := ch.File.Write(torn); err != nil {
		t.Fatal(err)
	}
	path := ch.File.Name()

	BeginShutdown()
	if err := ch.Cleanup(); err != nil {
		t.Fatal(err)
	}
	ch.waitForFinalizations()
	ch.WaitBackground()

	if got, _ := os.ReadFile(path); !bytes.Equal(got, torn) {
		t.Fatal("the recording was touched while the program was closing")
	}
	if b, _ := os.ReadFile(jp); !bytes.Contains(b, []byte(filepath.Base(path))) {
		t.Fatalf("the recording is not on the saved journal: %q", b)
	}

	// Next start.
	shutdown.Store(false)
	resetJournal()
	RecoverRecordings()
	got, _ := os.ReadFile(path)
	if !hasIndex(got) || firstTfdt(got) != 0 {
		t.Fatal("the recording was not repaired and indexed")
	}
	if times := allTfdts(got); len(times) != 2 || times[0] != 0 || times[1] != 90_000 {
		t.Fatalf("after the repair the fragments are %v, want the two complete ones [0 90000]", times)
	}
	if n := pendingCount(); n != 0 {
		t.Fatalf("the journal still lists %d recording(s)", n)
	}
}

func TestRecoverRecordingsHandlesMissingEmptyAndTSFiles(t *testing.T) {
	useJournal(t)
	dir := t.TempDir()
	missing := filepath.Join(dir, "gone.mp4")
	empty := filepath.Join(dir, "empty.mp4")
	ts := filepath.Join(dir, "live.ts")
	tiny := filepath.Join(dir, "tiny.ts")
	pkt := append([]byte{0x47}, bytes.Repeat([]byte{1}, 187)...)
	good := bytes.Repeat(pkt, 4)
	_ = os.WriteFile(empty, nil, 0644)
	_ = os.WriteFile(tiny, pkt[:50], 0644)
	_ = os.WriteFile(ts, append(append([]byte{}, good...), pkt[:60]...), 0644)
	for _, p := range []string{missing, empty, tiny, ts} {
		journalOpen(p)
		journalClosed(p)
	}
	resetJournal() // a new start reads the list from the file

	RecoverRecordings()

	for _, p := range []string{empty, tiny} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("%s was kept: it holds nothing playable", filepath.Base(p))
		}
	}
	if got, _ := os.ReadFile(ts); !bytes.Equal(got, good) {
		t.Fatalf("the TS file was not cut to whole packets (%d bytes)", len(got))
	}
	if n := pendingCount(); n != 0 {
		t.Fatalf("the journal still lists %d recording(s)", n)
	}
}

func TestRecoveryLeavesRecordingsThatAreOpenNow(t *testing.T) {
	useJournal(t)
	p := filepath.Join(t.TempDir(), "now.mp4")
	torn := mp4Fixture(1000, 2000)
	torn = torn[:len(torn)-5]
	_ = os.WriteFile(p, torn, 0644)
	journalOpen(p) // a channel is writing it right now

	RecoverRecordings()

	if got, _ := os.ReadFile(p); !bytes.Equal(got, torn) {
		t.Fatal("a recording that is being written was repaired under the writer")
	}
	if pendingCount() != 1 {
		t.Fatal("an open recording left the journal")
	}
}
