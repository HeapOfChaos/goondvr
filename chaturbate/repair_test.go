package chaturbate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func be32(v uint32) []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, v); return b }
func be64(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }

// fixtureInit is a minimal init segment: ftyp + moov.
func fixtureInit() []byte {
	ftyp := makeMP4Box("ftyp", []byte("isom\x00\x00\x02\x00isomiso2"))
	moov := makeMP4Box("moov", makeMP4Box("mvhd", make([]byte, 100)))
	return append(ftyp, moov...)
}

// fixtureFragment is moof(mfhd, traf(tfhd, tfdt v1)) + mdat, with the decode time t.
func fixtureFragment(track uint32, t uint64) []byte {
	tfhd := makeMP4Box("tfhd", append(be32(0), be32(track)...))
	tfdt := makeMP4Box("tfdt", append([]byte{1, 0, 0, 0}, be64(t)...))
	traf := makeMP4Box("traf", append(tfhd, tfdt...))
	moof := makeMP4Box("moof", append(makeMP4Box("mfhd", make([]byte, 8)), traf...))
	mdat := makeMP4Box("mdat", bytes.Repeat([]byte{0xAB}, 40))
	return append(moof, mdat...)
}

// fixtureFile is an init segment followed by one fragment per decode time.
func fixtureFile(times ...uint64) []byte {
	b := fixtureInit()
	for _, t := range times {
		b = append(b, fixtureFragment(1, t)...)
	}
	return b
}

func writeFixture(t *testing.T, b []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "rec.mp4")
	if err := os.WriteFile(p, b, 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func readAll(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// decodeTimes returns the tfdt of every moof, in file order.
func decodeTimes(t *testing.T, b []byte) []uint64 {
	t.Helper()
	var out []uint64
	for pos := 0; pos+8 <= len(b); {
		size := int(binary.BigEndian.Uint32(b[pos:]))
		if size < 8 || pos+size > len(b) {
			break
		}
		if string(b[pos+4:pos+8]) == "moof" {
			_, p := scanMoofForIndex(b[pos+8:pos+size], int64(pos))
			for _, patch := range p {
				out = append(out, binary.BigEndian.Uint64(b[patch.fileOffset:]))
			}
		}
		pos += size
	}
	return out
}

func equalTimes(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRepairMP4TailCutsBackToTheLastCompleteFragment(t *testing.T) {
	good := fixtureFile(1000, 2000, 3000)
	next := fixtureFragment(1, 4000)
	moofLen := int(binary.BigEndian.Uint32(next))

	// Cut at the start, inside the moof header, inside the moof, between moof and
	// mdat (a moof with no mdat), inside the mdat, and one byte short of complete.
	for _, n := range []int{1, 5, 9, moofLen - 1, moofLen, moofLen + 4, len(next) - 1} {
		p := writeFixture(t, append(append([]byte{}, good...), next[:n]...))
		if err := RepairMP4Tail(p); err != nil {
			t.Fatalf("cut %d: %v", n, err)
		}
		if got := readAll(t, p); !bytes.Equal(got, good) {
			t.Fatalf("cut %d: %d bytes left, want %d", n, len(got), len(good))
		}
	}
}

func TestRepairMP4TailLeavesGoodAndForeignFilesAlone(t *testing.T) {
	cases := map[string][]byte{
		"complete":     fixtureFile(1000, 2000),
		"empty":        nil,
		"not mp4":      bytes.Repeat([]byte{0xFF}, 500),
		"text":         []byte("this is not a video file at all, just text"),
		"only a start": fixtureInit()[:5],
	}
	for name, content := range cases {
		p := writeFixture(t, content)
		if err := RepairMP4Tail(p); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := readAll(t, p); !bytes.Equal(got, content) {
			t.Fatalf("%s: the file was changed", name)
		}
	}
	// A box that is not a plausible one in the middle of the file: not understood, not touched.
	bad := append(fixtureFile(1000), 0, 0, 0, 3, 'x', 'x', 'x', 'x')
	bad = append(bad, fixtureFragment(1, 2000)...)
	p := writeFixture(t, bad)
	if err := RepairMP4Tail(p); err != nil || !bytes.Equal(readAll(t, p), bad) {
		t.Fatalf("a file with a corrupt box in the middle was modified (err=%v)", err)
	}
}

func TestRepairTSTail(t *testing.T) {
	ts := append([]byte{0x47}, bytes.Repeat([]byte{1}, 187)...)
	good := bytes.Repeat(ts, 3)
	p := writeFixture(t, append(append([]byte{}, good...), ts[:50]...))
	if err := RepairTSTail(p); err != nil || !bytes.Equal(readAll(t, p), good) {
		t.Fatalf("a partial packet was not cut (err=%v)", err)
	}
	foreign := append(bytes.Repeat([]byte{9}, 188), 1, 2, 3)
	p = writeFixture(t, foreign)
	if err := RepairTSTail(p); err != nil || !bytes.Equal(readAll(t, p), foreign) {
		t.Fatalf("a file without the sync byte was modified (err=%v)", err)
	}
}

func TestBuildSeekIndexNormalizesOnceAndIsIdempotent(t *testing.T) {
	p := writeFixture(t, fixtureFile(5_000_000, 5_090_000, 5_180_000))
	if err := BuildSeekIndex(p); err != nil {
		t.Fatal(err)
	}
	b := readAll(t, p)
	if got := decodeTimes(t, b); !equalTimes(got, []uint64{0, 90_000, 180_000}) {
		t.Fatalf("times = %v", got)
	}
	if !bytes.Contains(b[len(b)-200:], []byte("mfro")) {
		t.Fatal("no index at the end of the file")
	}
	if _, err := os.Stat(seekJournalPath(p)); !os.IsNotExist(err) {
		t.Fatal("the journal was left behind")
	}
	if err := BuildSeekIndex(p); err != nil || !bytes.Equal(readAll(t, p), b) {
		t.Fatalf("a second run changed the file (err=%v)", err)
	}
}

// A kill while the times are being rewritten must not leave a file whose
// timeline jumps: the next run finishes the job and gives exactly what an
// uninterrupted run gives.
func TestBuildSeekIndexFinishesAfterAKill(t *testing.T) {
	times := []uint64{5_000_000, 5_090_000, 5_180_000, 5_270_000}
	ref := writeFixture(t, fixtureFile(times...))
	if err := BuildSeekIndex(ref); err != nil {
		t.Fatal(err)
	}
	want := readAll(t, ref)

	for kill := 0; kill < len(times); kill++ {
		p := writeFixture(t, fixtureFile(times...))
		applyHook = func(done int) bool { return done < kill }
		err := BuildSeekIndex(p)
		applyHook = nil
		if !errors.Is(err, errSimulatedCrash) {
			t.Fatalf("kill after %d edits: err = %v", kill, err)
		}
		if _, err := os.Stat(seekJournalPath(p)); err != nil {
			t.Fatalf("kill after %d edits: no journal: %v", kill, err)
		}
		if kill > 0 { // the file is half rewritten: this is what used to be unrecoverable
			if got := decodeTimes(t, readAll(t, p)); equalTimes(got, []uint64{0, 90_000, 180_000, 270_000}) {
				t.Fatalf("kill after %d edits: the crash did not interrupt anything", kill)
			}
		}
		if err := BuildSeekIndex(p); err != nil {
			t.Fatalf("kill after %d edits: resume: %v", kill, err)
		}
		if got := readAll(t, p); !bytes.Equal(got, want) {
			t.Fatalf("kill after %d edits: resumed file differs from the uninterrupted one (times %v)", kill, decodeTimes(t, got))
		}
		if _, err := os.Stat(seekJournalPath(p)); !os.IsNotExist(err) {
			t.Fatalf("kill after %d edits: the journal was left behind", kill)
		}
	}
}

func TestBuildSeekIndexRepairsATornTailFirst(t *testing.T) {
	good := fixtureFile(5_000_000, 5_090_000)
	ref := writeFixture(t, good)
	if err := BuildSeekIndex(ref); err != nil {
		t.Fatal(err)
	}
	torn := writeFixture(t, append(append([]byte{}, good...), fixtureFragment(1, 5_180_000)[:30]...))
	if err := BuildSeekIndex(torn); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAll(t, torn), readAll(t, ref)) {
		t.Fatal("the index was appended after the torn fragment")
	}
}

func TestSeekJournalOfAnotherFileIsNotApplied(t *testing.T) {
	content := fixtureFile(5_000_000, 5_090_000)
	p := writeFixture(t, content)
	// An edit whose "old" value is not what the file holds: not this file's journal.
	if err := writeSeekJournal(p, []tfdtEdit{{Off: 40, Ver: 1, Old: 123, New: 0}}); err != nil {
		t.Fatal(err)
	}
	if err := applySeekJournal(p); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAll(t, p), content) {
		t.Fatal("a journal that does not describe the file changed it")
	}
	if _, err := os.Stat(seekJournalPath(p)); !os.IsNotExist(err) {
		t.Fatal("the useless journal was kept")
	}
}
