package persist

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func read(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestWriteFileIsAtomicAndLeavesNoTemp(t *testing.T) {
	p := filepath.Join(t.TempDir(), "channels.json")
	if err := WriteFile(p, []byte(`[1]`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(p, []byte(`[1,2]`), 0600); err != nil {
		t.Fatal(err)
	}
	if got := read(t, p); got != `[1,2]` {
		t.Fatalf("got %q", got)
	}
	if _, err := os.Stat(p + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("the temporary file was left behind: %v", err)
	}
}

// A kill in the middle of a write leaves a half-written .tmp next to an intact
// original: the original must still be what is read, and the next write must
// replace the leftover.
func TestInterruptedWriteLeavesTheOriginalIntact(t *testing.T) {
	p := filepath.Join(t.TempDir(), "settings.json")
	if err := WriteFile(p, []byte(`{"a":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p+".tmp", []byte(`{"a":`), 0600); err != nil { // what a killed write leaves
		t.Fatal(err)
	}
	var v map[string]int
	if note, err := ReadJSON(p, &v); err != nil || note != "" || v["a"] != 1 {
		t.Fatalf("note=%q err=%v v=%v", note, err, v)
	}
	if err := WriteFile(p, []byte(`{"a":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	if got := read(t, p); got != `{"a":2}` {
		t.Fatalf("got %q", got)
	}
}

func TestWriteSameBytesIsSkippedButRecreatesAMissingFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.json")
	_ = WriteFile(p, []byte(`{}`), 0600)
	st1, _ := os.Stat(p)
	time.Sleep(20 * time.Millisecond)
	_ = WriteFile(p, []byte(`{}`), 0600)
	st2, _ := os.Stat(p)
	if !st2.ModTime().Equal(st1.ModTime()) {
		t.Fatal("identical data was written again")
	}
	_ = os.Remove(p)
	if err := WriteFile(p, []byte(`{}`), 0600); err != nil || read(t, p) != `{}` {
		t.Fatalf("a deleted file was not recreated: %v", err)
	}
}

func TestBackupRulesAndRotation(t *testing.T) {
	oldEvery, oldKeep := BackupEvery, BackupKeep
	defer func() { BackupEvery, BackupKeep = oldEvery, oldKeep }()
	BackupEvery, BackupKeep = time.Hour, 3

	dir := t.TempDir()
	p := filepath.Join(dir, "channels.json")

	// A damaged file is never backed up.
	_ = os.WriteFile(p, []byte(`[{"x":`), 0600)
	Backup(p, true)
	if n := len(backupNames(p)); n != 0 {
		t.Fatalf("a damaged file was backed up (%d)", n)
	}

	_ = os.WriteFile(p, []byte(`[1]`), 0600)
	Backup(p, true)
	Backup(p, true) // same content as the newest backup: no duplicate
	if n := len(backupNames(p)); n != 1 {
		t.Fatalf("backups = %d, want 1", n)
	}

	_ = os.WriteFile(p, []byte(`[2]`), 0600)
	Backup(p, false) // inside BackupEvery: skipped
	if n := len(backupNames(p)); n != 1 {
		t.Fatalf("the rate limit did not apply: %d", n)
	}

	for i := 3; i <= 8; i++ {
		_ = os.WriteFile(p, []byte("["+string(rune('0'+i))+"]"), 0600)
		time.Sleep(3 * time.Millisecond) // the name carries milliseconds
		Backup(p, true)
	}
	names := backupNames(p)
	if len(names) != 3 {
		t.Fatalf("kept %d backups, want 3: %v", len(names), names)
	}
	if got := read(t, filepath.Join(dir, backupDir, names[0])); got != "[8]" {
		t.Fatalf("newest backup holds %q", got)
	}
}

func TestReadJSONRestoresTheNewestGoodBackup(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "channels.json")
	_ = os.WriteFile(p, []byte(`["old"]`), 0600)
	Backup(p, true)
	time.Sleep(3 * time.Millisecond)
	_ = os.WriteFile(p, []byte(`["new"]`), 0600)
	Backup(p, true)

	_ = os.WriteFile(p, []byte(`["new`), 0600) // a write cut short
	var v []string
	note, err := ReadJSON(p, &v)
	if err != nil || note == "" || len(v) != 1 || v[0] != "new" {
		t.Fatalf("note=%q err=%v v=%v", note, err, v)
	}
	if got := read(t, p); got != `["new"]` {
		t.Fatalf("the file was not restored: %q", got)
	}
	matches, _ := filepath.Glob(p + ".corrupt-*")
	if len(matches) != 1 || read(t, matches[0]) != `["new` {
		t.Fatalf("the damaged file was not kept: %v", matches)
	}

	// The newest backup is damaged too: the next one is used.
	names := backupNames(p)
	_ = os.WriteFile(filepath.Join(dir, backupDir, names[0]), []byte(`[`), 0600)
	_ = os.WriteFile(p, nil, 0600) // empty file
	v = nil
	if note, err = ReadJSON(p, &v); err != nil || note == "" || len(v) != 1 || v[0] != "old" {
		t.Fatalf("note=%q err=%v v=%v", note, err, v)
	}
}

func TestReadJSONWithoutBackupStartsEmptyAndKeepsTheDamagedFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "settings.json")
	_ = os.WriteFile(p, []byte(`{"cookies": "x`), 0600)
	v := map[string]string{"stale": "value"}
	note, err := ReadJSON(p, &v)
	if err != nil || note == "" || len(v) != 0 {
		t.Fatalf("note=%q err=%v v=%v", note, err, v)
	}
	if matches, _ := filepath.Glob(p + ".corrupt-*"); len(matches) != 1 {
		t.Fatalf("the damaged file was not kept: %v", matches)
	}
}

func TestReadJSONMissingFile(t *testing.T) {
	var v []string
	if _, err := ReadJSON(filepath.Join(t.TempDir(), "nope.json"), &v); !IsNotExist(err) {
		t.Fatalf("err = %v", err)
	}
}
