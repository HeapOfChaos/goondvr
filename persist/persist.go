// Package persist writes the small files the program lives on (settings.json,
// channels.json, the list of open recordings) so that a kill, a closed console
// window or a power cut never leaves them empty or half written, keeps rolling
// backups of them, and restores the newest good backup when one is damaged.
package persist

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	// BackupEvery is the shortest time between two automatic backups of a file.
	// (Variables only so that tests can change them.)
	BackupEvery = 10 * time.Minute
	// BackupKeep is how many backups of each file are kept.
	BackupKeep = 30
)

const backupDir = "backup"

var (
	mu         sync.Mutex // writes are small and rare: one lock for all of them
	lastSum    = map[string][32]byte{}
	lastBackup = map[string]time.Time{}
)

// WriteFile replaces path atomically (see Replace) and skips the write when the
// bytes are the same as the last ones written to that path.
func WriteFile(path string, data []byte, perm os.FileMode) error {
	mu.Lock()
	defer mu.Unlock()
	return writeLocked(path, data, perm)
}

func writeLocked(path string, data []byte, perm os.FileMode) error {
	sum := sha256.Sum256(data)
	if lastSum[path] == sum {
		if _, err := os.Stat(path); err == nil {
			return nil // nothing changed since the last write
		}
	}
	if err := Replace(path, data, perm); err != nil {
		return err
	}
	lastSum[path] = sum
	return nil
}

// Replace writes path so that a reader (or the next start after a crash) sees
// either the old file or the new one, never a mix: the data goes to a temporary
// file in the same folder, is synced to the disk and is then renamed over path.
// The caller must not call it for the same path from two goroutines at once.
func Replace(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// On Windows the rename can fail for a moment if an antivirus or an indexer
	// has the target open: try a few times before giving up.
	for i := 0; i < 6; i++ {
		if err = os.Rename(tmp, path); err == nil {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 25 * time.Millisecond)
	}
	_ = os.Remove(tmp)
	return err
}

// Backup copies path into the backup folder next to it, but only if it holds
// valid JSON (a damaged file is never backed up), differs from the newest
// backup, and (unless force) the last backup is older than BackupEvery. Errors
// are not fatal for the caller: the backup is a safety net, not the data.
func Backup(path string, force bool) {
	mu.Lock()
	defer mu.Unlock()
	if !force && time.Since(lastBackup[path]) < BackupEvery {
		return
	}
	b, err := os.ReadFile(path)
	if err != nil || !json.Valid(b) {
		return
	}
	dir := filepath.Join(filepath.Dir(path), backupDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return
	}
	lastBackup[path] = time.Now()
	names := backupNames(path)
	if len(names) > 0 {
		if prev, err := os.ReadFile(filepath.Join(dir, names[0])); err == nil && bytes.Equal(prev, b) {
			return
		}
	}
	base, ext := splitName(path)
	name := fmt.Sprintf("%s-%s%s", base, time.Now().Format("20060102-150405.000"), ext)
	if err := Replace(filepath.Join(dir, name), b, 0600); err != nil {
		return
	}
	if all := backupNames(path); len(all) > BackupKeep {
		for _, old := range all[BackupKeep:] {
			_ = os.Remove(filepath.Join(dir, old))
		}
	}
}

func splitName(path string) (base, ext string) {
	name := filepath.Base(path)
	ext = filepath.Ext(name)
	return strings.TrimSuffix(name, ext), ext
}

// backupNames lists the backups of path, newest first.
func backupNames(path string) []string {
	base, ext := splitName(path)
	entries, err := os.ReadDir(filepath.Join(filepath.Dir(path), backupDir))
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		n := e.Name()
		if !e.IsDir() && strings.HasPrefix(n, base+"-") && strings.HasSuffix(n, ext) {
			names = append(names, n)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names))) // the timestamp in the name sorts by age
	return names
}

// ReadJSON reads path into v (a pointer). A missing file gives an error for
// which errors.Is(err, os.ErrNotExist) is true. A file that is empty or is not
// valid JSON is moved aside (path.corrupt-<time>: it is never deleted) and the
// newest valid backup is restored in its place; note then says what happened.
// If there is no valid backup either, v is left empty and note says where the
// damaged file went, so the program can start (empty) instead of refusing to.
func ReadJSON(path string, v any) (note string, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if json.Unmarshal(b, v) == nil {
		return "", nil
	}
	reflect.ValueOf(v).Elem().SetZero()

	mu.Lock()
	defer mu.Unlock()
	corrupt := fmt.Sprintf("%s.corrupt-%s", path, time.Now().Format("20060102-150405"))
	if err := os.Rename(path, corrupt); err != nil {
		return "", fmt.Errorf("%s is damaged and cannot be moved aside: %w", path, err)
	}
	dir := filepath.Join(filepath.Dir(path), backupDir)
	for _, name := range backupNames(path) {
		good, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || json.Unmarshal(good, v) != nil {
			reflect.ValueOf(v).Elem().SetZero()
			continue
		}
		if err := writeLocked(path, good, 0600); err != nil {
			return "", fmt.Errorf("restore %s from %s: %w", path, name, err)
		}
		return fmt.Sprintf("%s was damaged (kept as %s); restored from the backup %s", path, corrupt, name), nil
	}
	return fmt.Sprintf("%s was damaged and there is no valid backup (kept as %s); starting with it empty", path, corrupt), nil
}

// IsNotExist reports whether err says that the file is missing.
func IsNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }
