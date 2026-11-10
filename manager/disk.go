package manager

import (
	"os"
	"path/filepath"
	"strings"
)

// DiskStats holds usage information for a filesystem.
type DiskStats struct {
	Path    string
	Total   uint64
	Used    uint64
	Free    uint64
	Percent float64
}

// recordingDir extracts the base directory from a filename pattern like
// "videos/{{.Username}}_{{.Year}}-..." → "videos".
func recordingDir(pattern string) string {
	idx := strings.Index(pattern, "{{")
	if idx == -1 {
		return "."
	}
	dir := filepath.Dir(pattern[:idx])
	if dir == "" || dir == "." {
		return "."
	}
	return dir
}

// existingDir returns dir, or its closest parent that exists: the recording
// folder is only created when the first file is written, but the disk it will
// be on is the disk of that parent.
func existingDir(dir string) string {
	dir = filepath.Clean(dir)
	for {
		if st, err := os.Stat(dir); err == nil && st.IsDir() {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return dir
		}
		dir = parent
	}
}

// freeSpaceFor reports the free space (bytes) of the disk a filename pattern
// records to. ok is false when it cannot be told (the caller decides what to do).
func freeSpaceFor(pattern string) (free uint64, ok bool) {
	st, err := getDiskStats(existingDir(recordingDir(pattern)))
	if err != nil {
		return 0, false
	}
	return st.Free, true
}
