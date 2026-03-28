//go:build !windows

package manager

import (
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
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

func getDiskStats(path string) (DiskStats, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return DiskStats{}, fmt.Errorf("statfs %s: %w", path, err)
	}
	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)
	used := total - free
	var pct float64
	if total > 0 {
		pct = float64(used) / float64(total) * 100
	}
	return DiskStats{Path: path, Total: total, Used: used, Free: free, Percent: pct}, nil
}
