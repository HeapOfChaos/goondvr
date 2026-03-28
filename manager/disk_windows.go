//go:build windows

package manager

import "fmt"

// DiskStats holds usage information for a filesystem.
type DiskStats struct {
	Path    string
	Total   uint64
	Used    uint64
	Free    uint64
	Percent float64
}

func recordingDir(pattern string) string {
	return "."
}

func getDiskStats(path string) (DiskStats, error) {
	return DiskStats{}, fmt.Errorf("disk stats not supported on Windows")
}
