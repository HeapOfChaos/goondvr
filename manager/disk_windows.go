//go:build windows

package manager

import (
	"fmt"
	"path/filepath"
	"syscall"
	"unsafe"
)

var procGetDiskFreeSpaceExW = syscall.NewLazyDLL("kernel32.dll").NewProc("GetDiskFreeSpaceExW")

// getDiskStats reports the usage of the disk that holds path
// (GetDiskFreeSpaceExW: the free space is the one available to this user, so
// quotas are respected).
func getDiskStats(path string) (DiskStats, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return DiskStats{}, fmt.Errorf("abs %s: %w", path, err)
	}
	p, err := syscall.UTF16PtrFromString(abs)
	if err != nil {
		return DiskStats{}, fmt.Errorf("path %s: %w", path, err)
	}
	var avail, total, totalFree uint64
	r, _, callErr := procGetDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(p)),
		uintptr(unsafe.Pointer(&avail)),
		uintptr(unsafe.Pointer(&total)),
		uintptr(unsafe.Pointer(&totalFree)),
	)
	if r == 0 {
		return DiskStats{}, fmt.Errorf("GetDiskFreeSpaceEx %s: %w", abs, callErr)
	}
	var used uint64
	if total > totalFree {
		used = total - totalFree
	}
	var pct float64
	if total > 0 {
		pct = float64(used) / float64(total) * 100
	}
	return DiskStats{Path: path, Total: total, Used: used, Free: avail, Percent: pct}, nil
}
