package channel

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/HeapOfChaos/goondvr/chaturbate"
	"github.com/HeapOfChaos/goondvr/detect"
	"github.com/HeapOfChaos/goondvr/server"
)

// Pattern holds the date/time and sequence information for the filename pattern
type Pattern struct {
	Username string
	Site     string
	Year     string
	Month    string
	Day      string
	Hour     string
	Minute   string
	Second   string
	Sequence int
}

// NextFile prepares the next file to be created, by cleaning up the last file and generating a new one.
// ext is the file extension to use (e.g. ".ts" or ".mp4").
func (ch *Channel) NextFile(ext string) error {
	ch.fileMu.Lock()
	defer ch.fileMu.Unlock()

	if err := ch.cleanupLocked(); err != nil {
		return err
	}
	filename, err := ch.generateFilenameLocked()
	if err != nil {
		return err
	}
	if err := ch.createNewFileLocked(filename, ext); err != nil {
		return err
	}

	// Increment the sequence number for the next file
	ch.Sequence++
	return nil
}

// Cleanup cleans the file and resets it, called when the stream errors out or before next file was created.
func (ch *Channel) Cleanup() error {
	ch.fileMu.Lock()
	defer ch.fileMu.Unlock()

	return ch.cleanupLocked()
}

func (ch *Channel) cleanupLocked() error {
	if ch.File == nil {
		return nil
	}
	filename := ch.File.Name()

	defer func() {
		ch.Filesize = 0
		ch.Duration = 0
	}()

	// Sync the file to ensure data is written to disk
	if err := ch.File.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("sync file: %w", err)
	}
	if err := ch.File.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("close file: %w", err)
	}
	ch.File = nil
	journalClosed(filename)
	ch.detectFileClosed(filename)

	// Delete the empty file
	fileInfo, err := os.Stat(filename)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("stat file delete zero file: %w", err)
	}
	if fileInfo != nil && fileInfo.Size() == 0 {
		if err := os.Remove(filename); err != nil {
			return fmt.Errorf("remove zero file: %w", err)
		}
		_ = os.Remove(detect.SidecarPath(filename))
		journalDone(filename)
		ch.scanAsync()
	} else if fileInfo != nil {
		ch.startFinalization()
		go ch.finalizeRecording(filename)
	}

	return nil
}

// scanAsync recounts the disk usage of the channel in the background. The scan
// is tracked in scanWG so that WaitBackground (tests) can wait for it.
func (ch *Channel) scanAsync() {
	ch.scanWG.Add(1)
	go func() {
		defer ch.scanWG.Done()
		ch.ScanTotalDiskUsage()
	}()
}

// GenerateFilename creates a filename based on the configured pattern and the current timestamp
func (ch *Channel) GenerateFilename() (string, error) {
	ch.fileMu.RLock()
	defer ch.fileMu.RUnlock()

	return ch.generateFilenameLocked()
}

func (ch *Channel) generateFilenameLocked() (string, error) {
	var buf bytes.Buffer

	// Parse the filename pattern defined in the channel's config
	tpl, err := template.New("filename").Parse(ch.Settings().Pattern)
	if err != nil {
		return "", fmt.Errorf("filename pattern error: %w", err)
	}

	// Get the current time based on the Unix timestamp when the stream was started
	ch.stateMu.RLock()
	streamedAt := ch.StreamedAt
	ch.stateMu.RUnlock()
	t := time.Unix(streamedAt, 0)
	pattern := &Pattern{
		Username: ch.Config.Username,
		Site:     ch.Config.Site,
		Sequence: ch.Sequence,
		Year:     t.Format("2006"),
		Month:    t.Format("01"),
		Day:      t.Format("02"),
		Hour:     t.Format("15"),
		Minute:   t.Format("04"),
		Second:   t.Format("05"),
	}

	if err := tpl.Execute(&buf, pattern); err != nil {
		return "", fmt.Errorf("template execution error: %w", err)
	}
	return buf.String(), nil
}

// CreateNewFile creates a new file for the channel using the given filename and extension.
func (ch *Channel) CreateNewFile(filename, ext string) error {
	ch.fileMu.Lock()
	defer ch.fileMu.Unlock()

	return ch.createNewFileLocked(filename, ext)
}

func (ch *Channel) createNewFileLocked(filename, ext string) error {

	// Ensure the directory exists before creating the file
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return fmt.Errorf("mkdir all: %w", err)
	}

	// Open the file in append mode, create it if it doesn't exist
	file, err := os.OpenFile(filename+ext, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("cannot open file: %s: %w", filename, err)
	}

	ch.File = file
	journalOpen(file.Name())
	ch.detectFileOpened()
	return nil
}

// recordingDirFromPattern extracts the base directory from a filename pattern
// like "videos/{{.Username}}_..." → "videos".
func recordingDirFromPattern(pattern string) string {
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

func completedDirForChannel(ch *Channel) string {
	if server.Config.CompletedDir != "" {
		return server.Config.CompletedDir
	}
	return filepath.Join(recordingDirFromPattern(ch.Settings().Pattern), "completed")
}

func finalOutputExt(filename string) string {
	if server.Config.FFmpegContainer == "mkv" {
		return ".mkv"
	}
	if server.Config.FinalizeMode == "none" {
		return filepath.Ext(filename)
	}
	return ".mp4"
}

func finalOutputPath(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	return base + finalOutputExt(filename)
}

// ScanTotalDiskUsage calculates the total bytes of all recordings for this channel
// by walking the recording directory for files whose name starts with the username.
// The result is stored in TotalDiskUsageBytes.
func (ch *Channel) ScanTotalDiskUsage() {
	recordingDir := filepath.Clean(recordingDirFromPattern(ch.Settings().Pattern))
	dirs := []string{recordingDir}
	completedDir := completedDirForChannel(ch)
	cleanCompletedDir := filepath.Clean(completedDir)
	if cleanCompletedDir != "" &&
		cleanCompletedDir != recordingDir &&
		!strings.HasPrefix(cleanCompletedDir+string(os.PathSeparator), recordingDir+string(os.PathSeparator)) {
		dirs = append(dirs, completedDir)
	}
	prefix := ch.Config.Username
	var total int64
	for _, dir := range dirs {
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if strings.HasPrefix(filepath.Base(path), prefix) {
				if info, err2 := d.Info(); err2 == nil {
					total += info.Size()
				}
			}
			return nil
		})
	}
	ch.fileMu.Lock()
	ch.TotalDiskUsageBytes = total
	ch.fileMu.Unlock()
}

// ShouldSwitchFile determines whether a new file should be created.
func (ch *Channel) ShouldSwitchFile() bool {
	ch.fileMu.RLock()
	defer ch.fileMu.RUnlock()

	return ch.shouldSwitchFileLocked()
}

func (ch *Channel) shouldSwitchFileLocked() bool {
	set := ch.Settings()
	maxFilesizeBytes := set.MaxFilesize * 1024 * 1024
	maxDurationSeconds := set.MaxDuration * 60

	return (ch.Duration >= float64(maxDurationSeconds) && set.MaxDuration > 0) ||
		(ch.Filesize >= maxFilesizeBytes && set.MaxFilesize > 0)
}

// isMP4InitSegment reports whether b looks like an fMP4 init segment containing
// top-level ftyp/moov boxes and no media fragments yet.
func isMP4InitSegment(b []byte) bool {
	var hasFtyp bool
	var hasMoov bool

	for pos := 0; pos+8 <= len(b); {
		size := int(binary.BigEndian.Uint32(b[pos:]))
		if size < 8 || pos+size > len(b) {
			return false
		}

		switch string(b[pos+4 : pos+8]) {
		case "ftyp":
			hasFtyp = true
		case "moov":
			hasMoov = true
		case "moof", "mdat", "mfra":
			return false
		}
		pos += size
	}

	return hasFtyp && hasMoov
}

func (ch *Channel) finalizeRecording(filename string) {
	defer ch.finishFinalization()
	if shuttingDown() {
		return // the program is closing: the file stays on the journal and is finished at the next start
	}
	if err := chaturbate.RepairTail(filename); err != nil { // a write that failed or was cut short leaves a broken end
		ch.Error("repair the end of `%s`: %s", filename, err.Error())
	}

	finalPath := filename
	if server.Config.FinalizeMode == "none" {
		if strings.HasSuffix(filename, ".mp4") {
			if err := chaturbate.BuildSeekIndex(filename); err != nil {
				log.Printf("WARN  seek index %s: %v", filename, err)
			}
		}
	} else {
		processedPath, err := ch.runFFmpegFinalizer(filename)
		if err != nil {
			ch.Error("ffmpeg %s failed for `%s`: %s", server.Config.FinalizeMode, filename, err.Error())
			ch.Info("keeping original recording because finalization failed")
		} else {
			if processedPath != filename {
				if err := os.Remove(filename); err != nil {
					ch.Error("remove original after ffmpeg finalization `%s`: %s", filename, err.Error())
				}
			}
			finalPath = processedPath
		}
	}

	if shuttingDown() {
		return // ffmpeg was stopped by the closing program: the original is intact and stays where it is
	}
	completedDir := completedDirForChannel(ch)
	if completedDir != "" {
		dst, err := moveRecordingToDir(finalPath, recordingDirFromPattern(ch.Settings().Pattern), completedDir)
		if err != nil {
			ch.Error("move completed recording `%s`: %s", finalPath, err.Error())
		} else {
			ch.Info("completed recording moved to `%s`", dst)
			finalPath = dst
		}
	}
	ch.cutClips(shutdownCtx, filename, finalPath)

	journalDone(filename)
	ch.scanAsync()
}

func moveRecordingToDir(src, recordingRoot, completedDir string) (string, error) {
	dstDir := completedDir

	srcDir := filepath.Dir(src)
	cleanRoot := filepath.Clean(recordingRoot)
	cleanSrcDir := filepath.Clean(srcDir)
	if relDir, err := filepath.Rel(cleanRoot, cleanSrcDir); err == nil && relDir != ".." && !strings.HasPrefix(relDir, ".."+string(os.PathSeparator)) {
		if relDir != "." {
			dstDir = filepath.Join(completedDir, relDir)
		}
	}

	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir completed dir: %w", err)
	}

	dst := filepath.Join(dstDir, filepath.Base(src))
	if src == dst {
		return dst, nil
	}

	if err := os.Rename(src, dst); err == nil {
		return dst, nil
	} else if !isCrossDeviceRename(err) {
		return "", fmt.Errorf("rename completed file: %w", err)
	}

	if err := copyFile(src, dst); err != nil {
		return "", err
	}
	if err := os.Remove(src); err != nil {
		return "", fmt.Errorf("remove source after copy: %w", err)
	}
	return dst, nil
}

func isCrossDeviceRename(err error) bool {
	linkErr := &os.LinkError{}
	return errors.As(err, &linkErr) && errors.Is(linkErr.Err, syscall.EXDEV)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source file: %w", err)
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return fmt.Errorf("stat source file: %w", err)
	}

	// Copy under a temporary name and rename at the end: a copy that is cut short
	// must never look like a finished recording.
	part := dst + ".part"
	out, err := os.OpenFile(part, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
	if err != nil {
		return fmt.Errorf("create destination file: %w", err)
	}
	_, err = io.Copy(out, in)
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(part, dst)
	}
	if err != nil {
		_ = os.Remove(part)
		return fmt.Errorf("copy file: %w", err)
	}
	return nil
}

func (ch *Channel) runFFmpegFinalizer(filename string) (string, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return "", fmt.Errorf("ffmpeg not found in PATH")
	}

	outExt := finalOutputExt(filename)
	finalPath := finalOutputPath(filename)
	tempOutput := strings.TrimSuffix(filename, filepath.Ext(filename)) + ".finalizing" + outExt
	_ = os.Remove(tempOutput)

	args := []string{"-nostdin", "-y", "-i", filename}
	switch server.Config.FinalizeMode {
	case "remux":
		args = append(args, "-c", "copy")
		if outExt == ".mp4" {
			args = append(args, "-movflags", "+faststart")
		}
	case "transcode":
		encoder := strings.TrimSpace(server.Config.FFmpegEncoder)
		if encoder == "" {
			encoder = "libx264"
		}
		args = append(args, "-c:v", encoder)
		args = append(args, qualityArgsForEncoder(encoder, server.Config.FFmpegQuality)...)
		if preset := strings.TrimSpace(server.Config.FFmpegPreset); preset != "" {
			args = append(args, "-preset", preset)
		}
		args = append(args, "-c:a", "copy")
		if outExt == ".mp4" {
			args = append(args, "-movflags", "+faststart")
		}
	default:
		return "", fmt.Errorf("unsupported finalization mode %q", server.Config.FinalizeMode)
	}
	args = append(args, tempOutput)

	ch.Info("running ffmpeg %s for `%s`", server.Config.FinalizeMode, filepath.Base(filename))
	cmd := exec.CommandContext(shutdownCtx, "ffmpeg", args...)
	outputBytes, err := cmd.CombinedOutput()
	if err != nil {
		_ = os.Remove(tempOutput)
		msg := strings.TrimSpace(string(outputBytes))
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("%s", msg)
	}
	if finalPath == filename {
		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			_ = os.Remove(tempOutput)
			return "", fmt.Errorf("remove original before replace: %w", err)
		}
	}
	if err := os.Rename(tempOutput, finalPath); err != nil {
		_ = os.Remove(tempOutput)
		return "", fmt.Errorf("rename finalized output: %w", err)
	}
	return finalPath, nil
}

func qualityArgsForEncoder(encoder string, quality int) []string {
	if quality <= 0 {
		quality = 23
	}
	lower := strings.ToLower(strings.TrimSpace(encoder))
	switch {
	case strings.Contains(lower, "nvenc"):
		return []string{"-cq", fmt.Sprintf("%d", quality)}
	case strings.Contains(lower, "qsv"), strings.Contains(lower, "vaapi"), strings.Contains(lower, "amf"):
		return []string{"-global_quality", fmt.Sprintf("%d", quality)}
	default:
		return []string{"-crf", fmt.Sprintf("%d", quality)}
	}
}
