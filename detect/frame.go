package detect

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// FrameFromSegments decodes the newest key frame that the given pieces of a stream hold and returns it as
// a JPEG no wider than 640 px. For fragmented MP4 the first piece must be the init segment. It needs ffmpeg.
// Only key frames are decoded, so it costs a few milliseconds of CPU.
func FrameFromSegments(ctx context.Context, parts ...[]byte) ([]byte, error) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		return nil, errors.New("ffmpeg is needed to look at the recording and was not found in PATH")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	readers := make([]io.Reader, len(parts))
	for i, p := range parts {
		readers[i] = bytes.NewReader(p)
	}
	cmd := exec.CommandContext(ctx, ffmpeg, "-nostdin", "-v", "error", "-skip_frame", "nokey",
		"-analyzeduration", "1000000", "-probesize", "2000000", "-i", "pipe:0",
		"-vf", "scale='min(640,iw)':-2", "-q:v", "4", "-f", "image2pipe", "-c:v", "mjpeg", "pipe:1")
	cmd.Stdin = io.MultiReader(readers...)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	runErr := cmd.Run()
	if img := lastJPEG(out.Bytes()); img != nil {
		return img, nil // some ffmpeg versions complain about a cut last packet after giving the picture
	}
	msg := strings.TrimSpace(errOut.String())
	if msg == "" && runErr != nil {
		msg = runErr.Error()
	}
	if msg == "" {
		msg = "no key frame in the last seconds"
	}
	return nil, fmt.Errorf("frame of the recording: %s", msg)
}

// lastJPEG returns the last picture of a stream of concatenated JPEGs, or nil. (In JPEG data a 0xFF is
// always followed by 0x00 unless it starts a marker, so FF D8 FF only appears at the start of a picture.)
func lastJPEG(b []byte) []byte {
	i := bytes.LastIndex(b, []byte{0xFF, 0xD8, 0xFF})
	if i < 0 {
		return nil
	}
	return b[i:]
}
