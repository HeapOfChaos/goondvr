package chaturbate

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/HeapOfChaos/goondvr/persist"
)

// Repairs for recordings that a killed process, a closed console window or a
// full disk left unfinished. They only ever remove what is incomplete at the
// very end of a file (it was never playable); the rest is not touched.

// RepairTail cuts the unfinished end off a recording: an incomplete MP4
// fragment, or part of an MPEG-TS packet. Other file types are left alone.
func RepairTail(path string) error {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".mp4":
		return RepairMP4Tail(path)
	case ".ts":
		return RepairTSTail(path)
	}
	return nil
}

// RepairTSTail cuts a partial last packet (MPEG-TS packets are 188 bytes). A
// file that does not start with the sync byte is not touched.
func RepairTSTail(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	rest := st.Size() % 188
	if rest == 0 || st.Size() < 188 {
		return nil
	}
	var first [1]byte
	if _, err := f.ReadAt(first[:], 0); err != nil || first[0] != 0x47 {
		return nil
	}
	return f.Truncate(st.Size() - rest)
}

// RepairMP4Tail cuts a fragmented MP4 back to its last complete fragment: a
// box that was cut short, or a moof whose mdat never arrived. A file that does
// not look like a sequence of MP4 boxes is not touched, and a file is never
// cut down to nothing.
func RepairMP4Tail(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()

	hdr := make([]byte, 16)
	var pos, cut int64
	pendingMoof := false // a moof is complete but its mdat is not (yet)
scan:
	for pos+8 <= size {
		if _, err := f.ReadAt(hdr[:8], pos); err != nil {
			return err
		}
		boxSize := int64(binary.BigEndian.Uint32(hdr[:4]))
		typ := string(hdr[4:8])
		if pos == 0 && !plausibleFourCC(typ) {
			return nil
		}
		headerLen := int64(8)
		switch boxSize {
		case 0: // the box runs to the end of the file
			boxSize = size - pos
		case 1: // 64-bit size
			if pos+16 > size {
				break scan
			}
			if _, err := f.ReadAt(hdr[8:16], pos+8); err != nil {
				return err
			}
			boxSize, headerLen = int64(binary.BigEndian.Uint64(hdr[8:16])), 16
		}
		if boxSize < headerLen {
			return nil // not something this understands: leave the file alone
		}
		if pos+boxSize > size {
			break // cut short at the end of the file
		}
		switch typ {
		case "moof":
			pendingMoof = true
		case "mdat":
			pendingMoof = false
			cut = pos + boxSize
		default:
			if !pendingMoof {
				cut = pos + boxSize
			}
		}
		pos += boxSize
	}
	if (pos == size && !pendingMoof) || cut == 0 || cut >= size {
		return nil
	}
	return f.Truncate(cut)
}

func plausibleFourCC(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == ' ') {
			return false
		}
	}
	return len(s) == 4
}

// The seek index rewrites the time of every fragment in place (see
// BuildSeekIndex). Killed half way it would leave a file whose timeline jumps,
// so the new values are written down first, as absolute values, in a journal
// next to the file; applying them is safe to repeat until it has finished.

type tfdtEdit struct {
	Off int64  `json:"o"`
	Ver byte   `json:"v"`
	Old uint64 `json:"a"`
	New uint64 `json:"b"`
}

var (
	errJournalMismatch = errors.New("the file is not the one the journal was made for")
	errSimulatedCrash  = errors.New("simulated crash")
	// applyHook lets a test stop applyEdits after some edits, as a kill would.
	applyHook func(done int) bool
)

func seekJournalPath(path string) string { return path + ".seekidx" }

func writeSeekJournal(path string, edits []tfdtEdit) error {
	b, err := json.Marshal(edits)
	if err != nil {
		return err
	}
	return persist.Replace(seekJournalPath(path), b, 0600)
}

func readTfdt(f *os.File, off int64, ver byte) (uint64, error) {
	if ver == 1 {
		var b [8]byte
		if _, err := f.ReadAt(b[:], off); err != nil {
			return 0, err
		}
		return binary.BigEndian.Uint64(b[:]), nil
	}
	var b [4]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		return 0, err
	}
	return uint64(binary.BigEndian.Uint32(b[:])), nil
}

func writeTfdt(f *os.File, off int64, ver byte, v uint64) error {
	if ver == 1 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], v)
		_, err := f.WriteAt(b[:], off)
		return err
	}
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	_, err := f.WriteAt(b[:], off)
	return err
}

// applyEdits sets every time to its new value. A value that is neither the old
// nor the new one means the file is not the one the edits were planned for.
func applyEdits(f *os.File, edits []tfdtEdit) error {
	for i, e := range edits {
		if applyHook != nil && !applyHook(i) {
			return errSimulatedCrash
		}
		cur, err := readTfdt(f, e.Off, e.Ver)
		if err != nil {
			return errJournalMismatch
		}
		if cur == e.New {
			continue
		}
		if cur != e.Old {
			return errJournalMismatch
		}
		if err := writeTfdt(f, e.Off, e.Ver, e.New); err != nil {
			return err
		}
	}
	return nil
}

// applySeekJournal finishes the change that an earlier run was killed in.
func applySeekJournal(path string) error {
	jp := seekJournalPath(path)
	b, err := os.ReadFile(jp)
	if err != nil {
		return nil // no journal: nothing was interrupted
	}
	var edits []tfdtEdit
	f, ferr := os.OpenFile(path, os.O_RDWR, 0)
	if json.Unmarshal(b, &edits) != nil || ferr != nil {
		_ = os.Remove(jp)
		return nil
	}
	defer f.Close()
	if err := applyEdits(f, edits); err != nil {
		if errors.Is(err, errJournalMismatch) {
			_ = os.Remove(jp) // never touch a file the journal does not describe
			return nil
		}
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return os.Remove(jp)
}
