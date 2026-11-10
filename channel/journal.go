package channel

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/HeapOfChaos/goondvr/chaturbate"
	"github.com/HeapOfChaos/goondvr/persist"
)

// The recordings that are open, or closed but not yet finished (seek index,
// ffmpeg, move to the completed folder), are listed in a small file. If the
// program is killed, or its console window is closed, those files are still
// in the list at the next start and RecoverRecordings repairs them: it cuts the
// half-written end off and finishes the seek index. A recording that finished
// normally is taken off the list.

// journalPath is where the list is saved ("" = in memory only, for tests).
var journalPath = "./conf/open-recordings.json"

var journal struct {
	sync.Mutex
	loaded  bool
	pending map[string]struct{} // open or not finished yet (this is what is saved)
	open    map[string]struct{} // being written right now by this run
}

// shutdown says that the program is closing. From then on no recording
// finalization starts (it would not finish in the few seconds Windows gives
// after the console window is closed) and a running ffmpeg is stopped; the
// recordings stay in the journal and are finished at the next start.
var (
	shutdown                         atomic.Bool
	shutdownCtx, shutdownCancelFinal = context.WithCancel(context.Background())
)

// BeginShutdown tells the channels that the program is closing.
func BeginShutdown() {
	shutdown.Store(true)
	shutdownCancelFinal()
}

func shuttingDown() bool { return shutdown.Load() }

func loadJournalLocked() {
	if journal.loaded {
		return
	}
	journal.loaded = true
	journal.pending = map[string]struct{}{}
	journal.open = map[string]struct{}{}
	if journalPath == "" {
		return
	}
	var list []string
	if b, err := os.ReadFile(journalPath); err == nil && json.Unmarshal(b, &list) == nil {
		for _, p := range list {
			journal.pending[p] = struct{}{}
		}
	}
}

func saveJournalLocked() {
	if journalPath == "" {
		return
	}
	list := make([]string, 0, len(journal.pending))
	for p := range journal.pending {
		list = append(list, p)
	}
	sort.Strings(list)
	b, _ := json.Marshal(list)
	if err := os.MkdirAll(filepath.Dir(journalPath), 0700); err == nil {
		if err := persist.Replace(journalPath, b, 0600); err != nil {
			log.Printf("WARN  open-recordings list: %v", err)
		}
	}
}

// journalOpen records that path is being written.
func journalOpen(path string) {
	journal.Lock()
	defer journal.Unlock()
	loadJournalLocked()
	journal.pending[path] = struct{}{}
	journal.open[path] = struct{}{}
	saveJournalLocked()
}

// journalClosed records that the file is closed; it stays on the list until finished.
func journalClosed(path string) {
	journal.Lock()
	defer journal.Unlock()
	loadJournalLocked()
	delete(journal.open, path)
}

// journalDone takes a finished (or removed) recording off the list.
func journalDone(path string) {
	journal.Lock()
	defer journal.Unlock()
	loadJournalLocked()
	delete(journal.open, path)
	if _, ok := journal.pending[path]; ok {
		delete(journal.pending, path)
		saveJournalLocked()
	}
}

// RecoverRecordings finishes the recordings that an earlier run did not: it
// cuts off the unfinished end of the file and builds the seek index. Call it
// once at start (in a goroutine: a long recording takes a moment). It only
// repairs: the finalization mode (ffmpeg) and the move to the completed folder
// are not applied to these files.
func RecoverRecordings() {
	journal.Lock()
	loadJournalLocked()
	var list []string
	for p := range journal.pending {
		if _, open := journal.open[p]; !open {
			list = append(list, p)
		}
	}
	journal.Unlock()
	sort.Strings(list)

	for _, p := range list {
		if shuttingDown() {
			return
		}
		journal.Lock()
		_, open := journal.open[p]
		journal.Unlock()
		if !open {
			recoverRecording(p)
		}
	}
}

func recoverRecording(path string) {
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			journalDone(path) // it was moved or deleted: nothing left to repair
		}
		return
	}
	// nothing was written, or less than one TS packet: it never held anything playable
	if st.Size() == 0 || (strings.EqualFold(filepath.Ext(path), ".ts") && st.Size() < 188) {
		_ = os.Remove(path)
		journalDone(path)
		return
	}
	log.Printf("INFO  repairing the unfinished recording `%s`", path)
	if err := chaturbate.RepairTail(path); err != nil {
		log.Printf("WARN  repair `%s`: %v", path, err)
		return
	}
	if strings.EqualFold(filepath.Ext(path), ".mp4") {
		if err := chaturbate.BuildSeekIndex(path); err != nil {
			log.Printf("WARN  seek index `%s`: %v", path, err)
			return
		}
	}
	journalDone(path)
}
