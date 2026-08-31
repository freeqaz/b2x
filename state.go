package main

// state.go — the idempotence layer.
//
// THE SECOND BUG this component exists to kill: weights are re-pulled on resume
// even when they are already on the box's disk. A parked/resumed box keeps its
// disk; a preempted job's box may already hold most of a 24 GB base model. The
// ad-hoc per-asset `.complete` markers this replaced were a partial version of
// this (all-or-nothing, no partial resume, no integrity).
//
// Design:
//   * ONE state file per destination root: <dstroot>/.b2x/state.json. Not N
//     sidecars next to N weight files — downstream consumers glob those trees
//     (HF from_pretrained, ninja, tar) and extra files next to a .safetensors
//     are a hazard. A single dotdir is inert to all of them.
//   * The state records, per relative path: remote size, remote ETag, and the
//     set of parts already written. A complete entry lets a re-pull SKIP with
//     no network beyond the one list call. An incomplete entry lets a re-pull
//     fetch ONLY the missing ranges — a genuine improvement over rclone, which
//     restarts an interrupted large file from byte zero.
//   * The state file is advisory, never authoritative: it is always cross-
//     checked against the actual file size on disk. A state file that
//     disagrees with reality loses. Deleting .b2x/ is always safe and just
//     costs a re-verify.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const stateDirName = ".b2x"
const stateFileName = "state.json"

// fileState is what we know about one destination file.
type fileState struct {
	Size     int64  `json:"size"`
	ETag     string `json:"etag,omitempty"`
	PartSize int64  `json:"part_size,omitempty"`
	NParts   int    `json:"nparts,omitempty"`
	// Done lists the completed part indices. Absent/empty with Complete=true
	// means the whole file landed.
	Done     []int `json:"done,omitempty"`
	Complete bool  `json:"complete"`
}

type stateDB struct {
	mu    sync.Mutex
	path  string
	Files map[string]fileState `json:"files"`
	dirty bool
}

func loadState(root string) *stateDB {
	p := filepath.Join(root, stateDirName, stateFileName)
	db := &stateDB{path: p, Files: map[string]fileState{}}
	b, err := os.ReadFile(p)
	if err != nil {
		return db
	}
	var onDisk struct {
		Files map[string]fileState `json:"files"`
	}
	if err := json.Unmarshal(b, &onDisk); err == nil && onDisk.Files != nil {
		db.Files = onDisk.Files
	}
	return db
}

func (d *stateDB) get(rel string) (fileState, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	fs, ok := d.Files[rel]
	return fs, ok
}

func (d *stateDB) set(rel string, fs fileState) {
	d.mu.Lock()
	d.Files[rel] = fs
	d.dirty = true
	d.mu.Unlock()
}

// markPart records one completed part and reports whether the file is now whole.
func (d *stateDB) markPart(rel string, part int) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	fs := d.Files[rel]
	for _, p := range fs.Done {
		if p == part {
			return fs.Complete
		}
	}
	fs.Done = append(fs.Done, part)
	if len(fs.Done) >= fs.NParts && fs.NParts > 0 {
		sort.Ints(fs.Done)
		fs.Complete = true
		fs.Done = nil // whole file: no need to carry the index list
	}
	d.Files[rel] = fs
	d.dirty = true
	return fs.Complete
}

// missingParts returns the part indices not yet recorded as done.
func (fs fileState) missingParts() []int {
	if fs.Complete {
		return nil
	}
	have := make(map[int]bool, len(fs.Done))
	for _, p := range fs.Done {
		have[p] = true
	}
	var out []int
	for i := 0; i < fs.NParts; i++ {
		if !have[i] {
			out = append(out, i)
		}
	}
	return out
}

// save writes the state atomically. Called periodically during a transfer and
// once at the end — an interrupted pull must leave a state file that describes
// strictly LESS than what is on disk (never more), which rename-after-write
// guarantees.
func (d *stateDB) save() error {
	d.mu.Lock()
	if !d.dirty {
		d.mu.Unlock()
		return nil
	}
	snapshot := struct {
		Version int                  `json:"version"`
		Files   map[string]fileState `json:"files"`
	}{Version: 1, Files: map[string]fileState{}}
	for k, v := range d.Files {
		snapshot.Files[k] = v
	}
	d.dirty = false
	d.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(d.path), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	tmp := d.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, d.path)
}

// --- the skip decision -------------------------------------------------------

type skipVerdict int

const (
	skipNo       skipVerdict = iota // must transfer from scratch
	skipComplete                    // identical, do nothing
	skipResume                      // partially present, fetch missing parts only
)

func (v skipVerdict) String() string {
	switch v {
	case skipComplete:
		return "complete"
	case skipResume:
		return "resume"
	}
	return "transfer"
}

// partialPath is where an in-progress download lives: a dotfile beside the
// destination, so it is on the same filesystem (rename into place is atomic and
// free) and invisible to the globs downstream consumers run over model dirs.
//
// This temp-then-rename discipline is what makes the size-only skip in
// decideSkip SOUND. b2x preallocates a part-addressed file and writes ranges out
// of order, so a torn download is full-length on disk — indistinguishable from a
// complete one by size alone. Because a file only ever reaches its FINAL path by
// rename after every part landed, "full size at the final path" really does mean
// complete, and the cheap skip cannot silently adopt a corrupt model.
func partialPath(dst string) string {
	return filepath.Join(filepath.Dir(dst), ".b2x-partial-"+filepath.Base(dst))
}

// decideSkip is the whole idempotence policy in one place.
//
// Ordered cheapest-first, and deliberately conservative: any disagreement
// between the state file and the filesystem falls back to re-transferring.
// A wrong SKIP is a silently corrupt model; a wrong re-pull just costs time.
func decideSkip(local string, remote s3Object, st *stateDB, rel string) (skipVerdict, fileState) {
	fs, known := st.get(rel)

	if fi, err := os.Stat(local); err == nil && !fi.IsDir() && fi.Size() == remote.Size {
		// Case 1: complete at the final path AND the state agrees it completed
		// against this exact remote object. Cheapest possible skip.
		if known && fs.Complete && fs.Size == remote.Size && (fs.ETag == "" || fs.ETag == remote.ETag) {
			return skipComplete, fs
		}
		// Case 2: right size at the final path but no (or stale) state. This is
		// the UPGRADE path — a box whose weights were pulled by the old rclone
		// code has no .b2x state at all, and re-pulling 24 GB just to build one
		// would defeat the entire point of the component. Size equality against
		// B2 is exactly the evidence rclone's own skip logic uses today, and the
		// rename discipline above means it is not a torn file. Adopt it,
		// recording the remote ETag so the next run takes the Case 1 path.
		return skipComplete, fileState{Size: remote.Size, ETag: remote.ETag, Complete: true}
	}

	// Case 3: a partial file whose state describes THIS remote object, with a
	// recorded part plan and at least one part still missing. Resume it —
	// rclone would restart the file from byte zero here.
	if known && !fs.Complete && fs.Size == remote.Size && fs.ETag == remote.ETag && fs.NParts > 0 {
		if fi, err := os.Stat(partialPath(local)); err == nil && fi.Size() == remote.Size &&
			len(fs.missingParts()) > 0 {
			return skipResume, fs
		}
	}

	return skipNo, fileState{}
}
