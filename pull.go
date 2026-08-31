package main

// pull.go — B2 -> box. Idempotent, resumable, saturating, zero required knobs.
//
// The scheduler: every object is split by plan.go into parts, all parts across
// all objects go into ONE queue, and N workers drain it. That single queue is
// why b2x has no --transfers/--multi-thread-streams distinction — a 4-shard
// model dir and one 22 GB monolith both simply fill the same budget.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type pullOpts struct {
	includes []string
	excludes []string
	deadline time.Duration
	dryRun   bool
	verify   bool // force a sha256 verify of every transferred file that carries our metadata
}

// pullJob is one part of one object.
type pullJob struct {
	rel  string
	key  string
	dst  string // the .b2x-partial path being written
	part int
	off  int64
	n    int64
}

type pullTarget struct {
	obj      s3Object
	rel      string
	dst      string
	plan     partPlan
	verdict  skipVerdict
	resuming fileState
}

func runPull(ctx context.Context, c *s3Client, cfg *config, srcKey, dstPath string, o pullOpts, st *stats) error {
	// --- enumerate the source ------------------------------------------------
	// One HEAD first: if srcKey is itself an object, this is a single-file pull
	// (rclone `copyto` semantics) and we skip the list entirely.
	//
	// 403 IS A NEGATIVE ANSWER HERE, NOT AN ERROR. A prefix-restricted or
	// bucketIds-restricted B2 key answers HeadObject on a key it cannot see
	// with 403, never 404 — it will not confirm or deny existence. Every box in
	// the fleet carries exactly such a key (b2_mint_key.mint_pair's `-ro`,
	// listFiles+readFiles on one bucketId), so treating 403 as fatal made EVERY
	// prefix pull fail before listing anything. It was invisible because every
	// call site is `b2x_pull … || <rclone line>` and the rclone line works.
	// Falling through costs nothing: the LIST below is the authoritative check
	// and a genuinely unentitled key fails there with its own message.
	var objs []s3Object
	singleFile := false
	if obj, _, err := c.head(ctx, srcKey); err == nil {
		objs = []s3Object{obj}
		singleFile = true
	} else if s := statusOf(err); s != 0 && s != 404 && s != 401 && s != 403 {
		return err
	} else {
		prefix := srcKey
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		objs, err = c.list(ctx, prefix)
		if err != nil {
			return err
		}
		if len(objs) == 0 {
			return &httpError{Status: 404, Code: "NoSuchKey", Msg: "no objects under prefix", Method: "GET", Key: srcKey}
		}
		srcKey = prefix
	}

	// --- resolve destinations + skip decisions -------------------------------
	stateRoot := dstPath
	if singleFile {
		stateRoot = filepath.Dir(dstPath)
	}
	db := loadState(stateRoot)

	var targets []pullTarget
	for _, ob := range objs {
		rel := ob.Key
		dst := dstPath
		if !singleFile {
			rel = strings.TrimPrefix(ob.Key, srcKey)
			if rel == "" {
				continue
			}
			dst = filepath.Join(dstPath, filepath.FromSlash(rel))
		} else {
			rel = path.Base(ob.Key)
		}
		if !matchFilters(rel, o.includes, o.excludes) {
			continue
		}
		st.totalBytes += ob.Size
		v, fs := decideSkip(dst, ob, db, rel)
		targets = append(targets, pullTarget{obj: ob, rel: rel, dst: dst, plan: planParts(ob.Size), verdict: v, resuming: fs})
	}
	if len(targets) == 0 {
		return fmt.Errorf("b2x pull: nothing matched under %s", srcKey)
	}

	// Biggest first: on a fixed budget the long pole should start earliest, and
	// it means an interrupted pull has made progress on what matters.
	sort.Slice(targets, func(i, j int) bool { return targets[i].obj.Size > targets[j].obj.Size })

	// --- build the part queue -------------------------------------------------
	var jobs []pullJob
	var toFetch int64
	for _, t := range targets {
		if t.verdict == skipComplete {
			st.skippedObjs.Add(1)
			st.skippedBytes.Add(t.obj.Size)
			// Adopt/refresh the state entry so the next run gets the cheap path.
			db.set(t.rel, fileState{Size: t.obj.Size, ETag: t.obj.ETag, Complete: true})
			continue
		}
		st.objs.Add(1)
		part := partialPath(t.dst)
		var want []int
		if t.verdict == skipResume {
			want = t.resuming.missingParts()
		} else {
			for i := 0; i < t.plan.NParts; i++ {
				want = append(want, i)
			}
			db.set(t.rel, fileState{Size: t.obj.Size, ETag: t.obj.ETag,
				PartSize: t.plan.PartSize, NParts: t.plan.NParts})
		}
		for _, i := range want {
			off, n := t.plan.partRange(i)
			jobs = append(jobs, pullJob{rel: t.rel, key: t.obj.Key, dst: part, part: i, off: off, n: n})
			toFetch += n
		}
	}

	if o.dryRun {
		fmt.Fprintf(os.Stderr, "b2x: DRY RUN — %d objects (%s), %d to transfer (%s in %d parts), %d already present (%s)\n",
			len(targets), humanBytes(st.totalBytes), st.objs.Load(), humanBytes(toFetch), len(jobs),
			st.skippedObjs.Load(), humanBytes(st.skippedBytes.Load()))
		for _, t := range targets {
			fmt.Fprintf(os.Stderr, "  %-9s %-60s %10s  parts=%d partsize=%s\n",
				t.verdict, t.rel, humanBytes(t.obj.Size), t.plan.NParts, humanBytes(t.plan.PartSize))
		}
		return nil
	}

	if len(jobs) == 0 {
		fmt.Fprintf(os.Stderr, "b2x: all %d objects already present (%s) — nothing to do\n",
			len(targets), humanBytes(st.skippedBytes.Load()))
		_ = db.save()
		return nil
	}
	fmt.Fprintf(os.Stderr, "b2x: pull %d objects, %s to fetch in %d parts (%s already present), %d streams\n",
		st.objs.Load(), humanBytes(toFetch), len(jobs), humanBytes(st.skippedBytes.Load()), st.concurrency)

	// --- preallocate the partial files ---------------------------------------
	// Sparse full-length files let workers WriteAt any range in any order.
	files := map[string]*os.File{}
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	for _, t := range targets {
		if t.verdict == skipComplete {
			continue
		}
		part := partialPath(t.dst)
		if err := os.MkdirAll(filepath.Dir(part), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(part, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return err
		}
		if err := f.Truncate(t.obj.Size); err != nil {
			f.Close()
			return fmt.Errorf("preallocate %s (%s): %w", t.rel, humanBytes(t.obj.Size), err)
		}
		files[part] = f
	}

	// --- drain the queue ------------------------------------------------------
	// GUARDS (guard.go): a wall-clock ceiling and an aggregate throughput floor,
	// both armed only around the byte-moving phase. An explicit --deadline always
	// wins over the derived one — a caller with a hard budget (preempt_trap's
	// 40 s flush) means it literally.
	dl, dlWhy := o.deadline, "explicit --deadline"
	if dl <= 0 {
		dl, dlWhy = autoDeadline(toFetch, cfg), "derived from bytes / B2X_MIN_MBPS"
	}
	if dl > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dl)
		defer cancel()
		fmt.Fprintf(os.Stderr, "b2x: ceiling %s (%s), floor %.2f MB/s over %s, stall %s\n",
			dl.Round(time.Second), dlWhy, cfg.minMBps, cfg.floorWindow, cfg.stallTimeout)
	}
	floorCtx, floorCancel := context.WithCancel(ctx)
	defer floorCancel()
	ctx = floorCtx
	fw := startFloorWatch(st, cfg.minMBps, cfg.floorWindow, floorCancel)
	defer fw.halt()
	st.startProgress(15 * time.Second)

	done := make(map[string]bool) // rel -> whole file complete
	var doneMu sync.Mutex

	err := runParts(ctx, st.concurrency, len(jobs), func(i int) error {
		j := jobs[i]
		f := files[j.dst]
		if f == nil {
			return fmt.Errorf("internal: no open file for %s", j.rel)
		}
		if err := fetchPart(ctx, c, cfg, f, j, st); err != nil {
			return err
		}
		st.parts.Add(1)
		if db.markPart(j.rel, j.part) {
			doneMu.Lock()
			done[j.rel] = true
			doneMu.Unlock()
		}
		return nil
	}, func() { _ = db.save() })

	st.stop()
	fw.halt()
	// Always persist what landed, even on failure — that is what makes the next
	// attempt a resume instead of a restart.
	_ = db.save()
	// ORDER MATTERS: the floor watch cancelled ctx, so `err` here is a generic
	// context.Canceled. Reporting that would hand the operator exactly the
	// anonymous failure this work exists to eliminate — name the verdict first.
	if serr := fw.err("pull", st.done.Load()); serr != nil {
		return serr
	}
	if err != nil {
		return err
	}

	// --- promote completed partials to their final paths ----------------------
	// Rename only after every part landed. See partialPath's comment: this is
	// what makes the cheap size-only skip sound.
	for _, t := range targets {
		if t.verdict == skipComplete {
			continue
		}
		part := partialPath(t.dst)
		if f := files[part]; f != nil {
			if err := f.Sync(); err != nil {
				return fmt.Errorf("fsync %s: %w", t.rel, err)
			}
			f.Close()
			delete(files, part)
		}
		if !done[t.rel] {
			return fmt.Errorf("internal: %s not complete after drain", t.rel)
		}
		if err := verifyIfPossible(ctx, c, t, part, o.verify); err != nil {
			return err
		}
		if err := os.Rename(part, t.dst); err != nil {
			return fmt.Errorf("promote %s: %w", t.rel, err)
		}
	}
	_ = db.save()
	return nil
}

// verifyIfPossible checks the sha256 we stored on OUR OWN uploads
// (x-amz-meta-b2x-sha256). Objects written by anything else carry no such
// metadata and are size-verified only — the same standard as today's rclone
// paths, so this is never a regression, only an upgrade where we own the writer.
func verifyIfPossible(ctx context.Context, c *s3Client, t pullTarget, partPath string, force bool) error {
	_, meta, err := c.head(ctx, t.obj.Key)
	if err != nil {
		if force {
			return fmt.Errorf("verify %s: HEAD failed: %w", t.rel, err)
		}
		return nil
	}
	want := meta["b2x-sha256"]
	if want == "" {
		if force {
			return fmt.Errorf("verify %s: object carries no b2x-sha256 metadata (not written by b2x)", t.rel)
		}
		return nil
	}
	got, err := fileSHA256(partPath)
	if err != nil {
		return fmt.Errorf("verify %s: %w", t.rel, err)
	}
	if got != want {
		return &integrityError{Rel: t.rel, Want: want, Got: got}
	}
	return nil
}

type integrityError struct{ Rel, Want, Got string }

func (e *integrityError) Error() string {
	return fmt.Sprintf("integrity: %s sha256 mismatch (want %s got %s)", e.Rel, e.Want, e.Got)
}

// fetchPart transfers ONE part, retrying a stall or a transient network error at
// the same byte offset.
//
// Retrying here rather than failing the transfer is the whole point of guard 1:
// a ranged GET is restartable by construction, so one dead flow out of 128 on a
// flaky host should cost a re-requested range, not the pull. What must NOT be
// retried is a verdict that will not change — a 403 from a revoked key or a 404
// for a missing object — because burning three attempts on those delays the
// diagnostic that actually helps.
func fetchPart(ctx context.Context, c *s3Client, cfg *config, f *os.File, j pullJob, st *stats) error {
	// CLAMP, do not trust the field. A config built literally rather than by
	// loadConfig (every test does this, and so could a future caller) carries
	// partTries=0, and an unclamped `attempt < 0` loop would return nil having
	// transferred NOTHING — a silent, content-corrupting success. The b2x suite
	// caught exactly that; the clamp is what keeps it caught.
	tries := cfg.partTries
	if tries < 1 {
		tries = 1
	}
	var last error
	for attempt := 0; attempt < tries; attempt++ {
		if attempt > 0 {
			st.retries.Add(1)
			fmt.Fprintf(os.Stderr, "b2x: %s part %d: %v — retry %d/%d\n",
				j.rel, j.part, last, attempt, tries-1)
			select {
			case <-time.After(time.Duration(250<<uint(attempt-1)) * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		n, err := fetchPartOnce(ctx, c, cfg, f, j, st)
		if err == nil {
			if n == j.n {
				return nil
			}
			// A short read with no error means the peer closed early. Same
			// treatment as a stall: restartable, so retry rather than abort.
			last = fmt.Errorf("short read on %s part %d: got %d want %d", j.rel, j.part, n, j.n)
			continue
		}
		last = err
		// The PARENT context died (ceiling, floor, SIGTERM) — stop immediately.
		// Note this is the parent, not the per-part context the stall guard
		// cancels, so a stall does not land here.
		if ctx.Err() != nil {
			return err
		}
		if s := statusOf(err); s != 0 && s < 500 && s != 408 && s != 429 {
			return err // terminal: auth, not-found, malformed
		}
	}
	return last
}

func fetchPartOnce(ctx context.Context, c *s3Client, cfg *config, f *os.File, j pullJob, st *stats) (int64, error) {
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Armed BEFORE the request: a peer that completes the handshake and then
	// never sends response headers is as stalled as one that dies mid-body.
	sg := newStallGuard(cfg.stallTimeout, cancel)
	defer sg.stop()

	body, err := c.getRange(pctx, j.key, j.off, j.n)
	if err != nil {
		return 0, fmt.Errorf("get %s part %d: %w", j.rel, j.part, sg.classify(err, j.rel, j.part))
	}
	defer body.Close()
	n, err := copyToOffset(f, body, j.off, st, sg)
	if err != nil {
		// UNCOUNT what this attempt moved. The retry re-fetches the same range,
		// and leaving the bytes counted would inflate the very rate the
		// throughput floor is measuring — a flapping flow would look FAST.
		if n > 0 {
			st.addBytes(-n)
		}
		return n, fmt.Errorf("write %s part %d: %w", j.rel, j.part, sg.classify(err, j.rel, j.part))
	}
	return n, nil
}

// copyToOffset streams body into f at off, counting bytes as they land so the
// progress meter is real rather than interpolated, and ticking the stall guard
// so a silent flow is bounded rather than parked forever.
func copyToOffset(f *os.File, body io.Reader, off int64, st *stats, sg *stallGuard) (int64, error) {
	buf := make([]byte, 1<<20)
	var total int64
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, werr := f.WriteAt(buf[:n], off+total); werr != nil {
				return total, werr
			}
			total += int64(n)
			st.addBytes(int64(n))
			if sg != nil {
				sg.tick()
			}
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

// runParts drains n work items across `workers` goroutines, stopping on the
// first error. checkpoint is called periodically so state survives a kill.
func runParts(ctx context.Context, workers, n int, do func(i int) error, checkpoint func()) error {
	if workers > n {
		workers = n
	}
	if workers < 1 {
		workers = 1
	}
	idx := make(chan int)
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				if ctx.Err() != nil {
					return
				}
				if err := do(i); err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
			}
		}()
	}

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				checkpoint()
			case <-ctx.Done():
				return
			}
		}
	}()

	feed := func() {
		defer close(idx)
		for i := 0; i < n; i++ {
			select {
			case idx <- i:
			case <-ctx.Done():
				return
			}
		}
	}
	go feed()
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}
	if err := ctx.Err(); err != nil && errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}
