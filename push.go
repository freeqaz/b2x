package main

// push.go — box -> B2. Same planner, same single budget, plus the two things
// the upload path specifically needs:
//
//  1. CONCURRENCY, because this is a DURABILITY fix, not a speed one.
//     A spot-eviction trap flushes the final checkpoint inside `timeout 45`
//     at stock rclone concurrency. A multi-GB flush at stock settings does not
//     finish in 45 s on a per-flow-shaped host, so eviction silently loses
//     state. More parts in flight is what makes that budget achievable.
//
//  2. NEWEST-FIRST ordering under a deadline. If the budget runs out anyway,
//     what survives should be the newest checkpoint, not whatever the walk
//     happened to reach first. b2x sorts by mtime descending and completes each
//     object before starting the next tranche, so a truncated flush leaves a
//     PREFIX of the newest files fully uploaded rather than N torn ones.
//     Multipart uploads are atomic — an object only appears at
//     CompleteMultipartUpload — so an interrupted push never publishes a torn
//     object either way.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type pushOpts struct {
	includes []string
	excludes []string
	minAge   time.Duration
	deadline time.Duration
	dryRun   bool
}

type pushItem struct {
	abs   string
	rel   string
	size  int64
	mtime time.Time
}

// name is what a log line should call this item. rel is empty for a single-file
// push (see runPush), where the source path is the only meaningful name.
func (it pushItem) name() string {
	if it.rel != "" {
		return it.rel
	}
	return it.abs
}

func runPush(ctx context.Context, c *s3Client, cfg *config, srcPath, dstKey string, o pushOpts, st *stats) error {
	fi, err := os.Stat(srcPath)
	if err != nil {
		return err
	}

	var items []pushItem
	if fi.IsDir() {
		root := strings.TrimRight(srcPath, "/")
		err = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return nil // unreadable entries must not abort a durability flush
			}
			if info.IsDir() || !info.Mode().IsRegular() {
				return nil
			}
			rel, rerr := filepath.Rel(root, p)
			if rerr != nil {
				return nil
			}
			rel = filepath.ToSlash(rel)
			// our own transient artifacts never ship
			if strings.HasPrefix(rel, stateDirName+"/") || strings.Contains(rel, "/.b2x-partial-") ||
				strings.HasPrefix(filepath.Base(rel), ".b2x-partial-") {
				return nil
			}
			if !matchFilters(rel, o.includes, o.excludes) {
				return nil
			}
			if o.minAge > 0 && time.Since(info.ModTime()) < o.minAge {
				return nil
			}
			items = append(items, pushItem{abs: p, rel: rel, size: info.Size(), mtime: info.ModTime()})
			return nil
		})
		if err != nil {
			return err
		}
	} else {
		// A file source with a dir-ish destination keeps the basename; otherwise
		// the destination key is taken literally (rclone copyto semantics).
		if strings.HasSuffix(dstKey, "/") {
			dstKey += filepath.Base(srcPath)
		}
		// rel stays empty for a single file: the destination key is already the
		// whole key, so pushKey must not append anything to it. Log lines use
		// name() rather than rel for exactly that reason.
		items = []pushItem{{abs: srcPath, size: fi.Size(), mtime: fi.ModTime()}}
	}
	if len(items) == 0 {
		fmt.Fprintf(os.Stderr, "b2x: push — nothing to upload from %s\n", srcPath)
		return nil
	}

	// NEWEST FIRST — the deadline-truncation policy (see file header).
	sort.Slice(items, func(i, j int) bool { return items[i].mtime.After(items[j].mtime) })

	// --- skip what is already on B2 ------------------------------------------
	// Size equality against the remote listing, the same standard the current
	// rclone `copy` paths use. One list call for the whole prefix beats a HEAD
	// per file (a 20-checkpoint tree is hundreds of HEADs on a billed box).
	prefix := strings.TrimSuffix(dstKey, "/")
	remote := map[string]s3Object{}
	if fi.IsDir() {
		if got, lerr := c.list(ctx, prefix+"/"); lerr == nil {
			for _, ob := range got {
				remote[strings.TrimPrefix(ob.Key, prefix+"/")] = ob
			}
		}
	}

	var todo []pushItem
	for _, it := range items {
		st.totalBytes += it.size
		lookup := it.rel
		if !fi.IsDir() {
			lookup = ""
		}
		if ob, ok := remote[lookup]; ok && ob.Size == it.size {
			st.skippedObjs.Add(1)
			st.skippedBytes.Add(it.size)
			continue
		}
		todo = append(todo, it)
	}

	if o.dryRun {
		fmt.Fprintf(os.Stderr, "b2x: DRY RUN push — %d files, %d to upload, %d already present (%s)\n",
			len(items), len(todo), st.skippedObjs.Load(), humanBytes(st.skippedBytes.Load()))
		for _, it := range todo {
			p := planParts(it.size)
			fmt.Fprintf(os.Stderr, "  upload  %-60s %10s  parts=%d partsize=%s\n",
				pushKey(prefix, it.rel, fi.IsDir()), humanBytes(it.size), p.NParts, humanBytes(p.PartSize))
		}
		return nil
	}
	if len(todo) == 0 {
		fmt.Fprintf(os.Stderr, "b2x: push — all %d files already on B2 (%s) — nothing to do\n",
			len(items), humanBytes(st.skippedBytes.Load()))
		return nil
	}

	// GUARDS (guard.go). Same two signals as pull, same precedence: an explicit
	// --deadline is a hard budget the caller means (a spot-eviction trap flushing
	// inside a 40 s hard timeout) and always wins. Without one, a push that would
	// otherwise hang on a half-open socket gets a ceiling derived from its own
	// byte count.
	dl, dlWhy := o.deadline, "explicit --deadline"
	if dl <= 0 {
		dl, dlWhy = autoDeadline(sumSizes(todo), cfg), "derived from bytes / B2X_MIN_MBPS"
	}
	if dl > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dl)
		defer cancel()
	}
	floorCtx, floorCancel := context.WithCancel(ctx)
	defer floorCancel()
	ctx = floorCtx
	fw := startFloorWatch(st, cfg.minMBps, cfg.floorWindow, floorCancel)
	defer fw.halt()
	fmt.Fprintf(os.Stderr, "b2x: push %d files (%s), %d streams%s\n",
		len(todo), humanBytes(sumSizes(todo)), st.concurrency,
		map[bool]string{true: fmt.Sprintf(", ceiling %s (%s), floor %.2f MB/s over %s, newest first",
			dl.Round(time.Second), dlWhy, cfg.minMBps, cfg.floorWindow), false: ""}[dl > 0])
	st.startProgress(15 * time.Second)

	// Upload files one at a time, but each file's PARTS in parallel across the
	// whole budget. Serial-across-files is what gives the deadline its
	// prefix-completion property: at any interruption, the newest K files are
	// fully on B2 and file K+1 is an aborted MPU that never became visible.
	var firstErr error
	for _, it := range todo {
		if ctx.Err() != nil {
			break
		}
		key := pushKey(prefix, it.rel, fi.IsDir())
		if err := uploadOne(ctx, c, it, key, st); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if ctx.Err() != nil {
				break
			}
			// A single bad file must not abandon the rest of a durability flush.
			fmt.Fprintf(os.Stderr, "b2x: push %s FAILED: %v (continuing)\n", it.name(), err)
			continue
		}
		st.objs.Add(1)
	}
	st.stop()
	fw.halt()

	// Name the SLOW verdict before the generic one: the floor watch cancels the
	// same ctx a deadline does, so reporting ctx.Err() first would mislabel every
	// floor breach as a deadline and lose the rate that explains it.
	if serr := fw.err("push", st.done.Load()); serr != nil {
		return serr
	}
	if ctx.Err() != nil {
		return fmt.Errorf("push deadline exceeded after %s (%d/%d files uploaded, newest first): %w",
			humanBytes(st.done.Load()), st.objs.Load(), len(todo), ctx.Err())
	}
	return firstErr
}

func pushKey(prefix, rel string, isDir bool) string {
	if !isDir || rel == "" {
		return prefix
	}
	return prefix + "/" + rel
}

func sumSizes(items []pushItem) int64 {
	var n int64
	for _, it := range items {
		n += it.size
	}
	return n
}

// uploadOne writes a single file, single-PUT when small, multipart otherwise.
// Every b2x-written object carries x-amz-meta-b2x-sha256 so a later pull can
// verify it exactly (see verifyIfPossible).
func uploadOne(ctx context.Context, c *s3Client, it pushItem, key string, st *stats) error {
	f, err := os.Open(it.abs)
	if err != nil {
		return err
	}
	defer f.Close()

	// HASH EXACTLY THE BYTES WE UPLOAD — never "the file".
	//
	// it.size is captured at WALK time; the upload plan (planParts, partRange)
	// is derived from it and moves exactly that many bytes. A file that is
	// still being appended to — the checkpoint lane's whole job, an NDJSON
	// growing a row at a time — is LONGER by the time we hash it. Hashing the
	// open-ended file therefore records a b2x-sha256 over size+delta bytes for
	// an object holding size bytes, so `pull --verify` would reject an object
	// that is in fact a correct prefix. Bounding both the hash and the read to
	// it.size makes "the recorded sum covers the uploaded bytes" true by
	// construction rather than true only for a quiescent file.
	sum, err := fileSHA256N(it.abs, it.size)
	if err != nil {
		return err
	}
	meta := map[string]string{"b2x-sha256": sum, "b2x-mtime": it.mtime.UTC().Format(time.RFC3339)}

	p := planParts(it.size)
	if p.NParts <= 1 {
		// Bounded, not io.ReadAll: an unbounded read would PUT more bytes than
		// the plan (and than the hash above) accounts for.
		data := make([]byte, it.size)
		if _, err := io.ReadFull(f, data); err != nil {
			return fmt.Errorf("%s: short read at %d bytes: %w", it.name(), it.size, err)
		}
		if err := c.putObject(ctx, key, data, meta); err != nil {
			return err
		}
		st.addBytes(int64(len(data)))
		st.parts.Add(1)
		return nil
	}

	uploadID, err := c.createMPU(ctx, key, meta)
	if err != nil {
		return err
	}
	parts := make([]completePart, p.NParts)
	err = runParts(ctx, st.concurrency, p.NParts, func(i int) error {
		off, n := p.partRange(i)
		buf := make([]byte, n)
		// A short ReadAt means the file SHRANK below the walk-time size. The
		// old `rerr != io.EOF` swallow uploaded the zero tail of buf for those
		// bytes — a silent content corruption, and one the recorded sha256
		// would not catch since it was computed off a different read. os.File
		// .ReadAt fills buf completely or returns an error, so n short is the
		// whole signal.
		if rn, rerr := f.ReadAt(buf, off); rerr != nil || int64(rn) != n {
			if rerr == nil || rerr == io.EOF {
				return fmt.Errorf("%s: part %d short read (%d of %d bytes at %d) — file shrank under the upload",
					it.name(), i+1, rn, n, off)
			}
			return rerr
		}
		etag, uerr := c.uploadPart(ctx, key, uploadID, i+1, buf)
		if uerr != nil {
			return uerr
		}
		parts[i] = completePart{PartNumber: i + 1, ETag: etag}
		st.addBytes(n)
		st.parts.Add(1)
		return nil
	}, func() {})
	if err != nil {
		// Orphaned parts bill on B2. Abort on a fresh, short-budget context so
		// a deadline-cancelled parent context cannot skip the cleanup.
		actx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = c.abortMPU(actx, key, uploadID)
		cancel()
		return err
	}
	return c.completeMPU(ctx, key, uploadID, parts)
}

// fileSHA256 hashes a whole file. Correct for the pull-side --verify pass,
// where the file is one b2x just finished writing and nothing else appends to
// it. On the PUSH side use fileSHA256N: the source can still be growing.
func fileSHA256(p string) (string, error) {
	fi, err := os.Stat(p)
	if err != nil {
		return "", err
	}
	return fileSHA256N(p, fi.Size())
}

// fileSHA256N hashes exactly the first n bytes of p. Short file => error: the
// caller's upload plan is already committed to n bytes, so a file that cannot
// supply them is a failed transfer, not a smaller one.
func fileSHA256N(p string, n int64) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	got, err := io.Copy(h, io.LimitReader(f, n))
	if err != nil {
		return "", err
	}
	if got != n {
		return "", fmt.Errorf("%s: only %d of %d bytes readable — file shrank under the upload", p, got, n)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
