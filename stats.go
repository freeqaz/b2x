package main

// stats.go — ALWAYS-ON throughput observability.
//
// Three consumers, three shapes, all emitted unconditionally (there is no
// --stats flag to forget):
//
//  1. HUMANS / a captured boot log — a progress line on stderr every 15 s, and
//     a one-line summary at the end. A boot script that ships its log off-box
//     periodically makes a stalled pull visible live.
//
//  2. SHELL CALLERS — `--stats-env FILE` writes B2X_BYTES / B2X_SECS /
//     B2X_MBPS / B2X_SKIPPED_BYTES / B2X_OBJECTS as sourceable KEY=VALUE lines.
//     The field names were chosen to match what boot scripts already published
//     from scraped rclone output, so a caller keeps its existing throughput
//     contract and merely gets numbers measured precisely rather than grepped
//     out of prose.
//
//  3. MACHINES — `--json` writes one JSON object per line to stdout
//     (a final `b2x_done` record always). Supervisors that parse a live MB/s
//     out of a stats file by regexing rclone's "12.3 MiB/s" text get a real
//     field instead. That old regex still matches our stderr progress line, so
//     an UNMIGRATED reader keeps working — see the deliberate "MiB/s" spelling
//     in progressLine.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type stats struct {
	Op   string
	Src  string
	Dst  string
	t0   time.Time
	done atomic.Int64 // bytes actually transferred

	skippedBytes atomic.Int64
	skippedObjs  atomic.Int64
	objs         atomic.Int64
	totalBytes   int64
	parts        atomic.Int64
	retries      atomic.Int64
	concurrency  int

	// verdict is how a SHELL caller tells the three outcomes apart without
	// parsing prose: "ok", "slow" (under the throughput floor), "timeout" (the
	// wall-clock ceiling), "stalled", or "error". It reaches callers through
	// B2X_VERDICT in --stats-env and the `verdict` field in --json, so a timeout
	// is distinguishable from a failure and from success at every consumer.
	verdict string

	jsonOut  bool
	mu       sync.Mutex
	stopOnce sync.Once
	stopCh   chan struct{}
}

func newStats(op, src, dst string, jsonOut bool, concurrency int) *stats {
	return &stats{Op: op, Src: src, Dst: dst, t0: time.Now(), jsonOut: jsonOut,
		concurrency: concurrency, verdict: "ok", stopCh: make(chan struct{})}
}

// setVerdict classifies the transfer's terminal error into the vocabulary above.
// Mirrors report()'s exit-code mapping; keep the two in step.
func (s *stats) setVerdict(err error) {
	switch {
	case err == nil:
		s.verdict = "ok"
	case errorsAs[*slowError](err):
		s.verdict = "slow"
	case errors.Is(err, context.DeadlineExceeded):
		s.verdict = "timeout"
	case errorsAs[*stallError](err):
		s.verdict = "stalled"
	default:
		s.verdict = "error"
	}
}

func errorsAs[T error](err error) bool {
	var t T
	return errors.As(err, &t)
}

func (s *stats) addBytes(n int64) { s.done.Add(n) }

func (s *stats) elapsed() float64 {
	e := time.Since(s.t0).Seconds()
	if e < 0.001 {
		return 0.001
	}
	return e
}

func (s *stats) mbps() float64 {
	return float64(s.done.Load()) / (1 << 20) / s.elapsed()
}

// progressLine is deliberately spelled with a trailing "<N> MiB/s" token so the
// legacy boot-script scrapers that regex a rate out of a stats file
// ([0-9.]+ [KMGT]?i?B/s) still find one when b2x wrote it. Backward
// compatibility with an unmigrated reader is cheaper than migrating the reader.
func (s *stats) progressLine() string {
	d := s.done.Load()
	pct := ""
	if s.totalBytes > 0 {
		pct = fmt.Sprintf(" %.0f%%", 100*float64(d+s.skippedBytes.Load())/float64(s.totalBytes))
	}
	return fmt.Sprintf("b2x: %s%s %s / %s, %.1f MiB/s, %.0fs elapsed, %d streams",
		s.Op, pct, humanBytes(d), humanBytes(s.totalBytes), s.mbps(), s.elapsed(), s.concurrency)
}

// startProgress emits a progress line every interval until stop() is called.
func (s *stats) startProgress(interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				fmt.Fprintln(os.Stderr, s.progressLine())
				if s.jsonOut {
					s.emitJSON("b2x_progress")
				}
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *stats) stop() { s.stopOnce.Do(func() { close(s.stopCh) }) }

func (s *stats) record() map[string]any {
	return map[string]any{
		"event":         "",
		"op":            s.Op,
		"src":           s.Src,
		"dst":           s.Dst,
		"bytes":         s.done.Load(),
		"secs":          round2(s.elapsed()),
		"mbps":          round2(s.mbps()),
		"objects":       s.objs.Load(),
		"skipped":       s.skippedObjs.Load(),
		"skipped_bytes": s.skippedBytes.Load(),
		"total_bytes":   s.totalBytes,
		"parts":         s.parts.Load(),
		"retries":       s.retries.Load(),
		"streams":       s.concurrency,
		"verdict":       s.verdict,
	}
}

func (s *stats) emitJSON(event string) {
	r := s.record()
	r["event"] = event
	b, err := json.Marshal(r)
	if err != nil {
		return
	}
	s.mu.Lock()
	fmt.Fprintln(os.Stdout, string(b))
	s.mu.Unlock()
}

// finish emits the terminal summary in all three shapes.
func (s *stats) finish(statsEnvPath string) {
	s.stop()
	if s.jsonOut {
		s.emitJSON("b2x_done")
	}
	// The verdict word is on the LAST line of stderr for any non-ok outcome, so
	// the tail of a stats file (which is all a boot-script beacon or a
	// `tail -c 200` diagnostic ever reads) carries the cause rather than the last
	// progress line.
	fmt.Fprintf(os.Stderr,
		"b2x: %s %s — %s in %.1fs (%.1f MiB/s), %d objects, %d skipped (%s already present), %d parts, %d retries\n",
		s.Op, map[bool]string{true: "done", false: "ENDED " + s.verdict}[s.verdict == "ok"],
		humanBytes(s.done.Load()), s.elapsed(), s.mbps(),
		s.objs.Load(), s.skippedObjs.Load(), humanBytes(s.skippedBytes.Load()),
		s.parts.Load(), s.retries.Load())

	if statsEnvPath == "" {
		return
	}
	// KEY=VALUE for `source`/`eval` in the shell call sites this replaced. Field
	// names match what those boot scripts already published for a pull, so a
	// scraper joining on them keeps working.
	body := fmt.Sprintf(
		"B2X_BYTES=%d\nB2X_SECS=%.1f\nB2X_MBPS=%.1f\nB2X_OBJECTS=%d\nB2X_SKIPPED=%d\nB2X_SKIPPED_BYTES=%d\nB2X_STREAMS=%d\nB2X_RETRIES=%d\nB2X_VERDICT=%s\n",
		s.done.Load(), s.elapsed(), s.mbps(), s.objs.Load(),
		s.skippedObjs.Load(), s.skippedBytes.Load(), s.concurrency,
		s.retries.Load(), s.verdict)
	if err := os.WriteFile(statsEnvPath, []byte(body), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "b2x: warning: could not write --stats-env %s: %v\n", statsEnvPath, err)
	}
}

func round2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

func humanBytes(n int64) string {
	const u = 1024
	if n < u {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(u), 0
	for m := n / u; m >= u && exp < 4; m /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTP"[exp])
}
