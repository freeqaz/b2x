package main

// guard.go — the box-side answer to "the transfer is not moving".
//
// WHY THIS FILE EXISTS (owner ruling 2026-08-02). Stall DETECTION for a box is a
// control-plane function: a wedged box is exactly the one that will not answer a
// probe, so anything judging box HEALTH must be derivable from what the
// workstation observes (vast API samples, fleetd's journal, spend). That ruling
// carved out ONE corollary as its own scope, and this is it:
//
//   "pulling from b2 on the box is another place where we need to have timeouts
//    set to observe for slow, flaky box networking on hosts."
//
// This is NOT stall detection for the box. It is the much narrower thing a
// transfer must do for itself: bound its own runtime so a slow or flaky host
// surfaces as a NAMED, distinguishable failure instead of a silent hang that
// burns the boot deadline and tells nobody why.
//
// Before this file, b2x had NO time bound of any kind on a pull:
//
//   * http.Client carried a Dialer/TLS timeout but no Client.Timeout, and none
//     is possible here anyway — a legitimate 176 MiB part read may take minutes.
//   * copyToOffset looped on body.Read() forever. A peer that completes the TCP
//     handshake, returns 200 with a Content-Length, and then stops sending
//     (a shaped host under a retransmit storm, a half-open NAT mapping) parks
//     that goroutine for the life of the process. No error, no log line, no exit.
//   * --deadline existed but only the eviction-trap flush ever
//     passed it. Every PULL call site passed none, so ctx had no deadline at all.
//
// Note the irony this fixes: rclone — the fallback b2x replaced on the hot paths
// — defaults to `--timeout 5m` (IO idle) and `--contimeout 1m`, so the OLD code
// was bounded and the NEW transport was not. The three guards below restore and
// tighten that property, with the two signals the owner asked for on the
// docker-pull side (herdd `_job_pull_watchdog_tick`): a wall-clock ceiling and
// a bytes-per-second floor.
//
// THE THREE GUARDS, and why each one has to exist separately:
//
//  1. STALL (per part, B2X_STALL_S). One flow of 128 goes silent while the other
//     127 are healthy. The aggregate rate still looks fine, so the floor never
//     fires; the transfer simply never finishes because one part never lands.
//     This is also the only RECOVERABLE case — a ranged GET restarts at an exact
//     offset — so a stall retries the part rather than failing the transfer.
//
//  2. THROUGHPUT FLOOR (aggregate, B2X_MIN_MBPS over B2X_MIN_MBPS_WINDOW_S).
//     Everything is moving, just far too slowly to be worth the billed box. The
//     fast kill: a verdict inside one window instead of at the wall clock.
//
//  3. WALL-CLOCK CEILING (auto-derived). The backstop for the pathological shape
//     the windowed floor can miss: alternating stall / burst that keeps every
//     window average just above the floor forever. Derived from bytes-to-move
//     and the floor rate rather than being a constant — see autoDeadline.
//
// All three are ON by default and every one is disable-able by setting its knob
// to 0, because a guard that cannot be switched off in the field is a guard that
// gets removed in a panic.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// --- 1. stall: no bytes on THIS part for stallTimeout -------------------------

// stallGuard cancels a per-part context when no progress has been reported for
// d. The caller ticks it after every successful read; the timer only ever fires
// when the flow has genuinely gone silent.
//
// d is a per-READ idle bound, NOT a per-part budget: it is compared against the
// gap between consecutive 1 MiB reads, so it does not scale with part size and
// cannot false-positive on a big object.
type stallGuard struct {
	d        time.Duration
	cancel   context.CancelFunc
	fired    atomic.Bool
	beat     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

func newStallGuard(d time.Duration, cancel context.CancelFunc) *stallGuard {
	g := &stallGuard{d: d, cancel: cancel,
		beat: make(chan struct{}, 1), done: make(chan struct{})}
	if d <= 0 {
		return g // disabled — no goroutine, tick() is a no-op
	}
	go func() {
		t := time.NewTimer(d)
		defer t.Stop()
		for {
			select {
			case <-g.beat:
				if !t.Stop() {
					select {
					case <-t.C:
					default:
					}
				}
				t.Reset(d)
			case <-t.C:
				g.fired.Store(true)
				g.cancel()
				return
			case <-g.done:
				return
			}
		}
	}()
	return g
}

// tick reports progress. Non-blocking by construction: a full beat channel
// already means "a reset is pending", so dropping the signal is correct and the
// read path never waits on the watchdog.
func (g *stallGuard) tick() {
	if g.d <= 0 {
		return
	}
	select {
	case g.beat <- struct{}{}:
	default:
	}
}

func (g *stallGuard) stop() { g.stopOnce.Do(func() { close(g.done) }) }

// classify renames the generic "context canceled" that OUR OWN cancel produced
// into a stall the log can name. Without this the operator sees the same
// "context canceled" a Ctrl-C produces and learns nothing.
func (g *stallGuard) classify(err error, rel string, part int) error {
	if g.fired.Load() {
		return &stallError{Rel: rel, Part: part, After: g.d}
	}
	return err
}

type stallError struct {
	Rel   string
	Part  int
	After time.Duration
}

func (e *stallError) Error() string {
	return fmt.Sprintf("stalled: %s part %d received no bytes for %s (B2X_STALL_S)",
		e.Rel, e.Part, e.After)
}

// --- 2. throughput floor: aggregate rate under minMBps for a FULL window ------

// floorWatch samples the transferred-byte counter and condemns when the rate
// across a full window sits under the floor.
//
// Deliberately mirrors the control-plane docker-pull watchdog
// (herdd `_job_pull_watchdog_tick`, BOOT_MIN_MBPS over BOOT_MBPS_WINDOW_S):
// AGGREGATE bytes over a FULL window, never an instantaneous sample. Vast hosts
// shape per TCP flow, so a single slow flow with a healthy aggregate must never
// condemn — the same reason the boot-side knob is documented as aggregate.
//
// It arms only while parts are actually in flight, so the LIST pass, the sha256
// --verify pass, and an all-skipped idempotent re-pull (all of which legitimately
// move zero bytes) can never trip it.
type floorWatch struct {
	slow     atomic.Bool
	mbps     atomic.Uint64 // observed windowed rate * 1000, for the diagnostic
	stop     chan struct{}
	stopOnce sync.Once
	floor    float64
	window   time.Duration
}

type sample struct {
	t time.Time
	b int64
}

// startFloorWatch begins sampling st. Returns a *floorWatch whose slow flag the
// caller must consult after the transfer loop — the cancel it fires surfaces as
// a generic context.Canceled otherwise, which is exactly the anonymous failure
// this whole file exists to stop.
func startFloorWatch(st *stats, floor float64, window time.Duration, cancel context.CancelFunc) *floorWatch {
	w := &floorWatch{stop: make(chan struct{}), floor: floor, window: window}
	if floor <= 0 || window <= 0 {
		return w // disabled
	}
	// Six samples per window: enough resolution to compute a rate across very
	// nearly a full window, cheap enough to be free (one atomic load per tick).
	// Floor of 50ms only so a test can use a sub-second window; in production the
	// window is 300 s, which makes this a 50 s tick — one atomic load per tick.
	step := window / 6
	if step < 50*time.Millisecond {
		step = 50 * time.Millisecond
	}
	go func() {
		t := time.NewTicker(step)
		defer t.Stop()
		hist := []sample{{t: time.Now(), b: st.done.Load()}}
		for {
			select {
			case <-t.C:
				now := time.Now()
				hist = append(hist, sample{t: now, b: st.done.Load()})
				// Drop samples older than the window, but KEEP the newest such
				// sample as the window's left edge — dropping it too would
				// shorten the measured span and make the verdict noisier.
				cut := 0
				for i, s := range hist {
					if now.Sub(s.t) >= window {
						cut = i
					}
				}
				hist = hist[cut:]
				span := now.Sub(hist[0].t)
				if span < window {
					continue // not a FULL window yet — never condemn early
				}
				moved := st.done.Load() - hist[0].b
				rate := float64(moved) / (1 << 20) / span.Seconds()
				w.mbps.Store(uint64(rate * 1000))
				if rate < floor {
					w.slow.Store(true)
					cancel()
					return
				}
			case <-w.stop:
				return
			}
		}
	}()
	return w
}

func (w *floorWatch) halt() { w.stopOnce.Do(func() { close(w.stop) }) }

// err returns the named verdict, or nil when the floor was never breached.
func (w *floorWatch) err(op string, moved int64) error {
	if !w.slow.Load() {
		return nil
	}
	return &slowError{Op: op, MBps: float64(w.mbps.Load()) / 1000,
		Floor: w.floor, Window: w.window, Bytes: moved}
}

type slowError struct {
	Op     string
	MBps   float64
	Floor  float64
	Window time.Duration
	Bytes  int64
}

func (e *slowError) Error() string {
	return fmt.Sprintf(
		"slow host: %s sustained %.2f MB/s over %s, under the %.2f MB/s floor (B2X_MIN_MBPS); %s moved",
		e.Op, e.MBps, e.Window, e.Floor, humanBytes(e.Bytes))
}

// --- 3. wall-clock ceiling, derived rather than constant ----------------------

// autoDeadline is the ceiling a transfer gets when the caller passed no explicit
// --deadline.
//
// A CONSTANT ceiling cannot work here: the same binary moves a 6 MB bundle and a
// 22 GB monolith, and any constant generous enough for the second is useless
// against the first. So the ceiling is the floor rate integrated over the bytes
// actually being moved, plus a fixed slack for the parts of a transfer that are
// not byte-bound (LIST, HEAD, preallocation, fsync, the sha256 verify pass).
//
// The consequence — and the reason this is safe to have on by default — is that
// it can only fire when the throughput floor ALREADY should have: a transfer
// that sustains exactly the floor finishes at bytes/floor, strictly inside
// bytes/floor + slack. Its job is the one shape the windowed floor misses,
// where every individual window averages just above the floor but the transfer
// never converges.
//
// Setting B2X_MIN_MBPS=0 disables the floor AND this ceiling together, on
// purpose: with no floor rate there is no defensible number to derive.
func autoDeadline(bytes int64, cfg *config) time.Duration {
	if cfg.minMBps <= 0 || bytes <= 0 {
		return 0
	}
	need := time.Duration(float64(bytes) / (cfg.minMBps * (1 << 20)) * float64(time.Second))
	d := need + cfg.deadlineSlack
	if d < cfg.minDeadline {
		d = cfg.minDeadline
	}
	return d
}
