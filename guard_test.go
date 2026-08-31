package main

// guard_test.go — the three transfer guards (guard.go).
//
// What these pin is not "a timeout exists" but the properties that make one
// safe to leave armed on a billed box: a stall RETRIES rather than failing, the
// throughput floor never condemns before a FULL window, a legitimately fast
// transfer is never touched, and every abort is DISTINGUISHABLE from every other
// abort and from success at the exit code, the verdict word, and the stats file.
//
// No network, no credentials, no B2 — same house style as transfer_test.go.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// --- a server that can go silent mid-stream ----------------------------------

// stallS3 serves exactly one object. `stallsLeft` GETs write one byte, flush,
// and then hang until the client gives up; every GET after that serves the whole
// body. That is the shape the guard exists for: the peer completed the
// handshake, answered 200 with a Content-Length, and then stopped — which is why
// no HTTP-level error is ever produced and why a bare io.Copy waits forever.
type stallS3 struct {
	key        string
	data       []byte
	stallsLeft atomic.Int64
	gets       atomic.Int64
}

func (s *stallS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.Itoa(len(s.data)))
		w.Header().Set("ETag", `"deadbeef"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(200)
		return
	}
	if r.Method != "GET" {
		w.WriteHeader(405)
		return
	}
	s.gets.Add(1)
	lo, hi := 0, len(s.data)-1
	if rg := r.Header.Get("Range"); strings.HasPrefix(rg, "bytes=") {
		parts := strings.SplitN(strings.TrimPrefix(rg, "bytes="), "-", 2)
		lo, _ = strconv.Atoi(parts[0])
		if len(parts) > 1 && parts[1] != "" {
			hi, _ = strconv.Atoi(parts[1])
		}
	}
	body := s.data[lo : hi+1]
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(206)

	if s.stallsLeft.Add(-1) >= 0 {
		w.Write(body[:1])
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		<-r.Context().Done() // silence until the client's stall guard cancels
		return
	}
	w.Write(body)
}

func stallHarness(t *testing.T, size int, stalls int64) (*stallS3, *s3Client, *config) {
	t.Helper()
	s := &stallS3{key: "eval-env/env.tar.zst", data: blob(size, 7)}
	s.stallsLeft.Store(stalls)
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	cred := credentials{keyID: "k", secret: "s", region: "us-west-004"}
	cfg := &config{bucket: "testbucket", endpoint: srv.URL, region: "us-west-004",
		concurrency: 4, readCred: cred, writeCred: cred,
		stallTimeout: 200 * time.Millisecond, partTries: 3}
	return s, newS3Client(srv.URL, "testbucket", cred, 4), cfg
}

// --- 1. stall ----------------------------------------------------------------

func TestStalledPartIsRetriedAndSucceeds(t *testing.T) {
	// THE load-bearing property. One flow going silent on a flaky host must cost
	// a re-requested range, not the transfer — otherwise arming the guard would
	// turn recoverable flakiness into re-rented boxes, and it would rightly get
	// switched off.
	s, c, cfg := stallHarness(t, 2<<20, 1) // 2 MiB -> 1 part; stall the first GET
	dst := filepath.Join(t.TempDir(), "env.tar.zst")
	st := newStats("pull", "", dst, false, cfg.concurrency)

	if err := runPull(context.Background(), c, cfg, s.key, dst, pullOpts{}, st); err != nil {
		t.Fatalf("a recoverable stall must not fail the pull: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil || len(got) != len(s.data) {
		t.Fatalf("content wrong after retry (err=%v, %d bytes)", err, len(got))
	}
	if st.retries.Load() != 1 {
		t.Errorf("retries = %d, want 1", st.retries.Load())
	}
	if s.gets.Load() != 2 {
		t.Errorf("GETs = %d, want 2 (the stalled one + the retry)", s.gets.Load())
	}
	// And the byte counter must not have kept the stalled attempt's byte —
	// double-counting there would make a flapping flow look FAST to the floor.
	if st.done.Load() != int64(len(s.data)) {
		t.Errorf("counted %d bytes, want exactly %d", st.done.Load(), len(s.data))
	}
}

func TestPersistentStallFailsNamedNotHung(t *testing.T) {
	s, c, cfg := stallHarness(t, 2<<20, 99) // never recovers
	dst := filepath.Join(t.TempDir(), "env.tar.zst")
	st := newStats("pull", "", dst, false, cfg.concurrency)

	done := make(chan error, 1)
	go func() {
		done <- runPull(context.Background(), c, cfg, s.key, dst, pullOpts{}, st)
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("pull HUNG on a silent peer — the guard did not fire")
	}
	if err == nil {
		t.Fatal("want an error")
	}
	var se *stallError
	if !errors.As(err, &se) {
		t.Fatalf("want a NAMED stallError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "B2X_STALL_S") {
		t.Errorf("the diagnostic must name the knob that governs it: %v", err)
	}
	// A stall is a transfer failure, NOT a slow-host verdict and NOT a timeout:
	// the three have different remedies and must not share an exit code.
	if got := report(err, cfg); got != exitTransfer {
		t.Errorf("exit %d, want %d (transfer)", got, exitTransfer)
	}
	st.setVerdict(err)
	if st.verdict != "stalled" {
		t.Errorf("verdict %q, want \"stalled\"", st.verdict)
	}
}

func TestStallGuardDisabledAtZero(t *testing.T) {
	// A guard that cannot be switched off in the field is a guard that gets
	// deleted. 0 must mean "never fire", not "fire immediately".
	fired := atomic.Bool{}
	g := newStallGuard(0, func() { fired.Store(true) })
	defer g.stop()
	g.tick()
	time.Sleep(120 * time.Millisecond)
	if fired.Load() || g.fired.Load() {
		t.Fatal("a 0 stall timeout must disarm the guard")
	}
	if err := g.classify(fmt.Errorf("boom"), "x", 0); err.Error() != "boom" {
		t.Errorf("a disarmed guard must not relabel errors: %v", err)
	}
}

// --- 2. throughput floor ------------------------------------------------------

func TestFloorCondemnsOnlyAfterAFullWindow(t *testing.T) {
	st := newStats("pull", "s", "d", false, 4)
	var cancelled atomic.Bool
	w := startFloorWatch(st, 100.0 /* MB/s — unreachable */, 400*time.Millisecond,
		func() { cancelled.Store(true) })
	defer w.halt()

	// Before a full window has elapsed there is not enough evidence to condemn,
	// and condemning early is how a guard earns a reputation for false kills.
	time.Sleep(150 * time.Millisecond)
	if w.slow.Load() {
		t.Fatal("condemned before a full window elapsed")
	}
	for i := 0; i < 20 && !w.slow.Load(); i++ {
		time.Sleep(100 * time.Millisecond)
	}
	if !w.slow.Load() || !cancelled.Load() {
		t.Fatal("a transfer moving no bytes must be condemned once a window is full")
	}

	err := w.err("pull", st.done.Load())
	if err == nil {
		t.Fatal("want a named verdict")
	}
	var se *slowError
	if !errors.As(err, &se) {
		t.Fatalf("want slowError, got %T", err)
	}
	if !strings.Contains(err.Error(), "B2X_MIN_MBPS") {
		t.Errorf("the diagnostic must name its knob: %v", err)
	}
	// Distinct exit code and distinct verdict word from a timeout — that is what
	// lets a shell caller (and a human reading the boot log) tell a BAD HOST from
	// a budget that was simply too small.
	if got := report(err, cfg0()); got != exitSlow {
		t.Errorf("exit %d, want %d (slow)", got, exitSlow)
	}
	st.setVerdict(err)
	if st.verdict != "slow" {
		t.Errorf("verdict %q, want \"slow\"", st.verdict)
	}
}

func TestFloorNeverCondemnsAHealthyTransfer(t *testing.T) {
	st := newStats("pull", "s", "d", false, 4)
	// 1 MiB per 20 ms ~= 50 MB/s, against a 3 MB/s floor: the real-world margin
	// is wider still (the SLOWEST arm ever measured on a rented box was 58 MB/s;
	// measured on the fleet this grew in, see docs/DESIGN.md §5b).
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				st.addBytes(1 << 20)
			}
		}
	}()
	defer close(stop)

	w := startFloorWatch(st, defaultMinMBps, 300*time.Millisecond, func() {
		t.Error("condemned a transfer running ~16x over the floor")
	})
	defer w.halt()
	time.Sleep(1200 * time.Millisecond)
	if w.slow.Load() {
		t.Fatal("false positive on a healthy transfer")
	}
	if w.err("pull", 0) != nil {
		t.Fatal("no verdict expected")
	}
}

func TestFloorDisabledAtZero(t *testing.T) {
	st := newStats("pull", "s", "d", false, 4)
	w := startFloorWatch(st, 0, 300*time.Millisecond, func() {
		t.Error("a 0 floor must not condemn anything")
	})
	defer w.halt()
	time.Sleep(500 * time.Millisecond)
	if w.slow.Load() || w.err("pull", 0) != nil {
		t.Fatal("B2X_MIN_MBPS=0 must fully disarm the floor")
	}
}

// --- 3. derived wall-clock ceiling --------------------------------------------

func TestAutoDeadlineIsDerivedNotConstant(t *testing.T) {
	cfg := &config{minMBps: defaultMinMBps,
		deadlineSlack: defaultDeadlineSlackS * time.Second,
		minDeadline:   defaultMinDeadlineS * time.Second}

	// Small transfers get the floor, not a two-second budget.
	if d := autoDeadline(6<<20, cfg); d != defaultMinDeadlineS*time.Second {
		t.Errorf("6 MiB ceiling = %s, want the %ds minimum", d, defaultMinDeadlineS)
	}
	// A 22 GB monolith at the 3 MB/s floor: ~2h05m, comfortably above any real
	// pull (the same object moved at 341 MB/s on the fat-host ceiling test).
	big := autoDeadline(22<<30, cfg)
	if big < 2*time.Hour || big > 3*time.Hour {
		t.Errorf("22 GB ceiling = %s, want ~2h05m", big)
	}
	// THE safety property: a transfer that merely sustains the floor must finish
	// strictly INSIDE its ceiling, so the ceiling can only fire where the floor
	// already should have. Anything else makes it an independent false-kill source.
	for _, n := range []int64{1 << 30, 10 << 30, 22 << 30, 100 << 30} {
		atFloor := time.Duration(float64(n) / (cfg.minMBps * (1 << 20)) * float64(time.Second))
		if d := autoDeadline(n, cfg); d <= atFloor {
			t.Errorf("%s: ceiling %s <= floor-rate runtime %s", humanBytes(n), d, atFloor)
		}
	}
	// Disabling the floor disables the derived ceiling with it: with no floor
	// rate there is no defensible number to derive one from.
	if d := autoDeadline(22<<30, &config{minMBps: 0}); d != 0 {
		t.Errorf("B2X_MIN_MBPS=0 must disable the derived ceiling, got %s", d)
	}
}

func TestDeadlineAndSlowAreDistinguishable(t *testing.T) {
	// The whole point of the exercise: a caller must be able to tell a timeout
	// from a slow host from a plain failure from success. Four inputs, four
	// distinct (exit code, verdict) pairs.
	cases := []struct {
		err     error
		exit    int
		verdict string
	}{
		{nil, exitOK, "ok"},
		{context.DeadlineExceeded, exitDeadline, "timeout"},
		{&slowError{Op: "pull", MBps: 0.4, Floor: 3, Window: 300 * time.Second}, exitSlow, "slow"},
		{&stallError{Rel: "a", Part: 1, After: time.Minute}, exitTransfer, "stalled"},
		{&httpError{Status: 403, Code: "InvalidAccessKeyId"}, exitAuth, "error"},
	}
	for _, tc := range cases {
		if got := report(tc.err, cfg0()); got != tc.exit {
			t.Errorf("%v: exit %d, want %d", tc.err, got, tc.exit)
		}
		st := newStats("pull", "", "", false, 1)
		st.setVerdict(tc.err)
		if st.verdict != tc.verdict {
			t.Errorf("%v: verdict %q, want %q", tc.err, st.verdict, tc.verdict)
		}
	}
}

func TestStatsEnvCarriesTheVerdict(t *testing.T) {
	// --stats-env is how the shell learns the outcome without parsing prose, and
	// the boot/train scripts this replaced already source that file. A timeout
	// that reaches the shell as an indistinguishable non-zero exit is the failure
	// mode we are removing.
	p := filepath.Join(t.TempDir(), "stats.env")
	st := newStats("pull", "src", "dst", false, 8)
	st.addBytes(1 << 20)
	st.setVerdict(&slowError{Op: "pull", MBps: 0.4, Floor: 3, Window: 300 * time.Second})
	st.finish(p)

	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "B2X_VERDICT=slow") {
		t.Errorf("stats-env missing the verdict:\n%s", b)
	}
	if !strings.Contains(string(b), "B2X_RETRIES=") {
		t.Errorf("stats-env missing the retry count:\n%s", b)
	}
}

func TestMalformedKnobFallsBackToTheDefaultNotToOff(t *testing.T) {
	// A typo in an env var must never silently disarm a guard — that is how a
	// safety net disappears without anyone deciding to remove it.
	t.Setenv("B2X_MIN_MBPS", "three")
	if got := envFloat("B2X_MIN_MBPS", defaultMinMBps); got != defaultMinMBps {
		t.Errorf("malformed knob -> %v, want the default %v", got, defaultMinMBps)
	}
	// 0 is a MEANINGFUL value and must survive.
	t.Setenv("B2X_STALL_S", "0")
	if got := envSeconds("B2X_STALL_S", defaultStallS); got != 0 {
		t.Errorf("explicit 0 -> %v, want 0 (disabled)", got)
	}
	// A negative is nonsense, not a disable: fall back rather than invert.
	t.Setenv("B2X_STALL_S", "-5")
	if got := envSeconds("B2X_STALL_S", defaultStallS); got != defaultStallS*time.Second {
		t.Errorf("negative knob -> %v, want the default", got)
	}
}

func cfg0() *config { return &config{bucket: "b"} }
