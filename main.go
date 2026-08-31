package main

// b2x — the ONE way artifacts move between Backblaze B2 and a rented GPU box.
//
// Why this exists (see also docs/DESIGN.md):
//   * Throughput on a rented box comes only from parallelism (hosts
//     traffic-shape PER TCP FLOW at ~4-16 MB/s), and the tuned rclone
//     convention was applied by hand at each call site — so most sites never
//     got it. There was no shared definition to reuse: the boot script defined
//     its flag set as a bash ARRAY, which cannot be exported to a child process.
//   * Even the tuned sites were capped by an rclone flag interaction no caller
//     set (--multi-thread-chunk-size); see plan.go for the measurement and the
//     structural fix.
//   * Weights were re-pulled on resume even when already on the box's disk.
//
// Usage is deliberately knob-free:
//   b2x pull  <b2-path> <local>   [--exclude G] [--include G] [--stats-env F] [--json]
//   b2x push  <local>  <b2-path>  [--min-age D] [--deadline D] ...
//   b2x cat   <b2-path>
//   b2x ls    <b2-path> [--json]
//   b2x stat  <b2-path>
//   b2x version | selftest
//
// b2-path may be given bare ("base-models/qwen3-14b") or in the rclone spelling
// the shell call sites this replaced already used
// ("b2:$B2_BUCKET/base-models/qwen3-14b"), so a migrating call site keeps its
// existing $B2 variable verbatim.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"
)

// version is stamped at build time: -ldflags "-X main.version=<v>"
var version = "dev"

// Exit codes — stable, documented, and distinct so a shell caller can branch.
const (
	exitOK        = 0
	exitUsage     = 2 // bad arguments or missing configuration
	exitAuth      = 3 // 401/403 — bad or out-of-scope credentials
	exitNotFound  = 4 // 404 — source object/prefix does not exist
	exitTransfer  = 5 // transfer failed after retries
	exitIntegrity = 6 // checksum mismatch
	exitDeadline  = 7 // wall-clock ceiling expired with work outstanding (partial)
	exitSlow      = 8 // sustained throughput under the floor — bad HOST, not bad transfer
)

func main() {
	os.Exit(run())
}

func usage() {
	fmt.Fprint(os.Stderr, `b2x `+version+` — B2 <-> vast box artifact transfer

  b2x pull <b2-path> <local-path>    idempotent download (skips what is present, resumes partials)
  b2x push <local-path> <b2-path>    multipart upload (newest-first, deadline-aware)
  b2x cat  <b2-path>                 stream one object to stdout
  b2x ls   <b2-path>                 list objects under a prefix
  b2x stat <b2-path>                 size/etag of one object
  b2x version                        print version
  b2x selftest                       verify credentials + a round-trip under a scratch prefix

Common flags:
  --include GLOB     only paths matching (repeatable)
  --exclude GLOB     skip paths matching (repeatable)
  --stats-env FILE   write sourceable B2X_BYTES / B2X_SECS / B2X_MBPS / ...
  --json             emit machine-readable progress + a final b2x_done record on stdout
  --dry-run          print the transfer plan and exit
  --deadline DUR     hard time budget (e.g. 40s); push completes newest files first
  --min-age DUR      (push) skip files modified more recently than this
  --verify           (pull) fail unless every object's b2x-sha256 metadata matches

Concurrency is COMPUTED from object size and CPU count — there is no
--transfers/--streams flag by design. B2X_CONCURRENCY overrides for debugging.

Environment: B2_BUCKET, B2_KEY_ID, B2_APPLICATION_KEY, B2_S3_ENDPOINT, B2_REGION.
Writes prefer the scoped B2_WRITE_KEY_ID / B2_WRITE_APPLICATION_KEY when present.

Transfer guards (guard.go) — every one is ON by default and disables at 0:
  B2X_STALL_S=120             no bytes on ONE part for this long -> retry the part
  B2X_PART_TRIES=3            attempts per stalled part before the transfer fails
  B2X_MIN_MBPS=3              aggregate throughput floor (also derives the ceiling)
  B2X_MIN_MBPS_WINDOW_S=300   the floor must be under water for a FULL window
  B2X_DEADLINE_SLACK_S=300    non-byte-bound allowance in the derived ceiling
  B2X_MIN_DEADLINE_S=900      the derived ceiling is never shorter than this
Without an explicit --deadline the wall-clock ceiling is derived from the bytes
to move: bytes / B2X_MIN_MBPS + slack, floored at B2X_MIN_DEADLINE_S.

Exit codes: 0 ok · 2 usage/config · 3 auth · 4 not-found · 5 transfer · 6 integrity
            7 deadline (ceiling expired) · 8 slow (under the throughput floor)
`)
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(s string) error { *m = append(*m, s); return nil }

func run() int {
	if len(os.Args) < 2 {
		usage()
		return exitUsage
	}
	cmd := os.Args[1]
	switch cmd {
	case "-h", "--help", "help":
		usage()
		return exitOK
	case "version", "--version":
		fmt.Println(version)
		return exitOK
	}

	fs := flag.NewFlagSet("b2x "+cmd, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var includes, excludes multiFlag
	fs.Var(&includes, "include", "only paths matching glob (repeatable)")
	fs.Var(&excludes, "exclude", "skip paths matching glob (repeatable)")
	statsEnv := fs.String("stats-env", "", "write sourceable KEY=VALUE stats here")
	jsonOut := fs.Bool("json", false, "machine-readable events on stdout")
	dryRun := fs.Bool("dry-run", false, "print the plan and exit")
	verify := fs.Bool("verify", false, "pull: require b2x-sha256 metadata to match")
	deadline := fs.Duration("deadline", 0, "hard time budget")
	minAge := fs.Duration("min-age", 0, "push: skip files newer than this")
	if err := fs.Parse(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "b2x: %v\n\n", err)
		usage()
		return exitUsage
	}
	args := fs.Args()

	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "b2x: %v\n", err)
		return exitUsage
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "pull":
		if len(args) != 2 {
			usage()
			return exitUsage
		}
		src := cfg.normKey(args[0])
		c := newS3Client(cfg.endpoint, cfg.bucket, cfg.readCred, cfg.concurrency)
		st := newStats("pull", src, args[1], *jsonOut, cfg.concurrency)
		err := runPull(ctx, c, cfg, src, args[1], pullOpts{
			includes: includes, excludes: excludes,
			deadline: *deadline, dryRun: *dryRun, verify: *verify,
		}, st)
		if !*dryRun {
			st.setVerdict(err)
			st.finish(*statsEnv)
		}
		return report(err, cfg)

	case "push":
		if len(args) != 2 {
			usage()
			return exitUsage
		}
		dst := cfg.normKey(args[1])
		// ONE static selection by destination prefix. There is deliberately no
		// retry with another credential: a 403 here means the launcher's scope
		// decision and this destination disagree, and silently re-presenting a
		// wider key would turn a least-privilege boundary into a suggestion.
		cred, credName := cfg.pushCredFor(dst)
		cfg.usedGrant = strings.SplitN(credName, "(", 2)[0]
		fmt.Fprintf(os.Stderr, "b2x: push %s using the %s grant\n", dst, credName)
		c := newS3Client(cfg.endpoint, cfg.bucket, cred, cfg.concurrency)
		st := newStats("push", args[0], dst, *jsonOut, cfg.concurrency)
		err := runPush(ctx, c, cfg, args[0], dst, pushOpts{
			includes: includes, excludes: excludes,
			minAge: *minAge, deadline: *deadline, dryRun: *dryRun,
		}, st)
		if !*dryRun {
			st.setVerdict(err)
			st.finish(*statsEnv)
		}
		return report(err, cfg)

	case "cat":
		if len(args) != 1 {
			usage()
			return exitUsage
		}
		c := newS3Client(cfg.endpoint, cfg.bucket, cfg.readCred, 4)
		body, err := c.getRange(ctx, cfg.normKey(args[0]), 0, 0)
		if err != nil {
			return report(err, cfg)
		}
		defer body.Close()
		if _, err := io.Copy(os.Stdout, body); err != nil {
			return report(err, cfg)
		}
		return exitOK

	case "ls":
		if len(args) != 1 {
			usage()
			return exitUsage
		}
		c := newS3Client(cfg.endpoint, cfg.bucket, cfg.readCred, 4)
		prefix := cfg.normKey(args[0])
		objs, err := c.list(ctx, prefix)
		if err != nil {
			return report(err, cfg)
		}
		var total int64
		for _, o := range objs {
			total += o.Size
			if *jsonOut {
				fmt.Printf("{\"key\":%q,\"size\":%d,\"etag\":%q}\n", o.Key, o.Size, o.ETag)
			} else {
				fmt.Printf("%12d %s\n", o.Size, o.Key)
			}
		}
		fmt.Fprintf(os.Stderr, "b2x: %d objects, %s\n", len(objs), humanBytes(total))
		return exitOK

	case "stat":
		if len(args) != 1 {
			usage()
			return exitUsage
		}
		c := newS3Client(cfg.endpoint, cfg.bucket, cfg.readCred, 4)
		ob, meta, err := c.head(ctx, cfg.normKey(args[0]))
		if err != nil {
			return report(err, cfg)
		}
		p := planParts(ob.Size)
		fmt.Printf("key       %s\nsize      %d (%s)\netag      %s\nmodified  %s\nplan      %d parts x %s\n",
			ob.Key, ob.Size, humanBytes(ob.Size), ob.ETag, ob.LastModified.Format(time.RFC3339),
			p.NParts, humanBytes(p.PartSize))
		for k, v := range meta {
			fmt.Printf("meta.%-4s %s\n", k, v)
		}
		return exitOK

	case "selftest":
		return selftest(ctx, cfg)

	default:
		fmt.Fprintf(os.Stderr, "b2x: unknown command %q\n\n", cmd)
		usage()
		return exitUsage
	}
}

// report maps an error to a documented exit code and prints a legible,
// secret-free message.
func report(err error, cfg *config) int {
	if err == nil {
		return exitOK
	}
	var ie *integrityError
	if errors.As(err, &ie) {
		fmt.Fprintf(os.Stderr, "b2x: %v\n", err)
		return exitIntegrity
	}
	// A slow HOST and an expired ceiling are different findings with different
	// remedies (re-rent elsewhere vs. give it more budget), so they get different
	// exit codes and different words. Checked before DeadlineExceeded because a
	// floor breach cancels the same context a ceiling does.
	var se *slowError
	if errors.As(err, &se) {
		fmt.Fprintf(os.Stderr, "b2x: TIMEOUT/SLOW: %v\n"+
			"     the transfer was moving, just far under the floor — treat as a HOST verdict.\n"+
			"     override with B2X_MIN_MBPS=<mbps> (0 disables the floor and the derived ceiling).\n", err)
		return exitSlow
	}
	if errors.Is(err, context.DeadlineExceeded) {
		fmt.Fprintf(os.Stderr, "b2x: TIMEOUT: %v\n"+
			"     override with --deadline, or B2X_MIN_DEADLINE_S / B2X_DEADLINE_SLACK_S for the derived ceiling.\n", err)
		return exitDeadline
	}
	var ste *stallError
	if errors.As(err, &ste) {
		fmt.Fprintf(os.Stderr, "b2x: STALLED: %v (exhausted B2X_PART_TRIES)\n", err)
		return exitTransfer
	}
	switch statusOf(err) {
	case 401, 403:
		// Name the grant that was ACTUALLY used. A box carries three, and a
		// hint pointing at B2_WRITE_* when the push presented B2_PUBLISH_*
		// sends the reader to re-scope the wrong key.
		hint := "check B2_KEY_ID / B2_APPLICATION_KEY"
		switch {
		case cfg != nil && cfg.usedGrant == "publish":
			hint = "the publish key is namePrefix-scoped to " + publishPrefix +
				" — a write outside it returns 403 by design"
		case cfg != nil && cfg.usedGrant == "write" && cfg.writeScoped:
			hint = "the write key is namePrefix-scoped — a write outside its prefix returns 403 by design"
		case cfg != nil && cfg.writeScoped:
			hint = "the write key is namePrefix-scoped — a write outside its prefix returns 403 by design"
		}
		fmt.Fprintf(os.Stderr, "b2x: access denied: %v\n     %s\n", err, hint)
		return exitAuth
	case 404:
		fmt.Fprintf(os.Stderr, "b2x: not found: %v\n", err)
		return exitNotFound
	}
	fmt.Fprintf(os.Stderr, "b2x: transfer failed: %v\n", err)
	return exitTransfer
}

// matchFilters applies --include/--exclude to a relative path. Globs match
// either the full relative path or the basename, and a trailing "/**" matches
// everything under a directory — the shapes the rclone call sites this replaced
// use (e.g. --exclude 'checkpoint-*/**', --exclude STATUS). A LEADING "/"
// anchors the pattern at the transfer root (rclone semantics), which is what an
// adapter-publish site spells as --include "/adapter_model.safetensors".
func matchFilters(rel string, includes, excludes []string) bool {
	for _, g := range excludes {
		if globMatch(g, rel) {
			return false
		}
	}
	if len(includes) == 0 {
		return true
	}
	for _, g := range includes {
		if globMatch(g, rel) {
			return true
		}
	}
	return false
}

// globMatch answers "does this rclone filter pattern select this root-relative
// path". `rel` is always relative to the TRANSFER ROOT (push walks with
// filepath.Rel, pull trims the source prefix), so "anchored at the root" is
// simply "match rel whole".
//
// The leading-"/" rule is NOT a bare strip. Unanchored patterns fall back to
// matching the BASENAME, so a strip alone would make "/adapter_config.json"
// keep matching "checkpoint-40/adapter_config.json" — the precise widening a
// publish site uses root anchoring to prevent. Anchoring therefore both strips
// the "/" AND withdraws the basename fallback.
func globMatch(pattern, rel string) bool {
	anchored := strings.HasPrefix(pattern, "/")
	if anchored {
		pattern = pattern[1:]
	}
	if strings.HasSuffix(pattern, "/**") {
		base := strings.TrimSuffix(pattern, "/**")
		for cur := rel; ; {
			i := strings.LastIndex(cur, "/")
			if i < 0 {
				break
			}
			cur = cur[:i]
			if ok, _ := path.Match(base, cur); ok {
				return true
			}
		}
		if ok, _ := path.Match(base, rel); ok {
			return true
		}
		return false
	}
	if strings.Contains(pattern, "**") {
		// "**" as a path-crossing wildcard: reduce to a substring-ish match by
		// matching each side.
		parts := strings.SplitN(pattern, "**", 2)
		return strings.HasPrefix(rel, strings.TrimSuffix(parts[0], "/")) &&
			strings.HasSuffix(rel, strings.TrimPrefix(parts[1], "/"))
	}
	if ok, _ := path.Match(pattern, rel); ok {
		return true
	}
	if anchored {
		return false
	}
	if ok, _ := path.Match(pattern, path.Base(rel)); ok {
		return true
	}
	return false
}
