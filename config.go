package main

// config.go — credentials and endpoint resolution.
//
// Credentials come from the environment, never a config file:
//   B2_KEY_ID / B2_APPLICATION_KEY               bucket-wide READ key
//   B2_WRITE_KEY_ID / B2_WRITE_APPLICATION_KEY   prefix-scoped WRITE key (optional)
//   B2_BUCKET / B2_S3_ENDPOINT / B2_REGION
//
// This mirrors the conventional [b2] / [b2w] rclone remote split: reads use the
// bucket-wide key, writes prefer the scoped key and degrade to the read key
// when no B2_WRITE_* is present, so a box with one key behaves as it always did.
//
// SECRETS: config values are never printed. String() on this type is
// deliberately not defined so a %v of a config can't leak a key.

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	bucket    string
	endpoint  string
	region    string
	readCred  credentials
	writeCred credentials
	// writeScoped is true when a distinct B2_WRITE_* pair was supplied. Used
	// only for diagnostics ("this key is prefix-scoped, a 403 may be expected").
	writeScoped bool
	// publishCred is the checkpoints/ grant (B2_PUBLISH_*, the box's [b2p]
	// rclone remote). It exists as a THIRD key because a B2 application key
	// carries exactly one namePrefix and jobs/ and checkpoints/ share none —
	// the only single-key alternative is a bucket-wide write, which is strictly
	// worse.
	publishCred   credentials
	publishScoped bool
	// usedGrant names the credential a push actually presented ("publish",
	// "write", …) so a 403 can point at the right key. Diagnostics only — it is
	// set AFTER pushCredFor has already chosen, and never feeds the choice.
	usedGrant   string
	concurrency int

	// --- transfer guards (guard.go) -----------------------------------------
	// Defaults are chosen against MEASURED fleet throughput, not taste; the
	// evidence for each number is on its field. Every one disables at 0.
	stallTimeout  time.Duration // B2X_STALL_S: idle bound on ONE part's reads
	minMBps       float64       // B2X_MIN_MBPS: aggregate throughput floor
	floorWindow   time.Duration // B2X_MIN_MBPS_WINDOW_S: floor averaging window
	deadlineSlack time.Duration // B2X_DEADLINE_SLACK_S: non-byte-bound allowance
	minDeadline   time.Duration // B2X_MIN_DEADLINE_S: ceiling never below this
	partTries     int           // B2X_PART_TRIES: attempts per stalled part
}

// Guard defaults. Stated here rather than inline so a reviewer can check every
// number against its evidence in one place.
const (
	// 120 s of ZERO bytes on one part. b2x reads into a 1 MiB buffer, so even a
	// flow shaped to the bottom of the observed per-flow band (~1 MB/s; hosts
	// shape at ~4-16 MB/s per flow) returns a read about every second. 120 s is
	// a flow delivering under ~8 KB/s — dead, not slow. rclone's equivalent
	// (--timeout, IO idle) defaults to 5m; we are tighter because a stall here
	// RETRIES the part instead of failing the transfer, so the cost of being
	// wrong is one re-requested range, not a re-rented box.
	defaultStallS = 120

	// 3 MB/s aggregate. Every B2 pull ever measured from a rented box came in at
	// 58-101 MB/s on 1 Gbps-class hosts and up to 1008 MB/s on a fat host
	// (measured on the fleet this grew in; see docs/DESIGN.md §5b) — the SLOWEST
	// arm on the slowest box was 58.0 MB/s. 3 MB/s is ~19x below that, so a false
	// positive would need a host nineteen times slower than the worst one ever
	// rented. The boot-side docker-pull watchdog cuts a bad host at 5 MB/s; this
	// floor deliberately takes the more conservative 3, because the boot watchdog
	// kills a box that is not yet billing GPU while this one aborts work in
	// progress.
	defaultMinMBps = 3.0

	// 300 s window, matching the boot-side docker-pull watchdog's averaging
	// window (BOOT_MBPS_WINDOW_S) exactly. A full window must
	// elapse before any verdict, so this is also the minimum time-to-condemn.
	// Long enough that a B2 5xx storm riding out its backoff cannot trip it.
	defaultFloorWindowS = 300

	// 300 s of slack on the derived ceiling, for the parts of a transfer that
	// move no bytes: LIST over a large prefix, HEAD per object, preallocation of
	// a 22 GB sparse file, fsync, and the optional sha256 --verify pass.
	defaultDeadlineSlackS = 300

	// No derived ceiling is ever shorter than 15 min, so small transfers keep a
	// bound that is about liveness rather than about their size. (The 6 MB
	// b2x bootstrap fetch would otherwise derive a ~2 s budget.)
	defaultMinDeadlineS = 900

	// 3 attempts per part. A ranged GET restarts at an exact byte offset, so a
	// retry is cheap and idempotent; 3 covers a transient flow death without
	// letting a genuinely dead peer be re-dialled indefinitely.
	defaultPartTries = 3
)

func envAny(names ...string) string {
	for _, n := range names {
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			return v
		}
	}
	return ""
}

var errNoCreds = errors.New("no B2 credentials in environment (set B2_KEY_ID and B2_APPLICATION_KEY)")

func loadConfig() (*config, error) {
	c := &config{
		bucket:   envAny("B2_BUCKET"),
		endpoint: envAny("B2_S3_ENDPOINT"),
		region:   envAny("B2_REGION"),
	}
	if c.region == "" {
		c.region = "us-west-004"
	}
	if c.bucket == "" {
		return nil, fmt.Errorf("B2_BUCKET not set")
	}
	if c.endpoint == "" {
		// Derivable from the region for B2 — one less thing a caller must set.
		c.endpoint = "https://s3." + c.region + ".backblazeb2.com"
	}
	if !strings.HasPrefix(c.endpoint, "http://") && !strings.HasPrefix(c.endpoint, "https://") {
		c.endpoint = "https://" + c.endpoint
	}

	kid := envAny("B2_KEY_ID")
	key := envAny("B2_APPLICATION_KEY")
	if kid == "" || key == "" {
		return nil, errNoCreds
	}
	c.readCred = credentials{keyID: kid, secret: key, region: c.region}

	wid := envAny("B2_WRITE_KEY_ID")
	wkey := envAny("B2_WRITE_APPLICATION_KEY")
	if wid != "" && wkey != "" {
		c.writeCred = credentials{keyID: wid, secret: wkey, region: c.region}
		c.writeScoped = true
	} else {
		c.writeCred = c.readCred
	}

	pid := envAny("B2_PUBLISH_KEY_ID")
	pkey := envAny("B2_PUBLISH_APPLICATION_KEY")
	if pid != "" && pkey != "" {
		c.publishCred = credentials{keyID: pid, secret: pkey, region: c.region}
		c.publishScoped = true
	}

	c.concurrency = defaultConcurrency()
	if v := envAny("B2X_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.concurrency = n
		}
	}

	// Guards (guard.go). A malformed value falls back to the constant rather
	// than to "off" — a typo in an env var must never silently disarm a guard.
	c.stallTimeout = envSeconds("B2X_STALL_S", defaultStallS)
	c.minMBps = envFloat("B2X_MIN_MBPS", defaultMinMBps)
	c.floorWindow = envSeconds("B2X_MIN_MBPS_WINDOW_S", defaultFloorWindowS)
	c.deadlineSlack = envSeconds("B2X_DEADLINE_SLACK_S", defaultDeadlineSlackS)
	c.minDeadline = envSeconds("B2X_MIN_DEADLINE_S", defaultMinDeadlineS)
	c.partTries = int(envFloat("B2X_PART_TRIES", defaultPartTries))
	if c.partTries < 1 {
		c.partTries = 1
	}
	return c, nil
}

// envSeconds reads a non-negative number of seconds. 0 is a MEANINGFUL value
// (it disables the guard) and is therefore honored; only an unset or unparseable
// value falls back to the default.
func envSeconds(name string, def float64) time.Duration {
	f := envFloat(name, def)
	if f < 0 {
		f = def
	}
	return time.Duration(f * float64(time.Second))
}

func envFloat(name string, def float64) float64 {
	v := envAny(name)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "b2x: warning: %s=%q is not a number — using %g\n", name, v, def)
		return def
	}
	return f
}

// normKey strips a leading "b2:<bucket>/", "b2w:<bucket>/", "b2p:<bucket>/" or
// leading slash, so call sites can pass the same string they passed rclone.
// This is what lets a migrated shell line keep its existing "$B2/path" variable
// verbatim instead of every site growing a new path spelling.
func (c *config) normKey(p string) string {
	// Every remote spelling a call site may pass must be stripped here: an
	// unrecognised one comes back verbatim as the key, which is both the wrong
	// destination and unable to match the checkpoints/ prefix pushCredFor
	// routes on. b2p: is the CHECKPOINTS remote.
	for _, pre := range []string{"b2w:", "b2p:", "b2:", "b2eu:"} {
		if strings.HasPrefix(p, pre) {
			p = strings.TrimPrefix(p, pre)
			p = strings.TrimPrefix(p, c.bucket)
			break
		}
	}
	return strings.TrimPrefix(p, "/")
}

// publishPrefix is the one namePrefix the B2_PUBLISH_* grant covers. Matching
// what the key is scoped to is the whole mechanism; a wider test would send
// jobs/ writes at a key that cannot make them.
//
// Hardcoded on purpose, and MEASURED so: the launcher that mints these scoped
// keys resolves the publish prefix on the WORKSTATION at mint time and ships
// only the KEY_ID/APPLICATION_KEY pair into box env — the granted prefixes
// never travel. A b2x on a box therefore cannot observe a retarget, and reading
// an env var here would let a stray workstation value change routing on a box
// whose key it does not describe. If a B2_PUBLISH_PREFIX ever starts shipping
// alongside the key, honour it with this as the default.
const publishPrefix = "checkpoints/"

// pushCredFor picks the credential that can actually WRITE this key. It returns
// the grant NAME ("publish", "write" or "read") — which is what the 401/403 hint
// keys off — plus an optional note carrying the detail a log line wants, empty
// when there is none.
//
// A box carries up to three grants and they are disjoint by construction: read
// is bucket-wide and write-less, B2_WRITE_* covers one prefix (jobs/ on a jobs
// box), B2_PUBLISH_* covers checkpoints/. rclone routes them as three remotes
// ([b2]/[b2w]/[b2p]); b2x had only ever known the first two, so every push to
// checkpoints/ presented the jobs-scoped key and 403'd — silently, because
// every call site falls back to its rclone line on any b2x failure. That is the
// same shape as a publish stage 403'ing AFTER training completed, which is what
// bought the third key in the first place.
func (c *config) pushCredFor(key string) (cred credentials, grant, note string) {
	if strings.HasPrefix(key, publishPrefix) {
		if c.publishScoped {
			return c.publishCred, "publish", ""
		}
		// No grant. Fall through to the write cred rather than refusing: on a
		// box with a bucket-wide key that write SUCCEEDS, and b2x is not the
		// right place to re-litigate a scope the launcher decided.
		return c.writeCred, "write", "no publish grant — a 403 here is the scope, not b2x"
	}
	if c.writeScoped {
		return c.writeCred, "write", ""
	}
	return c.writeCred, "read", "bucket-wide"
}
