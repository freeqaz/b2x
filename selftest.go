package main

// selftest.go — `b2x selftest`: prove credentials, signing, and a full
// multipart round-trip work on THIS box, under a scratch prefix.
//
// This is the on-box preflight a boot script can run before committing to a
// multi-GB pull, and the $0 check an agent can run from the laptop. It never
// touches your data prefixes — everything happens under
// _b2x_selftest/<date>-<pid>/.
//
// That default only works with a bucket-wide write key. A B2 key whose grant is
// namePrefix-scoped (say to jobs/) cannot write outside its prefix, so the write
// leg 403s BY CONSTRUCTION — an honest failure, reported as exit 3 (auth), but
// one that says nothing about the box. Set B2X_SELFTEST_PREFIX to put the
// scratch inside the granted prefix instead, e.g.
// B2X_SELFTEST_PREFIX=jobs/_b2x_selftest.
//
// Note it does NOT delete: B2 keys handed to a rented box are best minted
// WITHOUT deleteFiles, so a self-cleaning test would fail wherever that policy
// is followed. It writes small objects under a dated scratch prefix and leaves
// them for a bucket lifecycle rule to reap instead.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// selftestPrefix is the scratch base for the round-trip, overridable because a
// namePrefix-scoped write key cannot reach the default one. Slashes are trimmed
// so callers can pass "jobs/scratch/" or "/jobs/scratch" and get one join.
func selftestPrefix() string {
	if v := strings.Trim(envAny("B2X_SELFTEST_PREFIX"), "/"); v != "" {
		return v
	}
	return "_b2x_selftest"
}

func selftest(ctx context.Context, cfg *config) int {
	fmt.Fprintf(os.Stderr, "b2x %s selftest — bucket=%s endpoint=%s region=%s concurrency=%d\n",
		version, cfg.bucket, cfg.endpoint, cfg.region, cfg.concurrency)
	if cfg.writeScoped {
		fmt.Fprintln(os.Stderr, "  write key: scoped (B2_WRITE_*)")
	} else {
		fmt.Fprintln(os.Stderr, "  write key: same as read key")
	}

	rc := newS3Client(cfg.endpoint, cfg.bucket, cfg.readCred, 4)
	wc := newS3Client(cfg.endpoint, cfg.bucket, cfg.writeCred, cfg.concurrency)
	base := selftestPrefix()

	// 1. READ path: a list call proves signing + credentials without writing.
	if _, err := rc.list(ctx, base+"/"); err != nil {
		fmt.Fprintf(os.Stderr, "  FAIL list: %v\n", err)
		return report(err, cfg)
	}
	fmt.Fprintln(os.Stderr, "  ok  list (signing + read credentials)")

	// 2. WRITE + multipart round-trip. 24 MiB forces a real MPU under the 8 MiB
	// part floor (3 parts), so CreateMPU/UploadPart/CompleteMPU are all covered.
	prefix := fmt.Sprintf("%s/%s-%d", base, time.Now().UTC().Format("20060102"), os.Getpid())
	key := prefix + "/roundtrip.bin"
	size := 24 << 20
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(i * 7)
	}
	tmp, err := os.MkdirTemp("", "b2x-selftest-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "  FAIL tempdir: %v\n", err)
		return exitTransfer
	}
	defer os.RemoveAll(tmp)
	src := filepath.Join(tmp, "roundtrip.bin")
	if err := os.WriteFile(src, buf, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "  FAIL write temp: %v\n", err)
		return exitTransfer
	}

	upStats := newStats("push", src, key, false, cfg.concurrency)
	if err := uploadOne(ctx, wc, pushItem{abs: src, rel: "", size: int64(size), mtime: time.Now()}, key, upStats); err != nil {
		fmt.Fprintf(os.Stderr, "  FAIL multipart upload: %v\n", err)
		return report(err, cfg)
	}
	fmt.Fprintf(os.Stderr, "  ok  multipart upload (%s in %d parts, %.1f MiB/s)\n",
		humanBytes(int64(size)), planParts(int64(size)).NParts, upStats.mbps())

	// 3. Parallel ranged-GET round-trip + integrity.
	dst := filepath.Join(tmp, "back.bin")
	dlStats := newStats("pull", key, dst, false, cfg.concurrency)
	if err := runPull(ctx, rc, cfg, key, dst, pullOpts{verify: true}, dlStats); err != nil {
		fmt.Fprintf(os.Stderr, "  FAIL ranged pull: %v\n", err)
		return report(err, cfg)
	}
	got, err := os.ReadFile(dst)
	if err != nil || !bytes.Equal(got, buf) {
		fmt.Fprintf(os.Stderr, "  FAIL round-trip content mismatch\n")
		return exitIntegrity
	}
	fmt.Fprintf(os.Stderr, "  ok  parallel ranged pull + sha256 verify (%.1f MiB/s)\n", dlStats.mbps())

	// 4. IDEMPOTENCE: an immediate re-pull must transfer zero bytes.
	again := newStats("pull", key, dst, false, cfg.concurrency)
	if err := runPull(ctx, rc, cfg, key, dst, pullOpts{}, again); err != nil {
		fmt.Fprintf(os.Stderr, "  FAIL re-pull: %v\n", err)
		return report(err, cfg)
	}
	if again.done.Load() != 0 {
		fmt.Fprintf(os.Stderr, "  FAIL idempotence: re-pull transferred %s (expected 0)\n", humanBytes(again.done.Load()))
		return exitTransfer
	}
	fmt.Fprintf(os.Stderr, "  ok  idempotent re-pull (0 bytes, %s skipped)\n", humanBytes(again.skippedBytes.Load()))

	fmt.Fprintf(os.Stderr, "b2x selftest PASSED (scratch objects left under %s/ for the bucket lifecycle)\n", prefix)
	return exitOK
}
