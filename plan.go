package main

// plan.go — the part planner. THIS FILE IS THE STRUCTURAL FIX.
//
// The bug we are eliminating (verified empirically on rclone 1.74.4):
//
//     $ rclone copyto --multi-thread-streams 16 --multi-thread-cutoff 64M -vv 555MB.bin dst
//     DEBUG : multi-thread copy: number of streams 16 was bigger than number of chunks 9
//     DEBUG : Starting multi-thread copy with 9 chunks of size 64Mi with 9 parallel streams
//
// rclone fixes the CHUNK SIZE (--multi-thread-chunk-size, default 64Mi) and
// derives the chunk COUNT from it, then silently clamps the requested stream
// count down to that. So the concurrency you actually get is
// `min(streams, ceil(size/64Mi))` — a 150 MB adapter asking for 16 streams gets
// 3, a 300 MB one gets 5. Since vast hosts traffic-shape PER TCP FLOW at
// ~4-16 MB/s, that clamp IS the throughput. Nothing in the repo ever set
// --multi-thread-chunk-size, so every "tuned" call site was quietly capped.
//
// The fix is not "also set the chunk-size flag" — that just moves the same
// coupling behind a second knob that the next call site will forget. The fix is
// to INVERT the derivation:
//
//     rclone:  chunkSize fixed  ->  partCount = size/chunkSize  ->  streams = min(streams, partCount)
//     b2x:     partCount computed from size  ->  partSize = size/partCount
//
// Part COUNT is the independent variable and part SIZE is derived, so the
// concurrency for an object is never a function of a fixed byte constant.
// It is arithmetically impossible for b2x to under-parallelize a large object
// the way rclone does, because there is no constant to be clamped against.
//
// The second structural simplification: there is exactly ONE concurrency
// dimension. rclone has two (--transfers across files, --multi-thread-streams
// within a file) and they interact multiplicatively, which is why the repo's
// call sites disagree about which to set. b2x schedules PARTS — a part of a
// small file and a part of a big file are the same unit of work — against one
// global in-flight budget. A 4-shard model dir and one 22 GB monolith both just
// fill the budget. Nothing to tune per call site; nothing to get wrong.

import (
	"runtime"
)

const (
	// minPartSize is the floor on a part. Below ~8 MiB the per-request overhead
	// (TLS + signing + TCP slow-start ramp) starts to dominate the transfer, so
	// splitting finer buys nothing even on a fast link. It is also comfortably
	// above S3's 5 MiB multipart minimum, so upload plans are always legal.
	minPartSize = 8 << 20

	// maxPartsPerObject caps the fan-out for a single object. A fat-host ceiling
	// benchmark (summarized in docs/DESIGN.md §5b)
	// measured a single 4.9 GB shard at 229 MB/s @ 8 streams, 344 @ 16, 415 @
	// 32, 467 @ 64 — still climbing at 64, but with clearly diminishing returns
	// and rising memory. 128 leaves headroom above the measured knee without
	// making the in-flight buffer set unbounded.
	maxPartsPerObject = 128

	// S3 hard limit: an MPU may have at most 10000 parts. maxPartsPerObject is
	// far below this, but partSizeFor asserts it for very large objects.
	s3MaxParts = 10000
)

// partPlan describes how one object is split.
type partPlan struct {
	Size     int64
	PartSize int64
	NParts   int
}

// planParts computes the split for an object of the given size.
//
// The rule, in one line: take as many parts as the object can supply at
// minPartSize, capped at maxPartsPerObject, then spread the bytes evenly.
func planParts(size int64) partPlan {
	if size <= 0 {
		return partPlan{Size: size, PartSize: 0, NParts: 0}
	}
	if size <= minPartSize {
		return partPlan{Size: size, PartSize: size, NParts: 1}
	}

	// How many parts can this object supply without going below the floor?
	//
	// FLOOR division, deliberately. Rounding UP here asks for one more part
	// than the object can fund, so the derived part size lands just BELOW
	// minPartSize — and for objects in (8 MiB, 16 MiB] it produced 2 parts of
	// ~4 MiB, under S3's 5 MiB multipart minimum, which B2 rejects. Caught by
	// TestPlanRespectsFloorAndCap.
	n := int(size / minPartSize)
	if n < 1 {
		n = 1
	}
	if n > maxPartsPerObject {
		n = maxPartsPerObject
	}

	// Derive the part size from the count (the inversion), rounding UP so that
	// n parts always cover the object. Since n was floored, ps >= minPartSize.
	ps := ceilDiv(size, int64(n))

	// Rounding up can leave the last part short enough that we need one fewer
	// part than we asked for; recompute the count from the real part size so
	// NParts is always exactly the number of ranges we will issue.
	n = int(ceilDiv(size, ps))

	// Safety: never exceed S3's 10000-part MPU limit (only reachable if
	// maxPartsPerObject were raised drastically).
	if n > s3MaxParts {
		ps = ceilDiv(size, s3MaxParts)
		n = int(ceilDiv(size, ps))
	}
	return partPlan{Size: size, PartSize: ps, NParts: n}
}

// partRange returns the byte offset and length of part i (0-based).
func (p partPlan) partRange(i int) (off, n int64) {
	off = int64(i) * p.PartSize
	n = p.PartSize
	if off+n > p.Size {
		n = p.Size - off
	}
	return off, n
}

func ceilDiv(a, b int64) int64 {
	if b == 0 {
		return 0
	}
	return (a + b - 1) / b
}

// defaultConcurrency is the global in-flight part budget — the ONE number that
// governs parallelism, and the only one a caller could ever want to change
// (B2X_CONCURRENCY overrides it; no call site sets it).
//
// Sized from CPU count because that is the only thing we can measure cheaply
// and locally that correlates with host class: a bigger vast box is both a
// faster NIC and more cores. The bench's best multi-file result (996 MB/s) ran
// 16 transfers x 16 streams = up to 256 concurrent flows, so the ceiling here
// is deliberately generous; the floor keeps a 2-core CPU box from serializing.
func defaultConcurrency() int {
	n := runtime.NumCPU() * 8
	if n < 16 {
		n = 16
	}
	if n > 192 {
		n = 192
	}
	return n
}
