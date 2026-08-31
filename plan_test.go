package main

import (
	"fmt"
	"testing"
)

// TestPlanNeverUnderParallelizes is the regression test for the bug this
// component exists to eliminate. Measured on rclone 1.74.4:
//
//	rclone copyto --multi-thread-streams 16 --multi-thread-cutoff 64M -vv 555MB.bin dst
//	DEBUG: multi-thread copy: number of streams 16 was bigger than number of chunks 9
//	DEBUG: Starting multi-thread copy with 9 chunks of size 64Mi with 9 parallel streams
//
// rclone's concurrency is min(streams, ceil(size/64Mi)). This test asserts b2x
// beats that for every object size we actually move, which is the property that
// makes the bug class structurally impossible rather than merely fixed.
func TestPlanNeverUnderParallelizes(t *testing.T) {
	const rcloneChunk = 64 << 20
	const rcloneStreams = 16 // what the "tuned" call sites asked for

	cases := []struct {
		name string
		size int64
	}{
		{"150MB adapter", 150 << 20},
		{"300MB adapter", 300 << 20},
		{"555MB eval-env tarball", 555 * 1000 * 1000},
		{"2.9GiB base shard", 2900 << 20},
		{"4.9GB model shard", 4_878_000_000},
		{"22GB gemma monolith", 22_000_000_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := planParts(tc.size)

			rcloneParts := int(ceilDiv(tc.size, rcloneChunk))
			rcloneActual := rcloneStreams
			if rcloneParts < rcloneActual {
				rcloneActual = rcloneParts
			}

			if p.NParts < rcloneActual {
				t.Fatalf("b2x %d parts < rclone's effective %d streams", p.NParts, rcloneActual)
			}
			// The whole object must be covered exactly.
			var total int64
			for i := 0; i < p.NParts; i++ {
				off, n := p.partRange(i)
				if n <= 0 {
					t.Fatalf("part %d has non-positive length %d", i, n)
				}
				if off != total {
					t.Fatalf("part %d starts at %d, expected %d (gap or overlap)", i, off, total)
				}
				total += n
			}
			if total != tc.size {
				t.Fatalf("parts cover %d bytes, object is %d", total, tc.size)
			}
			t.Logf("%s: b2x=%d parts of %s (rclone effective: %d streams) -> %.1fx",
				tc.name, p.NParts, humanBytes(p.PartSize), rcloneActual,
				float64(p.NParts)/float64(rcloneActual))
		})
	}
}

// TestPlanRespectsFloorAndCap pins the two invariants that keep the plan sane:
// no part below the 8 MiB floor (except a whole small object), and no object
// fanned out past the cap.
func TestPlanRespectsFloorAndCap(t *testing.T) {
	for _, size := range []int64{1, 1 << 10, minPartSize - 1, minPartSize, minPartSize + 1,
		100 << 20, 1 << 30, 22_000_000_000, 1 << 40} {
		p := planParts(size)
		if p.NParts > maxPartsPerObject {
			t.Errorf("size %d: %d parts exceeds cap %d", size, p.NParts, maxPartsPerObject)
		}
		if size > minPartSize && p.PartSize < minPartSize {
			t.Errorf("size %d: part size %d below floor %d", size, p.PartSize, minPartSize)
		}
		if p.NParts > 1 && p.PartSize < 5<<20 {
			t.Errorf("size %d: part size %d below S3's 5MiB multipart minimum", size, p.PartSize)
		}
		if p.NParts > s3MaxParts {
			t.Errorf("size %d: %d parts exceeds S3's 10000-part MPU limit", size, p.NParts)
		}
	}
}

func TestPlanZeroAndTiny(t *testing.T) {
	if p := planParts(0); p.NParts != 0 {
		t.Errorf("empty object should plan 0 parts, got %d", p.NParts)
	}
	p := planParts(1024)
	if p.NParts != 1 || p.PartSize != 1024 {
		t.Errorf("small object should be one whole part, got %d x %d", p.NParts, p.PartSize)
	}
}

func TestDefaultConcurrencyBounds(t *testing.T) {
	n := defaultConcurrency()
	if n < 16 || n > 192 {
		t.Errorf("defaultConcurrency() = %d, outside [16,192]", n)
	}
}

func Example_planParts() {
	for _, s := range []int64{150 << 20, 555 * 1000 * 1000, 22_000_000_000} {
		p := planParts(s)
		fmt.Printf("%s -> %d parts of %s\n", humanBytes(s), p.NParts, humanBytes(p.PartSize))
	}
	// Output:
	// 150.0MiB -> 18 parts of 8.3MiB
	// 529.3MiB -> 66 parts of 8.0MiB
	// 20.5GiB -> 128 parts of 163.9MiB
}

func TestWorkersForNarrowsBudgetToWhatObjectsAbsorb(t *testing.T) {
	// The measured defect: a single-object pull handed the whole budget ran
	// ~128 concurrent range-GETs against one key and lost ~3x throughput.
	if got := workersFor(192, 1); got != maxInFlightPerObject {
		t.Errorf("workersFor(192, 1) = %d, want %d", got, maxInFlightPerObject)
	}
	if got := workersFor(192, 4); got != 4*maxInFlightPerObject {
		t.Errorf("workersFor(192, 4) = %d, want %d", got, 4*maxInFlightPerObject)
	}
	// Many objects: the global budget binds again, so a wide model directory
	// keeps its parallelism.
	if got := workersFor(192, 32); got != 192 {
		t.Errorf("workersFor(192, 32) = %d, want 192 (budget binds)", got)
	}
	// A budget already below the ceiling is never raised.
	if got := workersFor(8, 4); got != 8 {
		t.Errorf("workersFor(8, 4) = %d, want 8", got)
	}
	if got := workersFor(16, 0); got != 16 {
		t.Errorf("workersFor(16, 0) = %d, want 16 (degenerate object count)", got)
	}
}

func TestInterleaveByObjectSpreadsConsecutiveJobs(t *testing.T) {
	// Queue grouped by object: a,a,a,b,b,b — the shape the pull planner emits.
	keys := []string{"a", "a", "a", "b", "b", "b"}
	got := interleaveByObject(len(keys), func(i int) string { return keys[i] })
	if len(got) != len(keys) {
		t.Fatalf("interleave returned %d indices, want %d", len(got), len(keys))
	}
	// Every job appears exactly once.
	seen := map[int]bool{}
	for _, i := range got {
		if seen[i] {
			t.Fatalf("index %d emitted twice", i)
		}
		seen[i] = true
	}
	// Adjacent jobs alternate objects — the property the per-object ceiling
	// relies on to hold in practice.
	want := []int{0, 3, 1, 4, 2, 5}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("interleave = %v, want %v", got, want)
		}
	}
	// Parts of one object keep their order, so resume still writes forward.
	if got := interleaveByObject(3, func(i int) string { return "solo" }); got[0] != 0 || got[2] != 2 {
		t.Errorf("single-object interleave reordered parts: %v", got)
	}
	if interleaveByObject(0, func(int) string { return "" }) != nil {
		t.Error("empty queue should interleave to nil")
	}
}
