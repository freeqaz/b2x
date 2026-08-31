# Benchmarks

Measured 2026-08-31. Every number here came from the run described below;
nothing is carried over from an earlier era or a different host.

**Read the caveats before quoting any of this.** These are single-host
measurements against one bucket in one region over about an hour. They are
enough to show a 3× defect and fix it; they are not a general claim about how
these tools rank on your infrastructure.

## Setup

| | |
|---|---|
| Host | Rented GPU box, 80 cores, 377 GB RAM, advertised 8.6 Gbps down, US |
| Storage | Backblaze B2, `us-west-004`, S3-compatible endpoint (no CDN in front) |
| Objects | `qwen36-27b` safetensors shards — 15 objects, 3.88–4.00 GB each |
| Destination | `/dev/shm` (RAM), so disk write speed is never the bottleneck |
| Versions | b2x at this commit, rclone v1.75.0, s5cmd v2.3.0 |

Throughput is wall-clock: bytes actually on the destination divided by elapsed
seconds, timed outside the process. b2x's own progress line agreed with the
external timing, so the two instruments corroborate each other.

## Does B2 serve repeat reads faster? No.

Run first, because the answer decides whether a benchmark may reuse objects.
Same tool, same object, three times back to back:

| repeat | b2x | s5cmd |
|---|---|---|
| 1 | 657.1 | 446.5 |
| 2 | 641.3 | 489.8 |
| 3 | 672.6 | 460.6 |

No warming trend in either tool. There is no read cache to fall into, so a
benchmark may reuse an object as long as it varies only one thing at a time.

This also rules out the most common way a transfer benchmark flatters itself:
if the tools run in a fixed order against a shared object, a warm cache would
hand the last tool a free win. That is not happening here — but ordering bias
is still real for other reasons (see Method), so the comparison below
randomizes it anyway.

## Tool comparison

Each arm pulls its own 4 GB object that no other arm touches, so no arm can
benefit from another's transfer. Two rounds, group order reversed in the
second. Every read is cold. All arms at defaults except where noted.

| tool | run 1 | run 2 | mean MB/s |
|---|---|---|---|
| **b2x** (this commit) | — | — | **≈650** ¹ |
| s5cmd `-c 32` | 462.7 | 459.2 | 461.0 |
| rclone `--multi-thread-streams 32` | 303.5 | 295.0 | 299.3 |
| rclone (stock flags) | 88.9 | 121.3 | 105.1 |
| b2x *before* the concurrency fix | 218.5 | 220.8 | 219.7 |

¹ The fixed default was measured in a separate back-to-back A/B (below) rather
than in this matrix, which predates the fix. Equivalent settings measured
508.6 / 609.0 on two shards in the matrix and 657.1 / 641.3 / 672.6 on another,
so treat ≈650 as a range, not a constant. The competitor numbers are
untouched by the fix and stand as measured.

Stock rclone is slow for a known reason, not a mystery: it fixes the chunk
size and derives the stream count from it, so a request for more streams is
silently clamped. That behavior is what b2x's part planner was written to
avoid, and it is why the tuned rclone row exists alongside the stock one.

## The concurrency defect this benchmark found

b2x shipped a global in-flight budget of `NumCPU × 8` (capped at 192) with no
regard for how many objects it was spread across. On this 80-core host that
put up to 128 concurrent range requests against a *single key*. B2 degrades
sharply under that. Varying only the in-flight count, part count fixed at 128,
same 4 GB object:

| in-flight | 8 | 16 | 24 | 32 | 48 | 64 | 96 | 128 | 192 |
|---|---|---|---|---|---|---|---|---|---|
| MB/s | 266 | 540 | **740** | 631 | 464 | 265 | 277 | 234 | 224 |

The knee is narrow and the far side is a cliff. The old default sat at the
bottom of it.

The fix (see `plan.go`) is two coupled changes: interleave the pull queue
round-robin across objects, and size the worker pool to
`nObjects × maxInFlightPerObject` with the per-object ceiling at 32. Part
planning is untouched — objects still split into many small parts for cheap
resume; only the number in flight changed.

The cap is **32, not the measured peak of 24**, deliberately. One host's
optimum is not a constant, an older measurement on different hardware showed
throughput still climbing at 64, and 32 sits inside the plateau on both. If
your host disagrees, `B2X_CONCURRENCY` still overrides the budget.

### Before / after, at defaults, no flags

Back-to-back on the same host and objects:

| shape | before | after | |
|---|---|---|---|
| single 4 GB object | 223.4 | **667.6** | 3.0× |
| 4 objects, 14.2 GiB | 369.2 | **651.9** | 1.8× |
| push, 8 GiB | 165.0 | **227.0** | 1.4× |

Multi-file gained less than single-file because the old code only concentrated
its first 128 workers on object 1 before spilling to object 2 — bad, but not
as bad as putting all of them on one key. Push gained least because a push is
also bounded by the host's upstream path, which is not the same as its
download path.

## Method, and what would make these numbers wrong

- **Ordering bias is the trap.** The first version of this benchmark ran the
  tools in a fixed order against a shared object. b2x was permanently in the
  cold-first slot and s5cmd permanently last. That run was discarded, not
  published. The results above use one private object per arm plus reversed
  group order.
- **Reuse is safe here only because the cache probe says so.** On storage that
  does cache reads, the single-variable sweeps above would need distinct
  objects too.
- **One host, one region, one hour.** Spot instances vary; this box was in
  fact outbid twice mid-benchmark. Treat the *shape* of the concurrency curve
  as the finding and the absolute numbers as one sample.
- **Not measured:** other regions, other providers, small-object workloads
  (thousands of tiny files, where per-request overhead rather than per-key
  concurrency dominates), or sustained multi-hour transfers.

Reproducing is deliberately plain: time `b2x pull` against your own object,
vary `B2X_CONCURRENCY`, and compare against `rclone copyto` and `s5cmd cp` on
the same object. If your curve peaks somewhere else, the knob is there.
