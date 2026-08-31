# Benchmarks

Measured 2026-08-31. Everything here came from the run described below.

These are single-host measurements against one bucket in one region over about
an hour. Enough to size the defaults; not a general claim about how these tools
rank on your infrastructure.

## Setup

| | |
|---|---|
| Host | Rented GPU box, 80 cores, 377 GB RAM, advertised 8.6 Gbps down, US |
| Storage | Backblaze B2, `us-west-004`, S3-compatible endpoint (no CDN in front) |
| Objects | 4 GB safetensors shards, one private object per arm |
| Destination | `/dev/shm` (RAM), so disk write speed is never the bottleneck |
| Versions | b2x at this commit, rclone v1.75.0, s5cmd v2.3.0 |

Throughput is wall-clock: bytes on the destination divided by elapsed seconds,
timed outside the process.

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

## Tool comparison

Each arm pulls its own object that no other arm touches. Two rounds, group
order reversed in the second. Every read is cold. Defaults except where noted.

| tool | run 1 | run 2 | mean MB/s |
|---|---|---|---|
| **b2x** | — | — | **≈650** ¹ |
| s5cmd `-c 32` | 462.7 | 459.2 | 461.0 |
| rclone `--multi-thread-streams 32` | 303.5 | 295.0 | 299.3 |
| rclone (stock flags) | 88.9 | 121.3 | 105.1 |

¹ b2x at its shipped defaults was measured in the separate back-to-back run
quoted below (667.6 single-object, 651.9 multi-file) rather than in this
matrix. Across other shards the same settings ranged 508.6–672.6, so ≈650 is a
range, not a constant. The competitor rows are exactly as measured.

Stock rclone is slow for a known reason, not a mystery: it fixes the chunk size
and derives the stream count from it, so a request for more streams is silently
clamped. Avoiding that is what b2x's part planner is for, and it is why the
tuned rclone row exists alongside the stock one.

## Where the concurrency defaults come from

B2 degrades sharply when one key is read by too many connections at once.
Varying only the in-flight count, part count fixed at 128, same 4 GB object:

| in-flight | 8 | 16 | 24 | 32 | 48 | 64 | 96 | 128 | 192 |
|---|---|---|---|---|---|---|---|---|---|
| MB/s | 266 | 540 | **740** | 631 | 464 | 265 | 277 | 234 | 224 |

The knee is narrow and the far side is a cliff. So b2x caps in-flight parts
**per object** at 32, sizes the worker pool to `nObjects × 32`, and interleaves
the pull queue round-robin across objects so that ceiling holds in practice.
Part planning is separate: objects still split into many small parts for cheap
resume; only the number in flight is capped.

The cap is 32 rather than the measured peak of 24 deliberately. One host's
optimum is not a constant — an older measurement on different hardware was
still climbing at 64 — and 32 sits inside the plateau on both. If your host
disagrees, `B2X_CONCURRENCY` overrides the budget.

Measured at defaults, no flags: 667.6 MB/s pulling one 4 GB object, 651.9
pulling a four-object 14.2 GiB directory, 227.0 pushing 8 GiB. Push is lower
because it is bounded by the host's upstream path, which is not the same as its
download path.

## What would make these numbers wrong

- **Ordering bias.** An earlier version of this benchmark ran the tools in a
  fixed order against a shared object, which permanently put one tool in the
  cold-first slot. That run was discarded. The results above use one private
  object per arm plus reversed group order.
- **Reuse is safe here only because the cache probe says so.** On storage that
  does cache reads, the single-variable sweeps would need distinct objects too.
- **One host, one region, one hour.** This box was in fact outbid twice
  mid-benchmark. Treat the shape of the concurrency curve as the finding and
  the absolute numbers as one sample.
- **Not measured:** other regions, other providers, small-object workloads
  (thousands of tiny files, where per-request overhead rather than per-key
  concurrency dominates), or sustained multi-hour transfers.

Reproducing is deliberately plain: time `b2x pull` against your own object,
vary `B2X_CONCURRENCY`, and compare against `rclone copyto` and `s5cmd cp` on
the same object.
