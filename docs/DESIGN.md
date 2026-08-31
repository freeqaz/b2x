# b2x — the B2 ↔ box artifact transport

**Status: LANDED 2026-08-01.** One static Go binary (`b2x`) owns every artifact
transfer between Backblaze B2 and a rented GPU box.

> This document was written inside the private research monorepo where b2x grew,
> and is preserved here as the design record. References to sibling tooling —
> boot scripts, the job daemon, the fleet controller — are to
> [herdd](https://github.com/freeqaz/herdd), the vast.ai control CLI extracted
> from the same monorepo. A few internal-only measurement docs it cites were not
> extracted; those citations are described inline rather than linked.
> Bucket names, hosts and image tags are placeholders.

## TL;DR

1. **Concurrency was the whole lever and it was silently clamped.** rclone caps
   `--multi-thread-streams` at `ceil(size / --multi-thread-chunk-size)`, and
   nothing in the repo ever set that chunk-size flag. Measured on the real
   575 MB eval-env tarball: the "tuned" 16-stream spelling got **9 flows**.
2. **b2x inverts the derivation** — part *count* from object size, part *size*
   derived — so concurrency is never a function of a fixed byte constant. Same
   object: **68 flows**, zero knobs.
3. **There is now exactly ONE concurrency dimension.** rclone has two
   (`--transfers` across files × `--multi-thread-streams` within a file) and they
   interact; b2x schedules *parts* against one global budget. Nothing to set per
   call site is why nothing can drift per call site.
4. **Pulls are idempotent.** A re-pull of a present tree moves 0 bytes; an
   interrupted pull resumes only its missing parts; a tree pulled by the old
   rclone code is adopted on a size match rather than re-fetched.
5. **Uploads got the same treatment as a DURABILITY fix**, not a speed one — the
   preempt flush was losing state inside `timeout 45`.

## 1. The bug that motivated a new component

Vast hosts traffic-shape **per TCP flow** (~4–16 MB/s), so throughput is
`flows × per-flow-rate`. The repo knew this and had a tuned convention (16
streams / 16 transfers / 64M cutoff) — but the boot script defined it as a
**bash array** (`RC_FAST=(...)`), and a bash array cannot be exported to a child
process. So there was no shared definition to reuse, and every run re-invented
the pull without flags. Most multi-GB pull sites ran stock.

Worse, the tuned sites were capped too. Verified on rclone 1.74.4:

```
$ rclone copyto --multi-thread-streams 16 --multi-thread-cutoff 64M -vv 555MB.bin dst
DEBUG : multi-thread copy: number of streams 16 was bigger than number of chunks 9
DEBUG : Starting multi-thread copy with 9 chunks of size 64Mi with 9 parallel streams
```

`--multi-thread-chunk-size` defaults to 64 MiB and **nothing set it**, so the
effective concurrency was `min(streams, ceil(size/64Mi))`: a 150 MB adapter
asking for 16 got 3, a 300 MB one got 5. This is the likely explanation for an
anomalous ~14 MB/s env-tarball pull on a 10 Gbps host recorded during an earlier
remote-run campaign.

### Measured: peak concurrent TCP flows to B2

Real 575 MB `eval-env/env-<ver>.tar.zst`, per-process socket counting,
reproduced twice:

| variant | peak flows |
|---|---|
| rclone STOCK — what the eval sidecar and the 5 run pulls did | **4** |
| rclone TUNED, `--multi-thread-streams 16` | **9** |
| rclone TUNED + explicit `--multi-thread-chunk-size 8M` | 16 |
| **b2x**, zero knobs | **68** |

Flow count is the causal variable under per-flow shaping. Wall-clock on a laptop
link cannot show this (the link saturates ~70–95 MB/s and all variants converge);
it is visible on a shaped host, which is where every one of these calls runs.

## 2. Why native Go rather than "shell out to rclone with correct flags"

Both options fix today's symptom. The deciding question was which one
**permanently eliminates the bug classes** instead of relocating them.

| bug class | central rclone wrapper | native Go |
|---|---|---|
| scattered untuned invocations | fixed (one wrapper) | fixed (one binary) |
| stream/chunk clamp | fixed *by setting a second flag correctly* — the coupling survives behind a knob, and the clamp formula is rclone-internal and free to change between versions | **structurally impossible** — no fixed constant exists to be clamped against |
| two interacting concurrency dimensions | preserved | collapsed to one |
| idempotence / partial resume | must be built anyway (rclone restarts an interrupted file) | built |
| observability | scrape prose from `--stats` | exact counters |
| **tool-version skew** | **preserved** | **retired** |

The last row decided it. We have a recorded incident where
`rclone.org/install.sh` silently installed rclone **1.53 (2020)** when `unzip`
was missing — a client that cannot
multi-thread *at all*, capping ~73 MB/s. Shelling out keeps that failure mode
forever: "the tool under us is not the tool we think it is." A static
`CGO_ENABLED=0` binary with no runtime dependencies retires it.

**Cost accepted:** we reimplement retry/backoff, multipart, and integrity that
rclone already gets right. This is bounded because the S3 surface we need is
tiny — ListObjectsV2, HeadObject, ranged GetObject, and the three multipart calls
— and it is covered by an in-process S3 server in `transfer_test.go` plus a real
round-trip in `b2x selftest`.

**Stdlib-only, hand-rolled SigV4** (no aws-sdk-go-v2): matches the surrounding
stdlib-only ethos (herdd is stdlib-only by design), keeps the binary at
6.3 MB, and means the build has no module graph to resolve. SigV4 is ~120 lines
and is change-detected by `sigv4_test.go`.

## 3. The planner (`plan.go`) — the structural fix

```
rclone:  chunkSize fixed  ->  partCount = size/chunkSize  ->  streams = min(streams, partCount)
b2x:     partCount from size  ->  partSize = size/partCount
```

One rule: take as many parts as the object can supply at an 8 MiB floor, capped
at 128, then spread the bytes evenly. Because the count is the independent
variable, under-parallelizing a large object is arithmetically impossible.

| object | b2x | rclone effective @16 streams |
|---|---|---|
| 150 MB adapter | 18 parts | 3 |
| 555 MB env tarball | 66 | 9 |
| 4.9 GB model shard | 128 | 16 |
| 22 GB gemma monolith | 128 | 16 |

The 8 MiB floor is also what keeps every upload plan legal (S3's multipart
minimum is 5 MiB). `TestPlanRespectsFloorAndCap` caught a real bug here during
development: rounding the part count *up* produced 2 parts of ~4 MiB for objects
in (8, 16] MiB, which B2 would reject.

The global in-flight budget is `clamp(8 × NumCPU, 16, 192)` — the only number a
caller could want to change (`B2X_CONCURRENCY`), and no call site sets it.

## 4. Idempotence (`state.go`)

State lives in **one** file per destination root, `<dst>/.b2x/state.json` — not N
sidecars beside N weight files, which downstream globs (HF `from_pretrained`,
ninja, tar) would trip over.

Downloads land in `.b2x-partial-<name>` and are **renamed into place only after
every part lands**. That discipline is what makes the cheap size-only skip sound:
b2x writes ranges out of order into a preallocated file, so a torn download is
full-length on disk and indistinguishable from a complete one by size — but it
never reaches the final path.

The skip ladder:

1. right size at the final path + state agrees (ETag match) → **skip**
2. right size, no state → **adopt** (the upgrade path: a box whose weights were
   pulled by the old rclone code must not re-fetch 24 GB just to build a state
   file)
3. partial file + state describing this object → **resume the missing parts only**
4. otherwise → transfer

Objects b2x wrote carry `x-amz-meta-b2x-sha256`, so a later pull can verify
exactly; `--verify` makes that mandatory. Objects written by anything else are
size-verified only — the same standard as the rclone paths, never a regression.

## 5. Observability

Always on, three shapes, no flag to forget:

- **stderr** — a progress line every 15 s. Deliberately spelled with a
  `N MiB/s` token so `jobd.sh`'s existing `_last_mbps` regex still finds a rate
  in a b2x-written stats file.
- **`--stats-env FILE`** — sourceable `B2X_BYTES` / `B2X_SECS` / `B2X_MBPS` /
  `B2X_OBJECTS` / `B2X_SKIPPED_BYTES`. Field names match what `boot_mark
  pull_done` already publishes.
- **`--json`** — one JSON record per line, terminal `b2x_done`.

The train lane's `pull_done` / `pull_throughput` contract is **unchanged**: the
boot script computes its bytes from a local `du` of the pulled dirs, which is
transport-independent, so the host-scorecard join (on
`event==heartbeat, phase==pull_throughput`) keeps working untouched.

## 5b. Transfer guards (added 2026-08-03)

**b2x shipped with no time bound of any kind on a pull, and that was a
regression against the rclone it replaced.** `copyToOffset` looped on
`body.Read()` forever; `http.Client` carried a dial/TLS timeout but no
`Client.Timeout` (and none is possible — a legitimate 176 MiB part read takes
minutes); `--deadline` existed but only the eviction-trap flush ever
passed it, so every *pull* ran with no deadline at all. A peer that
completes the handshake, answers 200 with a Content-Length and then stops
sending — a shaped host under a retransmit storm, a half-open NAT mapping —
parked that goroutine for the life of the process: no error, no log line, no
exit. rclone defaults to `--timeout 5m` (IO idle) and `--contimeout 1m`, so the
migration had made the hot paths *less* bounded, not more.

Scope note (owner ruling 2026-08-02): stall detection for a **box** is a
control-plane function, because a wedged box is exactly the one that will not
answer a probe — fleetd owns that. This is the carved-out corollary: a
**transfer** bounding its own runtime so a slow or flaky host surfaces as a
named failure. Nothing here judges box health.

Three guards, all on by default, each disabling at 0:

| guard | knob (default) | what it catches | on fire |
|---|---|---|---|
| **stall** | `B2X_STALL_S` (120 s) | ONE flow of 128 goes silent while the rest are healthy — aggregate looks fine, the transfer just never converges | cancels that part's context and **retries the range** (`B2X_PART_TRIES`, 3) |
| **throughput floor** | `B2X_MIN_MBPS` (3) over `B2X_MIN_MBPS_WINDOW_S` (300 s) | everything moving, far too slowly to be worth a billed box | aborts, **exit 8** |
| **wall-clock ceiling** | derived; `B2X_DEADLINE_SLACK_S` (300), `B2X_MIN_DEADLINE_S` (900) | stall/burst that keeps every window average just above the floor forever | aborts, **exit 7** |

Design points worth keeping straight:

- **A stall RETRIES; it does not fail the transfer.** A ranged GET restarts at an
  exact byte offset, so one dead flow on a flaky host should cost a re-requested
  range. Bytes from a failed attempt are *uncounted* (`st.addBytes(-n)`) —
  leaving them in would make a flapping flow look FAST to the floor.
- **The ceiling is derived, not constant.** The same binary moves a 6 MB bundle
  and a 22 GB monolith. `bytes / B2X_MIN_MBPS + slack`, floored at
  `B2X_MIN_DEADLINE_S`. A transfer sustaining exactly the floor finishes
  *strictly inside* its ceiling, so the ceiling can only fire where the floor
  already should have — that is what makes it safe to leave armed
  (`TestAutoDeadlineIsDerivedNotConstant` pins the property). At the floor a
  22 GB pull gets ~2h05m; the same object moved at 341 MB/s on the fat-host
  ceiling test.
- **The floor is AGGREGATE and needs a FULL window**, mirroring
  `BOOT_MIN_MBPS`/`BOOT_MBPS_WINDOW_S`. Hosts shape per TCP flow, so one slow
  flow must not condemn. It arms only around the byte-moving phase, so LIST,
  the `--verify` sha256 pass, and an all-skipped idempotent re-pull cannot trip
  it.
- **Why 3 MB/s.** Every B2 pull ever measured from a rented box: 58–101 MB/s on
  1 Gbps hosts, up to 1008 MB/s on a fat one (§2/§2b above). 3 is ~19× under the
  slowest arm on the slowest box. It is also the number the owner named as the
  bad-host cut on 2026-08-02; we take 3 rather than the boot lane's 5 because
  this aborts work in progress while the boot watchdog kills an unbilled box.
- **Outcomes are distinguishable everywhere.** Exit **8** (slow) beside **7**
  (deadline) and **5** (transfer/stall); a verdict word on the final stderr
  line; `B2X_VERDICT` + `B2X_RETRIES` in `--stats-env`; `verdict` in `--json`.
  A timeout is never reported as a plain failure and never as success.

**Redistributed 2026-08-03** as `20260803-144801bf`
(`227ef0004562a3342b6d7fee2134c2acd7bc81c0de325f6103eaf772a499e259`), which is
also the first build carrying the push-side planned-extent fix (§6b). `LATEST`
points at it and both shim keys are stamped, so a fresh box now boots the
guarded binary. **No image rebake was needed** — see §6a for why that took a
ladder fix rather than a publish alone.

> An earlier revision of this section claimed the pinned train image "predates
> b2x entirely". **That was wrong**, and it mattered: the registry config history
> for that tag shows it was rebuilt `2026-08-01T02:44:34Z` with
> `COPY b2x /usr/local/bin/b2x`, carrying the `20260801-82727dd5` build. The
> claim is left here rather than deleted because it is exactly the assumption
> that would have made a publish look sufficient when it was not.

## 6. Distribution

### 6-0. Rung 0 of the PULL ladder: the CDN weights mirror (2026-08-27)

*Historical — describes the fleet this grew in (see herdd), not anything shipped
in this repo.*

`b2x_pull` tries the Cloudflare-fronted mirror **before** b2x, for sources under
`base-models/` only. It lives inside the wrapper, so no call site changed. Gate:
`B2_CDN_HOST`/`B2_CDN_BUCKET`/`B2_CDN_PREFIX` all set, a mappable `base-models/`
path, `cdn_pull.py` and `python3` present. Anything else — including a model not
mirrored yet (404 manifest), one lost chunk, or a flag with no manifest
equivalent such as `--exclude` — logs `cdn miss -> b2x (<reason>)` and falls
through to everything below, unchanged. Fail open, never closed.

Warm-edge throughput ties b2x-direct on a 6 Gb/s box (647.9 vs 614.8 MB/s, both
~80-85% of line rate); what it actually buys is that the bytes **never touch
B2**, so the pull stops contributing to the account-scoped `503 SlowDown` a heavy
fleet-wide pull raises for everyone. A COLD edge does not: it passes every miss
through and inherits the same rate limit. (Measured in an internal
cache-ceiling benchmark not carried into this repo.)

The one invariant: `cdn_pull.py` PREALLOCATES its destinations, so the CDN pull
goes into a temp sibling directory and is renamed across only on exit 0 with
every chunk sha1-verified. Written in place, a failed pull would leave full-size
holey files that **both** fallbacks skip — b2x preallocates identically, rclone
size-compares — and the box would run on zero-filled weights with every layer
reporting success. `cdn_pull.py` ships beside `b2x_boot.sh` on every channel in
§6 below, plus a bounded last-resort `rclone` fetch of `eval-env/cdn_pull.py`.

*Historical — the bake/boot ladder below belongs to the fleet this grew in (see
herdd); this repo ships only `publish.sh` and the binary it uploads.*

Chicken-and-egg: on-box scripts need the transport before they have one. Two
complementary channels:

- **Baked** into the train-env image at `/usr/local/bin/b2x` (a 6.3 MB layer).
  A zero-byte stub — meaning `bake.sh` found no Go toolchain — is deleted during
  the build so `command -v b2x` stays false and the ladder takes over rather than
  finding a broken binary.
- **Published** to `b2:<bucket>/tools/b2x/` and fetched by
  `onstart/b2x_boot.sh`'s `b2x_ensure`, which walks: **already-present → rclone →
  python3+SigV4 → give up**.

Rung 2 is what covers boxes already in the field: they all have a configured
`[b2]` remote, so they pick b2x up with **no image rebake and no relaunch**.
Using rclone here is not circular — it moves 6.3 MB once, not the multi-GB path.

There is no curl-only rung: the bucket is private, and Backblaze's **native**
`b2_authorize_account` API is unusable for our keys (verified 2026-08-01, both
v2 and v3 return `bad_request: not currently supported on API version number N`).

### 6a. Rung 1 is version-aware, and has to be

*Historical — describes the fleet this grew in (see herdd); what survives here is
the version-stamping `publish.sh` does, noted in present tense below.*

The two channels above are not independent: **baked beats published**, because
rung 1 is checked first. So for as long as rung 1 accepted any working binary,
the image silently won every conflict and rung 2's promise ("no image rebake
and no relaunch") held only for boxes whose image had *no* b2x. Since the
pinned train image bakes one, that was every box that matters, and a 6.5 MB
binary swap would have cost a full rebake of the one image train, serve and
eval all ride.

Rung 1 therefore accepts a present binary only when its `b2x version` equals
the version the shim asks for:

- `B2X_REQUIRE_VERSION` is **empty in the repo copy** (accept anything — the
  historical behavior, and what a baked-in shim keeps) and **stamped by
  `publish.sh`** into the copy it uploads, so shim and binary always travel as
  a matched pair. `B2X_VERSION` still overrides both.
- A mismatched candidate is **demoted, never discarded**. `fallback` holds the
  best present binary, and every failure path below returns to it — no
  credentials, no rclone, no python3, unwritable install dir. A stale b2x beats
  no b2x (the caller's alternative is single-flow rclone), so an upgrade that
  cannot happen never costs the box its transport.
- An exact required version is also what rung 2/3 **fetch**, so the common
  upgrade does not read `LATEST` at all.

`publish.sh` writes the stamped shim to **every** key a boot lane might read it
from (`tools/b2x/b2x_boot.sh` plus anything in `B2X_BOOT_SHIM_KEYS`). The extra
keys are not redundant: if a boot script reads an env-companion copy **first**,
publishing only `tools/b2x/` leaves the stale copy winning on the train lane — a
deploy that reports success and changes nothing. Whatever bakes that companion
env must re-stamp from the live `tools/b2x/LATEST` for the mirror-image reason:
shipping the unstamped repo copy would un-deploy b2x on the next env bake.

*[Present tense, this repo: the shim step is **opt-in**. `publish.sh` stamps and
publishes a shim only when `B2X_BOOT_SHIM` points at a shim file — to
`tools/b2x/b2x_boot.sh` plus every key listed in `B2X_BOOT_SHIM_KEYS` — and
prints "publishing binary + LATEST only" when it is unset. Under `--keep-latest`
the shim is skipped entirely, since that flag means boxes keep bootstrapping the
previous version and a fresh stamp would override exactly that. The
publish-to-every-key reasoning above is why `B2X_BOOT_SHIM_KEYS` exists: name
every key your boot lane might read the shim from, or the copy read first wins.]*

Verified 2026-08-03 without renting anything, against real B2, in the exact
field shape (stale `20260801-000bf1ba` at `/usr/local/bin/b2x`, no
`/workspace/bin/b2x`): both the rclone rung and — in a container with no rclone
at all — the python3+SigV4 rung detected the stale binary, installed
`20260803-144801bf` to `/workspace/bin/b2x` at a sha256 equal to the local
build, and reported `ceiling … floor 3.00 MB/s over 5m0s, stall 2m0s` on the
next transfer. With no stamp, both shims behave identically across all five
ladder states.

### 6b. Push hashes the PLANNED extent, not "the file"

`it.size` is captured when push walks the tree, and `planParts`/`partRange` are
committed to it. `uploadOne` used to hash the file open-ended — and, on the
single-PUT arm, `io.ReadAll` it open-ended too. Those agree only for a
quiescent file. For an actively-appended one, which is precisely what the
checkpoint lane ships:

- **multipart** uploaded exactly `it.size` bytes while `x-amz-meta-b2x-sha256`
  covered `it.size+delta`, so `pull --verify` would **reject a correct
  prefix**. Inert only because `jobd` passes no `--verify`.
- **single-PUT** uploaded `size+delta`, disagreeing with the plan and with
  push's own size-equality skip check on the next run.

Both arms are now bounded to `it.size`. The shrink direction was worse: a short
`ReadAt` was swallowed by `rerr != io.EOF` and the zero tail of the buffer went
up as content — a corruption the recorded sha256 could not catch, since it came
from an independent read. Short reads are now a named error on both arms, and
the tests were confirmed RED against the pre-fix `push.go`.

Publishing is immutable per version with `LATEST` written **last**, so a box can
never resolve a `LATEST` pointing at a partially-uploaded binary. `publish.sh`
dogfoods b2x to upload itself, then re-pulls and compares checksums.

## 7. Backward compatibility

*Historical — the call-site migration happened in the fleet this grew in (see
herdd); nothing in this repo shells out to rclone.*

Every migrated call site is one line whose `||` fallback is the **pre-existing
rclone command, unchanged and visible**:

```bash
b2x_pull "$B2/jobs/$jobid/checkpoints/" "$run/" \
  || rclone copy --fast-list "$B2/jobs/$jobid/checkpoints/" "$run/" 2>/dev/null \
  || log "job $jobid: checkpoint pull-back failed"
```

`b2x_pull`/`b2x_push` return 1 — sending the caller to rclone — when b2x is
unavailable, when credentials are absent, when `B2X_DISABLE=1`, **and when a b2x
transfer fails**. Failures are logged loudly (the line lands in `onstart.log`,
pushed to B2 every 45 s) so a b2x bug cannot hide behind a silent fallback.

This was validated on the real `jobd` inside the real train image via
`rehearse.sh --image`. That image predates b2x — precisely the STALE-IMAGE field
condition — and the job passed with the fallback engaged.

## 8. Usage

```bash
b2x pull <b2-path> <local>    # idempotent, resumable, parallel
b2x push <local> <b2-path>    # multipart, newest-first, --deadline aware
b2x ls|cat|stat <b2-path>
b2x selftest                  # credentials + multipart round-trip + idempotence
```

`<b2-path>` accepts the bare key or the `b2:$B2_BUCKET/...` spelling the existing
`$B2` variables carry, so a migrated line keeps its original variable.

Exit codes: `0` ok · `2` usage/config · `3` auth · `4` not-found · `5` transfer ·
`6` integrity · `7` deadline · `8` slow (under the throughput floor, §5b).

## 9. Not done (deliberate)

*Historical — the un-migrated sites are in the fleet this grew in (see herdd);
only the "no adaptive concurrency ramp" bullet is about this repo's code.*

- **~40 lower-impact rclone sites remain** (`b2_sync.sh`, `serve_vllm.sh`,
  `stage_run.sh`, `farm_*`, `chain-mining`, small `rcat`/`lsf` metadata calls).
  Small-object and control-plane calls get nothing from b2x; the metadata calls
  (`rcat` of a STATUS marker, `lsf` of a prefix) are latency-bound, not
  flow-bound. Migrating them is churn without benefit.
- **`b2_sync.sh` still owns rclone remote config** — b2x reads credentials from
  env directly and does not need `rclone.conf`, but the `[b2]`/`[b2w]` remotes
  are still what the fallbacks use, so that file stays.
- **No adaptive concurrency ramp.** The budget is computed, not measured. A
  probe-and-ramp would be strictly better on very fat hosts but adds a failure
  mode; the `--json` throughput records are being collected first so the
  question can be settled with fleet data rather than a guess.
- **The env-companion copy of `b2x_boot.sh` is not staged yet.** The env bake
  ships it as a companion from the next build; until then the boot script falls
  back to `tools/b2x/b2x_boot.sh`, which `publish.sh` already wrote.
