# b2x usage reference

Everything here is derived from the code as of this commit. For the design
rationale and the measurements behind the defaults, see [DESIGN.md](DESIGN.md).

## Invocation

```
b2x <command> [flags] <args>
```

**Flags go between the command and the paths.** Argument parsing stops at the
first non-flag word (standard Go `flag` behaviour), so
`b2x pull --dry-run src dst` works and `b2x pull src dst --dry-run` prints
usage and exits 2.

| command | arguments | what it does |
|---|---|---|
| `pull`     | `<b2-path> <local-path>` | idempotent parallel download; skips present files, resumes partials |
| `push`     | `<local-path> <b2-path>` | multipart upload, newest-first, deadline-aware |
| `ls`       | `<b2-path>`              | list objects under a prefix (size + key per line) |
| `cat`      | `<b2-path>`              | stream one object to stdout |
| `stat`     | `<b2-path>`              | size, ETag, mtime, metadata, and the part plan for one object |
| `selftest` | —                        | prove credentials + a real multipart round-trip |
| `version`  | —                        | print the version stamped at build time |

### Flags

| flag | commands | meaning |
|---|---|---|
| `--include GLOB` | pull, push | only paths matching (repeatable) |
| `--exclude GLOB` | pull, push | skip paths matching (repeatable; an exclude always wins) |
| `--dry-run`      | pull, push | print the transfer plan (per-file verdicts and part counts) and exit 0 |
| `--deadline DUR` | pull, push | hard wall-clock budget (`40s`, `10m`); replaces the derived ceiling |
| `--verify`       | pull       | fail (exit 6) unless every transferred object's `b2x-sha256` metadata matches |
| `--min-age DUR`  | push       | skip files modified more recently than this |
| `--stats-env FILE` | pull, push | write sourceable `KEY=VALUE` stats on completion |
| `--json`         | pull, push, ls | machine-readable records on stdout |

## Paths

A `<b2-path>` is a key or prefix inside `$B2_BUCKET`. All of these are
equivalent:

```
base-models/my-model
/base-models/my-model
b2:$B2_BUCKET/base-models/my-model
```

The `b2:`, `b2w:`, `b2p:` and `b2eu:` remote spellings (the conventional rclone
remote names for the read, write, publish and EU remotes) are stripped, along
with the bucket name and any leading slash — so a shell line migrated from
rclone keeps its existing `$B2`-style variable verbatim. There is exactly one
bucket per invocation: `b2x` never routes between buckets by spelling.

## Credentials and grant routing

Configuration is entirely environmental — there is no config file.

| variable | required | meaning |
|---|---|---|
| `B2_BUCKET` | yes | the bucket |
| `B2_KEY_ID` / `B2_APPLICATION_KEY` | yes | **read** key (typically bucket-wide, read-only) |
| `B2_REGION` | no | defaults to `us-west-004` |
| `B2_S3_ENDPOINT` | no | defaults to `https://s3.<region>.backblazeb2.com`; `https://` is prepended if you omit the scheme |
| `B2_WRITE_KEY_ID` / `B2_WRITE_APPLICATION_KEY` | no | prefix-scoped **write** key; when absent, writes use the read key |
| `B2_PUBLISH_KEY_ID` / `B2_PUBLISH_APPLICATION_KEY` | no | third grant scoped to `checkpoints/` |

All reads (`pull`, `ls`, `cat`, `stat`) use the read key. `push` picks its
credential from the **destination prefix**, once, before the transfer:

1. destination under `checkpoints/` and a publish grant is present → the
   **publish** grant;
2. destination under `checkpoints/` but no publish grant → the **write** grant
   (deliberate fall-through: on a box with a bucket-wide write key that write
   succeeds, and a 403 is the key's scope talking, not b2x);
3. anything else → the **write** grant (which is the read key when no
   `B2_WRITE_*` was supplied).

The chosen grant is named on stderr (`b2x: push … using the write grant`), and
a later 401/403 hint names the grant that was actually presented. There is
deliberately no retry with a wider credential: a 403 means the key's scope and
the destination disagree, and silently re-presenting a wider key would turn a
least-privilege boundary into a suggestion.

Why three keys: a B2 application key carries exactly one `namePrefix`, so a box
that writes `jobs/…` *and* publishes `checkpoints/…` needs two write-capable
keys (the only single-key alternative is a bucket-wide write key, strictly
worse).

## pull

```
b2x pull <b2-path> <local-path> [--include G] [--exclude G] [--verify] [--deadline DUR] [--dry-run]
```

- If `<b2-path>` names an **object**, this is a single-file pull and
  `<local-path>` is the destination *file* path (rclone `copyto` semantics).
  Otherwise the path is treated as a prefix and every object under it lands at
  `<local-path>/<relative-key>`.
- A 403 on the initial object probe is treated as "not an object", never as
  fatal — a prefix-scoped B2 key answers `HeadObject` outside its scope with
  403, not 404, and the subsequent LIST is the authoritative check.
- Objects are fetched **biggest first** (the long pole starts earliest), split
  into parts by the planner, and all parts across all objects drain one global
  worker budget.

Per-file skip ladder, in order:

1. file at the final path with the right size, and the state file agrees
   (ETag match) → **skip**;
2. right size, no state → **adopt** (a tree fetched by some other tool is not
   re-downloaded; a state entry is written so the next run gets the cheap path);
3. partial file plus state describing this object → **resume** only the missing
   parts;
4. otherwise → transfer.

Downloads are atomic: parts are written (in any order) into a preallocated
`.b2x-partial-<name>` file that is renamed into place only after every part
landed and was fsynced. A torn download never occupies the final path.

`--verify` requires the `b2x-sha256` object metadata to be present and to match
the downloaded bytes; an object not written by b2x fails with exit 6. Without
the flag, objects carrying the metadata are verified opportunistically and
everything else is size-checked only.

Failed or stalled parts retry at the same byte offset (see the guards below);
state is checkpointed every 10 s during the transfer, so even a `kill -9`
leaves a resumable tree.

## push

```
b2x push <local-path> <b2-path> [--include G] [--exclude G] [--min-age DUR] [--deadline DUR] [--dry-run]
```

- A **directory** source walks the tree; each file lands at
  `<b2-path>/<relative-path>`. b2x's own artifacts (`.b2x/` state,
  `.b2x-partial-*`) never ship.
- A **file** source with a `/`-terminated destination appends the basename;
  otherwise the destination key is taken literally (rclone `copyto` semantics).
- Files already on B2 **at the same size** are skipped (one LIST for the whole
  prefix, same standard as an rclone `copy`).
- Uploads run **newest-first**, one file at a time, each file's parts in
  parallel across the whole budget. With a `--deadline`, that ordering means an
  interrupted flush leaves a *prefix of the newest files fully uploaded* —
  never N torn ones. Multipart uploads only become visible at
  `CompleteMultipartUpload`, and a failed one is aborted so orphaned parts do
  not bill.
- One bad file does not abandon the rest of a flush; the first error becomes
  the exit status after everything else has been attempted.
- Every b2x-written object carries `x-amz-meta-b2x-sha256` (hash of exactly the
  planned bytes, so a source file still being appended to uploads a correct
  prefix) and `x-amz-meta-b2x-mtime`.

## ls / cat / stat

- `ls` prints `size key` per object to stdout and a `count, total-bytes`
  summary to stderr. `--json` emits `{"key":…,"size":…,"etag":…}` per line.
- `cat` streams one object to stdout.
- `stat` prints key, size, ETag, mtime, the part plan b2x would use, and every
  `x-amz-meta-*` value.

## selftest

```
b2x selftest
```

Four legs, in order, stopping at the first failure: a LIST with the read key
(signing + credentials, no writes), a 24 MiB multipart upload with the write
key (3 parts — CreateMPU/UploadPart/CompleteMPU all covered), a parallel
ranged-GET pull-back with sha256 verification, and an immediate re-pull that
must move zero bytes (idempotence).

The scratch prefix defaults to `_b2x_selftest/<date>-<pid>/`. **A
namePrefix-scoped write key cannot write there**, so the write leg 403s by
construction (an honest exit 3, but it says nothing about the box). Set
`B2X_SELFTEST_PREFIX` to place the scratch inside the granted prefix:

```sh
B2X_SELFTEST_PREFIX=jobs/_b2x_selftest b2x selftest
```

Selftest never deletes: keys handed to rented boxes are best minted without
`deleteFiles`, so the small scratch objects are left for a bucket lifecycle
rule to reap. It exercises the read and write grants only — the publish grant
is currently not covered.

## Filters

`--include`/`--exclude` take rclone-style globs matched against the path
relative to the transfer root:

- an **exclude always wins**; with no includes, everything not excluded ships;
  with includes, a path must match one;
- an unanchored pattern matches the full relative path **or the basename**
  (`--exclude STATUS` excludes a `STATUS` file at any depth);
- a **leading `/` anchors** at the transfer root and withdraws the basename
  fallback (`--include /adapter_config.json` matches only the root copy, not
  `checkpoint-40/adapter_config.json`);
- a trailing `/**` matches everything under a directory
  (`--exclude 'checkpoint-*/**'`);
- mixing includes and excludes is only well-defined when they cannot match the
  same path — rclone itself resolves an overlap by flag order, which b2x does
  not preserve. On an overlap b2x excludes (the cheaper error); see
  `TestMixedIncludeExcludeIsUndefinedInRclone`.

## Concurrency and the planner

There is no `--transfers`/`--streams` flag by design. Each object is split
into `min(size / 8 MiB, 128)` parts (floor 1) with the bytes spread evenly, so
part *size* is derived from part *count* — under-parallelizing a large object
is arithmetically impossible, which is the structural fix for the rclone
stream/chunk clamp described in [DESIGN.md](DESIGN.md) §1–3. All parts across
all objects share one global in-flight budget:

- `B2X_CONCURRENCY` — default `clamp(8 × NumCPU, 16, 192)`. A debugging
  override, not a tuning knob.

Every request retries internally with bounded exponential backoff + jitter
(6 attempts; 250 ms → 4 s): 5xx, 429 and 408 are transient, network errors are
retryable, and any other 4xx is terminal — retrying a 403 just burns deadline.

## Transfer guards

Three guards, all on by default, each disabled by setting its variable to `0`.
A malformed value falls back to the **default**, never to "off" — a typo must
not silently disarm a guard (a warning is printed).

| guard | knobs (defaults) | what it catches | on trigger |
|---|---|---|---|
| stall | `B2X_STALL_S=120`, `B2X_PART_TRIES=3` | one flow goes silent while the aggregate looks healthy | cancels that part and re-requests the same byte range; the transfer fails (exit 5) only after `B2X_PART_TRIES` attempts |
| throughput floor | `B2X_MIN_MBPS=3` over `B2X_MIN_MBPS_WINDOW_S=300` | everything moving, far too slowly to be worth a billed box | aborts with **exit 8** — a *host* verdict, not a transfer failure |
| wall-clock ceiling | derived; `B2X_DEADLINE_SLACK_S=300`, `B2X_MIN_DEADLINE_S=900` | stall/burst patterns that keep every window average just above the floor | aborts with **exit 7** |

Without an explicit `--deadline`, the ceiling is derived from the work:
`bytes-to-move / B2X_MIN_MBPS + B2X_DEADLINE_SLACK_S`, floored at
`B2X_MIN_DEADLINE_S`. A transfer sustaining exactly the floor finishes strictly
inside its ceiling, so the ceiling only fires where the floor already should
have. An explicit `--deadline` replaces the derived ceiling entirely. The floor
arms only around the byte-moving phase — LIST, the `--verify` pass, and an
all-skipped re-pull cannot trip it — and bytes from a failed part attempt are
*uncounted*, so a flapping flow cannot look fast.

The evidence behind each default number is in the comments on the constants in
`config.go` and in [DESIGN.md](DESIGN.md) §5b.

## Observability

Always on — there is no `--stats` flag to forget:

- **stderr** — a progress line every 15 s
  (`b2x: pull 42% 1.2GiB / 2.9GiB, 213.4 MiB/s, 30s elapsed, 128 streams`) and
  a one-line terminal summary. The `N MiB/s` token is a stable contract for
  legacy scrapers. On any non-ok outcome the last line carries the verdict
  word.
- **`--stats-env FILE`** — sourceable `KEY=VALUE` written on completion:
  `B2X_BYTES`, `B2X_SECS`, `B2X_MBPS`, `B2X_OBJECTS`, `B2X_SKIPPED`,
  `B2X_SKIPPED_BYTES`, `B2X_STREAMS`, `B2X_RETRIES`, `B2X_VERDICT`.
- **`--json`** — one JSON object per line on stdout: `b2x_progress` records
  every 15 s and a final `b2x_done`, with fields `event`, `op`, `src`, `dst`,
  `bytes`, `secs`, `mbps`, `objects`, `skipped`, `skipped_bytes`,
  `total_bytes`, `parts`, `retries`, `streams`, `verdict`.

`verdict` ∈ `ok` · `slow` (floor breach) · `timeout` (ceiling) · `stalled` ·
`error`, mirroring the exit codes.

## Exit codes

Stable and distinct so a shell caller can branch:

| code | meaning | typical remedy |
|---|---|---|
| 0 | ok | — |
| 2 | usage or missing configuration | fix the invocation / set `B2_BUCKET` and keys |
| 3 | 401/403 — bad or out-of-scope credentials | the hint names the grant used; check its scope |
| 4 | 404 — source object/prefix does not exist | check the path |
| 5 | transfer failed after retries (includes an exhausted stall guard) | retry; inspect the named part error |
| 6 | integrity — sha256 mismatch | re-transfer; investigate the writer |
| 7 | wall-clock ceiling expired with work outstanding (partial result on disk, resumable) | more budget: `--deadline`, or the `B2X_MIN_DEADLINE_S` / `B2X_DEADLINE_SLACK_S` knobs |
| 8 | sustained throughput under the floor | a *host* verdict: re-rent elsewhere, or lower `B2X_MIN_MBPS` |

## State on disk

One state file per destination root — `<dst>/.b2x/state.json` — holding size,
ETag and completed-part indices per file. Not N sidecars beside N weight
files, which downstream globs would trip over. In-flight downloads live in
`.b2x-partial-<name>` beside their final path. Both are excluded from `push`
automatically. Deleting `.b2x/` costs nothing but the cheap-skip metadata: the
next pull re-verifies by size and re-adopts.

## Helper scripts

- **`publish.sh`** — `build` produces a static, version-stamped
  `dist/b2x-<ver>-linux-amd64` (+ `.sha256`); `publish` additionally runs the
  tests, uploads binary + checksum under `tools/b2x/` in your bucket
  (dogfooding b2x itself), moves `LATEST` last (so a box can never resolve a
  half-uploaded binary), and verifies the published artifact round-trips.
  Versions are immutable (`<date>-<gitrev>`; override with `B2X_VERSION`).
  `--keep-latest` uploads without moving `LATEST`. Optionally set
  `B2X_BOOT_SHIM=/path/to/shim` to stamp and publish a box-side bootstrap shim
  alongside (plus every key in `B2X_BOOT_SHIM_KEYS`) — see
  [DESIGN.md](DESIGN.md) §6a. Reads `./.env` for credentials first.
- **`verify_prefix.sh <b2-prefix> <local-dir> <out.tsv>`** — byte-for-byte
  verification of every object under a prefix against a local tree:
  single-part objects by ETag (= content MD5), multipart objects by streaming
  sha256. Exit 1 on any `MISMATCH`/`MISSING`/`NO_ETAG` row. Point `B2X=` at
  the binary if it is not beside the script.
