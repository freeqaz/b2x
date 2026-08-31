# b2x

A single static Go binary that moves artifacts between Backblaze B2 (S3 API)
and a rented GPU box — fast, idempotently, and with no tuning knobs to get
wrong.

**Status: alpha.** Battle-used, not battle-polished. This code spent months
moving multi-GB model weights, checkpoints and environment tarballs on and off
rented spot instances, but it was recently extracted from a private research
monorepo — expect placeholder names where private infrastructure used to be,
and design docs that still read like internal records.

## Why this exists

Throughput to a rented box is `flows × per-flow-rate`, because hosts
traffic-shape **per TCP flow** (~4–16 MB/s measured). So parallelism is the
whole game — and rclone made it surprisingly hard to get:

- **The tuned convention could not be shared.** The flag set lived as a bash
  array in one boot script, and a bash array cannot be exported to a child
  process. Every call site re-invented the pull, and most ran stock.
- **Even the tuned sites were silently clamped.** rclone caps
  `--multi-thread-streams` at `ceil(size / --multi-thread-chunk-size)`, and
  nothing set that chunk size. Asking for 16 streams on a 555 MB object got
  **9 flows**; a 150 MB adapter asking for 16 got 3.
- **Two interacting concurrency dimensions** (`--transfers` across files ×
  `--multi-thread-streams` within a file) meant there was no single number to
  set correctly.
- **Tool-version skew.** `rclone.org/install.sh` once silently installed rclone
  1.53 (2020) when `unzip` was missing — a client that cannot multi-thread at
  all.

b2x inverts the derivation: part **count** comes from the object size, part
**size** is derived, and all parts schedule against one global in-flight
budget. Under-parallelizing a large object is arithmetically impossible. On the
same 555 MB object that rclone gave 9 flows, b2x got **68**, with zero knobs.
It is `CGO_ENABLED=0`, stdlib-only (including a hand-rolled SigV4 signer), so
there is no module graph and no runtime dependency to skew.

Full design record, including the measurements and the transfer guards:
[docs/DESIGN.md](docs/DESIGN.md).

## Install

```sh
go build -o b2x .
# or a static, stamped release build:
./publish.sh build     # -> dist/b2x-<ver>-linux-amd64
```

## Quick start

Configuration is entirely environmental — there is no config file:

```sh
export B2_BUCKET=example-runs-bucket
export B2_KEY_ID=...              # bucket-wide read key
export B2_APPLICATION_KEY=...
export B2_REGION=us-west-004      # endpoint is derived; defaults to us-west-004

# optional: a prefix-scoped write key, preferred for uploads when present
export B2_WRITE_KEY_ID=...
export B2_WRITE_APPLICATION_KEY=...

b2x selftest                          # credentials + a real multipart round-trip
b2x pull base-models/my-model /data/  # idempotent, resumable, parallel
b2x push /data/checkpoints/ jobs/123/checkpoints/
b2x ls   jobs/123/
b2x stat jobs/123/checkpoints/latest.safetensors
b2x cat  jobs/123/STATUS
```

Paths may be given bare (`base-models/my-model`) or in the rclone spelling
(`b2:$B2_BUCKET/base-models/my-model`), so a migrated shell call site keeps its
existing variable verbatim. Flags go **between the command and the paths**:
`b2x pull --dry-run src dst/`.

### Behaviour worth knowing

- **Pulls are idempotent.** Re-pulling a present tree moves 0 bytes; an
  interrupted pull resumes only its missing parts; a tree fetched by some other
  tool is adopted on a size match rather than re-downloaded. State is one
  `<dst>/.b2x/state.json`, not sidecars beside every file.
- **Downloads are atomic.** Parts land in `.b2x-partial-<name>` and are renamed
  into place only once every part is present, so a torn download can never be
  mistaken for a complete one.
- **Pushes are newest-first and deadline-aware**, so a flush cut short by a spot
  eviction leaves a prefix of the newest files fully uploaded rather than N torn
  ones. Objects b2x writes carry sha256 metadata that `pull --verify` checks.
- **Observability is always on** — a stderr progress line, `--stats-env FILE`
  for sourceable `B2X_BYTES`/`B2X_MBPS`/…, and `--json` for one record per line.
- **Guards are on by default** and each disables at `0`: a per-part stall
  detector that retries the range, an aggregate throughput floor, and a
  wall-clock ceiling derived from the bytes to move.

## Configuration reference

Credentials and endpoint (only the first three are required):

| variable | meaning |
|---|---|
| `B2_BUCKET` | the bucket |
| `B2_KEY_ID` / `B2_APPLICATION_KEY` | read key (typically bucket-wide) — used by `pull`/`ls`/`cat`/`stat` |
| `B2_REGION` | default `us-west-004` |
| `B2_S3_ENDPOINT` | default `https://s3.<region>.backblazeb2.com` |
| `B2_WRITE_KEY_ID` / `B2_WRITE_APPLICATION_KEY` | optional prefix-scoped write key, preferred for `push` |
| `B2_PUBLISH_KEY_ID` / `B2_PUBLISH_APPLICATION_KEY` | optional third grant; pushes under `checkpoints/` prefer it |

Tuning knobs — concurrency is computed from object size and CPU count; there is
deliberately no `--transfers`/`--streams` flag:

| variable | default | meaning |
|---|---|---|
| `B2X_CONCURRENCY` | `clamp(8 × NumCPU, 16, 192)` | global in-flight part budget (debugging override) |
| `B2X_STALL_S` | `120` | no bytes on ONE part for this long → retry that part |
| `B2X_PART_TRIES` | `3` | attempts per stalled part before the transfer fails |
| `B2X_MIN_MBPS` | `3` | aggregate throughput floor (also derives the ceiling) |
| `B2X_MIN_MBPS_WINDOW_S` | `300` | the floor must be under water for a FULL window |
| `B2X_DEADLINE_SLACK_S` | `300` | non-byte-bound allowance in the derived ceiling |
| `B2X_MIN_DEADLINE_S` | `900` | the derived ceiling is never shorter than this |
| `B2X_SELFTEST_PREFIX` | `_b2x_selftest` | scratch base for `selftest`; point it inside the granted prefix when your write key is namePrefix-scoped |

Exit codes are stable and distinct so a shell caller can branch: `0` ok · `2`
usage/config · `3` auth · `4` not-found · `5` transfer · `6` integrity · `7`
deadline · `8` slow (under the throughput floor).

The full reference — per-command semantics, filter globs, grant routing, guard
behaviour, `--stats-env`/`--json` field lists, on-disk state — is
[docs/USAGE.md](docs/USAGE.md).

## Extras

- `verify_prefix.sh <b2-prefix> <local-dir> <out.tsv>` — verify every object
  under a prefix against a local tree byte-for-byte. `push` exiting 0 says the
  bytes left the box, not that they arrived intact; single-part objects are
  checked against their ETag (which is the content MD5), multipart objects by
  streaming and comparing sha256.
- `publish.sh` — build a static binary and publish it to your own bucket for box
  bootstrap, immutable per version with `LATEST` written last. It dogfoods b2x
  to upload itself, then re-pulls and compares checksums.

## Tests

```sh
go test ./...
```

The S3 surface b2x needs is small (ListObjectsV2, HeadObject, ranged
GetObject, and the three multipart calls), and it is covered by an in-process S3
server in `transfer_test.go` plus a real round-trip in `b2x selftest`.

## Related

b2x grew alongside [herdd](https://github.com/freeqaz/herdd), a vast.ai control
CLI and fleet-supervision daemon extracted from the same monorepo. The design
docs here reference its boot scripts and job daemon by name. b2x has no
dependency on it and works fine on its own.

## License

MIT — see [LICENSE](LICENSE).
