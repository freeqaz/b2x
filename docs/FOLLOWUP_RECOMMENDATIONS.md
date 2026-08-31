# What else deserves the b2x treatment — prioritized, for owner decision

Written 2026-08-01 alongside the b2x landing (see [DESIGN.md](DESIGN.md)). These
are observations from auditing every rclone call site in the fleet tooling that
b2x was built for — now public as [herdd](https://github.com/freeqaz/herdd) —
and none of P1–P5 is actioned. **Nothing here proposes rewriting `jobd.sh` /
`train.sh` / the control CLI into Go** — that was explicitly out of scope and I
do not think it is the right next move either (see P5).

> Preserved as the design record from the private monorepo b2x grew in. The
> script names in P1–P5 are herdd's; internal-only docs it cited were not
> extracted and are described inline instead.

The through-line: b2x fixed one instance of a general pattern — **a convention
that exists only as prose or as an un-exportable shell fragment, applied by hand
at N sites, drifting at most of them.** The remaining items are other instances.

P0 was added later, on 2026-08-27, and is the one item about code that lives in
**this** repo; it leads. P1–P5 are the original memo and concern herdd-side shell
scripts.

---

# Applies to this repo

## P0 — `b2x selftest` cannot pass on ANY scoped box (2026-08-27)

Found while building the scoped-key proof for the publish migration, and
measured, not inferred.

`selftest.go` writes its round-trip under `_b2x_selftest/<pid>-<nanos>/` using
`cfg.writeCred`. On a real box that credential is the minted `-rw` key, scoped
to ONE namePrefix (`jobs/` on a jobs box), and the third grant is scoped to
`checkpoints/`. `_b2x_selftest/` is under neither, and the read key
cannot write. So the write leg 403s by construction.

Measured against freshly minted real keys (read `-ro`, write `-rw`
namePrefix=`jobs/`, publish `-pub` namePrefix=`checkpoints/`):

```
b2x selftest -> exit 3
  ok  list (signing + read credentials)
  FAIL multipart upload: POST /…/_b2x_selftest/20260827-744564/roundtrip.bin:
       HTTP 403 AccessDenied: not entitled
```

It fails honestly rather than silently — the hint correctly says the write key
is namePrefix-scoped — but the on-box preflight the file's own header advertises
("the $0 check before committing to a multi-GB pull") has never been runnable
where it matters. It passes only on a workstation's unrestricted key, which is
where it has always been run.

Two further gaps in the same place:

- **`selftest` never exercises the publish grant at all.** It builds clients
  from `readCred` and `writeCred` only, so the credential a training run's
  publish stage depends on is the one grant no preflight touches. That is the
  key whose absence 403'd both arms of a training run AFTER training completed.
- **b2x cannot learn the write prefix.** `B2_PUBLISH_PREFIX` and the write
  prefix are both workstation-side mint inputs; the launcher's B2-env shipper
  ships only key IDs and secrets. So the fix is not local to `selftest.go` — it
  needs the launcher to ship the granted prefixes (say `B2_WRITE_PREFIX` /
  `B2_PUBLISH_PREFIX`) and the selftest to write inside one of them, testing
  each grant it was actually given. That is a launcher change, hence a decision
  rather than a patch.

**Effort:** ~2 h including the launch-spec change and its tests. **Risk:** low,
but it touches the FROZEN shape of the launcher's B2-env return value, so read
that first.

**Addendum 2026-08-31 (this repo).** The standalone half is now fixed:
`B2X_SELFTEST_PREFIX` lets the operator place the selftest scratch inside the
prefix their write key is actually granted — `B2X_SELFTEST_PREFIX=jobs/_b2x_selftest`
against a `namePrefix=jobs/` write key — so the round-trip runs where it matters
without any launcher change. The prefix is operator-supplied, not discovered, so
the second gap above stands unchanged: b2x still cannot learn its granted
prefixes, and **`selftest` still builds clients from `readCred`/`writeCred` only
and never exercises the publish grant.**

---

# Historical — the fleet tooling this grew alongside (now herdd)

P1–P5 are the original 2026-08-01 memo. They audit shell scripts that stayed in
the private monorepo and are now public as
[herdd](https://github.com/freeqaz/herdd); none is actionable from this repo.
They are kept because the reasoning — and the measurements behind it — is the
value.

## P1 — `b2_sync.sh` now disagrees with the convention it predates

`b2_sync.sh` sets its own transfer defaults (`B2_TRANSFERS=8`, `B2_MT_STREAMS=8`,
`B2_MT_CUTOFF=128M`) — the *old* pre-2026-07-09 numbers. `train.sh`'s `RC_FAST`
says 16/16/64M. `fetch_eval_env.sh` says 16/16/64M. `rehydrate_train_env.sh` says
8 streams. Four files, three opinions, and `b2_sync.sh` is the one most likely to
be reached for by new code because it is the documented entry point.

Now that b2x owns the high-impact paths, `b2_sync.sh`'s numbers only govern the
*fallback* — but a fallback that is half-speed is a bad fallback, and the
disagreement is exactly the drift b2x was built to end.

**Cheapest fix:** make `b2_sync.sh push/pull` try `b2x` first (it already has the
credentials in env), same one-line `||` pattern as the migrated sites. Second
best: align its constants to 16/16/64M **and add `--multi-thread-chunk-size 8M`**,
without which the streams are clamped exactly as documented in
[DESIGN.md](DESIGN.md) §1.

**Effort:** ~1 hour. **Risk:** low — it is the same fallback shape already proven.

## P2 — The rclone bootstrap ladder is copy-pasted in ≥5 files

The same `curl .deb → install.sh → apt-get` block appears verbatim in
`b2_sync.sh`, `fetch_eval_env.sh`, `eval_sidecar.sh`, `train_boot.sh`, and
`jobd_boot.sh`. This is the block whose recorded failure mode silently
installed rclone 1.53, which cannot multi-thread at all and capped the fleet at
~73 MB/s until someone noticed.

Five copies means five places a version guard has to be added, and today **none
of them checks the installed version**. `train.sh` records it in the
`rclone_ready` boot mark — *after* the fact, for forensics, not as a gate.

**Recommendation:** one `onstart/ensure_rclone.sh` with a hard minimum version
(≥1.64, the first release that can multi-thread), sourced by all five. Now that
b2x carries the multi-GB paths this is less urgent than it was, but rclone is
still the fallback and a 2020 client makes that fallback nearly useless.

**Effort:** ~2 hours. **Risk:** low, mechanical.

## P3 — `eval_sidecar.sh` and `fetch_eval_env.sh` are two implementations of one job

Both fetch `eval-env/env-<ver>.tar.zst`, verify its sha256 against the manifest,
unpack to `/workspace/eval`, and self-heal the venv. `fetch_eval_env.sh` says in
its own header that it was factored out of `eval_sidecar.sh` "~L181-293" — but
the original was left in place rather than being switched over to call it.

The tuning drift between them was one of b2x's motivating examples (one stock,
one tuned, same artifact) and is now moot, but the *duplication* is not: any
future change to env acquisition has to land twice, and the sha256 verification
logic in particular is exactly the kind of code that must not diverge.

**Recommendation:** make `eval_sidecar.sh` call `fetch_eval_env.sh` and delete
its inline copy. Guarded by `test_eval_job_lib.py` / `test_job_serve.py`.

**Effort:** ~half a day, mostly re-reading the two paths carefully.
**Risk:** medium — `eval_sidecar.sh` is on the serve/eval boot path and its
write-gate interacts with scoped keys.

## P4 — There is no guard against transfer-flag drift coming back

`test_path_refs.py` exists because a moved script is a box-side failure that
local tests would not catch. Transfer-flag drift is the same shape of bug — it
fails on a rented box, silently, as slowness rather than an error — and has no
equivalent guard.

**Recommendation:** a `test_transfer_sites.py` asserting that any
`rclone copy|copyto|sync` in the fleet tooling on a path known to carry multi-GB
artifacts (`base-models/`, `checkpoints/`, `eval-env/`, `runsets/`, `artifacts/`,
`jobs/*/checkpoints/`) is either (a) preceded by a `b2x_pull`/`b2x_push`
alternative on the same logical line, or (b) explicitly allowlisted with a
reason — the `_KNOWN_ABSENT` pattern that file already uses.

This is what keeps the migration from silently eroding as new runsets are
written. **I think this is the highest-leverage item in this memo per unit of
effort**, because it converts "we fixed the sites" into "the sites stay fixed."

**Effort:** ~2 hours. **Risk:** none (a test).

## P5 — What I do NOT recommend

- **Rewriting `jobd.sh` in Go.** It is ~2100 lines of genuinely intricate bash
  (GPU assignment, flock, handoff epochs, two-writer fences, event emission) and
  it *works*. The b2x case was strong because the transfer layer had a
  well-specified, narrow interface and a bug class that shell could not express
  correctly. `jobd`'s logic has neither property: no clean seam, and no bug class
  that Go would make impossible. A rewrite would be a large regression surface
  for a legibility win.
- **Moving off B2.** The earlier transport-measurement campaign's analysis still
  holds, and b2x strengthens it — the constraint was always flow count, and B2
  served 8 Gbps to a fat host. Revisit only on that campaign's stated triggers.
- **Sharding the 22 GB single-file model.** That same campaign listed this as a
  last resort worth ~3×. b2x already takes that object from 16 flows to
  128, so re-measure before spending effort on re-sharding — the ceiling test
  that motivated it was run against a 16-stream client.

---

**Suggested order if any of this is taken up (herdd-side):** P4 (guard the win),
then P1 (cheap, removes the last contradicting default), then P2, then P3.
