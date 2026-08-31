#!/usr/bin/env bash
# publish.sh — build the static b2x binary and publish it to B2 for box bootstrap.
#
#   ./publish.sh build          # build only -> dist/ (no network)
#   ./publish.sh publish        # build + test + upload + move LATEST
#   ./publish.sh publish --keep-latest   # upload but do NOT move LATEST
#
# Publishing writes three objects under tools/b2x/ :
#   b2x-<ver>-linux-amd64          the static binary
#   b2x-<ver>-linux-amd64.sha256   its checksum (the box-side shim verifies this)
#   LATEST                         the version boxes bootstrap by default
#
# Objects are IMMUTABLE per version (the version carries the git rev), so a
# publish never overwrites a binary a live box might be mid-download of. Only
# LATEST is mutable, and it is written last — so a box can never resolve a
# LATEST that points at a binary which is not fully uploaded yet.
#
# Env: reads ./.env for B2 credentials, then the environment.
# Optional: B2X_BOOT_SHIM=/path/to/b2x_boot.sh — the box-side bootstrap shim to
# stamp and publish alongside the binary (herdd ships one at
# tools/vast/onstart/b2x_boot.sh). Unset means binary + LATEST only.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$HERE"
DIST="$HERE/dist"

# Version = date + short git rev (+ -dirty). The rev is what makes a published
# binary traceable back to the source that built it.
git_rev() { git -C "$REPO_ROOT" rev-parse --short=8 HEAD 2>/dev/null || echo unknown; }
git_dirty() { git -C "$REPO_ROOT" diff --quiet -- "$HERE" 2>/dev/null || echo "-dirty"; }
VERSION="${B2X_VERSION:-$(date -u +%Y%m%d)-$(git_rev)$(git_dirty)}"

build() {
  mkdir -p "$DIST"
  local out="$DIST/b2x-${VERSION}-linux-amd64"
  echo ">> building b2x ${VERSION} (static, CGO_ENABLED=0, linux/amd64)"
  ( cd "$HERE" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
      go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o "$out" . )
  sha256sum "$out" | awk '{print $1}' > "$out.sha256"
  # A binary that is not static would fail to run on a slim box image, so this
  # is a hard gate rather than a cosmetic check.
  if command -v file >/dev/null 2>&1 && ! file "$out" | grep -q "statically linked"; then
    echo "!! refusing to publish: $out is not statically linked" >&2; exit 1
  fi
  echo ">> $out"
  echo ">> $(cat "$out.sha256")  ($(du -h "$out" | cut -f1))"
  BIN="$out"
}

publish() {
  local keep_latest=0
  [ "${1:-}" = "--keep-latest" ] && keep_latest=1

  ( cd "$HERE" && go test ./... >/dev/null ) || { echo "!! go test failed — not publishing" >&2; exit 1; }
  echo ">> go test passed"
  build

  # shellcheck disable=SC1091
  [ -f "$REPO_ROOT/.env" ] && { set -a; . "$REPO_ROOT/.env"; set +a; }
  : "${B2_BUCKET:?publish: B2_BUCKET required}"

  # Dogfood: b2x publishes itself. If the freshly built binary cannot talk to
  # B2, we find out here rather than on a billed box.
  local base="tools/b2x/b2x-${VERSION}-linux-amd64"
  echo ">> uploading $base"
  "$BIN" push "$BIN" "$base"
  "$BIN" push "$BIN.sha256" "$base.sha256"

  # The box-side shim rides along, STAMPED with the version published above so
  # the pair can never drift: a box whose image bakes an older /usr/local/bin/b2x
  # sees the stamp, demotes the baked binary, and upgrades. Without the stamp a
  # baked binary wins rung 1 outright and no publish can ever reach the box.
  #
  # Opt-in: set B2X_BOOT_SHIM to the shim your boot lane actually reads. If your
  # boot scripts read the shim from more than one key, publish to ALL of them —
  # a stale copy on the key that is read FIRST wins, which is a deploy that
  # reports success and changes nothing. Extra keys go in B2X_BOOT_SHIM_KEYS.
  # Skipped under --keep-latest: that flag means "boxes keep bootstrapping the
  # previous version", and a stamped shim would override exactly that.
  local shim="${B2X_BOOT_SHIM:-}"
  if [ "$keep_latest" = 1 ]; then
    echo ">> --keep-latest: boot shim NOT re-published (boxes keep the current stamp)"
  elif [ -z "$shim" ]; then
    echo ">> B2X_BOOT_SHIM unset: publishing binary + LATEST only"
  elif [ -f "$shim" ]; then
    local sdir; sdir="$(mktemp -d)"
    sed "s|^B2X_REQUIRE_VERSION=.*|B2X_REQUIRE_VERSION=\"\${B2X_REQUIRE_VERSION:-${VERSION}}\"|" \
      "$shim" > "$sdir/b2x_boot.sh"
    grep -q "B2X_REQUIRE_VERSION:-${VERSION}" "$sdir/b2x_boot.sh" \
      || { echo "!! refusing to publish: could not stamp B2X_REQUIRE_VERSION into the shim" >&2; exit 1; }
    bash -n "$sdir/b2x_boot.sh" \
      || { echo "!! refusing to publish: stamped shim is not valid bash" >&2; exit 1; }
    echo ">> uploading b2x_boot.sh (stamped B2X_REQUIRE_VERSION=${VERSION})"
    for key in "tools/b2x/b2x_boot.sh" ${B2X_BOOT_SHIM_KEYS:-}; do
      "$BIN" push "$sdir/b2x_boot.sh" "$key"
    done
    rm -rf "$sdir"
  else
    echo "!! B2X_BOOT_SHIM=$shim does not exist" >&2; exit 1
  fi

  if [ "$keep_latest" = 1 ]; then
    echo ">> --keep-latest: LATEST unchanged (boxes keep bootstrapping the previous version)"
  else
    local tmp; tmp="$(mktemp -d)"; printf '%s\n' "$VERSION" > "$tmp/LATEST"
    "$BIN" push "$tmp/LATEST" "tools/b2x/LATEST"
    rm -rf "$tmp"
    echo ">> LATEST -> ${VERSION}"
  fi

  echo ">> verifying the published artifact round-trips"
  local vdir; vdir="$(mktemp -d)"
  "$BIN" pull "$base" "$vdir/b2x" >/dev/null
  local got want
  got="$(sha256sum "$vdir/b2x" | awk '{print $1}')"; want="$(cat "$BIN.sha256")"
  rm -rf "$vdir"
  [ "$got" = "$want" ] || { echo "!! published binary does not match local build" >&2; exit 1; }
  echo ">> PUBLISHED b2x ${VERSION}"
}

case "${1:-}" in
  build)   build ;;
  publish) shift; publish "$@" ;;
  *) grep -E '^#( |$)' "$0" | sed 's/^# \?//'; exit 2 ;;
esac
