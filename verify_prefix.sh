#!/usr/bin/env bash
# Verify every object under a B2 prefix against a local tree, byte-for-byte.
#
# `b2x push` exiting 0 says the bytes left this box, not that they arrived
# intact — and `b2x ls` only shows sizes, which a truncated-then-repadded object
# would pass. This closes that gap without downloading what it does not have to:
#   * single-part objects  -> local MD5 == S3 ETag (the ETag IS the content MD5)
#   * multipart objects    -> stream the whole object and compare sha256
#     (a multipart ETag has an `-N` suffix and is not a content hash)
#
# Usage: ./verify_prefix.sh <b2-prefix> <local-dir> <out.tsv>
# Reads B2_* from the environment (`set -a; . ./.env; set +a`), like b2x itself.
# Output columns: verdict, relative path, bytes, local digest, remote digest.
# Verdicts: OK_MD5 OK_SHA256 MISMATCH_MD5 MISMATCH_SHA256 NO_ETAG MISSING
set -u

PREFIX="${1:?usage: $0 <b2-prefix> <local-dir> <out.tsv>}"
SRC="${2:?usage: $0 <b2-prefix> <local-dir> <out.tsv>}"
OUT="${3:?usage: $0 <b2-prefix> <local-dir> <out.tsv>}"
B2X="${B2X:-$(dirname "$0")/b2x}"
PREFIX="${PREFIX%/}"

[ -x "$B2X" ] || { echo "no b2x at $B2X (it is a build artifact, not in git)" >&2; exit 2; }

: > "$OUT"
"$B2X" ls "$PREFIX/" 2>/dev/null | grep -v '^b2x:' | while read -r size key; do
  rel="${key#"$PREFIX"/}"
  if [ ! -f "$SRC/$rel" ]; then
    printf 'MISSING\t%s\t%s\t-\t-\n' "$rel" "$size" >> "$OUT"
    continue
  fi
  etag=$("$B2X" stat "$PREFIX/$rel" 2>&1 | awk '$1=="etag"{print $2}')
  case "$etag" in
    "")   printf 'NO_ETAG\t%s\t%s\t-\t-\n' "$rel" "$size" >> "$OUT" ;;
    *-*)  # multipart: the ETag is a hash of part hashes, so stream and compare
          rsha=$("$B2X" cat "$PREFIX/$rel" 2>/dev/null | sha256sum | cut -d' ' -f1)
          lsha=$(sha256sum "$SRC/$rel" | cut -d' ' -f1)
          [ -n "$rsha" ] && [ "$rsha" = "$lsha" ] && v=OK_SHA256 || v=MISMATCH_SHA256
          printf '%s\t%s\t%s\t%s\t%s\n' "$v" "$rel" "$size" "$lsha" "$rsha" >> "$OUT" ;;
    *)    lmd5=$(md5sum "$SRC/$rel" | cut -d' ' -f1)
          [ "$lmd5" = "$etag" ] && v=OK_MD5 || v=MISMATCH_MD5
          printf '%s\t%s\t%s\t%s\t%s\n' "$v" "$rel" "$size" "$lmd5" "$etag" >> "$OUT" ;;
  esac
done

echo "--- $(wc -l < "$OUT") objects ---"
cut -f1 "$OUT" | sort | uniq -c
grep -q 'MISMATCH\|MISSING\|NO_ETAG' "$OUT" && exit 1
exit 0
