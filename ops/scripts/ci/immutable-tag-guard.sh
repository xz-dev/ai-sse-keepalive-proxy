#!/usr/bin/env bash
# Fail-closed immutable-tag guard. Exits 0 ONLY when the reference is
# proven absent from the registry. Existing tag, auth failure, network
# error, malformed output or unknown status all refuse (non-zero exit).
set -euo pipefail

if [ "$#" -ne 1 ] || [ -z "$1" ]; then
  echo "usage: immutable-tag-guard.sh <image-ref>" >&2
  exit 2
fi
ref="$1"

lookup_error="$(docker buildx imagetools inspect "$ref" 2>&1)" && {
  echo "immutable-tag-guard: tag already exists, refusing to overwrite: $ref" >&2
  exit 1
}
lookup_status=$?

case "${lookup_status}:${lookup_error}" in
  "1:ERROR: ${ref}: not found"|"1:ERROR: ${ref}: manifest unknown")
    echo "immutable-tag-guard: tag not found, safe to publish: $ref"
    exit 0
    ;;
  *)
    printf '%s\n' "immutable-tag-guard: refusing (status ${lookup_status}): ${lookup_error}" >&2
    exit "$lookup_status"
    ;;
esac
