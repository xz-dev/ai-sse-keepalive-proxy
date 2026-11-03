#!/usr/bin/env bash
# Offline unit cases for immutable-tag-guard.sh. Simulates the registry
# lookup with a stubbed docker CLI; the guard must allow ONLY the proven
# not-found outcomes and refuse every other result.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
guard="${here}/immutable-tag-guard.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
stub="${work}/docker"
bin_dir="${work}/bin"
mkdir -p "$bin_dir"
export PATH="${bin_dir}:$PATH"

cat > "$stub" <<'EOF'
#!/usr/bin/env bash
# Stub docker buildx imagetools inspect driven by DOCKER_STUB_* env vars.
case "${1:-} ${2:-}" in
  "buildx imagetools")
    [ "${3:-}" = "inspect" ] || { echo "unexpected subcommand" >&2; exit 64; }
    ;;
esac
case "${DOCKER_STUB_MODE:-}" in
  exists)
    echo "Name:      ${DOCKER_STUB_REF}"
    echo "MediaType: application/vnd.docker.distribution.manifest.list.v2+json"
    exit 0
    ;;
  not-found)
    echo "ERROR: ${DOCKER_STUB_REF}: not found" >&2
    exit 1
    ;;
  manifest-unknown)
    echo "ERROR: ${DOCKER_STUB_REF}: manifest unknown" >&2
    exit 1
    ;;
  auth)
    echo "error: failed to authorize: denied" >&2
    exit 1
    ;;
  network)
    echo "error during connect: dial tcp: lookup ghcr.io: no such host" >&2
    exit 1
    ;;
  unknown)
    echo "some future error shape" >&2
    exit 1
    ;;
  malformed)
    # Successful exit without recognizable manifest output.
    exit 0
    ;;
  *)
    echo "stub mode not set" >&2
    exit 64
    ;;
esac
EOF
chmod +x "$stub"
ln -s "$stub" "${bin_dir}/docker"

ref="ghcr.io/example/cli-proxy-api:v8.0.51-pr7134-abcdef0"
pass=0
fail=0

expect() { # expect <want:allow|refuse> <mode> <label>
  local want="$1" mode="$2" label="$3" got
  DOCKER_STUB_MODE="$mode" DOCKER_STUB_REF="$ref" bash "$guard" "$ref" >/dev/null 2>&1 && got=allow || got=refuse
  if [ "$got" = "$want" ]; then
    pass=$((pass + 1)); echo "PASS ${label}: ${got}"
  else
    fail=$((fail + 1)); echo "FAIL ${label}: got ${got}, want ${want}" >&2
  fi
}

expect refuse exists        "existing-tag"
expect allow  not-found      "not-found"
expect allow  manifest-unknown "manifest-unknown"
expect refuse auth           "auth-failure"
expect refuse network        "network-error"
expect refuse unknown        "unknown-error"
expect refuse malformed      "malformed-success-output"

# No-arg and empty-ref must also refuse.
if bash "$guard" >/dev/null 2>&1; then fail=$((fail + 1)); echo "FAIL no-arg: got allow, want refuse" >&2
else pass=$((pass + 1)); echo "PASS no-arg: refuse"; fi
if DOCKER_STUB_MODE=not-found DOCKER_STUB_REF="$ref" bash "$guard" "" >/dev/null 2>&1; then fail=$((fail + 1)); echo "FAIL empty-ref: got allow, want refuse" >&2
else pass=$((pass + 1)); echo "PASS empty-ref: refuse"; fi

echo "guard unit cases: pass=${pass} fail=${fail}"
[ "$fail" -eq 0 ]
