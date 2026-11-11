#!/bin/bash
# SPEC-0077 driven-items matrix: every consumer receipt that names the ledger
# leaves it drives, on one official binary and one candidate.
#
# The four harnesses emit `driven_items` next to the config they actually write,
# so `scripts/check-parity-ledger.py` can resolve a consumer-tier claim to the
# leaf it exercised. A leaf a profile does not write is not named.
#
# Usage: run-driven-items.sh <obs-dir> <go-binary> <rust-binary> <out-dir> <fixture-dir> <assigned-ipv4>
#   obs-dir holds obs-bind-host, obs-trace-https and obs-turn-listener-relay,
#   built from tests/interop/<name>/observe.go against references/livekit-stable.
set -u
OBS=${1:?observer directory}
GO=${2:?official go binary}
RS=${3:?rust binary}
OUT=${4:?output directory}
FIX=${5:?rtc-tls fixture directory}
V4=${6:?assigned non-loopback IPv4}

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
mkdir -p "$OUT" || exit 1
LOG="$OUT/commands.log"
: > "$LOG"
fail=0

run() {
  label=$1
  shift
  "$@" > "$OUT/$label.stdout" 2>&1
  code=$?
  printf '%s EXIT=%s CMD=%s\n' "$label" "$code" "$*" >> "$LOG"
  [ "$code" -eq 0 ] || fail=$((fail + 1))
}

for side in go rust; do
  case $side in
    go) bin=$GO ;;
    rust) bin=$RS ;;
  esac

  for profile in numeric hostname hostname-cli empty-list wildcard nil-list nonlocal malformed; do
    run "bind-host-$side-$profile" "$OBS/obs-bind-host" -binary "$bin" \
      -fixture "$FIX" -profile "$profile" -out "$OUT/bind-host-$side-$profile"
  done

  for profile in http trusted mtls wrong-root untrusted; do
    run "trace-https-$side-$profile" "$OBS/obs-trace-https" -binary "$bin" \
      -fixture "$FIX" -profile "$profile" -out "$OUT/trace-https-$side-$profile"
  done

  for profile in explicit-v4 wildcard-v4 explicit-v6; do
    for transport in udp tcp; do
      run "turn-$side-$profile-$transport" "$OBS/obs-turn-listener-relay" -binary "$bin" \
        -out "$OUT/turn-$side-$profile-$transport" -v4-bind "$V4" \
        -profile "$profile" -transport "$transport"
    done
  done

  run "stun-family-$side" python3 "$ROOT/tests/interop/stun-transport-family/observe.py" \
    --binary "$bin" --output "$OUT/stun-family-$side"
done

echo "TOTAL_FAILURES=$fail" >> "$LOG"
echo "MATRIX_DONE" >> "$LOG"
[ "$fail" -eq 0 ]