#!/bin/bash
# Repeated TURN listener/relay matrix across both daemons.
#
# SPEC-0077 batch 22d. The external-TLS TCP IPv4 ALLOCATE failure recorded
# against candidate 7ff70e9d… did not reproduce on later candidates; a single
# run cannot distinguish a fixed defect from an intermittent one, so the whole
# profile × transport × side matrix is repeated and every original exit code is
# logged. No pipe filters a result.
#
# Usage: run-matrix.sh <observer> <go-binary> <rust-binary> <output-dir> <assigned-ipv4> [rounds]
set -u
OBS=${1:?observer binary}
GO=${2:?official go binary}
RS=${3:?rust binary}
OUT=${4:?output dir}
V4=${5:?assigned non-loopback IPv4}
ROUNDS=${6:-3}

mkdir -p "$OUT" || exit 1
LOG="$OUT/commands.log"
: > "$LOG"
fail=0
for round in $(seq 1 "$ROUNDS"); do
  for profile in explicit-v4 wildcard-v4 explicit-v6; do
    for transport in udp tcp; do
      for side in go rust; do
        if [ "$side" = go ]; then B=$GO; else B=$RS; fi
        label="r${round}-${side}-${profile}-${transport}"
        d="$OUT/$label"
        if [ "$profile" = explicit-v6 ]; then
          "$OBS" -binary "$B" -out "$d" -profile "$profile" -transport "$transport" > "$OUT/$label.stdout" 2>&1
        else
          "$OBS" -binary "$B" -out "$d" -v4-bind "$V4" -profile "$profile" -transport "$transport" > "$OUT/$label.stdout" 2>&1
        fi
        code=$?
        echo "$label EXIT=$code" >> "$LOG"
        if [ "$code" -ne 0 ]; then fail=$((fail+1)); echo "FAILED: $label EXIT=$code"; fi
      done
    done
  done
done
echo "TOTAL_FAILURES=$fail" | tee -a "$LOG"
echo "MATRIX_DONE"
[ "$fail" -eq 0 ]