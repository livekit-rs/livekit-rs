#!/bin/sh
# Full W1 config differential: Go baseline first, then Rust, then compare.
# Usage (repo root): tests/oracles/config-observer/run.sh [evidence-dir]
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cd "$ROOT"
E=${1:-$ROOT/target/config-observer}
W=$(mktemp -d /tmp/cfgobs-work.XXXXXX)
trap 'rm -rf "$W"' EXIT
H=$ROOT/tests/oracles/config-observer
avail=$(df -k /tmp | awk 'NR==2 {print $4}')
[ "$avail" -gt 5242880 ] || { echo "need >5GB free in /tmp"; exit 2; }
mkdir -p "$E"
"$H/stage_go.sh" . "$W/stage"
(cd "$W/stage" && CONFIG_OBSERVER_LEAVES="$E/go-leaves.json" ./config-observer.test -test.run TestConfigObserver >/dev/null)
python3 -I "$H/gen_cases.py" "$E/go-leaves.json" "$E/cases.json"
(cd "$W/stage" && CONFIG_OBSERVER_CASES="$E/cases.json" CONFIG_OBSERVER_OUT="$E/go-results.json" ./config-observer.test -test.run TestConfigObserver -test.timeout 30m > "$W/go-run.log" 2>&1) || { tail -20 "$W/go-run.log"; exit 1; }
(CONFIG_OBSERVER_CASES="$E/cases.json" CONFIG_OBSERVER_OUT="$E/rust-results.json" \
   cargo test --locked -p lk-config --lib observer::tests::config_observer -- --ignored --exact >/dev/null)
python3 -I "$H/compare.py" "$E/cases.json" "$E/go-results.json" "$E/rust-results.json" "$E/go-leaves.json" "$E/report.json"
