# Coordinator-only actual execution

Run fixed official server FIRST, then candidate after actual production integration/GREEN, both canonical owner-get mode and same original12 source rows. Never use an error-only profile as success.

```sh
INTEROP_PACKAGE=/absolute/unchanged-interop/package.json \
LIVEKIT_URL=http://127.0.0.1:PORT \
LIVEKIT_API_KEY=owned_key LIVEKIT_API_SECRET=owned_secret \
OPEN_SOURCE_GO_CASES=/tmp/raw-track-source-open-enum-ws-v4/cases.json \
OPEN_SOURCE_GO_RESULTS=/tmp/raw-track-source-open-enum-ws-v4/fixed-go12-actual.jsonl \
OPEN_SOURCE_SERVER_PID=ACTUAL_PID \
OPEN_SOURCE_SERVER_BIN=/absolute/actual-binary \
OPEN_SOURCE_SERVER_CONFIG=/absolute/immutable-canonical.yaml \
OPEN_SOURCE_EXECUTION_RECEIPT=/absolute/root-startup-receipt.json \
OPEN_SOURCE_REPORT=/absolute/new-owned-report.json \
node /tmp/raw-track-source-open-enum-ws-v4/run.mjs
```

Receipt required fields: pid, livekit_url (literal LIVEKIT_URL), listener_port (actual canonical config port), binary_sha256, startup_config_sha256, api_owner_get:true, no_api_config_overrides:true, environment_api_override_absent:true. Parent records command/config origins privately and actual PID listener receipt. lsof must be on PATH or set LSOF=/absolute/lsof. Config file parent path must avoid ambiguous spaces on Darwin ps fallback. The actual current lsof is called by fixture, not a shell fake input.

After original and candidate each exit0/PASS_SCOPED and real cleanup:

```sh
python3 /tmp/raw-track-source-open-enum-ws-v4/compare.py \
 --official /absolute/official-report.json --rust /absolute/rust-report.json \
 --output /absolute/new-comparison.json
```

Comparison preserves original seven business columns, adds all new api_* columns/two exact update observations. No alias/enum/unknown/error normalization. Each report independently asserts against actual Go12 component source. Root must preserve every failure and exact binary/input SHA, then run known raw16 whole grant/history and original quality gates separately. No runtime results are present in this source package.
