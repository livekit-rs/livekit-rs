# Agent cold-start termination

This fixture runs the published Agent SDK and RTC client against two isolated
loopback LiveKit servers and a local Redis. The room belongs to node A. Node B
hosts the original Go worker endpoint. The SDK starts with zero idle processes
and waits inside its public `defineAgent.prewarm` hook.

After the public DeleteDispatch request is flushed, the fixture releases
availability to the actual SDK. It keeps prewarm held until it sees the
assigned job's real termination frame or the bounded observation interval
ends. An owned loopback relay forwards the worker connection byte for byte and
records only assignment and termination metadata from generated SDK messages.
The probe checks for the original room, SDK process, participant, and dispatch
before beginning cleanup. The archived runner independently verifies ownership,
socket release, process exits, and executable hashes.

The fixed Go v1.13.10 control and the Rust baseline can both reproduce the cold
race. `cold-race` asserts that the termination frame arrived while the SDK had
no active job and that the same child later joined and remained alive.
`retired` asserts that the first termination arrived after prewarm was released
and that the exact child and participant retired before cleanup. The latter
is a lifecycle safety correction for this case.

## Run on the dedicated Linux fixture host

Use the existing official Go binary, installed SDK projects, and
`livekit-delayed-agent45-kit-20261105` from the recorded reproduction archive
under `livekit-rs-docs/evidence/2026-11-04/spec0077-implementation/new-actual-fixture-reproduction-inputs/`.
The launcher verifies the archived runner, process helper, Go binary, and SDK
project manifests before execution.

```sh
r=/home/fixture/fixture
sdk=$r/candidate08-source/livekit-rs-server/tests
python3 livekit-rs-server/tests/agent-sdk/cold-termination/run.py \
  --fixture-root "$r/livekit-delayed-agent45-kit-20261105" \
  --go "$r/bin/livekit-official-candidate08-control" \
  --agent-sdk-root "$sdk/agent-sdk" --interop-project "$sdk/interop" \
  --node "$r/toolchain/node/bin/node" \
  --owner go --expect cold-race --output "$r/output/agent-cold-go"
```

For the Rust owner, use a fresh output directory, `--owner rust`, and
`--rust PATH --rust-sha256 SHA256` for the exact executable. Use
`--expect cold-race` for the baseline and `--expect retired` for the corrected
candidate. Each successful case produces `comparison.json`, `execution.json`,
and the original SDK `probe.json`. Exit zero certifies only the selected
expectation and process cleanup.
