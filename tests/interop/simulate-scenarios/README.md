# Official SDK simulateScenario differential

`run.py --go <pinned-go-server> --rust <livecloud> --deps <tests/interop> --output <new-dir> [--single false]`
runs every server-involving `room.simulateScenario` of the pinned livekit-client 2.23.0 against an owned
single-node pinned Go server and an owned Rust server, then compares the normalized observable outcome per
scenario (connection states, SID continuity, SDK events, what the other participant sees, audio and reliable
data afterwards, selected candidate protocol; speaker events for the `speaker` scenario). Raw timelines stay
in each `evidence.json`. `--recompare` recomputes `comparison.json` from an existing output directory.

Ports are taken from the OS; nothing is reused. The browser and both servers are owned by the run and
stopped at the end. New runs require the exact requested scenario set, matching SDK bytes, working
audio and data before fault injection, clean process exits, successful room deletion and released
server ports. Earlier receipts without those fields cannot pass the stricter comparator.

## Real transport paths

Run the additional scenarios in both PC modes. Generate an ephemeral fixture with the existing
TLS fixture generator and clean that fixture after saving the evidence.

```sh
deps=/absolute/path/to/livekit-rs-server/tests/interop
TLS_FIXTURE_NAME=transport-check node "$deps/generate-tls-fixture.mjs"
python3 "$deps/simulate-scenarios/run.py" \
  --go /absolute/path/to/fixed-go-server --rust /absolute/path/to/livecloud \
  --deps "$deps" --output /absolute/path/to/new-result \
  --single true --scenarios ice-network-change,transport-switch \
  --turn-tls-fixture "$deps/output/transport-check"
```

- `ice-network-change` starts client A on one authenticated TURN/UDP proxy, cuts both directions
  and requires recorded packet drops, then moves A's configured TURN route to a second proxy. The
  unchanged SDK's `signal-reconnect` triggers resume. The test requires the same PCs and participant
  SID, new local and remote ICE ufrags, and nomination of the replacement proxy on every PC. This
  covers a deterministic resume across a failed route; automatic OS network-change detection is a
  separate test.
- `transport-switch` starts on direct UDP and invokes the unchanged SDK's `force-tcp` then
  `force-tls`. It requires direct TCP and actual selected TURN/TLS (`relayProtocol=tls`), fresh ICE
  credentials at each step, and compares the number of replaced PCs and SID changes. These SDK
  switches can perform full reconnect, distinct from the same-PC restart above. The browser sends
  TURN/TLS traffic to the test's ephemeral port and pins only the owned fixture's SPKI.
- Each post-change phase requires new received RTP audio packets and an audible peer tone in both
  directions, plus phase-specific reliable data delivered in both directions.

`python3 -B -m unittest -v test_compare` covers false-pass cases in the comparator. Historical
evidence is at `livekit-rs-docs/evidence/2026-11-06/spec0077-m06-simulate-scenarios/`.

## No-TURN dual-PC fallback

Use `--single false --scenarios force-tls` for the unshaped SDK profile.
To test the closely spaced checking-start condition, add `--coalesce-rejoin-sdp`
and omit the TURN/TLS fixture. The Playwright route forwards the original SDK
frames in order, holding at most 64 client frames for up to 250 ms on the first
replacement WebSocket. It flushes when the actual subscriber answer and
publisher offer are both present. The positive fixture gate requires that
pair. The servers use their normal timers and the regular strict comparator
still checks all SDK outcomes. A pair that takes different callback branches
remains a mismatch. This profile tests a signaling burst and is separate from
the unshaped profile.
