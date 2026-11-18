# C45 cluster-blob-provenance: two-node cross-node Blob provenance differential

Launched-binary, two-node, paired Go/Rust differential for
`WIRE:ClusterSignal.ParticipantBlobProvenance` (`official_differential_lifecycle`
tier). A real writer + reader store/get a participant data Blob through the
FRONT (non-owner) node's signal WebSocket while the OWNER node holds the room,
so Join/Store/Get traverse the cross-node PSRPC Redis `RelaySignal` stream
(Go `pkg/routing/signal.go:103-109` -> `pkg/service/signal.go:134-222`; Rust
`crates/lk-compat/src/cluster_signal/`). Unknown-field provenance must survive
the relay identically on both implementations.

## Identities

- Official Go binary: `/tmp/parity-oracle/livekit-server-go`, SHA256
  `4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc`
  (checked by the runner before execution; abort on mismatch).
- Locked Go source: `references/livekit-stable` @
  `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07` (v1.13.10). NOT `references/livekit`.
- Rust candidate: `target/release/livekit-server` built from the task HEAD;
  its SHA256 is recorded in the evidence `binaries.json` and every result.json.
- Codec oracle: `tests/oracles/participant-blob/` (26 cases, fixed Go
  `rtc.ParticipantDataBlob` + limit checks). Produce the JSONL with:
  `GOTOOLCHAIN=go1.26.3 go run -mod=readonly . "$PWD/cases.json" > rows.jsonl`.

## What runs

`run-paired.py` (adapted from the C44 `cluster-config-consumer` runner) starts,
per pair, ONLY owned children: an isolated Redis broker on a free port, owner
node `b` (real registration witnessed via `redis-cli HKEYS nodes`), front node
`a`. Both nodes get the top-level `enable_participant_data_blob: true` gate
(Go `config.go` `EnableParticipantDataBlob`, Rust `official.rs`; default false)
plus the C44 non-default `signal_relay` block; psrpc stays at defaults. A Redis
MONITOR tap witnesses the actual `RelaySignal` PUBLISH envelopes
(`Signal|RelaySignal|<ownerNode>|STR`), proving the traffic crossed the bus.

`blob-relay.mjs` (run with cwd `tests/interop` so `node_modules` resolves):

1. Pins a fresh room to node `b` via the OWNER `RoomServiceClient`
   `createRoom({name, nodeId: ownerNode})` (same mechanism as C44 `probe-rpc.mjs`).
2. For each of the 26 oracle cases, connects a writer + reader raw authenticated
   WebSocket actor to the FRONT node (`/rtc?access_token=...&protocol=17`),
   modeled on the committed `../participant-data-blob/raw-peer.mjs` (unchanged),
   with per-response raw byte capture added. Wire reading uses the committed
   lossless bounded reader `../participant-data-blob/raw-protocol.mjs` unchanged.
3. Store (kind 22) with `blob = field(1,key=left) + field(2,[0,255,1]) +
   blob_unknown`, then Get (kind 23) with `participant_identity=writer` and
   `key=right`. Invalid keys must yield RequestResponse reason 11 + the oracle
   message and must not create a Blob; missing Gets yield reason 1
   `data blob not found`. Every response is compared to the oracle row via
   `comparePayload` (type + requestId + unknown cohort + all known semantics).
4. Captures per case: raw store/get response bytes (base64) AND the normalized
   parsed tree (requestId stripped). Request ids start at 100; the [127,128]
   varint boundary must be observed; valid>=20 && invalid>0; clean 1000 closes.
5. Writes `blob.json`; any failed case fails the probe (real exit code).

After both pure legs (`go-front-go-owner`, `rust-front-rust-owner`) run, the
runner deepEquals the Go-leg vs Rust-leg per-case normalized trees
(`paired-comparison.json`). This direct equality plus oracle conformance on
both legs is the acceptance; raw wire byte equality is recorded as informational.
Teardown is SIGTERM -> wait -> kill of owned children only, with exit-code
checks; a forced or non-zero owned exit fails the pair.

## Run

```sh
python3 tests/interop/cluster-blob-provenance/run-paired.py \
  --go-server /tmp/parity-oracle/livekit-server-go \
  --rust-server target/release/livekit-server \
  --go-cases tests/oracles/participant-blob/cases.json \
  --go-results <evidence>/participant-blob-go.jsonl \
  --evidence-dir <evidence>/matrix
```

Default runs both pure legs; `--pair` selects one of the four legs (mixed legs
are optional extras). Exit 0 only when every requested pair passes and, when
both pure legs ran, the paired tree equality holds. Evidence per pair:
`<pair>/result.json`, `blob.json`, `execution.json`, configs, server/redis logs,
`monitor.log`, `envelopes.json`, probe stdout/stderr.

## Falsifiability

Positive control: an isolated worktree mutant with `response_wire` returning
`None` (typed fallback erases unknown cohorts) MUST fail the Rust leg's
unknown-cohort comparisons while the Go leg with the same oracle still passes.
The mutant FAIL receipt is a control, never acceptance evidence.

## Scope limits

- Raw WebSocket signal only: no browser, no SDP/media, no SDK Blob decoder.
- The differential covers Store/Get response provenance across the relay; it
  does not re-verify join semantics beyond the authenticated Join gate.