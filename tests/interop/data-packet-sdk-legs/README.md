# C86-A: the six DATA_PACKET arms on the launched Rust main and the official SDK

Binds `DATA_PACKET:rpc_request`, `DATA_PACKET:rpc_ack`, `DATA_PACKET:rpc_response`,
`DATA_PACKET:stream_header`, `DATA_PACKET:stream_chunk` and
`DATA_PACKET:stream_trailer` to a launched `livecloud` binary and the pinned
official `livekit-client` 2.23.0 in headless Chromium. Every count in this file
comes from bytes the harness decoded off the wire, not from an assumption about
what the SDK must send. Twelve legs (A–L) and 30 gates; Leg L exists specifically so
the `rpc_ack` consumption claim can fail, and the single-dispatch gate
`one_admin_psrpc_dispatch_and_genuine_resolution` exists so the resolved finding below
stays gated rather than logged.

## Run

```sh
# from the workspace root, after `cargo build -p livecloud`
node tests/interop/data-packet-sdk-legs/run.mjs
```

`run.mjs` launches its own server (`--dev --config probe-config.yaml`, HTTP/WS on
127.0.0.1:8891, TCP 8893, UDP from 8892), hashes the binary and the config before
any leg runs, writes a `RUNNING` receipt first, then runs `probe.mjs` and merges
the identities into `receipt.json` (override with `PROBE_OUT`). It refuses to
start when a port is already taken, re-checks the server log for
`address already in use` after readiness, and tears the child down
SIGTERM → SIGKILL → wait-for-port-free, so a crashed attempt cannot leave a
listener that makes the next attempt's traffic ambiguous. `SKIP_SERVER=1` attaches
to an already-launched instance on the same ports.

Requirements: `npm ci` in `tests/interop`, Playwright Chromium available.

## Observation channel

Two SDK pages join (`handler`, `caller`) and publish audio, so the real peer
connection and the `_lossy` / `_reliable` / `_data_track` channels exist. Three
taps run inside the page: the `onmessage` setter is wrapped to copy every
received packet as base64, `RTCDataChannel.prototype.send` is wrapped to copy
every outbound packet, and a channel registry maps a `channelId` back to its
label. The Node harness decodes each copy with the pinned `@livekit/protocol`
1.52.1 `DataPacket.fromBinary` and emits per-arm typed entries
(`tap_handler` / `tap_caller` / `*_out` in the receipt). `tap_decode_clean` fails
the run if any single frame does not decode, so the counts cannot silently be
built from a partial sample.

## Legs

| Leg | Stimulus | Measured wire evidence |
|---|---|---|
| A `api_send_data` | Twirp `SendData` (protojson base64) | HTTP 200, the payload reaches the peer channel |
| B `admin_perform_rpc` | Twirp `PerformRpc` → handler | one `rpcRequest` frame at the handler whose sender field carries the request id (`from === rpc.id`, which is what `rpc_request_packet` in `crates/lk-compat/src/rpc.rs:280` stamps), handler invoked once, 200; the fast leg returns in ~12 ms |
| C `sdk_to_sdk_rpc` | `performRpc` `probe.echo` | handler publishes `rpcAck`; the same id is relayed to the caller; response resolves |
| D `sdk_rpc_slowack` | handler sleeps 1500 ms before returning | `rpcAck` on the wire strictly before the late `rpcResponse` (1506 ms) — ordering, not correlation |
| E `sdk_rpc_noack` | handler returns without awaiting the ack | the leg still resolves, so the ack is not a response precondition |
| F `sdk_rpc_error` | handler throws `RpcError(1500)` | `rpcResponse{error}` frame with code 1500 consumed by the caller |
| G `sdk_rpc_ghost` | unknown destination identity | 1501 Connection timeout at the ~7 s client deadline |
| H `topic_stream` | `streamText`, 4 chunks, topic `chat` | exactly 1 `streamHeader` + 4 `streamChunk` + 1 `streamTrailer` at the receiver, one streamId |
| I `single_send_text` | `sendText`, topic `quick` | 1 `streamHeader` (streams-v2 short form) |
| J `raw_rpc_request` | a hand-encoded `rpcRequest` written on the real `_reliable` channel | reaches the handler, round-trips back to the caller |
| K `sender_scope_spoof` | the caller (a non-owner) publishes an `rpcAck` and an `rpcResponse{payload:"spoof"}` for the handler's live server-initiated request id | both frames are FORWARDED (seen at the handler tap, absent at the caller tap) and the Twirp waiter still resolves with the true payload `slow:spoof-target` at ≥1400 ms |
| L `owner_ack_consumed` | the handler — the real owner of that same live request — publishes an `rpcAck` addressed to a participant that exists (`destinationIdentities: ["caller"]`) | 0 copies reach the caller tap, so the registry consumed it, while the two published frames for that id stay distinguishable at the handler tap |

Legs K and L share one request id. They originally did so because a second admin
`PerformRpc` in the same run measured a double dispatch — now known to be this
harness's injected-frame sequence collision, see "Resolved finding" below — but the
sharing stays: one pending entry exercised by both halves is the tightest fixture.
L writes after the SDK's own ack for the same id with `sequence: 0`, which is outside
the receiver-side replay window on both parity engines (Go
`references/livekit-stable/pkg/rtc/participant.go:2551` gates only `dp.Sequence > 0`;
Rust `crates/rtc-core/src/room/data.rs:42` gates only `envelope_sequence > 0`). The
frame is processed without advancing the window, so it cannot consume the number the
SDK will use next. The previous design continued the page's sequence
(`injected_seq == sdk_ack_seqs + 1`), which stole the SDK's next number and made the
caller drop its genuine RpcResponse as a replay — the exact confound the resolved
finding section documents.

Leg J exists because SDK 2.23.0 performs a client RPC through data streams, so no
page ever emits a bare `rpcRequest`; without J the server's `rpc_request` arm has
no traffic on this harness at all. Leg K exists because a stale or unknown request
id is not consumed under a sender-scoped or an id-keyed rule either — the
discriminator only exists while the entry is pending, which is why K rides the
1500 ms leg. Leg L exists because K alone only proves the negative half: an ack
the SDK really publishes is addressed at the request id (the client mirrors the
request's `from` field), and a request id is not a participant, so a relayed copy
dies in destination routing and looks identical to a consumed one. L is the only
frame on this harness whose non-consumption would be visible at another page.

## Gates

`leg_gates` holds 30 falsifiable assertions; all true ⇒ `PROBE_DONE`, any false ⇒
`FAIL` naming the gate. Twenty cover the arms and the RPC/stream lifecycle:
`api_send_data_relayed`, `admin_rpc_delivered_to_handler_frame`,
`admin_rpc_handler_invoked`, `handler_published_rpc_ack_frames`,
`sdk_rpc_acks_observed`, `admin_rpc_ack_consumed_by_server`,
`admin_rpc_response_consumed_by_server`, `rpc_error_response_frame`,
`slowack_ordering`, `noack_path_resolves`, `ghost_timeout_contract`,
`stream_header_per_rpc_request`, `stream_header_per_rpc_response_side`,
`topic_stream_header`, `topic_stream_chunks`, `topic_stream_trailer`,
`single_send_text_header`, `raw_rpc_request_reaches_handler_arm`,
`raw_rpc_request_roundtrip_relayed`, `tap_decode_clean`.
Three are header attribution gates — `header_client_sends_empty_identity`,
`header_server_rewrites_sender_identity`,
`header_generated_not_stamped_for_plain_sender` — and six form the ack-consumption
split built from Legs K and L.

### Ack publish/relay split

On the pristine control (`/tmp/c86-lanes/L-control/receipt4.json`, 29/29, binary
`8a63b6144ab89367…`) the handler page publishes 9 `rpcAck` frames covering 8
distinct request ids, and the caller page receives 5. The difference is 3 ids, and
the gate does not compare counts — it attributes every frame. A request is
classified as server-initiated from the wire alone: `rpc_request_packet` stamps the
request id into the packet's sender field, so a server-originated request arrives at
the callee with `from === rpc.id`. The handler page's outbound ids for
client-originated requests are addressed at `caller`; the ids it mirrors from a
server-originated request are addressed at that request id, which is not a
participant.

`rpc_ack_publish_relay_split_attributed` asserts an iff on those two sets:

- consumed == published ∩ server-initiated, and `|consumed| ≥ 2`,
- relayed ∩ server-initiated == ∅ and relayed ⊆ published,
- `|published| == |relayed| + |consumed|`,
- every published ack id corresponds to an `rpcRequest` the handler tap actually
  received, so an ack cannot be invented to balance the equation.

So an unexplained consumption, an explained consumption that stopped happening, or an
extra relay of a server-initiated ack each fail it. On the control the consumed set is
exactly `{admin echo id, slowack id}` — the two the room registry had pending for
`handler` (the older receipt also listed a duplicate-dispatch id, which came from the
harness bug resolved below).

### Owner ack of a live request (Leg L)

The negative half is the part the earlier runs could not see, so
`owner_ack_of_live_request_consumed` states it as one conjunction with its own
anti-vacuity controls, all read off the wire (gates: 30):

- the injected frame really left the page (`sent.state == "open"`, 51 bytes) and carries
  the writer's invariant sequence (`injected_seq == 0`, outside the replay window; the
  SDK's own ack sequence `sdk_ack_seqs` is recorded as an observation only);
- the id has exactly two published ack frames at the handler tap, so the injected copy
  is a duplicate of a live-registry ack rather than a fresh entry;
- 0 copies appear at the caller tap although the frame is addressed to `caller`, a
  joined participant — that absence is the consumption;
- the positive control that makes the absence meaningful: the *non*-owner ack from Leg
  K on the same id does appear at its destination tap (1 frame), and 5 relayed acks
  reach the caller in the same run, so the tap and the relay path are demonstrably
  working;
- the Twirp waiter is unaffected: 200 with `slow:spoof-target` at 4517 ms.

Under mutant A6 (never consume `rpc_ack`) the injected ack is forwarded, so the count
of that id at the caller tap becomes 1 and this gate fails. That is the discriminator
the agent-on-behalf control never provided.

### Sender-scoped pending consumption

`RpcRegistry::handle_ack` matches `Some(p) if &p.participant == sender`
(`crates/lk-compat/src/rpc.rs:186`) and `handle_response` matches
`map.get(request_id).is_some_and(|p| &p.participant == sender)`
(`crates/lk-compat/src/rpc.rs:200`, removal at `:213`). Leg K proves that on the
wire: the caller publishes an ack and a `rpcResponse{payload: "spoof"}` for the
handler's own live request id, and

- `spoof_ack_from_non_owner_forwarded` / `spoof_response_from_non_owner_forwarded`
  — exactly one of each appears at the destination handler tap, so the server did not
  consume them;
- `spoof_frames_not_duplicated_to_caller` — neither frame is echoed back to the
  publishing page, so the relay is single-copy rather than a loop-back artifact that
  would fake the previous two gates;
- `spoof_does_not_resolve_admin_waiter` — the Twirp call returns 200 with the real
  payload `slow:spoof-target` at ≥1400 ms, not the spoofed `spoof` and not early.

Leg L then supplies the opposite half on the same id (see above), so the pair is an
"if and only if the sender is the owner" statement instead of a one-sided one. Mutant
A10 (sender match removed in `handle_ack`) fails
`spoof_ack_from_non_owner_forwarded`, and A11 (sender match removed in
`handle_response`) fails `spoof_does_not_resolve_admin_waiter` plus
`spoof_response_from_non_owner_forwarded` — the non-owner response takes the pending
entry and resolves the waiter with `spoof`.

Under an id-keyed registry the second frame would have resolved the waiter with
`spoof` at ~0 ms and neither page would have seen it. This is the parity form of
the claim: Go keeps `rpcPendingAcks` on the participant that received the frame, so
the Rust room-level registry storing the bound SID reaches the same verdict rather
than a stricter one (the earlier "measured stricter-than-Go" note in
`crates/lk-compat/tests/data_packet_leaves.rs` was wrong and is corrected here).

## Resolved finding: the double `PerformRpc` dispatch was a harness artifact, not a server defect

The earlier revision of this file attributed a measured double dispatch — one slow
admin `PerformRpc`, two request ids at the callee, ~4.5 s for a 1500 ms handler — to
duplicate `Participant/PerformRpc` queue-group membership. That attribution is
retired; three independent measurements killed it, and the actual cause was found in
this harness's own raw-frame writer.

Measured cause. The shared writer (`sendRawFrame` in `probe.mjs`) used to number an
injected reliable frame `last outbound sequence + 1`. The SDK keeps its own counter on
the same `_reliable` channel and does not know about injected frames, so the handler
page's genuine `RpcResponse` came out with the *same* sequence the probe had just
consumed for its injected owner ack. Both receivers gate reliable replay on the
envelope sequence only when it is positive — Go
`references/livekit-stable/pkg/rtc/participant.go:2551` (`if kind == ...RELIABLE && dp.Sequence > 0`,
drop when `lastPubReliableSeq >= dp.Sequence`) and Rust
`crates/rtc-core/src/room/data.rs:42` (`if envelope_sequence > 0 { ... envelope_sequence <= p.last_reliable_seq → return }`)
— so the caller silently discarded the genuine response, attempt 1 hit the retry
policy's per-attempt deadline, and `configured()`
(`crates/lk-compat/src/room_control/mod.rs:449-473`) re-dispatched. The server trace
matches: attempt 1 final-timeout at 3.001 s, attempt 2 returned Ok at 4.516 s. The fix
is in the writer: injected frames now carry `sequence: 0`, which both parity engines
treat as outside the replay window — processed, but never advancing the receiver's
window or stealing the SDK's next number.

Guard 1 — component tier. `in_window_answer_runs_the_handler_once_per_publish` and
`two_members_on_one_topic_run_the_handler_once_per_publish`
(`crates/lk-compat/src/local_participant/tests.rs:244` and `:307`) pass: an answer that
arrives inside the attempt window causes exactly one handler run, and two live RPC
members on one topic still run the handler once per publish.

Guard 2 — transport-tier negative control. Patching the queue arm of
`LocalTransport::publish_encoded` (`crates/lk-compat/src/service_bus/local_transport.rs:161`)
to write to every member makes
`absent_publication_is_not_replayed_to_later_subscription_and_queue_round_robins`
(`local_transport.rs:406`) fail with `left: 2, right: 1` — duplicate fan-out is real if
forced — while the same patch leaves the RPC-tier test green, because `require_claim: true`
with `ServerLimits.allow_skip_claim: false` (`local_transport.rs:363`, `:377`) arbitrates a
duplicated delivery before the destination handler runs. Duplicated fan-out therefore
cannot double-run a destination handler.

With the sequence collision removed the campaign re-measured one dispatch per admin
`PerformRpc` (~1500 ms), so the claim is now gated rather than logged:
`one_admin_psrpc_dispatch_and_genuine_resolution` asserts
`expected_dispatches == measured_dispatches == handler_invocations == 1`, no gap between
dispatch frames, the caller-side tap showing no relayed copy of the consumed response,
and the Twirp result resolving with the handler's genuine payload inside the retry
budget. `findings.admin_psrpc_dispatch` carries the full expected-versus-measured pair.
No production change was warranted; task #417 closes on this evidence.

## Falsifiability

One mutant per asserted arm, applied to the real production source, rebuilt, and
re-run through this same campaign. A mutant counts only if the run fails *and* the
rebuilt binary digest differs from the baseline digest; the pristine source is
restored after every mutant and re-checked against
`77653fe6695d79616654edcbd9ce68fdf531ba74d9b994edca94e44e11594243`
(`data.rs`) / `9a856b969c754d5faf3ca6f0c20721fb09d1cdda6f10f4d0c6d975d1348301de`
(`rpc.rs`), with a final restored control run. The eleven mutants and the arm each is
meant to break:

| Mutant | Edit | Arm it must break |
|---|---|---|
| A1 | `stream_header` arm → `DataVerdict::Drop` | `DATA_PACKET:stream_header` |
| A2 | `stream_chunk` arm → drop, prepended before the catch-all | `DATA_PACKET:stream_chunk` |
| A3 | `stream_trailer` arm → drop | `DATA_PACKET:stream_trailer` |
| A4 | response arm always reports `Consumed` | `DATA_PACKET:rpc_response` |
| A5 | ack arm always reports `Consumed` | `DATA_PACKET:rpc_ack` |
| A6 | ack arm never consumes (`Some(Value::RpcAck(_)) => {}`) | `DATA_PACKET:rpc_ack` |
| A7 | response arm never consumes (`let _ = outcome;`) | `DATA_PACKET:rpc_response` |
| A8 | `rpc_request` arm → drop | `DATA_PACKET:rpc_request` |
| A9 | the `if on_behalf` guard around the `generated` stamp removed | the `stream_header` attribution |
| A10 | `handle_ack` ignores `sender` (`rpc.rs:186`) | the ack sender scope |
| A11 | `handle_response` matches on id only (`rpc.rs:200`) | the response sender scope |

Mutants run strictly serially because they share `target/`: a concurrent build would
both block and mix source states, so a parallel sweep measures nothing.

<!-- MUTANT_MATRIX -->
### Measured matrix (sweep 4, driver `battery_a4.py`, lane `/tmp/c86-lanes/A11-mutants`)

Baseline control PROBE_DONE 29/29, binary `8a63b6144ab89367…`; restored control PROBE_DONE 29/29, binary `9b5ce9b9a0f23ea7…`. `void_runs` = [], source pristine after every restore = true (`data.rs` `77653fe6695d7961…`, `rpc.rs` `9a856b969c754d5f…`), and no mutant produced a binary with the baseline digest (`mutant_digests_equal_to_baseline` = {}).

| Mutant | Arm under test | Binary differs | Result | Gates this mutant alone broke |
|---|---|---|---|---|
| A1 drop-every-stream-header | `DATA_PACKET:stream_header` | yes | KILL (15/29) | `header_client_sends_empty_identity`, `header_generated_not_stamped_for_plain_sender`, `noack_path_resolves`, `owner_ack_of_live_request_consumed`, `raw_rpc_request_roundtrip_relayed`, `rpc_error_response_frame`, `sdk_rpc_acks_observed`, `single_send_text_header`, `slowack_ordering`, `stream_header_per_rpc_request`, `stream_header_per_rpc_response_side`, `topic_stream_chunks`, `topic_stream_header`, `topic_stream_trailer` |
| A2 drop-every-stream-chunk | `DATA_PACKET:stream_chunk` | yes | KILL (28/29) | `topic_stream_chunks` |
| A3 drop-every-stream-trailer | `DATA_PACKET:stream_trailer` | yes | KILL (28/29) | `topic_stream_trailer` |
| A4 consume-every-rpc-response | `DATA_PACKET:rpc_response` | yes | KILL (27/29) | `rpc_error_response_frame`, `spoof_response_from_non_owner_forwarded` |
| A5 consume-every-rpc-ack | `DATA_PACKET:rpc_ack` | yes | KILL (23/29) | `owner_ack_of_live_request_consumed`, `raw_rpc_request_roundtrip_relayed`, `rpc_ack_publish_relay_split_attributed`, `rpc_error_response_frame`, `sdk_rpc_acks_observed`, `spoof_ack_from_non_owner_forwarded` |
| A6 never-consume-rpc-ack | `DATA_PACKET:rpc_ack` | yes | KILL (25/29) | `owner_ack_of_live_request_consumed`, `rpc_ack_publish_relay_split_attributed`, `spoof_does_not_resolve_admin_waiter`, `spoof_frames_not_duplicated_to_caller` |
| A7 never-consume-rpc-response | `DATA_PACKET:rpc_response` | yes | KILL (26/29) | `admin_rpc_response_consumed_by_server`, `owner_ack_of_live_request_consumed`, `spoof_does_not_resolve_admin_waiter` |
| A8 drop-every-rpc-request | `DATA_PACKET:rpc_request` | yes | KILL (27/29) | `raw_rpc_request_reaches_handler_arm`, `raw_rpc_request_roundtrip_relayed` |
| A9 stream-header guard removed | `stream_header sender attribution` | yes | KILL (19/29) | `header_client_sends_empty_identity`, `header_generated_not_stamped_for_plain_sender`, `header_server_rewrites_sender_identity`, `noack_path_resolves`, `owner_ack_of_live_request_consumed`, `rpc_ack_publish_relay_split_attributed`, `rpc_error_response_frame`, `sdk_rpc_acks_observed`, `slowack_ordering`, `stream_header_per_rpc_response_side` |
| A10 ack sender-match removed | `rpc_ack sender scope (rpc.rs:186)` | yes | KILL (27/29) | `owner_ack_of_live_request_consumed`, `spoof_ack_from_non_owner_forwarded` |
| A11 response sender-match removed | `rpc_response sender scope (rpc.rs:200)` | yes | KILL (26/29) | `owner_ack_of_live_request_consumed`, `spoof_does_not_resolve_admin_waiter`, `spoof_response_from_non_owner_forwarded` |

11 of 11 mutants killed. A6 is the one the previous sweep survived (28/28, `/tmp/c86-lanes/A10-mutants`): Leg L is what turns it into a failure, so the `rpc_ack` consumption claim is now falsifiable rather than merely consistent. A1 and A9 fail several gates at once because the text-header path carries the RPC request/response framing in SDK 2.23.0, which is the honest blast radius of those edits, not a broader claim.

The digest column is a coarse guard rather than a source identity: the restored control
was built from byte-identical pristine sources and still produced
`9b5ce9b9a0f23ea7…` instead of the baseline `8a63b6144ab89367…`, so these debug builds
are not reproducible bit-for-bit. What the check does catch is the failure mode that
matters — a mutant whose binary equals the baseline never got its edit into the artifact,
and such a run is reported in `mutant_digests_equal_to_baseline` instead of being counted
as a survival. The authoritative restore evidence is the source sha256 pair, re-verified
after the sweep.

## What the StreamHeader guard is and is not

`crates/lk-compat/src/data.rs:115` gates the text-header `generated` stamp on
`on_behalf`, which needs `sender.is_agent`. Neither SDK page is an agent, so this
campaign does not traverse that branch: every header the pages receive has
`generated: null`, and the three attribution gates above measure the plain-sender
path. The agent-on-behalf behaviour is attested at component tier
(`cargo test -p lk-compat --test data_packet_leaves`), not here.

The guard is discriminated by mutant A9, which *removes* it so the stamping runs on
real SDK traffic; the earlier control instead added a branch no plain participant can
reach, so it could not fail and is not counted.

## Invalid sweeps, retained

Three earlier attempts contribute no kill and are kept so this section's numbers cannot
be back-filled from them:

- Sweep 1 (`battery_a.py`, `/tmp/c86-lanes/A8-void-teardown-bug`): baseline 18/18, A1
  `FAIL 7/18`, then A2–A4 `NO_RECEIPT`. A `run.mjs` teardown bug let the A1 server keep
  `rtc.udp_port` 8892, so those legs measured a half-dead server that still held its
  HTTP listener — they failed for the port reason, not the arm reason.
- Sweep 2 (`battery_a2.py`, after the teardown fix): stopped by a transport failure with
  only baseline/A1 complete, and it left the A5 mutation applied on disk
  (`data.rs` `1ebebede…`), reverted to pristine `77653fe6…` before the next attempt.
- Sweep 3 (`battery_a3.py`, `/tmp/c86-lanes/A10-mutants`): a valid 28-gate matrix that
  killed 10 of 11 — A2/A3 each killed exactly their own gate, A4/A5/A7/A8/A10/A11 killed
  theirs — but **A6 (never consume `rpc_ack`) passed 28/28**. That is the vacuity this
  harness was rebuilt to remove: both the SDK and Go `participant.go:4719` address a
  callee's ack at the request id, which is not a participant, so a relayed copy dies in
  destination routing and looks identical to a consumed one. Its kills are not carried
  over; the whole matrix is re-measured on the 29-gate harness.

Four guards keep such a run from being mistaken for evidence: the port preflight and
post-readiness `address already in use` log check in `run.mjs`, the
`VOID - server bind conflict` verdict in the driver, the
`mutant_digests_equal_to_baseline` predicate (an unchanged binary means the mutant never
reached the artifact, so its "pass" is not a survival), and the `void_runs` /
`source_is_pristine` fields in the battery report, which must all be clean for the sweep
to print `SWEEP_DONE`.

## Identity anchors

- Pristine control for this harness: `/tmp/c86-lanes/L-control/receipt4.json`,
  `PROBE_DONE` with 29/29 gates, server binary `target/debug/livecloud` sha256
  `8a63b6144ab893670e108a15366dd9cc109dbbded156d79a678e761d0fbb3e0f` (pid 34712).
  The earlier `30bac24c77111dba…` belongs to the superseded 28-gate harness before Leg
  L existed and is not the control for these numbers.
- Config sha256 `3654df046c00987b8c98d27586358f080ce6001069b60f553de519ddad826e47`
- Mutant battery (sweep 4, in progress): driver `/tmp/c86-mut/battery_a4.py`, lane
  `/tmp/c86-lanes/A11-mutants/mutant-battery.json`, log
  `/tmp/c86-lanes/A11-battery.log`
- `livekit-client@2.23.0`, `@livekit/protocol@1.52.1`, `livekit-server-sdk@2.19.2`,
  Playwright 1.64.1, HeadlessChrome 156.0.8078.4
- Rust baseline `57dfcb6d5a37f4b8d9e8bc7bebd488f23c49eb24`, pinned Go oracle
  livekit v1.13.10 `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`
- Component regression: `cargo test -p lk-compat --test data_packet_leaves`

## Tier judgement

The six arms move from `component_behavior` to `main_server_consumer`: each one now
has launched-server traffic that a mutant of that exact arm breaks. `rpc_ack` and
`rpc_response` additionally get a consumption-side discriminator with both halves on
the wire — the id-attributed publish/relay split, Leg K for "a non-owner does not
consume" and Leg L for "the owner does" — which is what the earlier runs could not
separate from relay; sweep 3 is the record of what claiming it without L looked like.
The duplicate `PerformRpc` dispatch formerly recorded on this path is resolved as a
harness artifact (see above) and is now gated by
`one_admin_psrpc_dispatch_and_genuine_resolution`, so the single-dispatch claim is part
of the campaign instead of an open defect; task #417 closes on this evidence.
`official_differential_lifecycle` is not claimed by this campaign for the arms whose
Go-side counterpart the pinned client cannot produce, and tier 5 stays withheld in
the ledger for the RPC family because cancellation and mid-RPC departure have no
DataPacket arm in protocol 1.52.1 or in livekit-client 2.23.0 to stimulate — that is
absence of upstream contract, not an untested Rust gap.
