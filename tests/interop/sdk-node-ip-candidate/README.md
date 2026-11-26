# SPEC-0077 C99 lane C — `rtc.node_ip` consumed by the official SDK

Date: 2026-11-26 · batch `C99-20261126-main-consumers` · dispatch #386
Tier attested: `official_differential_lifecycle` (SPEC-0075 §2 tier 5)
Leafs: `YAML:rtc.node_ip`, `generated_flags:--rtc.node_ip.ipv4`, plus `YAML:rtc.tcp_port`
and the signal-scheme row observed at the client edge.

```
Rust candidate   target/release/livecloud  sha256 2621963285d9577fd930663e3711b10f7b817089023e3163fa90798ebcc7e9f8
Fixed Go oracle  references/livekit-stable @ e5655ac20267fa5005dccaf3f0e3bfc3ea688c07 (livekit v1.13.10),
                 binary sha256 4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc
Client           npm livekit-client@2.23.0 UMD, sha256 fb646f2260d84f91238d0a2dc10726c456631d6a26c7cafc43cfee5959b4d31d
                 served byte-for-byte from tests/interop/node_modules, never patched
Browser          Playwright Chromium 156.0.8078.4 (headless, fake capture devices)
Verdict          PARTIAL_LAUNCHED_MAIN_SDK_NODE_IP_WITH_FENCED_DIVERGENCES
                 4 parity + 5 fenced rows, 0 failed parity, 0 failed fences, 48/48 controls green
Receipts         filed in livekit-docs under server/evidence/2026-11-26/spec0077-c99-sdk-node-ip/runs/:
                   authoritative/        audio, 9 profiles x 2 daemons publishing an audio track
                   same-media-repeat/    audio repeat: 0 differences across all stable fields
                   video-sweep/          prior sweep, same 9 profiles publishing video
                   confirmation-video/   video confirmation sweep
                   mutant/, mutant-audio/  positive-control (mutant) sweeps, video + audio
                 This directory holds only the harness; the raw per-leg streams live with the receipts.
```

The audio sweep is the authoritative receipt. `authoritative` vs `same-media-repeat` differ in no stable
field; `authoritative` vs `video-sweep` differ in exactly `media_kind`, `audio_tracks` and `video_tracks` — 54
field differences over 9 profiles × 2 sides, and none in candidate text, candidate types, prflx counts,
nomination, ICE state history, counters or exits. Which fake device the client captures does not change
what either daemon advertises or what the client picks.

## Why this slot exists

`../rtc-node-ip-family/` (lane B) pairs the two daemons on the same `rtc.node_ip` inputs and
reads what each daemon **answers** to a pinned pion observer over WHIP. That proves the
rewrite reaches the SDP, but not that a real client can connect through the rewritten
address — which is exactly what the C98 review asked for:

> Cover the v4-mapped flag, explicit IPv4 precedence and normal IPv6 controls with actual
> advertised candidates and a connection using the original SDK.

This lane closes that gap. The client is the pinned official `livekit-client` UMD build
running in headless Chromium; the observables are the candidate lines in the remote
description that client received, the pair that client's own `RTCPeerConnection` nominated
via `transport.selectedCandidatePairId`, and whether RTP counters moved through it between
two samples 2 s apart. No daemon method is stubbed and no SDK code is modified.

## Method

`run-paired.py` runs 9 profiles × 2 daemons (18 launches), one profile per row:

| profile | group | input | what the row is about |
| --- | --- | --- | --- |
| `baseline-no-node-ip` | parity | no `node_ip` | harness positive control: connects, 2 udp + 2 tcp host candidates, RTP grows |
| `scalar-v4-mapped` | fence | `node_ip: "::ffff:127.0.0.2"` | scalar channel normalises to `127.0.0.2`; client is offered that text |
| `scalar-v4-mapped-self` | parity | `node_ip: "::ffff:127.0.0.1"` | consumption: nominated **host** pair carries the rewritten text and pushes RTP |
| `flag-v4-mapped` | fence | `--rtc.node_ip.ipv4=::ffff:127.0.0.2` | flag channel writes RAW text into the slot, same advertised text |
| `pair-v6-first-precedence` | fence | `node_ip: "2001:db8::7,127.0.0.2"` | explicit IPv4 precedence, v6 text listed first, no v6 candidate reaches client |
| `v6-only-control` | parity | `node_ip: "2001:db8::7"` | family scoping: v4 candidates stay `127.0.0.1` |
| `mux-single-port` | fence | `rtc.udp_port: P` + `node_ip` | advertised-but-unbindable candidate; ICE still recovers via prflx |
| `scheme-http-origin` | fence | `node_ip` + `http://` origin | SDK rewrites the origin to its `ws://…/rtc/v1` join |
| `tcp-port-zero-surface` | parity | `rtc.tcp_port: 0` | tcp host candidates drop 2 → 0, udp surface and nomination unchanged |

Launch mechanics that the measurements depend on:

* **Ports are held, not sampled.** `reserve_tcp()` / `reserve_udp()` bind a candidate port or
  a contiguous UDP block in a non-ephemeral band and keep the sockets open across the
  allocation window; the reservations are released (`release_reservations()`) immediately
  after `Popen`, because the daemon can only bind them once this process stops holding them.
  An earlier version allocated, closed and hoped — macOS reassigned a released port to an
  unrelated outbound socket between allocation and start, which surfaced as
  `bind: address already in use` on a later case.
* **Loopback only.** `bind_addresses: ["127.0.0.1"]`, `use_external_ip: false`,
  `interfaces.includes: [lo0]`. The lane never touches a real interface, never binds
  7881/7882, and never runs against the host's own server.
* **Observation tap.** `page.html` replaces `window.RTCPeerConnection` with a constructor
  wrapper that stores the object the SDK creates (a constructor that returns an object wins
  over `this`, so the SDK gets its real peer connection) and records
  `iceconnectionstatechange`. The pinned SDK bundle is served from disk and its SHA-256 is
  re-read into every receipt; `sdk_client_pinned_and_unpatched` asserts the recorded SHA,
  the version string and `patched: false` for all 18 legs.
* **Raw exits.** Each leg records the daemon's exit code, whether a second SIGTERM was
  needed, whether SIGKILL was required, and whether the HTTP/media ports were released.
  `shutdown_shape_matches_oracle` compares those three fields cross-side.

## Gate shape

A row is never accepted because both daemons disagree identically:

* `expect` is a **declared row derived from the Go column** of a completed measurement, and
  `declare()` checks it against each side independently (`go_declared_ok`,
  `rust_declared_ok`) before any cross-side comparison.
* `fence` rows record a measured Rust divergence this batch did not fix. A fence is green
  only while the divergence reproduces exactly as recorded *and* every other compared field
  stays equal *and* the shared claims still hold — `fence_reproduces` is the conjunction of
  `fence_divergence_reproduces`, `fence_other_fields_equal`, `fence_shared_claims_hold`.
  A fence is never absorbed into the parity count.
* Per-session random values (`connected_ms`, `selected_remote_port`, `token_sha256`,
  `track_sources` with its SID, `ports`) are in `RECORDED_ONLY` and excluded from `COMPARE`;
  `volatile_fields_excluded_from_compare` fails if an edit puts one back, which would make
  the lane flaky rather than wrong.
* `python3 run-paired.py --selftest-gate <execution.json>` re-derives all 9 rows from the
  persisted per-side records through the same `derive_entry()` the launch loop uses, then
  injects one fault per gate branch. `gate-falsification.json` records the result: with the
  untouched measurement green (PARTIAL as expected), each of
  `declared_rows_emptied`, `parity_declaration_wrong`, `fence_stops_reproducing`,
  `client_bundle_tampered`, `oracle_identity_broken`, `binary_hash_drifted`, `rust_needs_sigkill`,
  `volatile_field_compared`, `unredacted_address_in_receipt` moves the verdict off the
  expected branch. `--report` recomputes a verdict without starting anything.
* `binary_hash_now_matches_recorded` re-hashes `binaries.*.path` at finish time and compares it
  with the hash recorded during the launch. `binaries_immutable` only reports what each leg
  observed while it ran, so without this control an offline `--report` of an old receipt over a
  rebuilt binary stayed green; the drift now surfaces as that one named control.
* Running the same selftest against a **mutant** receipt discriminates 4 of those 9, and that is the
  ladder working rather than breaking: a mutant run already stops at the fences rung
  (`FAIL_SDK_NODE_IP_FENCES`), so an injection whose expected branch is the controls rung cannot also
  move the verdict — `client_bundle_tampered`, `oracle_identity_broken`, `binary_hash_drifted`,
  `volatile_field_compared` and `unredacted_address_in_receipt` are only reachable on a sweep that
  gets that far. Those are therefore proven on the clean sweeps; `mutant.json` records both cases
  per run.

## Results

Advertised candidates as the client saw them (Go column; Rust identical unless noted):

```
profile                  host_candidate_ips   node_ip_slots                     nominated   prflx(go,rust)
baseline-no-node-ip      ['127.0.0.1']        None                              host        (0, 0)
scalar-v4-mapped         ['127.0.0.2']        {"V4":"127.0.0.2","V6":""}        prflx       (0, 1)
scalar-v4-mapped-self    ['127.0.0.1']        {"V4":"127.0.0.1","V6":""}        host        (0, 0)
flag-v4-mapped           ['127.0.0.2']        {"V4":"::ffff:127.0.0.2","V6":""} prflx       (0, 1)
pair-v6-first-precedence ['127.0.0.2']        {"V4":"127.0.0.2","V6":"2001:db8::7"} prflx   (0, 1)
v6-only-control          ['127.0.0.1']        {"V4":"","V6":"2001:db8::7"}      host        (0, 0)
mux-single-port          ['127.0.0.2']        {"V4":"127.0.0.2","V6":""}        prflx       (0, 1)
scheme-http-origin       ['127.0.0.2']        {"V4":"127.0.0.2","V6":""}        prflx       (0, 1)
tcp-port-zero-surface    ['127.0.0.1']        None                              host        (0, 0)
```

The table above is the audio sweep; the video sweep produces the same ten columns field for field — the
54 cross-sweep differences are `media_kind`/`audio_tracks`/`video_tracks` and nothing else.

Three findings the review asked for, stated at the scope the receipts support:

1. **Advertisement reaches the client.** Every `127.0.0.2` leg shows that exact text in the
   udp *and* tcp `typ host` lines of the SDK's remote description, and the two arrival
   channels are distinguishable only in the daemon's own slot log: the YAML scalar is
   normalised (`{"V4":"127.0.0.2",…}`) while `--rtc.node_ip.ipv4` keeps the RAW mapped text
   (`{"V4":"::ffff:127.0.0.2",…}`) yet advertises the same `127.0.0.2`. Any parity claim
   about the slot rendering must say which channel it covers.
2. **Consumption is proven only where the rewrite is reachable.** `scalar-v4-mapped-self`
   nominates a host candidate carrying the rewritten text with `rtp_out_bytes_sent_positive`
   and `counters_grew` true. On macOS, lo0 owns 127/8 as a block, so an unassigned address
   such as `127.0.0.2` cannot be bound (`mux_alt_bindable: false` on both sides, measured
   with a real socket) and a socket bound to 127.0.0.1 never receives traffic for it — the
   `127.0.0.2` legs therefore prove advertisement and an attempted pair, not media flow over
   the rewritten candidate.
3. **Explicit IPv4 precedence holds at the consumer.** With `2001:db8::7,127.0.0.2` both
   slots fill, and the only candidates that reach the client are the rewritten IPv4 pair; no
   v6 address appears in the remote description at all.

## The one divergence this batch found

When the rewritten target is not deliverable, the Rust daemon additionally trickles a
`typ prflx` UDP candidate for the address its ICE checks arrived from:

```
scalar-v4-mapped / flag-v4-mapped / pair-v6-first-precedence / mux-single-port / scheme-http-origin
    go    udp_candidate_count 2   prflx_signalled_count 0   candidate_types ['host']
    rust  udp_candidate_count 3   prflx_signalled_count 1   candidate_types ['host', 'prflx']
```

Pion never signals a peer-reflexive candidate into the answer, so this is a real
answer-SDP surface difference rather than an ordering artifact — the session outcome is the
same on both sides (both connect over a prflx-observed pair), and every other compared field
is equal. It is recorded as 5 fences and filed as a follow-up; it is not counted as parity
and not silently fixed by widening a tolerance.

## Binary positive control: the divergence is attributed, not asserted

The section above would still read plausibly if the extra candidate came from the harness, the SDK or the
host. To rule that out, the rewrite site (`crates/livecloud/src/interfaces.rs:132`) was mutated from
`IpAddr::V4(_) => rtc.node_ip.v4.map(IpAddr::V4)` to `IpAddr::V4(_) => None`, built into its own target
dir (sha256 `c26535d19a60546343004a0f6fd65e5c8d94a28bb62f331d4daa5f3f5278515c`), and the sweep re-run over
`baseline-no-node-ip, scalar-v4-mapped, scalar-v4-mapped-self, flag-v4-mapped, v6-only-control`:

```
status FAIL_SDK_NODE_IP_FENCES   failed_parity []   failed_fences ['scalar-v4-mapped', 'flag-v4-mapped']
scalar-v4-mapped / flag-v4-mapped, rust side, clean -> mutant
    host_candidate_ips        ['127.0.0.2']            -> ['127.0.0.1']
    candidate_types           ['host', 'prflx']        -> ['host']
    prflx_signalled_count     1                        -> 0
    udp_candidate_count       3                        -> 2
    nominated_remote_type     'prflx'                  -> 'host'
```

The same mutant binary was then swept a second time with `--media audio` over the same five profiles, and
the two runs agree exactly: same status, same `failed_fences`, same empty `failed_parity`, same 12 failed
controls, same Rust-side collapse. The attribution therefore does not depend on which fake device the
client captures.

The Go side of all five profiles is unchanged field-for-field, and the three positive-control profiles
stay green under the same mutant — the binary still boots, publishes and connects; only the configured
address no longer reaches the client. `rewrite_changes_advertised_text_rust` and
`fence_divergence_reproduces` both flip to false, so the gate notices the collapse rather than scoring the
mutant as "better parity". After the run the source was restored byte-exact and the acceptance binary was
rebuilt to `2621963285d9577fd930663e3711b10f7b817089023e3163fa90798ebcc7e9f8`, which ties this receipt to
the same lineage as the parity sweep.

## Three expectations this batch refuted

These were written into the plan before the first measurement and are wrong; they are kept
here so the next reader does not re-adopt them.

* **`http://` origin is refused before any peer connection exists.** False for this client:
  `livekit-client@2.23.0` logs `signal connecting to ws://127.0.0.1:P/rtc/v1` for an
  `http://` origin, joins successfully, and the receipt shows `peer_connection_count: 1`,
  `room_state: connected`. The Twirp `Join` `400 invalid Websocket upgrade request` behaviour
  is a property of the daemon's handler surface, not of what this SDK ever sends. The row now
  reads the dialled scheme out of the SDK's own console line instead of inferring it from the
  launch argument.
* **`rtc.tcp_port: 0` leaves Rust with an ephemeral ICE-TCP candidate while upstream logs
  `use of closed file`.** False on both halves: `tcp_candidate_count` is 0 on each side (from
  2 in baseline), and `tcp_mux_zero_log` is false in both logs. The row became parity.
* **A single mux `rtc.udp_port` with an unroutable rewrite cannot connect.** False: the leg
  connects and streams, because ICE recovers through a peer-reflexive observation of the
  actual path. The control was rewritten to assert the measured shape
  (`mux_advertises_unbindable_target_and_connects_via_prflx_*`) — an assertion that fails on
  correct behaviour is not a positive control.

A fourth, mechanical refutation: `has_tcpcandidate` used to search the SDP for the token
`tcpcandidate`, which never appears — the SDP spells an ICE-TCP host candidate as
`1 tcp … typ host tcptype passive`, so that flag was permanently false. Counting
`transport == 'tcp'` per candidate is what makes the `tcp_port: 0` row discriminate at all.

## A host claim this batch had to retract

* **Published media kind is audio, and the earlier "this host wedges fake audio" note was
  wrong.** The first two sweeps ran `--media video` because a standalone probe
  (`../owned-fake-audio-check.mjs`) had reported `getUserMedia({audio: true})` and a bare
  `AudioContext.resume()` hanging under Chromium's fake-device flags. Re-running that probe
  before filing gave `gum-audio: ok:Fake Default Audio Input:live` and
  `audiocontext: running/48000`, and the audio sweep then published an audio track on 18/18
  legs with `publish_error_class: null`. So the wedge was a transient state of the machine,
  not a property either daemon could be blamed for — and not a reason to keep running the
  lane in a substituted shape. The receipt carries `media_kind`, `audio_tracks`,
  `video_tracks` and `gum_result_class`, and `gum_result_class == 'ok'` is declared per side
  so a capture failure cannot be misread as an ICE failure. `media_kind` itself stays
  undeclared: either kind is a legitimate input to this lane, and the flow claim is carried
  by `media_over_rewritten_candidate_{go,rust}` — RTP positive *and* growing across two
  samples 2 s apart, over the **host** pair whose remote is the rewritten advertised text,
  on a row whose advertised address differs from the un-rewritten baseline.
* **The rewrite target stays inside 127/8**, for the same reason every other lane here does:
  the harness must not touch the host's real interfaces. The consequence is stated above —
  connectable legs use `127.0.0.1`, advertisement-only legs use `127.0.0.2`.

## Nomination settle poll (C100)

The page used to take its second stats sample after a fixed 2 s sleep, and `run-paired.py` recorded
`sample_grow_ms = 2000` as a literal — the receipt asserted the harness's own sleep instead of
measuring it. Under load the transport's selected pair can still be `in-progress` 2 s after publish,
and that is what cost the first C100 lane C run its fence: the Go side of `flag-v4-mapped` reported
`nominated_pair_state: in-progress` against the Rust side's `succeeded`, so
`fence_divergence_reproduces` failed on a row that had merely been sampled too early.

The second sample is now taken by a bounded poll — 500 ms steps, floor of three, ceiling 10 s —
until the selected pair reaches a terminal state, and the page reports `nomination_settled` and
`nomination_settle_ms`, from which `sample_grow_ms` is read. A row that connected, still reads
`in-progress` and reports it never settled stops the lane rather than becoming a divergence. Two
controls hold that line, `connected_rows_reached_terminal_pair_state` and `sample_gap_is_measured`
(≥ 1500 ms), each with a matching `--selftest-gate` injection (`pair_never_settled`,
`sample_gap_unmeasured`). A terminal-but-not-`succeeded` state stays a behaviour row: the poll
separates "we did not wait long enough" from "the daemon did not finish", which is the distinction
the fixed sleep could not make.

## What this lane does not attest

* No claim about production NAT, a real public interface, TURN, or `use_external_ip`
  discovery: everything is loopback with `use_external_ip: false`.
* No TLS/WSS signalling and no WebTransport candidate path; the signal channel is plain
  WebSocket on an http origin.
* Subscriber-side receive behaviour is recorded (`rtp_in_bytes_received_positive`) but no
  second participant is launched, so no down-track claim is made.
* `rtc.node_ip` leaves that are not exercised here — `turn_policy`, the STUN/TURN port
  leaves, `intra_region_only`, interface include/exclude lists — remain at their previous
  tiers. This lane covers the two arrival channels of `node_ip` and the two incidental rows
  (`rtc.tcp_port`, signal scheme) that the same launches make observable.
* Egress byte totals across a daemon restart stay undecidable on this host (SPEC-0078's own
  conclusion) and are not asserted anywhere here.

## Re-running

```sh
redis-server --port <free> --bind 127.0.0.1    # harness starts its OWN redis on 127.0.0.1
python3 tests/interop/sdk-node-ip-candidate/run-paired.py \
    --go /tmp/c99-build/livekit-server-go \
    --rust target/release/livecloud \
    --output /tmp/c99-rerun-audio \
    --media audio
# Both offline modes read a persisted receipt and write beside it, so point them at the
# fresh scratch run rather than at the filed evidence.
python3 tests/interop/sdk-node-ip-candidate/run-paired.py \
    --selftest-gate /tmp/c99-rerun-audio/execution.json
python3 tests/interop/sdk-node-ip-candidate/raw-exit-table.py \
    /tmp/c99-rerun-audio/execution.json
```

Files: `run-paired.py` (orchestrator, profiles, gate), `page.html` (SDK observation page),
`probe.mjs` (Playwright driver, token minting via `livekit-server-sdk@2.19.2`, SDK/browser
identity capture), `raw-exit-table.py` (re-derives `raw-exits.txt` from a receipt).
The six runs listed at the top of this file are the receipts; only these four scripts and this
README live here, so the run directories below are what a fresh sweep produces.
Each
`work/<profile>/<profile>-<side>/` keeps the raw per-leg artifacts: `server.yaml`,
`config.sha256`, `argv.txt`, the daemon's `server.log`/`server.stdout`, the probe's
`probe.stdout`/`probe.stderr`, and `sdk-receipt.json`; the daemon and probe exit codes are the
`server_exit`/`probe_exit` fields recorded per leg in the receipt, and `raw-exits.txt` at the
run root is the table of them. Every committed stream is passed through
`../redact-evidence.py write` first and `check` must come back clean — classification is by
`ipaddress` validation, so loopback and RFC5737/`2001:db8` test addresses survive verbatim
while machine-local addresses do not.
