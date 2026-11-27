# SPEC-0077 C106 — the `limit.*` gate channels, judged on what a real client is refused with

**Batch:** C106-20261127-limit-gate-channels (evidence only — **no production code change**)
**Leafs:** 20 — eight `YAML:limit.*`, the three `YAML:room.max_*` alias copies, four `generated_flags:--limit.*`,
`WIRE:RTC.PerMessageDeflate`, `HTTP_BEHAVIOR:middleware.body_limit`,
`API_BEHAVIOR:RoomService.UpdateParticipant.owner_local_limit_error_echo`, and two ordering rows this lane minted
(`HTTP_BEHAVIOR:twirp.content_type_negotiation`, `HTTP_BEHAVIOR:twirp.decode_before_auth`).
**Tier:** 5 — both binaries launched side by side on identical YAML bytes (plus argv in one profile), then real
Twirp calls, real request bodies, real RFC 6455 joins and real worker-socket frames against each: 367 graded legs,
7 118 recorded cells, 16 launches. Nothing is judged on a config struct or a unit-tested predicate.
**Verdict:** `ACCEPTED` — 0 pair diffs, 54/54 family gates pass, 106/106 per-leg expectations pass, 20/20 rows pass,
0 privacy violations, against Go `4bc2378e…` (source `e5655ac…`) and Rust `f6fe7f03…`.

Before this lane the `limit.*` rows sat at the projection tier: C98 recorded *"projection tier only: no leg boots
the assembled binary"*, and the static reviews disagreed about what a non-positive number even means — one
`static_finding` read *"0/nonpositive disables"*, another had filed a signed-negative config as
`GAP_SIGNED_NEGATIVE_BECOMES_UNLIMITED`. C106 launches both daemons per profile and puts each gate's answer on the
wire, so those questions are measured instead of re-read.

## Mechanism read from the oracle, then measured

| fact | upstream | measured here |
| --- | --- | --- |
| `max_api_request_body_size` wraps every v1 handler in `NewRequestBodyLimiter` before auth | `pkg/service/server.go:484-490`, `pkg/service/utils.go:139-154` | 413 `request body too large` (22-byte `text/plain`) at 512 and 256, **and at the built-in 10 MiB** with a 10 MiB+1 body; an anonymous oversize body still gets 413, and an unknown route gets 413 rather than 404 — 9 or 10 body legs in each boundary profile, 2 where the configured number leaves no body boundary |
| a chunked body has no declared length, so the guard acts on bytes actually read | `pkg/service/utils.go:145-153` | `body_chunked_over` → **400** malformed `failed to read request body`, not 413, on both sides; `body_lying_length` (declared 10 over a huge body) → **200**: the transport, not the limiter, decides what the handler reads |
| the signal read limit is installed only when positive, and bounds the **payload** | `pkg/service/rtcservice.go:488-491`, `:503`, `pkg/service/wsprotocol.go:70-88` | `sig_payload_at` (payload = 2048) answered with a pong, `sig_payload_over` closed 1009, `sig_frame_over` (frame one past, payload under) answered — a header-counting guard would have refused — and `sig_fragmented_over` closed 1009, so fragments are judged as a whole |
| deflate has a second, application-tier bound | `pkg/service/wsprotocol.go:73-85` | `sig_deflate_plain_over`: compressed bytes under the bound, inflated bytes over → close **1000** with an empty reason, while an over-large *compressed* payload → **1009**; the two-tier signature is identical on both sides |
| the worker socket takes `agent_signal_message_size_limit`, not the participant number | `pkg/service/agentws.go:88`, `:96` | `agent_reg_payload_at` registers, `agent_reg_payload_over` is refused, `agent_reg_frame_over` registers, and the in-between message discriminates in **both orders**: refused in `tight` (agent 1024 < signal 2048, mid 1 536), answered in `agent-gt-sig` (agent 4096 > signal 1024, mid 2 560) — control `agent_mid_sits_on_one_bound` names `agent_signal_message_size_limit` |
| length gates are `limit == 0 \|\| len(value) <= limit`, so a negative number rejects everything | `pkg/config/config.go:399-425`, `pkg/service/roommanager.go:296-303`, `pkg/rtc/participant.go:740-752` | `signed-neg`: a 1-character room name is refused `room name length exceeds limits: max length -1`, a 4-character name is refused `name length exceeds limits -1`, and the join itself is refused `participant identity length exceeds limits: max length -1` — byte-identical text on both sides |
| byte gates are `if limit > 0 { install }`, so a negative number disables | `pkg/service/utils.go:140`, `pkg/service/rtcservice.go:488`, `pkg/service/wsprotocol.go:75` | the mirror half of the same profile: with `signal_message_size_limit: -1` a 4 096-byte frame is answered (`answered-without-close`) and with `max_api_request_body_size: -1` a 4 096-byte body reaches the handler for a 404 `requested room does not exist` rather than a 413, while `agent_signal_message_size_limit: -1` still registers a 4 096-byte worker message |
| `max_metadata_size`/`max_attributes_size` are `uint32`, so a negative value never parses | `pkg/config/config.go:373-378` | `unsigned-neg`: **both** daemons exit 1 in ~130 ms without binding anything, with the same two stderr lines — `could not parse config: yaml: unmarshal errors:` / `line 12: cannot unmarshal !!int \`-1\` into uint32` |
| three `room.*` fields overwrite their `limit.*` counterpart when non-zero | `pkg/config/config.go:739-749` | `legacy-alias` configures only `room.max_metadata_size: 128`, `room.max_room_name_length: 16`, `room.max_participant_identity_length: 16` and every metadata/room-name/identity leg then refuses at the alias number with the same text as the `limit.*` profile; the participant-name gate has no alias, so it keeps answering at the built-in 256 |
| the guards sit in a fixed order in front of each other | `pkg/service/server.go:484-498`, `pkg/service/roommanager.go:296-303`, `pkg/rtc/participant.go:740-752` | ordering map read leg-by-leg: body limiter → decode → auth → permission → limit → room lookup. `update_room_bad_proto_body` (valid token, garbage body) → 400 malformed; `update_room_over_bad_token` → 401 `token signature is invalid`; `update_room_over_no_token` → 400 with the limit text; `update_room_over_not_admin` → 401 `permissions denied`; `update_room_over_missing_room` → 400 with the limit text; `update_participant_double_violation` (name **and** metadata over) → the **name** gate answers |
| a join token claim is size-checked at join | `pkg/service/utils.go:368-370`, `:386-388`, `pkg/service/roommanager.go:296-303` | over-limit `identity` claim → 400 with no upgrade; over-limit `metadata`/`attributes` claim → **500** (`connection closed by media`); over-limit `name` claim → **101, joined**: the join path does not gate `name`, which is why that row is carried by the update channel |
| `0` disables every channel | same guards, `> 0` conditions, defaults at `pkg/config/config.go:620-632` | `zeros`: 65 536-byte signal frame, 8 192-byte worker message, 4 096-byte body and a 4 096-character claim all accepted on both sides — the same probes are refused in `tight` |

## What counts as an effect here

* **A status line, not a claim about code.** 276 legs per side carry an HTTP status (`http_code`, Twirp `code`, `msg`,
  `text`, `ctype`, `meta.argument`, byte count) from `http_raw`, which reads until the declared body is complete. Every
  refusal cell in the table above is text the running server wrote.
* **A socket that either answers or dies.** 258 websocket legs across the pair (129 per side) record the upgrade status,
  the frame sequence (`binary(join)`, `binary(pong)`, `close:1009`, …), the close code and how the stream ended; 65 joins
  per side reach `101 Switching Protocols` and a session. `gate_class` projects that into
  `refused`/`answered`/`silent` and is what the agent rows are graded on, so a difference in *how* the socket dies stays
  visible without being scored as a gate divergence.
* **A family, not a single refusal.** Each gate family must show a refused leg **and** an accepted leg whose verdicts
  differ, per side: 54 applicable family evaluations (the eight byte/length gate families × 5 profiles with a real
  boundary = 40, `claim_gate_talks` × 4, `update_metadata_reports` × 4, `ordering_map_distinct` × 6). A profile whose
  number leaves no boundary (0 or negative) records the family as not applicable **with the reason** — 23 such
  evaluations — so "nothing was refused" cannot read as parity.
* **The number we set, proved by two fixed probes.** `sig_fixed_4k` and `body_fixed_4k` are the same 4 KiB byte
  stream in every profile; `sig_fixed_4k_follows_the_configured_number` and
  `body_fixed_4k_follows_the_configured_number` require the verdict to *flip* across profiles and to agree across the
  sides. A daemon that ignored its config could not satisfy either.
* **Per-leg expectations, 106 of them.** Each profile states what its own limits imply (`yaml-vs-flag`: 4 KiB refused
  because argv says 1024/256; `signed-neg`: the join refused outright; `defaults`: a real-size client still works) and
  a side that obeys YAML instead of argv, or a length gate instead of a byte gate, fails there — not only in the pair
  column.
* **Startup as an answer.** `unsigned-neg` is graded on `ready`/`alive`/`self_exited`/`exit_class` — exit code, whether
  the port ever answered, whether the process exited by itself inside the grace window, and which sockets it held
  (none) — with the error text recorded but deliberately not paired.

## Profiles (8, seven live)

`tight` — eight small distinct numbers, the whole gate map in one launch. `zeros` — 0 on every channel, so the large
probes must pass. `signed-neg` — the signed fields negative: the length gates reject everything, the byte gates
disable. `unsigned-neg` — `max_metadata_size: -1` into a `uint32`: startup only. `legacy-alias` — only `room.max_*`
configured. `yaml-vs-flag` — YAML says 8192/4096/4096/8192 while argv says 1024/768/96/256 on the same keys.
`agent-gt-sig` — the agent bound *above* the participant bound, the mirror order of `tight`, which is what makes the
worker-socket bound question answerable. `defaults` — no limit block at all, probed with a real-size client.

## Differences recorded instead of averaged

Thirteen cell families differ while every graded cell matches. They belong to the two stacks, not to a gate (the counts are
the differing legs the report's histogram lists):

* **Worker-socket teardown** (12 `verdict` + 12 `verdict_class` legs): Go writes `close:1009` with an empty reason, Rust
  drops the socket without a frame. Both refuse; only the mechanism differs, so `gate_class` is graded and
  `verdict`/`close_code` stay in the histogram.
* **413 reason phrase** (18 legs): `Request Entity Too Large` vs `Payload Too Large`. The code and body text match.
* **Header casing** (178 legs): Go canonicalises (`Content-Length`), hyper lowercases (`content-length`). Pinned as a
  per-side claim (`http_header_casing`) rather than a pair, because a client that reads headers case-sensitively
  breaks on one side only.
* **Echoed protobuf** (65 `body_sha`, 25 `clen`, 25 `body_bytes`, 1 `wire` leg): an accepted body returns a
  `RoomInfo`/`ParticipantInfo` whose encoding differs by a byte or two and whose SID differs outright, so those legs
  grade status, code and message instead of length or digest.
* **Push order and volume** (`msgs` 43, `n` 28, `stream_end` 13, `held_frames` 6, `resp_fields` 1): the frames a
  socket happens to hold in the read window, including an extra `refresh_token` on one side.

## Seven claims this lane made and then measured false

1. *"A negative byte limit makes the stream unlimited, like the length gates."* The opposite: length gates are
   `len > limit` (a negative limit refuses everything) and byte gates are installed only `> 0` (a negative limit
   refuses nothing). The `signed-neg` profile was built to hold one rule for both kinds and had to split them.
2. *"Rust answers an undecodable Twirp body with 401 where Go answers 400."* It does not: that reading came from a
   case-sensitive header lookup in this harness, which let the Rust read wait for the window to expire. With
   `http_raw` case-insensitive both sides return 400 malformed, and the ordering row is a parity.
3. *"The 10 MiB body probe never got an answer."* It got 413 all along — the client abandoned the read after
   `sendall` raised while the limiter had already written its status line. Now the write failure is recorded and the
   read window still runs (`send-error:*` is a distinct sentinel from `connect-error`).
4. *"A frame whose header pushes it past the limit is refused."* Both sides answer it: the bound is on the payload.
   `sig_frame_over`/`agent_reg_frame_over` exist to keep that from being assumed.
5. *"An in-between message proves the two byte gates are separate."* It proves nothing on its own — it is consistent
   with either bound governing. `agent-gt-sig` was added so the same leg asks the question in both orders, and the
   answer names the agent limit.
6. *"The join path gates the `name` claim."* It does not, on either side: `tok_name_over` is a 101. The row is
   carried by `UpdateParticipant` instead, and the finding is written into its `accepted_scope`.
7. *"Whether the resident holder's read window ended is a parity fact."* It is harness timing: the third recording
   came back `NOT_ACCEPTED` on exactly that cell (`closed_during` 1 vs 0 in `zeros`). The cell is now recorded and
   shown but not graded, and the leg is graded on `http`/`held` — the claim it was written for.

## Falsifiability

15 named mutations, all RED against the filed receipt, `receipt_sha256_before == after` proving the sweep ran
read-only: `body_over_code`, `metadata_msg_neutral`, `signal_close_code`, `signal_frame_counting`,
`zeros_body_refuses`, `drop_agent_legs`, `leak_port`, `startup_agrees_with_rust`, `wipe_legs`, `identical_cells`,
`flips_every_accept`, `mid_bound_never_refuses`, `agent_class_diverges`, `identity_join_never_refuses`,
`headers_copied_from_the_other_stack`. Six of them failed their own audit first and were fixed rather than dropped:
`agent_reg_ok` keyed on a field (`register_success`) that `livekit_agent.proto` never emits, so the whole agent
family rested on an unproven handshake; the `sig_join_ok` expectation contradicted `signed-neg`'s own intent; the
family predicates read `verdict` while the graded cell read `gate_class`, so `agent_class_diverges` stayed green;
acceptance never looked at families, so `drop_agent_legs` stayed green; `orders == {(True, False), (False, True)}`
was too strict for equal-bound profiles; and `agent_mid` degenerated to `== agent` whenever the agent bound was the
larger one. Two more came from re-recordings rather than from the sweep: the third run failed on
`resident_joined/closed_during` (a harness read-window artifact in the pair column, now recorded but not graded), and
the fourth showed `YAML:limit.max_participant_identity_length` passing with `gates: []` — pair equality alone was
carrying it, so the lane gained the `identity_gate_talks` family and `identity_join_never_refuses` to keep that gate
falsifiable. The acceptance rule now requires 7 live profiles, ≥20 legs each, every applicable family passing, every
control, every expectation, zero pair diffs and zero privacy hits — and `--report`/`--selftest-gate` re-derive all of
it from cells alone.

## Privacy

Ports and the store port are tokenised (`<HTTP>`, `<STORE>`), the signing secret and the store password as
`<SECRET>`/`<STORE_PASS>`, paths as `<WORK>`, and the config written per launch is stored masked (`cfg_masked`).
No fixture payload reaches a receipt cell — only lengths, digests and the server's own refusal text; `tail` is a
window-end sentinel, never response bytes. `leak_port` proves the sweep can fail: it writes an unmasked allocated port
into a cell and the run stops. Nothing binds outside loopback, `7881`/`7882` and the host store `6379` are never used,
and no machine address appears in the receipt; every daemon is owned by this lane through its `/tmp/c106-*` config
path and stopped at the end of its profile.

## Honest residual

* **No media.** Every leg is signalling or HTTP: the lane says nothing about tracks, and no SDK client is used — the
  join is this harness's own RFC 6455 upgrade, so `has_join`/`close_code` are this lane's claims.
* **The worker socket is registered, not used.** `agent_*` legs prove the read bound on a live worker connection; no
  dispatch, job or function round trip is driven, and `WORKER_BACKEND`/`AGENT_WIRE` rows stay where they are.
* **One profile per flag channel.** `generated_flags:--limit.*` rows are named only by `yaml-vs-flag` (the only
  launch that puts those flags on argv); the 202 excluded (row, profile, leg) triples are listed in the report so the
  narrower scope is visible rather than implied.
* **`unsigned-neg` is startup-only** by construction: a daemon that never starts has no gate to probe, so the profile
  contributes one leg pair and no families.
* **Env channels untouched.** `LIVEKIT_LIMIT_*` remains a separate row per key; this lane drove YAML and argv.
* **UTF-8 byte-vs-rune width is not re-litigated here**: the earlier C16 rows already pin the multibyte boundary for
  the name/identity/room gates, so C106 probes ASCII counts and points at those rows.
* **Mechanism divergence is recorded, not decided.** Whether Rust should write a `close:1009` before dropping the
  worker socket is a product call this lane deliberately leaves open (see the divergence list).

## Reproduce

```
cd references/livekit-stable && GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off \
  go build -mod=readonly -o /tmp/c99-build/livekit-server-go ./cmd/server
cargo build --release --bin livecloud
python3 tests/interop/limit-gate-channels/run-paired.py --record --out /tmp/c106-lane
python3 tests/interop/limit-gate-channels/run-paired.py --report /tmp/c106-lane/receipt.json
python3 tests/interop/limit-gate-channels/run-paired.py --selftest-gate /tmp/c106-lane/receipt.json
```

`--report` and `--selftest-gate` need no binaries: they recompute every leg, family, control and expectation from the
recorded cells and re-derive the verdict. `run-paired.py` is assembled from `part1…part7` by `assemble.py` (the parts
are the readable source; the single file is what the receipts hash), and it refuses to record unless both binaries
match the pinned images. It imports the C102 launcher/allocator/socket observer/owned store/masker (`fff0615a…`),
the C103 certificate and JWT builders (`93abcdd6…`) and the C105 protobuf codec plus the raw RFC 6455 client
(`7b5afa84…`) rather than copying them, and records the digest of C104 (`a4b5eacb…`) whose material helpers C105
re-exports, so those lanes stay byte-identical and their filed receipts stay re-derivable.
