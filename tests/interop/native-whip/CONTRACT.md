# F10 native WHIP — implementation contract and isolated real-adapter draft

Status: integrated and scoped functional acceptance passed; current source/binary identities and limits are in SPEC0024/0025 and f14-functional-delivery evidence. The preparation checkpoints retained below describe historical preintegration steps, not current execution status. This is native `/whip/v1` RTC, distinct from external Ingress Create/List/Update/Delete or worker `/whip` endpoint. It must not be marked implemented by returning errors or synthetic SDP.

## Fixed source

LiveKit Server v1.13.10 `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`, protocol `9bb708cdb81918a6c63dd25e915b4644310b45b6`. SOURCE.json binds exact files. Apache-2.0 derivations retain original attribution in code; final integration must retain LICENSE/NOTICE obligations. Registry versions are not inferred here.

## Six public routes, exact source behavior

Source `pkg/service/whipservice.go:79-85` registers Go ServeMux patterns. GET also implies HEAD; real middleware auth/CORS may answer before handlers. Do not turn unsupported verbs into generic404 without paired Go HTTP verification.

| Route | Actual result / order |
|---|---|
| GET/HEAD `/whip/v1` |204, no resource lookup or handler-level authorization|
| OPTIONS `/whip/v1` |200; allow-origin `*`, allow-headers `*`, allow-methods `PATCH, OPTIONS, GET, POST, DELETE`, expose-headers `*`; no ICE server Link. Existing outer CORS preflight may answer204 first.|
| POST `/whip/v1` |Exact Content-Type `application/sdp` (parameters/case variants rejected400) before grant validation; set response Content-Type application/sdp; validate described below; actual room allocation→node lookup→actual RTC create.201 only after real answer.|
| GET/HEAD `/whip/v1/{participant_id}` |204 even nonexistent resource; no handler lookup/auth. Do not fabricate404 or304.|
| PATCH `/whip/v1/{participant_id}` |Exact `application/trickle-ice-sdpfrag` then response same Content-Type; missing If-Match428 BEFORE grants. Auth/room/identity/id checks, bounded body. If-Match exactly `*` means actual restart (200+fragment+new ETag), otherwise exact raw local ICE ufrag tag means trickle204.|
| DELETE `/whip/v1/{participant_id}` |Grant→roomJoin→nonempty room→nonempty identity→actual room/SID operation;200 empty on existing room even missing/stale SID; missing room or other participantRPC error404. No ETag precondition.|

The source ETag is raw ufrag, without added quotes. Location is relative `/whip/v1/<actual PA_ SID>`. POST Link entries derive actual selected node's ICE server urls/username/credential, not browser headers or invented STUN addresses. PATCH restart currently does not explicitly add Content-Type beyond the common earlier sdpfrag setting. GET does not expose secret SDP.

`handleError` writes `{"error":"..."}\n`, retaining any already-set SDP/sdpfrag Content-Type. It is not Twirp JSON. Source has a nil err bug in an untyped PATCH internal-error branch (`err.Error` panic); do not fabricate stack frames or call all500 equivalent. Actual fixed-Go oracle must bind this path before deciding a panic boundary.

## POST validation / source error order

`whipservice.go:121-208,210-267`:

1. Missing claims or nil video:401 `no permissions to access the room` (rtc.ErrPermissionDenied). Present video but roomJoinfalse:401 `permissions denied` (service.EnsureJoinPermission). These messages differ.
2. Empty token room:401 `room name cannot be empty`.
3. Room-name UTF8 byte limit:400 `room name length exceeds limits: max length N`.
4. Empty identity:400 `identity cannot be empty`; signed identity limit0unlimited, negative rejects nonempty and UTF8 bytes count:400 `participant identity length exceeds limits: max length N`.
5. X-LiveKit-ClientInfo first JSON decode into clientIp + subscribedParticipantTrackNames. Unknown fields ignored, struct field names accept Go case folding, JSONnull accepted, map values nil/null/empty supported. A single Decoder.Decode accepts trailing second JSON document; do not substitute serde_json::from_slice full-consumption semantics without source evidence. Bad JSON error prefix `malformed json in client info header: `.
6. X-Livekit-Ingress is true for any nonempty header, including text `false`.
7. Actual streamed MaxBytesReader bound1048576. Oversize413 `request body exceeds 1048576 bytes`; read error400 `body does not have SDP offer: <actual error>`; empty400 `body does not have SDP offer`. It is not the WebSocket signaling size limit.
8. Actual full SDP syntax parse,400 `malformed SDP offer: <source parser error>`. Prepared str0m validator performs real full parse, but exact Pion error text/grammar parity remains a separate oracle gate, not an assumed equivalence.
9. ParseClientInfo/augment actual HTTP form/query metadata; if protocol0 use17. Do not reuse signaling parse_join blindly: its auth/room/order differs. Name/metadata/attributes/grants/roomPreset/roomConfig are token-sourced; actual node StartSession separately checks metadata/attribute sizes, so their failure is503 through real create, not earlier invented400.
10. ParticipantInit AutoSubscribe=true, AdaptiveStream=false, DisableICELite=true. Room preset and token config must resolve with the same existing room creation semantics; code must not drop agents/playout config.

Room allocation/node failures500, StartSession/WHIP.Create RPC failures503, success201. ClientInfo.clientIp is currently carried by validateCreate but not used in actual WHIPCreateRequest (do not invent a trust policy based on it).

## Actual room / single-PC media architecture

Source `roommanager_service.go:46-164` starts real room participant with null signal source/sink and explicit `useOneShotSignallingMode=true`. It is NOT an ephemeral WebSocket connection and not merely ordinary `/rtc/v1` with a fake answer.

Required integration sequence:

1. Add explicit protocol-neutral session signaling mode (interactive versus one-shot) or equivalently tracked core policy. Keep ordinary SDK defaults untouched. One-shot is dependent for max-participant accounting (`participant.go4453`) and Verify additionally requires publisher ever connected (`1930-1936`). Do not hijack AGENT/EGRESS kind to obtain this property.
2. Retain actual SessionHandle across POST completion. Current Drop emits SignalClosed; dropping at HTTP return can remove a participant before ICE connects. Resource owner retains session, drains real signals, and closes only for actual room lifecycle/DELETE/shutdown. Room existing join/ICE verification deadlines remain authoritative, not a new arbitrary HTTP lease.
3. Single shared real publisher/subscriber PC with ICE-lite disabled. `TransportSpec.ice_lite=false` already reaches native `Rtc::set_ice_lite`. No second WebRTC stack or duplicate UDP socket needed. Native one-shot must not require application data channels before ready (`transport.go1015`).
4. Before offer processing synthesize AddTrack for every audio/video m-line, even recvonly/inactive (source does not filter directions). CID from exact msid secondtoken or first SSRC msid token; otherwise fresh TR_. Names `synthesized-microphone` / `synthesized-camera`; DTX disabled, stereo false, stream `camera`; video quality-only layers from up to3send RIDs or1dummy layer. Reuse core actual publication permission checks; do not synthesize media publication success from SDP alone.
5. Existing str0m accept_offer produces genuine answer, tracks and DTLS/SRTP. Current signaling.rs423–425 already adopts changed local ufrag and emits actual NetRouter remap. Native change/sdp.rs730–751 generates fresh local ICE credentials when remote offer credentials change, preserves local candidates and resets actual ICE. Thus restart reuses current PC and protected media graph; no fake random ETag.
6. Exact one-shot subscription semantics need explicit completion seam. Fixed source HandleOffer starts media work, waits every requested `(publisher identity,track name)` via true SubscriptionManager subscribed-track state, then GetAnswer. Current v1 emits LocalDescription immediately; simply inspecting publisher existence or queuing Subscription is NOT equivalent. A late named publication can require final answer regeneration with current real single-PC receive m-lines. This must be tested with a receiver-only offer and source arriving during wait; waiting on an impossible `bound` state before allowing native attach can deadlock. No fake success or silently ignored ClientInfo targets.
7. Suppress new interactive server offer/MediaSectionsRequirement behavior for one-shot only. Existing published/subscribed tracks within offered m-lines continue; no WebSocket client can answer a later arbitrary server offer.

## Resource / generation / control implementation supplied

`code/runtime.rs` implements a concrete real RoomManager/SessionHandle owner, actual Answer receipt, backpressured serialized queue, SID+room+identity ownership, caller deadline and same-PC offer restart. Initial params are real JoinParams with synthesized AddTrack and actual publisher offer. No synthetic SDP/worker/participant is present. Serialized command queue32 is backpressure, never drop/success-on-try_send. Runtime ownership avoids holding a clone of RoomManager inside its infinite drain loop.

`guarded-runtime-seam.json` proposes exact hash-guarded non-applied additions: generic SessionHandle.apply request+receipt, RoomInput.ApplySessionRequest validates SID+generation, performs real room mutation and settles effects before acknowledgment. This is needed for real DELETE and candidate operation receipts; existing SessionHandle.send means queued, not applied. DELETE must return only after actual model removal/effect path, not a fake accepted queue. Stale generation must never remove a new participant with reused identity. Existing room+wrong SID remains successful no-op per Go.

A late actual restart answer after caller timeout still updates resource remote/local credential snapshots in the owner; otherwise the old ETag would survive while native ICE has actually changed. The driver keeps per-offer pending documents for this reason. Requests have fixed caller deadlines; cached events must not renew timeouts. Resource cleanup uses unique PA_SID, never identity-only remove. Root integration needs explicit shutdown sequencing before HTTP drain and real callback cleanup; do not call detached runtime state production-ready until this is exercised.

This draft depends on the one-shot marker, native validation export and subscription completion seams above. It is NOT currently linked; do not activate handlers and claim completed six APIs merely from parser/runtime code. The next integration may stage genuine ingest-only actual media first, but the named-subscription parent item remains OPEN until implemented.

## Trickle and restart exact semantics

`roommanager_service.go297-354`, `transport.go1801-1998`:

- Lookup exact room + token identity + URL participant SID. No identity-only operation.
- Trickle compares supplied If-Match with CURRENT LOCAL publisher ufrag before parsing fragment. Mismatch is psrpc.FailedPrecondition and maps500 (not RFC412 invented by us). Message includes expected/got ufrag; safe evidence should omit credentials.
- Fragment must parse exactly one media section. Fragment MID must equal remote BUNDLE master MID. Extract credentials must exist and session/media credentials agree. Wrong nonempty credentials → `ErrICECredentialMismatch`. MID/credential/invalid-fragment map400; notfound404; other typed errors500. Invalid individual candidate AddICE failure is logged but does not fail whole operation (source calls every candidate). Fixed parser ignores candidates outside media and ignores bare `a=end-of-candidates`; do not silently 'fix' Go parity.
- Restart `If-Match:*` patches existing REMOTE full SDP credentials and candidates across ALL media sections per protocol/sdp.go610–727, including only replacing existing credential attributes and preserving rest. It validates ICE-lite/options compatibility and requested MID existence. Do not replace entire SDP with sdpfrag or recreate transport.
- Native then handles actual updated offer and creates real answer with gathering complete; extract bundled fragment and ETag from actual answer. Only malformed parse maps400 for restart; other patch errors wrappedInvalidArgument become500 by source HTTP switch. No304/412ETag optimistic semantics added.
- `code/fragment.rs` is source-derived parse/marshal/credential/patch/extract implementation; `Document` is only structural editing AFTER actual full SDP parse. It is NOT a substitute SDP validator and its full-marshalling parity is scoped to validated representation. No raw offer/ICE passwords should be written into public evidence.

## From-Ingress callback integration

`roommanager_service.go128-225`: on X-Livekit-Ingress nonempty, actual IngressHandler.WHIPRTCConnectionNotify topic=actualPA_SID once immediately and every10s, default PSRPC timeout; media audio/video state from actual published TrackInfo; actual participant close sends Closed=true with latest real media state and cancels timer. Source ordinary transient tick error logs and continues; initial error ends its notify loop. DELETE removes actual close callback after source close invocation ordering, not invented final success. Existing method descriptor is already in workers/methods.rs, but real source handling and cancellation must be integrated, not a no-op hook. All related callback/worker tests must use unmodified worker actual response.

## Required actual proof before closing nativeWHIP

1. Fixed-Go29 protocol vectors from `oracle/main.go` and real inputs `cases.json`: exact parse errors, credentials, canonical fragment, patched fullSDP and extracted fragment; Rust opt-in adapter `code/oracle_tests.rs`. All outputs must be generated by actual Go execution; no precomputed expected Rust values. Negative/missing MID/duplicate media/conflicting credentials/lastwins/whitespace/bareend/allmedia patch covered. Arbitrary invalid UTF8/fullSDP grammar parity remains explicitly separate.
2. Paired actual Go/Rust HTTP matrix for all six routes, HEAD, unsupported methods, outer CORS, absent/invalid video/nojoin/emptyroom/identity/negativeUTF8limits, exact Content-Type, If-Match order, malformed JSON/trailingJSON, body0/limit±1, malformed offers/fragments, wrongSID/identity/room, old/new ETag.
3. Real WebRTC WHIP client POST offer→real201answer/Location/ETag→actual ICE/DTLS/SRTP→440Hz decoded by unchanged LiveKit SDK. Existing SDK publishing880Hz with receiver-ready one-shot offer tests reverse content and delayed named track wait. Packet counters alone insufficient.
4. Actual PATCH real candidate (not fake204), wrong MID/credentials/currenttag error ordering; `*` remote ICE restart must produce native new creds, preserve same participant/track SIDs, update UDP ufrag routing and resume real decoded media. Old tag fails exactsource status. No recreated participant or backend swap.
5. DELETE actual media/DTLS/UDP allocation cleanup, SDK ParticipantDisconnected, other room participants remain; repeated/mismatched SID deletion source behavior. Concurrent identity replacement and old DELETE cannot kick replacement. Canceled POST/PATCH and held HTTP caller need no leaked signal owner/native sockets/tasks; source cancellation equivalence must be checked, not assumed.
6. Real unmodified Ingress worker nativeWHIP mode tests immediate/periodic/final connection notifications and actual ingress state/room/media, not callback mocks. Existing external WHIP worker path does not count as native server success.

No threshold, timeout, SDP/SDK/network mutation is authorized by this draft; functional changes require actual source-backed tests and existing media gates retained. No compile/services/oracles executed by this subagent.

## Continued concrete seam implementation checkpoint (supersedes earlier OPEN implementation status, not verification)

The isolated `payload/` now supplies actual existing-file implementations of the three previously identified media/runtime prerequisites. `payload-manifest.json` binds original and proposed file hashes; no shared file was changed and nothing was compiled.

- Neutral `ClientCapabilities.one_shot` defaults false in the ordinary SDK projection. It flows to actual TransportSpec; core dependent accounting includes one-shot without impersonating AGENT/EGRESS or changing participant kind. All existing explicit fixture struct literals are mechanically initialized false; existing assertions remain intact.
- Native PrepareOneShotOffer accepts actual offer on existing Rtc and prepares actual upstreams/m-line capacity but suppresses LocalDescription until finalization. No forged SDP is returned. Actual media Subscribe creates a real DownTrack, then emits SubscriptionPrepared. Core readiness requires that real prepared subscription object's track owner identity and actual published name both match. It does not infer completion from queued Subscribe or require final SDP bound state (which would deadlock). Teardown/failure invalidates readiness.
- Once all actual source-required subscription objects exist, runtime applies FinalizeOneShotAnswer. Native re-applies the same real remote offer against the now-current DownTrack graph and emits an actual newly generated answer, including true assigned stream/track IDs. Prepared transaction ID is exact; stale finalize cannot act. Finalized state is cleared, so cached finalization cannot replay an old offer after ICE restart.
- Native parse/accept failure on one-shot emits RemoteDescriptionRejected containing actual operation id/error. Core forwards through neutral existing UnclassifiedError control reply; owner matches exact request id and retires that pending remote document. Ordinary SDK invalid-offer behavior is unchanged. This is a real native result receipt, not queue acknowledgment or invented503.
- One-shot RoomTask signal sink discards interactive notifications like source NullMessageSink, retaining only actual operation answer/error control output. This avoids filling an unused WebSocket-style queue while a named source is awaited. Existing interactive queue policy remains unchanged.
- WhipSessions concrete owner prepares the real offer after joining with no automatic publisherOffer, waits actual named-subscription query, then finalizes. It retains the real session after HTTP completes, uses fixed request deadline, applies generation-checked DELETE with actual room settlement, and can consume DELETE while its initial POST is waiting for a named track. No temporary success state is granted.

New meaningful isolated regressions use actual native str0m Offer/Answer objects: late DownTrack must appear in final actual answer and be accepted by actual client; no early answer/stale transaction; native invalid offer gives real matching failure only on explicit one-shot path; actual ICE restart changes native ufrag on the same transport. Core tests cover requested-versus-prepared-versus-bound, publisher/name/generation mismatch, failure invalidation and source dependent accounting. These are executable component tests, not proof of decoded network media.

Additional source Go14 oracle uses exact extracted private production synthesizeAddTrackRequests method body with only receiver name substituted for a collector, importing real Pion/protocol/buffer helpers. This is explicitly a source-method domain oracle, not full official-server execution. Rust compares the complete projected AddTrack for every row; independently random TR_ identifiers are compared only by exact prefix/length/alphabet domain, while all remaining fields remain exact. No actual Go output is supplied or invented.

`interop/run.mjs` + `native.js` now prepare the actual content gate: real ordinary RTCPeerConnection440Hz publisher/sendrecv, true WHIP POST with named SDK880Hz target, actual gathered-candidate PATCH, real remote ICE restart/answer-fragment merge, old ETag source500, unchanged participant/track SIDs, decoded media progress after restart, and real DELETE/SDK room leave. Existing accepted SDK consumer track.attach/play is retained. No data channels are created. All inputs/standards remain pending actual execution. Full HTTP route assembly and X-Livekit-Ingress real notifications remain separate explicit integration work, not falsely marked complete by these concrete seams.

## Full isolated assembly checkpoint (2026-10-25, not compiled)

`payload/` now includes six real HTTP handlers, existing global auth/CORS layering,
owned resource map, streamed 1MiB bodies, ordered Go ClientInfo decoder, actual
one-shot offer/error/answer receipts, named DownTrack readiness, same-native-PC
ICE restart, generation-safe acknowledged DELETE, and server lifecycle hooks.
There is no success-producing fake backend. The actual native RTC must accept the
offer and emit an answer before HTTP201 can be sent. Existing ordinary SDK
capabilities default `one_shot=false`; all ordinary gates remain mandatory.

The new Ingress callback path uses the same actual `RpcClient` as worker services,
`IngressHandler.WHIPRTCConnectionNotify` on the actual participant SID with
require_claim=false/queue=true. Initial and 10-second calls use the pinned default
3-second timeout, independent of configured create/participant RPC timeout.
**Correction to the earlier notification paragraph:** pinned
`roommanager_service.go:165-190` continues after an ordinary initial or tick error;
only cancellation (and ParticipantNotFound on ticks) exits. The implementation
follows this actual code. A real participant-close watch cancels an in-flight
alive call, then independently sends Closed=true. Published tracks are already
cleared by `UpTrackManager.Close` before the pinned onClose callback; closed
notifications consequently have nil media. Callback failure is logged; it is
never turned into a successful worker result.

Alive AverageBitrate is derived from authenticated RTP observations, not
allocation/declared encoding rates: actual protected datagram length and SRTP tag
removal, authenticated header+unpadded payload, padding-only exclusion,
main-sequence deduplication, RTX prefix normalization and per-stream aggregate
elapsed time. Raw packet observation is enabled only for the explicit one-shot
transport. The6-case actual RTPStatsReceiver byte oracle is a required gate, not
an executed result. The API exposes actual media state only through a narrow
SID-guarded runtime query; no full engine configuration is exposed.

Two source corrections are included: native WHIP StartSession calls
getOrCreateRoom directly and does not call ordinary RTC ValidateCreateRoom, so
`room.auto_create=false` does not prohibit its granted one-shot request. Ordinary
SDK joins keep their original policy. Unknown roomPreset is node StartSession
failure (503), after metadata/attribute checks, and only consulted on new room
creation. Preset application preserves the current agents/playout fields.

### Remaining acceptance risks (not a feature support claim)

* Every file is still NOT_COMPILED/NOT_RUN until the coordinator applies and
  executes it. No Go result or media success has been fabricated.
* Real str0m full-offer parsing is implemented; exact malformed SDP grammar/error
  parity with Pion must be evaluated by the paired actual HTTP recorder. A parser
  error mismatch is a real OPEN compatibility defect, not an allowed wildcard.
* Source one-shot video-size-change semantics (`participant.go:3719`) have no
  complete current Rust codec dimensions consumer. The native440/880 audio gate
  does not prove VP8/VP9/H264/AV1 input dimensions. This remains a distinct media
  completeness gap; nil/0 is not mislabeled as a measured dimension.
* The source's untyped/nil PATCH error panic branch is not exercised by the local
  typed runtime adapter; it must not be rewritten into a generic400 or forged Go
  stack. Missing actual room/participant and typed malformed/credential/MID/tag
  outcomes are concretely modeled.
* AverageBitrate uses an actual bounded RTP observation ledger; its6-byte-vector
  oracle is not a full equivalence proof of upstream timestamp/sequence restart,
  FEC or jitter statistics. The actual external Ingress proxy/watchdog callback
  scenario still needs real worker execution.

Apply only `apply-guarded.py`/`payload-manifest.json`; the old
`guarded-runtime-seam.json` and `history-*` scripts are historical inputs already
incorporated into the payload, not a second patch to apply.


## Current execution authority

See `livekit-rs-docs/evidence/2026-10-25/f14-functional-delivery.json` and
SPEC0024/0025 for actual997 tests, explicit Go comparisons, HTTP45, native
audio/restart, three decoded video codecs, cache/live API and original-worker
proxy. Original draft NOT_RUN statements above are historical. C5 remote-owner,
opaque fullSDP, VP9 ONE SVC and missed-tick edges remain genuinely open.
