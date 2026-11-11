# F107v2 generation-bound unchanged SDK reconnect fixture

**ISOLATED / BROWSER NOT_RUN.** Pure Node criteria tests and syntax checks have run; no browser, compiler, Go, server or shared source was executed/modified. Parent owns server profiles and all execution. Browser client is actual npm `livekit-client@2.23.0`, checked at runtime and served byte-for-byte; original server SDK2.19.2 does actual RoomService SendData. No SDK patch, server fake MediaEvent, codec packet rewrite or synthetic error is used.

## Source contract

- Fixed `transportmanager.go:369–371` sends downstream via real primary transport. `handleSendDataResult:383–422` calls OnDataSendError only for actual SCTP stream-closed / closed-pipe writer results; it does not infer error merely from a receive close notification.
- `participant.go:4247–4251` reconnects on this real writer error only if configured. `IssueFullReconnect:4085–4090` closes old participant session, requiring a new session, not mere signal resume. Rust current `rtc-media/signaling.rs:1251–1307` and `rtc-core/room/media_events.rs:13–25` provide the actual producer→owner consumer path; prior unit evidence is not full SDK evidence.
- SDK `RTCEngine.ts:1958` exposes a runtime native-channel getter; source `data-channel/DataChannelManager.ts:102–115` returns subscriber-side handle separately from publisher handle. Fixture only invokes this existing runtime inspection method and the native RTCDataChannel.close; no method replacement. Actual npm2.23.0 UMD includes the same method. Prime SendData receipt **and native message count** must prove the selected handle is the actual server downstream path before close. Wrong handle is a failure, not skipped.
- SDK `RTCEngine.ts:1071–1084` logs unexpected publisher close without renegotiation. Close-only is independently observed five seconds: no reconnect event/SID turnover and real media still advances. Then a single actual API write to the closed kind triggers the causal case.

## Matrix and gates

Two genuine server profiles (flag false/true), each actual pinned Go then Rust, and each row is fresh room plus three SDK participants. In each server run both reliable/lossy × single/dual PC =4 rows. Four runs make16 rows; comparator rejects partial/combined campaigns. All raw results must be retained including FAIL.

Each row proves:

1. actual baseline 30 decoded video frames +10 audible 100ms samples in victim receiving source and witness receiving victim (30 seconds), three API-visible participants;
2. selected native handle actual prime data + actual close; five-second media-active no-reconnect control, non-target does not receive targeted data;
3. flag false: real subsequent SendData does not reconnect or change SID; selected trigger not delivered, alternate kind still delivers;
4. flag true: official SDK Reconnecting then Reconnected, new SID and new open native handle, two actual local tracks republished, actual source→victim **and victim→witness** media progress +30 video/+10 audible and post-rebuild targeted data. One immutable 30-second deadline begins at causal SendData for rebuild/media/data (no sequential30+30 extension);
5. five-second actual decoded audio/video sustain, three API participants without leak, non-target remains isolated; owned close/context/deleteRoom/browser/HTTP cleanup errors fail the result.

Video uses normal SDK track.attach/autoplay/play, actual requestVideoFrameCallback, synthetic canvas640×360 at15fps and max1.8M, same existing interop workload style. Audio440Hz is actual WebAudio capture→SDK→RTC→SDK normal playback/analyser, threshold0.01. This is not a congested-network/BWE campaign and does not replace original five+accuracy acceptance. No original timeout/network/threshold is relaxed. Playback errors are failures, not swallowed.

## Execution (parent only)

Prestart actual servers with identical existing configuration plus the **declared** disabled/enabled YAML fragments. Do not trust RECONNECT_FLAG to mutate the server: it is the expected profile assertion, parent must bind effective config/source/binary identities to results. Use separate owned output paths:

```
INTEROP_ROOT="$REPO/livekit-rs-server/tests/interop" LIVEKIT_URL=ws://127.0.0.1:17880 LIVEKIT_API_KEY=devkey LIVEKIT_API_SECRET=secret RECONNECT_FLAG=false SERVER_LABEL=official RECONNECT_OUTPUT=/owned/go-false.json node "$REPO/livekit-rs-server/tests/interop/reconnect-writer-error/run.mjs"
# repeat unchanged against Rust false, then actual Go true and Rust true
node "$REPO/livekit-rs-server/tests/interop/reconnect-writer-error/compare.mjs" /owned/go-false.json /owned/rust-false.json /owned/go-true.json /owned/rust-true.json
```

Leave SINGLE_PC unset for mandatory full matrix; a narrowed SINGLE_PC=true/false is diagnostic only and final comparator rejects it. TLS profile must use existing trusted CA browser setup before claiming WSS; no global certificate bypass is added here. Fixture records no JWT/SDP/ICE/server credentials or participant SID. Results contain controlled identities/codec/channel metadata and event order. Failure snapshots deliberately preserve media/event diagnostics, never catch cleanup failures as success.

## Publication/subscription flag scope — OPEN, not fabricated

Fixed `participant.go:4093–4114` distinguishes publication supervisor failure from fatal subscription failure; ordinary PC/ICE close can trigger unconditional transport recovery regardless either flag and is **not** a valid differential for these flags. Native `MediaStreamTrack.enabled=false` or normal mute/unpublish is not a publication failure; a silent eligible JS SDP registration already completes publication. `replaceTrack(null)` after publish often happens after registration, so cannot be advertised as pending-publication proof. Browser first RTP/padding and RID/SSRC eligibility must be measured before using such a scenario; current fixture does not fake the event or alter SDP to force it.

For subscription, normal unsubscribe/revoke/unpublish is nonfatal; a forced CodecUnsupported requires an actual unsupported codec path while preserving the SDK and negotiation. A native PC close proves transport failure, not the15-second fatal subscription consumer. Reliable isolated fault construction is not established here. These two SDK profiles remain OPEN, even if all16 data-channel rows pass. Future fixtures need genuine producer evidence + source default retry/supervision schedule before comparing flag branches; no mocked fatal callback or borrowed timer count can close them.


## F112 corrections, original fixture preserved

Original F107 at `/tmp/f107-official-sdk-reconnect-consumers` and review
`/tmp/f112-sdk-reconnect-fixture-review` remain unchanged. No original matrix
row, causal control, threshold, cleanup failure or strict event comparator was
removed. This is a new version, not a rewrite of old evidence.

### Actual generation producer

Each real TrackSubscribed observation binds all of:

- actual local participant SID **change** (kept privately, exported only as an
  integer local-session epoch);
- actual current remote participant instance/SID in the SDK room map, with a
  per-identity SID epoch; stale old callbacks do not mutate that epoch or credit
  media;
- actual SDK-provided RTCRtpReceiver membership in a current native PC's
  `getReceivers()` list;
- actual native RTCPeerConnection object identity (WeakMap integer ID);
- actual new TrackSubscribed track epoch, before any decoded-frame/audio counts.

The fixture reads the SDK's actual `pcManager.mode`, publisher/subscriber `_pc`
objects and actual instanceof RTCPeerConnection. It never calls the lazy `.pc`
getter (which could create a PC) or overrides an SDK method. Single requires
publisher-only + one native PC; dual requires publisher/subscriber mode + two
physically distinct native PCs. It verifies all three clients before fault and
the victim/witness actual topologies after recovery. Missing inspection seam is
failure; during a normal transient rebuild a missing topology is only an
unavailable snapshot, never invented valid topology.

For source→victim, qualifying media requires a new victim local SID epoch, a
new receiver PC object, a new subscribed track epoch and current participant
binding. The source's own SID need not change. For victim→witness, the witness
local session/PC must remain unchanged while its remote victim participant SID
and subscribed track epochs must be new. Thus each direction uses its actual
expected ownership boundary; the fixture does not wrongly demand that an
unaffected source or witness reconnect.

The shared `criteria.mjs` selects only these current-generation observations.
Old history stays in failure reports but cannot contribute a single frame or
sample to the recovered30-video/10-audible threshold. Both full thresholds and
640×360 decode must be met **before** the separate five-second sustain starts.
Sustain then requires positive progress from the same qualifying generations
for victim video/audio and witness video/audio. It cannot supply a missing part
of the earlier threshold. The original cumulative progress checks are also kept.

### One hard monotonic recovery deadline

The trigger, actual API writer receipt, SDK reconnect events, real PC/channel
replacement, new-generation media and post-rebuild targeted data share one
monotonic30-second deadline. `callBeforeDeadline` races an outstanding operation
against the remaining bound and rechecks time after awaiting it; `pollUntil`
also checks after each predicate. Each row checks actual elapsed before marking
rebuild PASS, and final comparator keeps the original elapsed bound. A delayed
true result cannot sneak past either gate. Initial/media/data/close-only and
five-second controls otherwise retain the original standards.

### Actual pure tests, not SDK execution

```
node --test --test-reporter=tap "$REPO/livekit-rs-server/tests/interop/reconnect-writer-error/criteria.test.mjs"
```

The actual persisted run is `actual-node-unit.tap`: **12 pass,0 fail,0 skip**.
These tests exercise exactly the helper imported by the real runner. They
include old300/100 + new1/1 rejection, genuine new30/10 positive, wrong physical
mode, old PC, unchanged participant, stale track, invalid receiver/current
participant binding, wrong witness ownership, missing topology, undersized /
29-frame/9-audible rejection, post-await late predicate and late writer receipt.
Pure unit snapshots are **predicate counterexamples, not server MediaEvents or
proof of an SDK reconnect**. No browser/service has been run, and all16 actual
SDK rows remain mandatory and NOT_RUN. Node syntax checks are parse-only.
