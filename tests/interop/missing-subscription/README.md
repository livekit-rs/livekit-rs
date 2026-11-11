# Genuine missing-track subscription SDK campaign

Author draft, NOT_RUN. No server/browser/compiler started. Only Node syntax checks and static source/hash checks executed. This is a new sibling fixture; no original fixture or SDK source changes. All original three-participant room/API assertions remain, including original 30 decoded frames, audible10,640x360/30s initial media gate,10s API/event gates, real unsubscribe decode-stop, same-SID resubscribe, reliable target/topic data and real PerformRpc. This is functionality, not a BWE/performance claim.

## True observation boundary

Actual unmodified livekit-client2.23.0 `SignalClient.ts1088-1090` decodes SubscriptionResponse and the original RTCEngine callback emits `subscriptionError` (`RTCEngine.ts319`). `Room.ts2204-2220` intentionally drops that error if no RemoteTrackPublication exists. Thus RoomEvent.TrackSubscriptionFailed cannot witness a never-published SID. The browser adds a read-only listener to the existing engine EventEmitter; it never replaces SignalClient callbacks, emits an event, creates a fake RemotePublication or sends handcrafted signaling. Receipt explicitly labels this as unchanged SDK internal decoded-event observation; no public event support is claimed.

Actual official RoomServiceClient.updateSubscriptions targets a fresh nonexistent SID. Error code2 is the actual protocol SE_TRACK_NOTFOUND enum, observed after real clock strictly >60000ms from initiating the public call. Original fixed source worker cadence3s and60s timeout are untouched. A10s bounded observation window after60s allows the original3s phase plus genuine transport/processing; no tolerance admits an error at/before60s. No fake accelerated clock. All peers must remain connected, have no reconnect/disconnect events, maintain native PC identities and original topology, and remain free of fixture errors.

## Independent profiles

Each invocation first executes the original three-peer API/media baseline. Then exactly one profile:

- `expiry`: request nonexistent SID; no error at/before60s, exactly one true code2 within the next original10s event bound, no repeat for another6.5s.
- `cancel`: request, observe20s, public unsubscribe, observe until70s from first request, zero errors. No re-request mixed into this profile.
- `rerequest`: request, observe30s, repeat the same public subscribe; the60s bound now starts at actual repeated request, no early error from the first request's clock, exactly one late code2/no repeat.
- `permission`: public UpdateParticipant removes canSubscribe, then request nonexistent SID. Wait65s with no missing error; restore canSubscribe through the real public API. Original intent age is retained; require real code2 within10s, not another60s, and no reconnect/repeat. This checks the source permission-before-track-resolution branch. Existing track subscription changes caused by permission are allowed; final recovery media must succeed.

After every profile, the real publisher SDK publishes a **new**640x360 VP8 track. Both receivers must receive actual TrackSubscribed and >=30 freshly decoded frames for that exact new SID within30s; independent publisher API readback must contain that SID. Per-SID decode counters are additional read-only observers; original aggregate media assertions are not reduced. Existing audio must still be audible. Since SDK cannot assign a future server TrackSID, this does NOT claim the never-published SID became the new track. Same-SID pending-to-publication resolution remains covered by core/owner gates, not fabricated SDK setup.

## Real identities and process inputs

Before any browser/server effects, `sdk-identity.mjs` resolves each actual package from INTEROP_PACKAGE and checks actual package version against locked2.19.2 ServerSDK/2.23.0 browserSDK. ServerSDK exact require-export realpath is dynamically imported; browserSDK exact require-export UMD bytes are read once and served unchanged. Browser snapshots must report actual LivekitClient.version matching its actual package. Package JSON/entry/tree/selected lock row/full lock/interop file/Node executable bytes are hashed, not handwritten identity fields. Nested node_modules excluded explicitly; no complete dependency-closure claim. Server config/binary input bytes are also hashed; coordinator must separately attest running process uses those files. No SDK/node_modules mutation.

SDK identity preflight is before cleanup try, so setup failure is a process failure and may have no RESULT; capture stderr/exit as FAILED_SETUP, never treat absent rows as passed. No secrets/JWT are written. Missing track/room/native IDs stay private; final contract compares stable source semantics and preserves detailed elapsed actual measurements separately. SDK auth grants use original own room/token logic.

## Coordinator execution (prestarted servers only)

Integrate sibling files with apply.py only after original guards match. Coordinator owns start/stop of fixedGo/Rust servers, exclusive Redis, browsers, certs, artifacts and cleanup. Keep actual effective config `rtc.reconnect_on_subscription_error: false` or true separately, plus normal source-compatible loopback/local ports/keys. Do not shorten the3s/60s source policy. The earlier draft's `room.` prefix was incorrect; both fixed Go RTCConfig and normalized Rust RtcConfig own this field.

For isolated fixture (before integration), INTEROP_PACKAGE is mandatory, pointing to existing installed actual package.json. After integration it defaults to ../package.json. Example one source campaign:

```sh
LIVEKIT_URL=ws://127.0.0.1:SOURCE_PORT LIVEKIT_API_KEY=devkey LIVEKIT_API_SECRET=secret \
INTEROP_PACKAGE=/ABS/repo/livekit-rs-server/tests/interop/package.json \
SERVER_CONFIG_FILE=/ABS/private-source.yaml SERVER_BINARY=/ABS/fixed-livekit-server \
SINGLE_PC=true RECONNECT_ON_SUBSCRIPTION_ERROR=false MISSING_PROFILE=expiry \
RESULT=/ABS/private-source-expiry.json \
node /tmp/f164-missing-subscription-sdk/payload/livekit-rs-server/tests/interop/missing-subscription/run.mjs
```

Require REFERENCE_CAPTURED and no cleanup errors, then unchanged Rust invocation with its actual binary/config/server URL and:

```sh
REFERENCE_RESULTS=/ABS/private-source-expiry.json RESULT=/ABS/private-rust-expiry.json
```

Require PASS. Profile, flag and Single/dual must match source reference; only stable semantic contracts compare, all elapsed/raw counters stay as actual evidence. Capture real browser version/launch process in coordinator receipt. Actual source failure remains FAIL and must not be converted to authored expected results.

Start bounded with `expiry` one flag/mode source→candidate. Then cancel/rerequest/permission separately. Full declared addon matrix:4profiles×2flags×2topologies×2servers=32 campaigns. A subset must retain exact executed count and cannot close all flags/topology/family. No new dependency/crate; existing Playwright and unchanged installed SDK only. All actual campaigns still NOT_RUN.

## V2 recovery audio proof correction

V1 is retained unchanged and was NOT_RUN. Its recovery predicate used only cumulative audible>=10, already achieved by the initial baseline. V2 additionally records each receiver's audible count immediately before publishing the real recovery track and requires at least10 new genuine audible samples within the same original30s video/media deadline. The original audible>=10, all31 original assertions, fresh exact-SID30decodedframes640x360 and actual event conditions remain. No synthetic samples, clock changes, SDK mutation or longer timeout.

This correction is reproduced by v1-v2-recovery-audio.patch over the retained v1 payload. Historical build.py/seal.py describe v1 assembly; do not rerun them as v2 generators. All32 actual campaigns remain NOT_RUN; coordinator runs source first and preserves failures.


## Reconnect reference binding supplement (NOT_RUN)

The original missingContract already included reconnectFlag and deepEqual already compared it. This supplement does not invent a prior missing-field defect. It adds explicit reference top-level profile/reconnectFlag assertions for input identity, preserving every original assertion,no source vectors are changed,60s/3s/10s/30s constants and original SDK bytes. The declared flag remains a coordinator startup premise: clean inherited LIVEKIT_*, canonical binary/config only, immutable startup config and PID/listener/binary/config receipt must bind actual rtc.reconnect_on_subscription_error. Merely hashing supplied files or a matching label cannot prove effective process configuration. Preserve all previous results and run fixedGo first, then matching candidate.
