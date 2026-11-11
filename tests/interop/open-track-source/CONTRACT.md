# Numeric open TrackSource real HTTP/WS fixture v3

Status: NOT_RUN / NOT_COMPILED. No production changes. Parent executes all processes. Original v2 candidate, source observer cases and actual source output remain immutable. This fixture is a source-driven author correction, not independent certification.

## Original inputs and observations

All twelve original case bytes are preserved from isolated-second/oracle/cases.json. `fixed-go12-actual.jsonl` is an exact copy of the coordinator's actual Go12 output, not filled expectations. run.mjs requires supplied cases/results SHA to match these sealed copies, twelve unique IDs in original order, original signed i32 request_source and every existing source column. Arbitrary raw values remain legal test protocol inputs, not fake SDK support.

The unchanged resolved ServerSDK2.19.2 mints ONLY canonical baseline without canPublishSources. Verify its actual HS256 signature and `{alg:HS256}` header, clone payload, change only video.canPublishSources to original case raw strings, independently sign standard HS256, then verify complete cloned payload/signature/sub/expiry/nbf. Baseline SDK raw string mint is never invoked. No installed SDK code changes. Protocol1.52.1 actual generated binary encoder/decoder carries every signed i32 AddTrack.source, including -2147483648 and 2147483647, without uint conversion or known-five narrowing.

Original initial actual Join public source list, AddTrack success/denial, exact signed TrackPublished.source/rejected original source echo, default stream, reason2 and empty rejection message assertions remain. Every before/after AddTrack gets explicit real source-shaped response: no absence/Pong success. Genuine pre-RTC pending reservation is NOT an encrypted media delivery or RTC connection.

## New actual public consumers

Actual unchanged RoomServiceClient HTTP GetParticipant uses explicitly enabled owner mode. For each case:
1. Real Join and live owner Get agree with original permission_sources. A separate admin nonempty metadata update produces an actual signed refresh; verify HMAC/sub/room/attributes and exact initial raw source vector.
2. Original numeric AddTrack checks original allowed and exact source/stream/denial assertions.
3. Build incoming permission by cloning actual owner permission and set canPublishSources=[ORIGINAL signed i32]. Real UpdateParticipant returns same participant SID. Compare exact returned version delta 0 or1 with actual Go api_matches, and exact api_permission_sources. Another fresh actual owner Get confirms returned version and public list.
4. Real nonempty metadata update (separate from permission version observation) forces a fresh signed token so even Matches=true has a real raw-source observer. Verify actual api_raw_sources, no copying expected values into observed result. Metadata does not alter permission. Each refresh event validates HMAC, subject, room, full attributes {} and expected raw vector.
5. Repeat the IDENTICAL original input permission, not reconstructed public source0, comparing actual owner version behavior with api_repeated_matches and the same source/public/signed raw observations. This distinguishes numeric99 from its public UNKNOWN0 projection.
6. Repeat explicit AddTrack using actual source api_allowed. Original whole-token/default-grant/full-history assurance remains the previously accepted known raw16 suite; this new twelve-row suite does NOT claim a complete JWT field spectrum.

Fixed source chain: RoomService.UpdateParticipant→RoomManager.UpdateParticipant886–910 synchronous UpdateMetadata then SetPermission; participant.go858–919 MatchesPermission early return vs UpdateFromPermission/dirty/claims; 929–937 dirty version increment. protocol/auth/grants.go396–469 compares ToPermission source enum list exactly; SetCanPublishSources306 uses lower(enum.String), unknown signed number becomes decimal, while GetCanPublishSources/sourceToProto intentionally projects unknown names to0. Forced metadata refresh is separate evidence, not source fake callback. The component Go12 has already actually executed; new HTTP/WS consumers have NOT.

## Ownership/config/process proof

Require canonical startup YAML with unique top-level port and exactly:

    api:
      enable_psrpc_for_get_list_participants: true

Runtime reads actual PID/executable/startup argv privately, rejects config-body and generated owner-get flag override, requires exactly supplied --config file, verifies binary/config SHA, API URL/config port/receipt listener port identity. It actually invokes lsof for that PID+TCP port+LISTEN and checks returned process/endpoint; only hash is persisted. Root receipt proves absence of overriding environment/config, which script cannot derive solely from current config bytes. Root must start from immutable canonical config, clear LIVEKIT_API_ENABLE_PSRPC_FOR_GET_LIST_PARTICIPANTS and ensure no wrapper or later file mutation. No cached read premise is accepted. Root owns standalone loopback Redis/server shutdown, not this fixture.

Every request/event wait retains 10000ms. No timing/QoS relaxation, no injected participant/events/worker/media. cleanup failures make FAIL and all owned cleanup is attempted. A preexisting report is never overwritten. Error output records only type/code/source line; no JWT/room/identity/secret values. Setup before report ownership may throw; root must persist that FAILED_SETUP separately, not call it server behavior RED.


## v4 independent review correction: explicit owner closure and forbidden CIDs

v3 is immutable and NOT_RUN; its first-response-only admission observation lacked a permanent denied-CID negative guard. v4 does not change original twelve cases, raw token grant fields, 10s waits, API version/source expectations or original response assertions.

Peer now owns forbiddenCids. Every expected-denied original CID is registered BEFORE sending; every DATA barrier CID is forbidden before sending. Any later actual TrackPublished on these CIDs fails immediately in the permanent binary message listener, including while close is in progress. close awaits actual clean1000 closure and rechecks stored failure. This is not a transient events.find/no-response quiet test.

After each original before/after reservation, send a fresh real AddTrack with TrackType_DATA=2 and EXACT same signed i32 source. Wait for its correlated actual RequestResponse, require permission-first NOT_ALLOWED2 if source disallowed or UNSUPPORTED_TYPE5 if source allowed, empty message, requestId0, full generated typed AddTrack echo equality including defaults. Only after this barrier is observed may reserve return and the row progress/close. No ownMetadata grant is added to token. No Ping used as barrier.

Fixed source proof: service/roommanager.go rtcSessionWorker synchronously invokes Participant.HandleSignalMessage for each request in its sourceChannel loop; rtc/signalling/signalhandler.go case AddTrack synchronously calls p.AddTrack. participant.go1399–1430 checks CanPublishSource BEFORE TrackType_DATA validation and sends cloned original request via sendRequestResponse. participant_signal.go sendRequestResponse invokes WriteMessage synchronously; accepted AddTrack sendTrackPublished uses the same Signaller writer. This known same-owner request FIFO/same-sink ordering justifies the explicit owner barrier for preceding source-defined emissions. It does NOT prove arbitrary wrong-server asynchronous goroutine ordering or RTC/media. Permanent forbidden guards remain necessary through actual close.

The new DATA messages are additional controls, not changes to actual Go12 inputs or its original observed columns. Their exact reason/order/echo is separately source-bound to the previously actual Go AddTrack four-row observer; they are NOT prefilled actual runtime results. Root must execute both servers and preserve any failure. Original known raw16 full signedgrant history remains separately accepted scope.
