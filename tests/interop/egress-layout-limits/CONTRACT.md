# Current-source minimal semantic rebase (2026-10-28)

Original /tmp/egress-layout-name-preflight-current and its later revisions are retained unchanged. Only two current Rust production targets: existing exact public name preflight extracted without semantic change; LocalWorkerRooms carries signed limit and invokes that typed validator before its existing metadata check. Current F150/attrs/RoomTask/CLI/width code is untouched outside target hunks. apply-narrow.py reconstructs existing files from exact current lines, not old whole-file copies. Current target snapshots, source and context guards all supplied.

The existing LocalWorkerRooms trait method baseline test uses ONLY old APIs/config fields and is a genuine adapter consumer; it is NOT public HTTP. Its identical assertion must actually fail old code and pass new code (one selected test, compiler failure not RED). Public API RED/GREEN is independently required: install the three original fixture files ONLY before old run, not production; preserve actual Go and old-Rust five-profile reports, then rerun unchanged fixtures after production. No authored source outputs are provided. The fixture directly calls public Egress RPC JSON; unchanged official ServerSDK handles Room lifecycle. It does not claim use of an unchanged SDK Egress method or successful recording.

# Derived Egress.UpdateLayout participant-name preflight

Author implementation, NOT_COMPILED / NOT_RUN. Only isolated files changed. This cut is independent from F150 CreateRoom/expiry, raw-key CLI, playout and SCTP restart. Preserve those already applied bodies; root must semantically rebase any guard drift rather than replacing a newer full file.

Fixed `pkg/service/egress.go:290-319` authenticates record, loads the EgressInfo, renders layout JSON, sets room-admin grant, then invokes **RoomService.UpdateParticipant** with empty Name and attributes. `roomservice.go:265-286` checks signed participant-name length BEFORE metadata/attributes/permissions/participant lookup. `LimitConfig.CheckParticipantNameLength` uses `limit==0 || len(name)<=limit`, so negative rejects even empty Name. The existing Rust LocalWorkerRooms adapter checked metadata then called manager directly. Core later checks the name for an existing owner, so this is NOT a write-bypass claim: it loses the public InvalidArgument/argument=-1 envelope and first-failure order (especially absent participant or competing metadata failure).

Production change: extract the existing exact public name validator into `check_participant_name_limit(i64,&str)`, retain its original typed TwirpError constructor, and call it from both original UpdateParticipant and the derived layout adapter. LocalWorkerRooms carries the same global signed name limit as the existing metadata limit. The adapter converts that exact typed error code/message/metadata into RpcError before existing metadata validation/manager work. No string matching, global startup rejection, new room lookup, new write, or changes to core admin echo. Auth/GetEgress/Go JSON order remains outside and before this derived preflight.

The test-only baseline patch uses the existing `LocalWorkerRooms::new` and `layout_metadata` API with signed name=-1, competing metadata-too-large and absent participant. It requires the public name error to win. Run identical full test name on original baseline, preserve actual behavioral RED, restore, then candidate GREEN. It uses real RoomManager/NetRouter lifecycle, not a hand-built error. All pre-existing metadata boundary tests remain. This component does not substitute for public HTTP or a recording worker success.

## Actual public consumer, coordinator owned

`fixture/run.mjs` starts no service/browser/compiler and patches no SDK. It uses unchanged ServerSDK to create/get/delete the actual room; an actual authenticated RawBlobPeer joins as EG_layout_owned (raw signal scope only). A source-compatible serialized EgressInfo is deliberately seeded in the owned store as test metadata, not a fabricated successful recording/callback. Real public Egress.UpdateLayout must update the actual participant's metadata through the real owner for positive profiles, and preserve it on denials. IOInfo/store lookup and the real HTTP service remain active. No StartEgress success or media proof is claimed.

Use a fresh declared profile on fixedGo and Rust, separately with exclusive owned Redis/main. Set identical config plus normal keys/ports and `limit` (singular upstream YAML):

| LAYOUT_PROFILE | limit.max_participant_name_length | limit.max_metadata_size |
|---|---:|---:|
| negative_name | -1 | 64000 |
| negative_name_and_metadata | -1 | 3 |
| unlimited_name | 0 | 64000 |
| positive_name | 1 | 64000 |
| metadata_only | 0 | 3 |

Each profile first checks missing-store failure, then genuine joined participant. Signed-negative profiles additionally wait for actual participant absence (public removal if still present, original10s bound) and verify name preflight still wins. The GetEgress-first negative is never reordered. Positive0/1 profiles require HTTP200, original stored Egress identity/room/SID relation, and actual participant metadata exactly `{"layout":"grid"}`, unchanged name/attributes and same participant SID. Denials check code/message/argument and no write. Both config limits are source accepted; no startup reject may substitute.

Example after coordinator starts source then Rust with identical declared effective profile:

    SOURCE_ROOT=/ABS/repo LIVEKIT_URL=http://127.0.0.1:PORT \
    LIVEKIT_API_KEY=devkey LIVEKIT_API_SECRET=secret \
    LAYOUT_PROFILE=negative_name_and_metadata LAYOUT_REDIS_ISOLATED=true \
    LAYOUT_REDIS_PORT=OWNED_PORT LAYOUT_REDIS_DB=2 \
    LAYOUT_REPORT=/ABS/private-go-profile.json \
    node /tmp/egress-layout-name-preflight-2026-10-28-fixture-v4/fixture/run.mjs

Repeat unchanged on Rust (fresh main/store/profile) to another report, then:

    python3 /tmp/egress-layout-name-preflight-2026-10-28-fixture-v4/fixture/compare.py \
      --official /ABS/private-go-profile.json --rust /ABS/private-rust-profile.json \
      --output /ABS/profile-comparison.json

The optional LAYOUT_REDIS_PASSWORD is passed only by environment to the private Redis helper. The helper modifies only EG_layout_* fields and their owned room-index member, never FLUSHDB. The fixture requires `LAYOUT_REDIS_ISOLATED=true`, unsubscribed raw socket close, real room deletion and store cleanup; failures are not swallowed. Room ID is the only response field mapped to a placeholder AFTER independently asserting exact actual-owned SID. All other HTTP status/content-type and full JSON fields plus actual participant effects compare exactly. Original raw body hashes remain separate; this is semantic JSON equality, never a fake byte-identical claim or broad prefix normalization. Error payloads are compared exactly with no synthetic expected Go result file.

Root must first execute actual fixedGo public profiles and preserve any contrary observation before changing assertions. This draft contains no actual results. The five profile matrix, original public regressions, source/build identities, full release/fmt/strict Clippy and cleanup are all coordinator-owned gates.

Component full test name:

    CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 cargo test --release -p lk-compat --locked worker_services::tests::actual_layout_adapter_empty_name_preflight_precedes_metadata_and_missing_identity -- --exact

Require one actual executed test, never zero tests. No Cargo/Go/node runtime/service command was executed by the author.

## Immutable fixture-v2 owner-get correction

Original five profiles, all HTTP/state/error assertions, comparator semantics, raw hashes, Redis metadata bytes and original10s bounds remain unchanged. Production two files byte-identical to the 2026-10-28 parent. This revision only strengthens actual process/config provenance before a room is created. All GetParticipant/ListParticipants calls must select true owner mode: actual source api.enable_psrpc_for_get_list_participants=true routes owner RPC; Rust official→normalize→manager live participant_owner_call and room_control owner RPC consume the same field. Negative before/after values therefore cannot be stale cached snapshots. Frontend Ping is not an owner barrier; no ping assertion was added.

Required root-owned actual profile canonical sections:

    api:
      enable_psrpc_for_get_list_participants: true
    limit:
      max_participant_name_length: PROFILE_SIGNED_NAME
      max_metadata_size: PROFILE_SIZE

Supply LAYOUT_SERVER_PID / LAYOUT_SERVER_BIN / LAYOUT_SERVER_CONFIG / LAYOUT_EXECUTION_RECEIPT alongside original env. Driver reads actual OS PID/executable/argv privately, requires sole matching --config, rejects config-body and generated API/limit flag overrides, verifies current config SHA equals coordinator startup receipt. Corresponding process ENV is NOT falsely claimed directly observable by the driver: root receipt must attest clearing API and two limit generated ENV variables before spawning. Receipt exact schema: pid, binary_sha256, startup_config_sha256, api_owner_get:true, no_api_config_overrides:true, no_limit_config_overrides:true, environment_api_override_absent:true, environment_limit_overrides_absent:true, effective_limits:{max_participant_name_length:<profile signed value>,max_metadata_size:<profile size>}. Preserve actual spawn/config/env proof in coordinator evidence; a fabricated receipt is never acceptance. No argv/rawconfig/credential is retained in fixture output, only actual executable path/PID/hash/receipt hash and bounded profile values.

### Negative name Join premise source reinspection

Fixed ValidateConnectRequest service/utils.go349-454 checks claims/video, room permission, identity and identity length, room name/room allocator then optional connection metadata/attributes permissions; it does NOT call CheckParticipantNameLength. Source search of service+rtc finds that predicate only in RoomService.UpdateParticipant and ParticipantImpl.checkMetadataLimits (called by UpdateMetadata), not constructor/Join. RoomManager join constructs real ParticipantParams with token name and grants; ParticipantImpl.NewParticipant has no participant-name limit validation. Thus signed participant-name=-1 does not statically prevent this empty-name authenticated Join. Rust Room::join similarly constructs info.name from accepted JoinParams without that update-only predicate. This is a source-based reachable premise, NOT an executed Go Join PASS: actual fixed Go profile must still yield real Join and live-owner GetParticipant before the existing after-update assertions can pass. If transport/departure occurs first, retain genuine failure; do not skip or reinterpret it as no-write success.

Original missing-store-first remains before participant creation; original missing-identity negative remains after actual close/removal and actual owner-mode list absence. It is a distinct name-before-lookup claim and cannot substitute for an actual present-participant no-write consumer. If real Join premise fails in any run, preserve failure and run separate source/fixture investigation; do not mark the five-profile matrix complete based solely on missing identity. Redis seeded EgressInfo remains explicitly metadata fixture, not fake recording or worker/IO callback.

All static-only: no Go/Node/Cargo/services/git/shared modifications executed. Root should execute original component RED/GREEN and unchanged actual source/Rust five profiles after independent review.

## Immutable fixture-v3 success dialect correction

Parent actually executed negative_name and negative_name_and_metadata PASS. Actual fixed Go unlimited_name success then failed v2 fixture checking body.egressId undefined; preserve that original report as fixture failure, not product behaviorRED. Source generated livekit_egress.twirp.go NewEgressServer1108 defaults jsonCamelCase=false; serveUpdateLayoutJSON MarshalOptions UseProtoNames:!s.jsonCamelCase, EmitUnpopulated:!s.jsonSkipDefaults at1481. Fixed service/server.go serverOptions does not enable JSONCamelCase. Therefore actual successful EgressInfo fields are strictly egress_id, room_name, room_id, matching their proto names. V3 asserts precisely these fields and replaces only verified room_id with the existing owned-SID placeholder. No camel/snake alias acceptance, JSON normalization or relaxed equality added. All5 profile definitions, source order/error assertions, original10s limits, comparator and Redis helper unchanged. Production two payloads remain byte-identical parent.

Actual successful response body is saved BEFORE assertions in successCaptures with status/contentType/raw original body SHA. The diagnostic rawBodyWithOwnedRoomSidPlaceholder retains actual JSON text/spelling/whitespace, replacing only task-owned generated roomSID; it is not the raw-hash input. This is safe controlled synthetic fixture metadata only (explicit isolated EG_layout_owned seed, no user Egress/worker output). Preserve coordinator ownership and never use this helper on a production/broad Redis dataset. Exact fullJSON comparison remains existing observations; capture is causal evidence, not substituted expected values. All runtime/syntax checks remain root-owned NOT_RUN in this revision.

## Final listener binding and complete executable contract (v4)

V3 strict snake_case fix retained byte-for-byte except additional listener provenance. Driver now requires receipt.livekit_url exactly LIVEKIT_URL, receipt.listener_port exactly URL port, and unique actual config `port: PORT` exactly the same. URL credentials/query/hash are forbidden. Existing PID/executable/config/fileSHA/override checks remain. Coordinator additionally must actually record `lsof -a -p PID -iTCP:PORT -sTCP:LISTEN` (or OS equivalent) before fixture, binding the declared PID to real listening socket; URL receipt alone is not falsely described as actual OS socket attestation. Root launcher constructs config port, URL, actual spawned PID together and retains that execution evidence.

Required receipt keys in full: pid, livekit_url, listener_port, binary_sha256, startup_config_sha256, api_owner_get:true, no_api_config_overrides:true, no_limit_config_overrides:true, environment_api_override_absent:true, environment_limit_overrides_absent:true, effective_limits:{max_participant_name_length:SIGNED_NAME,max_metadata_size:SIZE}. Actual startup environment must have no generated API/limit override; receipt does not invent proof.

Complete final command (coordinator owns server and exclusive broker):

    SOURCE_ROOT=/Users/zoujiaqing/projects/livekit-rs \
    LIVEKIT_URL=http://127.0.0.1:PORT LIVEKIT_API_KEY=OWNED_KEY LIVEKIT_API_SECRET=OWNED_SECRET \
    LAYOUT_PROFILE=PROFILE LAYOUT_REDIS_ISOLATED=true LAYOUT_REDIS_PORT=BROKER_PORT LAYOUT_REDIS_DB=2 \
    LAYOUT_SERVER_PID=ACTUAL_PID LAYOUT_SERVER_BIN=/ABS/actual-binary LAYOUT_SERVER_CONFIG=/ABS/actual-config.yaml \
    LAYOUT_EXECUTION_RECEIPT=/ABS/actual-launch-receipt.json LAYOUT_REPORT=/ABS/fresh-profile-report.json \
    node /tmp/egress-layout-name-preflight-2026-10-28-fixture-v4/fixture/run.mjs

Optional PYTHON must select actual working Python3; LAYOUT_REDIS_PASSWORD only if owned broker requires auth. Run all five unchanged profiles on fixed Go then Rust; compare using this directory's unchanged fixture/compare.py. Production is ALREADY adopted and component RED/GREEN by coordinator; do not reapply the old before-guarded production payload over current accepted source. This package carries original production snapshots only for provenance; final fixture installation is additive three-file copy under root guard management. No F158/context overwrite authorized. All actual tests remain coordinator-owned.
