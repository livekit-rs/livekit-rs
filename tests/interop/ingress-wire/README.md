# Actual Ingress HTTP/protobuf request-owned unknown fixture

PREPARED / NOT_RUN. No compiler, service or shared repository input was changed.
Root executes against two real dedicated main servers and fixed Go RedisStore
oracle, preserves actual red results before any implementation correction.

## Invocation

Required: INTEROP_PROJECT (absolute existing locked tests/interop installed
project), REFERENCE_URL, LIVEKIT_URL, IO_REFERENCE_REDIS_URL, IO_RUST_REDIS_URL,
IO_GO_ORACLE (existing tests/oracles/io-info actual fixed Go executable),
INGRESS_WIRE_OUTPUT (new private safe result), INGRESS_DEDICATED_BROKERS=1.
API credentials privately LIVEKIT_API_KEY/SECRET. Both catalogs must begin empty.
Root runs `node /tmp/f10-ingress-unknown-wire/run-wire.mjs`.

SDK2.19.2 AccessToken signs real ingressAdmin JWT. Actual fetch sends real
application/protobuf Twirp four APIs, consumes full encoded response buffer.
Known request fields are encoded by the ACTUAL fixed Go generated descriptor
via original IO oracle --mode encode. Small explicit varint/tag functions only
append controlled FUTURE fixture fields; they are not a replacement production
codec or fake backend. Actual Go --mode decode observes full message known
projection and GetUnknown tree, and --mode store reads/writes production
RedisStore. No service handlers, IOInfo callbacks or worker successes are mocked.

## Frozen source cases

1. Create reusable RTMP with audio+video children and actual selected presets.
   Audio includes future length-delimited field502 and legal unknown wrong-wire
   occurrence of known string name#1 as varint7. Video has future field504.
   Request root has future field501 with noncanonical varint value. Info must
   preserve exactly actual-Go decoded audio/video unknowns but must not move
   CreateRequest-root unknown into a different IngressInfo root type. Entire
   public response and actual stored message must compare known+unknown trees.
2. Real fixed GoStore.UpdateIngress seeds original IngressInfo-root unknown503
   with noncanonical value. Public Update changes known name/metadata and
   explicit enabled=false while children absent. New known values must win,
   existing same-type root and unchanged children must retain exact unknowns.
   No request-root501 transfer is permitted. Optional false must remain present.
3. Public Update explicitly replaces audio with new name/preset0+future505.
   Old audio unknowns must disappear; new child survives; absent video retains
   original. Existing Info-root503 persists. No old known field resurrection.
4. Separate Update omits optional enabled: last explicit true persists; fresh
   metadata changes again and prior known name does not revert. Store equality.
5. Public List-by-ID binary full response has one item, exact current known
   fields and complete unknown tree transformed only by actual `.items[0]`
   message boundary. ListRequest-root future must not transplant to response.
6. Independent new WHIP Create has explicit enableTranscoding=false/enabled=false
   and no unknown. Both optional false values must be present; no raw provenance
   from previous objects/requests may leak. Entire real stored object compares.
7. Actual Delete binary response retains same-type complete provenance; actual
   generated SDK List-by-ID after delete must throw exact not_found, not an
   invented empty list. Dedicated final list must be empty.

Go and Rust transcripts compare full known+unknown semantics. Only validated
random ingress IDs and streamKey are substituted for pair comparison. Unknown
hex is compared exactly from actual Go decoding, not sorted/re-encoded into a
synthetic equivalent. Business fields and optional presence are not stripped.
The independent Go decoder is authoritative for its canonical tag treatment;
fixture unknown noncanonical values preserve actual expected behavior.

## Bounds, cleanup and proof scope

Each child request <=1MiB, captured stdout <=4MiB, stderr<=64KiB, process15s and
HTTP15s; failures preserve FAIL. Go child exit statuses and stream hashes are
recorded, not log contents. Safe report includes only scope/counts/status/hash,
no JWT/streamkeys/generated IDs/addresses/raw messages. Finally deletes exact
owned records via real public API; if that fails a real exact-owned GoStore
Delete can clean while leaving cleanup FAIL, never FLUSH. Root owns/stops
servers/brokers and persists stable input/binary/source identities.

NOT_RUN: all HTTP/store assertions here. Not proved: active external worker,
URL/RTMP/WHIP media, arbitrary unknown protobuf graph, parser fuzz/security,
noncanonical known scalar messages, performance. The source cases are precise
management-fidelity leaves, not a claim of 100% service or worker compatibility.
