Actual fixed Go and final Rust candidates passed both486/503 complete rejection profiles. Evidence and identity: SPEC0019 and F07 delivery; whole-service compatibility remains separate.

# F07 genuine authenticated SIP rejections

INTEGRATED_FIXTURE / RUNTIME_NOT_RUN. No compiler, browser, server, SIP worker, Redis process,
runtime Node test or git action was executed during preparation/integration. Shared Cargo sources and
active sip-media fixtures are untouched. Coordinator owns actual execution.

## What is real

Each of 486 Busy Here and 503 Service Unavailable is exercised against both
fixed Go and candidate Rust. Each endpoint has a real independent broker/store
and unmodified real SIP worker. The unchanged official server SDK creates an
actual outbound trunk/room and calls CreateSIPParticipant(waitUntilAnswered).
The local `RejectingSipPeer` is an actual UDP SIP UAS derived from the current
legitimate peer fixture, never a PSRPC mock. It issues the original true401
Digest challenge, verifies actual username/realm/nonce/URI/MD5 response, and only
then sends the configured real SIP rejection. The final non-2xx transaction ACK
must arrive. Retransmitted UDP INVITEs get the same cached response and are not
counted as independent calls. No successful200/SDP answer or RTP is generated.

This asks the genuine worker to turn actual SIP failure into its native typed
SIPStatus details, transmit them over real PSRPC, pass them through public HTTP,
and expose them as the original SDK's real SipCallError metadata. Neither the
fixture nor the gateway is allowed to synthesize the expected error.

## Complete HTTP and SDK evidence

The SDK normally reformats ServerError.message into SipCallError.message, and
could hide the original public error message from a simple catch-only fixture.
The profile therefore uses a bounded local **byte-transparent HTTP observation
relay** only for the SDK's public SIP calls. It forwards original request body
and authorization unchanged to the actual upstream endpoint, retains every real
upstream response byte, and returns it to the unmodified SDK with the original
status/headers. It does not fabricate an HTTP error. Transport failure destroys
the connection and fails the fixture rather than inventing an upstream status.
This relay is not in the SIP/RTP/RTC or PSRPC paths and does not change network
limits, payloads, source protocol, error mapping or content standards.

Output retains raw response base64, raw decoded text, actual parsed body with
all code/msg/meta including error_details, and SDK name/message/status/code/full
metadata plus the public SIP status getters. JSON key order is ignored when
comparing metadata; **no metadata key is removed**. Only a structured explicit
call_id/sip_call_id/transfer_id field with a genuine generated SCL_/STR_ value
may be replaced for comparison. Its original remains in raw evidence. Arbitrary
text/base64 is never searched heuristically to discard differences. If encoded
error_details differ they remain a real mismatch pending source investigation.
No HTTP error code or message is guessed from the SIP status. Both statuses must
be actual HTTP failures carrying actual sip_status_code metadata. All other
HTTP codes/messages/metadata are compared against the real official result.

The observer only accepts POST public SIP methods, with262144-byte request and
response limits and8 captured call responses. The UA bounds rejection state8
transactions; its shared base has cache64/error16/sample64000 bounds. Overflow
is a failure. No JWT, API secret, SIP Authorization header or SDP is captured.

## Coordinator execution

1. Complete F07 positive real SIP acceptance first; do not treat the first
   official fixture failures as a Rust media result.
2. Keep current unmodified pinned SIP worker source/binary and isolated Go/Rust
   servers, each with an independent actual Redis/worker registration. No paid
   PSTN needed. The test never starts/stops these external processes.
3. Reuse locked interop npm dependencies. No new package or SDK source edits.
4. The UAS imports its own peer-base.mjs: a byte-identical100-line frozen
   snapshot of the previously integrated SIP UA input. It does not import or
   modify sip-media/peer.mjs at runtime. SOURCE.json binds both original source
   and independent copied input, so later positive-fixture edits cannot silently
   alter this negative profile. No unknown helper is fetched.

```
REFERENCE_URL=http://127.0.0.1:<go-http> \
LIVEKIT_URL=http://127.0.0.1:<rust-http> \
REFERENCE_API_KEY=<go-key> REFERENCE_API_SECRET=<go-secret> \
LIVEKIT_API_KEY=<rust-key> LIVEKIT_API_SECRET=<rust-secret> \
SIP_FAILURES_WORKERS_READY=1 \
INTEROP_PACKAGE=/absolute/repo/livekit-rs-server/tests/interop/package.json \
SIP_FAILURES_OUTPUT=/private/task/sip-failures.json \
node livekit-rs-server/tests/interop/sip-failures/run.mjs
```

All upstream URLs must be local plain HTTP for this narrowly scoped observer.
SDK public options disable Cloud failover, not the actual SIP worker logic.
Default devkey/secret are local fixtures, not production credentials.

The fixture deletes only its own trunk/room, stops its own UAS/HTTP relay, and
requires no residual actual participant for the failed call. It does not assert
that a participant could never transiently exist: source may join before failure.
It does not claim successful RTC/SIP media on a rejected call. Cleanup errors
are failures. Four real authenticated failed calls are expected total (two
statuses × two servers), not a fake prefilled error table.

## Source and remaining scope

Pinned SIP v1.21.0 commit ee19da1a7ef178593e232a27365de7b4ba8f8829:
`pkg/sip/outbound.go` real authLoop classifies486 and503 response errors;
fixed protocol livekit/sip.go SIPStatus.GRPCStatus/TwirpErrorMeta contains the
real status details contract; unchanged SDK TwirpRPC/SipClient wraps those
metadata into SipCallError. SOURCE.json binds these actual files.

This profile covers Create refusal metadata only. Transfer rejection, timeout,
CANCEL racing200, negative credentials, TLS/SRTP, inbound/PIN and provider-specific
headers remain OPEN. No unsupported slice is counted as covered merely because
these two real statuses pass. Actual mismatches must be retained and fixed,
never waived or converted to accepted messages by the fixture.

Static syntax checks use `node --check` on the three .mjs inputs only. They do
not evaluate modules, create sockets or run a scenario. Runtime status stays
NOT_RUN until the coordinator records actual worker evidence. See the isolated
negative acceptance addendum in `livekit-rs-docs/specs/0019-sip-failure-acceptance-plan.json`.
