# F04a SIP configuration HTTP differential

Status: PASS for the F04 final candidate: 102 paired real HTTP steps across14 configuration methods, with no response differences. Exact input and binary identities are in `livekit-rs-docs/evidence/2026-10-25/f04-functional-delivery.json`. The coordinator owns server/compiler execution.

Run against an actual pinned official server and Rust candidate, each using its
own real broker/store namespace (separate Redis servers or DBs). Both namespaces
must initially contain no SIP configuration. This fixture calls the real public
HTTP API; it does not inject or mock store state. Reference-only creation followed
by a Rust lookup of that exact generated ID must return NotFound, proving the
observable namespaces are separate. Each server uses its own optional key/secret
variables. All tokens come from the installed locked, unchanged official
livekit-server-sdk (AccessToken.addSIPGrant), loaded from the project interop
node_modules; no handwritten JWT signer or modified SDK is used.

```
REFERENCE_URL=http://127.0.0.1:17880 LIVEKIT_URL=http://127.0.0.1:27880 \
node functional-sip-config.mjs
```

Optional REFERENCE_API_KEY/REFERENCE_API_SECRET and LIVEKIT_API_KEY/SECRET override
local development defaults. Do not persist these environment values in evidence.
SIP_CONFIG_EVIDENCE chooses output path. Default is tests/interop/output/functional-sip-config.json.
No configuration fixture means servers are suitable for calls: all telephone
numbers are reserved synthetic +1202555… examples and addresses use .invalid /
192.0.2.0/24. No SIP call or worker dispatch is attempted.

The fixture covers exactly the 14 registered configuration RPCs: ListSIPTrunk;
Create/Update/Get/List SIPInboundTrunk and SIPOutboundTrunk; DeleteSIPTrunk;
Create/Update/List/Delete SIPDispatchRule. The deleted CreateSIPTrunk endpoint is
not invented. CreateSIPParticipant and TransferSIPParticipant remain OPEN and are
not included in a configuration success count.

Real effects include fresh-store empty lists; SIP admin auth across all routes
(no token, call-only and video room-admin fail); missing/invalid values; new trunk
ID rejection; hard invalid header; allowed deprecated To header (soft warning,
HTTP succeeds); create/get/filter; ListUpdate add/remove/deduplicate/set/clear;
explicit empty optional strings; replace with matching/empty ID vs mismatched ID;
modern and legacy dispatch-rule request shapes; rule list/filter/update/replace;
missing IDs/NotFound; idempotent repeated trunk delete vs rule second-delete
NotFound; final empty stores. Source contract is fixed
references/livekit-stable/pkg/service/sip.go and protocol-stable/livekit/sip.go,
sip_validation.go and protobufs/livekit_sip.proto. The domain/store/adapter work
must satisfy these actual cases rather than returning disconnected errors.

Every paired HTTP response is recorded, including exact status/content-type/body.
Differences accumulate; they are never waived. A prerequisite create/status failure
stops dependent steps and marks FAIL rather than fabricating IDs/store objects.
Known real generated IDs are mapped to stable labels, including the same IDs in
error text; positive generated created_at/updated_at timestamps are normalized.
No business fields, list entries, error codes/text, defaults or absent/null values
are dropped. The only populated legacy all-list comparison is taken while one
trunk exists, avoiding a false cross-instance ordering assumption from unrelated
random ID ordering. No general list sorting or pagination weakening is applied.

The finally block deletes only IDs actually created by this fixture, in reverse
order. Rule cleanup after an already observed delete can correctly be NotFound;
all cleanup responses/errors remain evidence. It does not flush a database,
remove another task's data, stop servers or delete shared outputs. Coordinator
owns process and temporary cleanup after persisting evidence.

Separate fixtures also executed: unchanged official SipClient15 steps per server; actual generated Protobuf client25 paired cases/14 methods; public pagination10 paired cases over1005 records; actual store cross-language74 cases and15 corrupt-wire/error cases; five real public Redis fault/recovery cases; Rust graceful restart and both-server explicit crash restart. See F04 evidence for distinct profiles. The initial reference graceful-stop watchdog failure is retained and is not reclassified as a successful combined graceful test. Broker HA/failover, arbitrary concurrency and telephone workers remain outside this delivered slice. No result claims100% project compatibility.
