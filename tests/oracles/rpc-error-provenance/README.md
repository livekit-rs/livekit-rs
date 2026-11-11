# Actual PSRPC Any-wrapper provenance oracle

The source generator invokes actual fixed psrpc/internal.Response proto.Unmarshal,
psrpc.NewErrorFromResponse, and protocol xtwirp.ToError. Its module prefix permits
the same real Go internal import as the existing PSRPC compatibility oracle.

Coordinator builds/runs with fixed Go; stdout JSONL is saved as
`livekit-rs-docs/evidence/2026-10-25/f07-rpc-error-provenance-go-actual.jsonl`.
This actual reference result, not Rust-generated expected bytes, is consumed by
`service_bus::rpc::provenance_tests`.

14 vectors cover canonical, unknown noncanonical values/keys/lengths/order,
groups, duplicate unknowns, known-number wrong-kind Any fields, two independent
wrapper cohorts, unknown type, malformed known detail payload, empty wrapper with
unknowns, malformed wrapper and invalid UTF8 wrapper. The last two are genuine
Response decode failures. A malformed opaque Any.value is instead valid wrapper
input and remains in serialized full Status. Original unknown keys follow actual
Go canonicalization; values and ordering are checked against actual output.

The implementation retains only selected request-owned Any children; it does
not transplant Response root unknowns into google.rpc.Status. Existing encoded
and bounded gzip/decompressed limits are unchanged. Raw bodies are decompressed
once. Private response decode retains IDs used by the unchanged client matching
path. Both worker metadata and actual server response emission preserve these
wrappers. Direct typed-vector mutation after preservation fails closed rather
than silently moving unknowns to another cohort; explicit replace_error_detail
updates one same-source child. No global cache.

Optional real Redis test (coordinator supplies a dedicated broker):
`IO_RPC_TEST_REDIS_URL=<private-url> cargo test --locked -p lk-compat --lib actual_redis_plain_and_gzip_error_wrappers_survive_server_client_and_forwarding -- --ignored`
The full Rust test name contains `service_bus::rpc::provenance_tests::`; either
pass that exact name or omit `--exact` for the short filter. It proves actual
claimed server/client response and error forwarding, both plain/compressed
configurations; pure tests additionally require actual gzip compression and
original byte bounds. It is not a SIP/Ingress/Egress media fixture.

Implementing agent did not compile or execute these Go/Rust/service tests.
Fresh coordinator execution is required; implementation status is not PASS.
