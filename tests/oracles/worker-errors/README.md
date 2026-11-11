# Source-bound worker RPC error bridge draft

NOT_COMPILED_NOT_RUN. Isolated files only; parent integrates and executes.

`worker_errors.rs` is intended as `lk-compat/src/twirp/worker_error.rs` with a parent module declaration. Existing Egress and new SIP/Ingress public handlers delegate their RpcError converter to `worker_error::failure`. It preserves local `application_metadata`, then adds source-ordered actual detail metadata and **`error_details`** (fixed xtwirp.DetailsMetaKey, not grpc_status). This value is the complete google.rpc.Status protobuf encoded with standard base64: actual gRPC numeric code, original error message and all Any details in original order, with each original Any payload bytes intact. No string parsing of business errors, no HTTP dependency added to service_bus.

Source `protocol-stable/utils/xtwirp/errors.go:24–116` defines mapping, order and serialized details. The actual PSRPC response constructor source `psrpc/errors.go:267–280,351–357` supplies status code/message/details, including empty response code→Unknown. Malformed request maps gRPC InvalidArgument; malformed result maps gRPC Internal via xtwirp.ToError (not the separate psrpc.ErrorCode.ToTwirp convenience conversion). The draft maps those exact source strings.

The only ErrorMeta implementations in the fixed protocol are `livekit.SIPStatus` and `livekit.SIPTransferError`. Any URL resolution uses the final fully qualified type component (same resolver semantics), decodes known projection via existing Go-compatible binary helper, and applies actual source custom metadata. Unknown type/malformed known payload contributes no custom keys but remains in full Status details. Repeated detail keys overwrite in order. SIP status empty text uses full generated enum name (not ShortName); unknown enum values use decimal. Transfer reasons likewise use enum name/decimal. Original status/transfer Any values are never re-encoded for the full Status.

`oracle/main.go` imports and calls **actual unchanged** psrpc.NewErrorFromResponse and xtwirp.ToError. It produces 33 rows:20 code mappings and13 typed/unknown/malformed/URL/order cases. Parent runs actual fixed Go toolchain with original local replace inputs; stdout JSONL becomes a committed public fixture. Only then replace `WORKER_ERROR_GO_RESULTS_JSONL_FIXTURE_PATH` in `oracle_test.rs.template` and append its tests to draft module. The Rust test compares every output code/message/full metadata base64 to that actual Go result; it does not regenerate expected values in Rust. Local application metadata has a separate pure test because that map is an internal Rust bridge for upstream in-process Twirp errors, not a new PSRPC field.

Resource bounds: normal production response envelope/decoded-byte bounds are established by RpcClient before this conversion; this bridge introduces no global cache, network I/O or extra unbounded parsing policy. Known Any decode failure is ignored only for metadata extraction, matching actual Go st.Details behavior; original Any still serialized. This implementation does not prove preservation of unknown fields on the outer Any wrapper itself: current RpcError exposes typed pbjson_types::Any, which has no raw wrapper provenance. Existing raw Any payload values ARE preserved. If outer-wrapper unknown-byte fidelity is required, extend RPC request-owned raw-detail representation separately rather than claim an impossible guarantee from typed Any.

Original SIP draft review `/tmp/f07-sip-draft-review.json` has been corrected to the exact `error_details` key. No shared source/production behavior or Go expected fixture was modified by this draft. Actual malformed and custom-URL behavior remains pending oracle execution, not statically called PASS.

F07 expanded code corpus is37 rows: original33 case IDs retained, plus actual
PSRPC upstream_server_error/upstream_client_error/not_acceptable/
unprocessable_entity. Actual fixed Go maps the two upstream codes to
Internal/InvalidArgument; the last two strings are unknown to fixed PSRPC and
map to Unknown. The fresh output path is
`f07-worker-error-go-expanded-actual.jsonl`; the original33-row actual fixture
remains historical and is never overwritten. All detail rows compare the exact
full `error_details` status bytes, not merely custom metadata subsets.

Any-wrapper unknown preservation is independently covered by the sibling
`rpc-error-provenance` actual Go corpus and real Redis opt-in response path.
