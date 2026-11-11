# Pinned Google ProtoJSON typed oracle

Status: **NOT_RUN**. This is a development fixture, never a server dependency.
The coordinator owns compiler invocation and final cleanup. No reference checkout
is modified by this tool.

`go.mod` and `go.sum` are copied from fixed `references/livekit-stable` commit
`e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`. The module name is changed; all require,
tool and existing replace directives are preserved. Two additional replace
entries resolve protocol `9bb708cdb81918a6c63dd25e915b4644310b45b6` and psrpc
`e9701ee1b5a981cfa3c7c69503d4775ddff13729` from immutable local references.
The selected Google protobuf remains **v1.36.12**, with upstream Go **1.26.0**.
Source hashes and explicit transformations are recorded in SOURCE.json.

From this directory, after the coordinator checks host load and source identity:

```sh
GOTOOLCHAIN=local GOMAXPROCS=2 go run -mod=readonly . cases.json > /tmp/livekit-rs-protojson-go-results.jsonl
```

Use the project's fixed Go 1.26 toolchain on PATH. If the locked module needs a
new sum/graph change, fail and investigate it; do not float versions or edit
references to make the command pass. `-mod=readonly` protects the copied manifest.
The output is newline-delimited JSON, one record for every input case. An input
that is correctly rejected still produces a record and is not an oracle process
failure. The coordinator must inspect and persist actual results before marking
this fixture as executed; successful process exit alone is not a compatibility
pass.

The 22 literal-JSON inputs are copied from the independent next-cut review. They
cover null/duplicates/presence, unknown enum numbers and names, integer numeric
lexemes and overflow, plus Duration/Timestamp WKT boundaries. `main.go` resolves
the real generated request message from Google's registry, reads only the first
JSON value with json.Decoder, and invokes actual protojson with DiscardUnknown.
Successful results include canonical typed JSON with original proto field names,
so unknown numeric enum preservation and absent/default values can be checked;
a 401 HTTP comparison alone would not establish these properties.

Future Rust red/green tests must consume these **actual Go outputs**. No expected
success/failure or decoded message is fabricated in cases.json. Preserve all
original HTTP/decode/CRUD gates as separate requirements. This tool checks typed
JSON decoding only, not auth, request validation, Twirp responses, workers,
telephony or any backend side effects.

## Residual corpus (separate, NOT_RUN)

`residual-cases.json` adds **49** independent cases without changing the original
22-case file. The same executable accepts either corpus:

```sh
GOTOOLCHAIN=local GOMAXPROCS=2 go run -mod=readonly . residual-cases.json > /tmp/livekit-rs-protojson-residual-go-results.jsonl
```

The development oracle explicitly imports Google's existing WKT registration
packages for FieldMask/Struct/Value/ListValue/wrappers/Empty. This introduces no
new module or version. It permits direct registered WKT tests instead of
inventing LiveKit request fields. Map vectors use actual
CreateAgentDispatchRequest.attributes and RTPStats.gap_histogram; Any is nested
in actual SIPCallInfo.call_context. Broader protocol messages and WKT roots are
not mislabeled as RPC request coverage.

Each case must supply exactly one of `body` (literal JSON string) or
`body_base64` (strict base64 containing exact request bytes). Invalid fixture
shape/base64 or unregistered message type aborts the process: those are setup
errors, never accepted as semantic rejections. The base64 cases exercise raw
invalid UTF-8/overlong encoding without a host-language string replacing bytes.
Old `body` inputs and output record shape are unchanged. Re-run the original 22
cases too when the coordinator executes this new fixture; the new WKT imports
must not silently change prior results.

No expected result is assigned to these 49 cases. Save actual Google output and
its source identity first, then use it to add Rust red tests. Maps, Any duplicate
ordering, FieldMask grammar, Timestamp grammar, explicit Value/NullValue,
discarded unknown numbers and malformed surrogate/UTF-8 inputs are deliberately
not assumed to share one rule. A successful process exit only means all records
were produced, not that all input JSON was accepted or Rust matched it.

## Typed protobuf output (strong comparison)

Every successful decode now additionally emits `wire_base64` from the actual
Google `proto.Marshal(msg)` call. The `message` JSON field remains diagnostic.
Rust must decode these real bytes into the same message descriptor and compare
typed values, field presence and maps. Do not compare raw protobuf byte ordering
or treat JSON `1` versus `1.0` as different google.protobuf.Value numbers.
This also verifies empty Any through its real protobuf value, without a special
expected-JSON branch. Missing wire output is a setup failure; there is no JSON
fallback. The coordinator must re-run the oracle before the updated Rust tests.
Old non-wire evidence is preserved as historical input/output.

## Response enum rendering corpus (separate F04 boundary)

`response-enum-cases.json` sets the explicit top-level `emit_unpopulated: true`
flag. The development oracle feeds that option to the actual Google JSON
marshaler; old corpora omit the flag and keep their prior behavior. This corpus
uses real SIP scalar/repeated/nested enum fields, int32 unknown endpoints,
known-enum companions, a string map and optional false presence. There is no
enum-valued map in the pinned public schema, so that shape is not invented as
real API coverage. The coordinator must generate actual output with this file
and bind it as the F04 response oracle before running the new response test.
Unknown enums remain actual numeric values, not defaults or arbitrary strings.
