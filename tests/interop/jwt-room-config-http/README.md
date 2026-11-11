# F109 current public JWT diagnostic source review + actual HTTP draft

**Read-only source audit. No Cargo/Go/server/browser/Node execution.** This is an author reinspection of F104, not an independent implementation or runtime certification. Current shared F104/F102 source is reviewed; nothing shared was edited.

## Actual provenance correction

`/tmp/f103-jwt-diagnostic-contract/diagnostic-tests.rs` calls actual `verify_token`, `AuthHttpError::into_response` and body-to-bytes in memory. It does not bind a socket, invoke Router/auth middleware, send HTTP or run the SDK. It must be called a **component response test**, not a public HTTP fixture. Its actual results do not close transport/route/header or SDK error.message gates.

## Go13 source-binary difference, exact proof

`go13-prefix-only-proof.json` checks raw old/new input outputs field-by-field. Both have13 rows;6 rejected IDs have12 changed `direct_error`/`verify_error` strings. At the one exact `proto:` position, old has U+00A0 (`c2a0`) and new has U+0020 (`20`); all bytes before/after and all other values match. The original JSONL file hashes differ and `exact_identical=false` remains. **No normalized output is produced, and this is not a public HTTP pair PASS.** Fixed protobuf `internal/errors/errors.go:28–38` explicitly chooses the two prefixes using executable-dependent detrand (`internal/detrand/rand.go:5–9,37–68`). Fixed binary means its actually captured choice remains the exact reference for that HTTP run. No broad whitespace normalization is justified.

## Current public call chain

`lk-compat/src/http.rs:216–236` extracts bearer/query token and calls `authenticate_token`, converts errors into the actual response, then forces validate CORS `*`. `auth/mod.rs:158–174` obtains the key, calls actual jwt::verify with F102 raw occurrence validator followed by normal claim decode. `room_config_wire.rs:34–47` passes each exact nested RawValue into `json::diagnostic::decode`. `jwt.rs:201–207` wraps the custom error in `token is malformed: could not JSON decode claim:`; `AuthHttpError::Verify` prefixes `invalid authorization token:` and returns401 plain text.

Fixed Go `auth.go:86–102` has the same initial parse→key→full verification stage; `utils.go:84–87` writes error.Error bytes, no JSON envelope/newline. Official SDK `SignalClient.ts:1342–1345` classifies401/403 and retains the body as message. This review does not mistake SDK status classification for complete text parity.

## Specific source findings

- **No new confirmed defect in the reviewed Go25 diagnostic family.** Known duplicate raw lexical key is retained including quotes/backslash escapes; no decoded-key reconstruction is used. `Context::fail` takes its token from the original source range, not from a substring search or error message.
- Descriptor scalar names are fixed schema names, while raw tokens originate in syntactically validated JSON. Literal CR/LF are illegal inside a string; escaped `\\n`/`\\r` remain escaped raw bytes. HTTP response has a fixed plain-text content type/status and never uses a raw token as a header. No source-backed need for additional HTML/JSON escaping was found; extra escaping would itself diverge from Go raw diagnostic tokens. This is not a general JWT security audit.
- Position is based on the original custom RoomConfig value (not the whole JWT) and counts `\n` plus Unicode scalar characters. This matches fixed `internal/encoding/json/decode.go:225–232`. Escaped keys count their raw ASCII escape characters, as Go does. Source `decode.go:210` uses raw name token, so preserving `"empty\\u0054imeout"` rather than canonicalizing it is correct.
- Earlier known scalar validation occurs within the field loop before the next sibling's duplicate check (`diagnostic.rs Context::message/value`). Thus the actual Go25 `wrong-scalar-before-duplicate` yields uint32 invalid-value at1:17 rather than the later duplicate. F104 runs only after original admission failure, so its successful-value/admission bypass remains intact.
- Seen known field is marked before null skip (source duplicate-null), oneof nonnull ownership is checked before scalar, map duplicate is checked before its value, and discarded unknown enum map values do not populate the duplicate set. The reviewed ordinary field paths match the fixed Go order. Unknown data remains discarded.
- **Explicit unresolved scope**, not a new regression: WKT/Any/depth/malformed JSON diagnostic families fall back to original errors; an existing admitted/rejected-language gap is not repaired by this diagnostic-only API. The source renderer default Display always emits ASCII, so exact HTTP body against an NBSP reference binary still fails even when the suffix is right. This is visible compatibility scope, not an excuse to normalize arbitrary text or claim all diagnostics complete.
- Root-level malformed claim/registered-field order and ordinary top-level case-variant winner semantics remain separate original JWT scopes. No broad promotion is made here.

## Genuine HTTP fixture — NOT_RUN

`http.mjs` uses Node built-ins only and **prestarted actual servers**. It signs exact raw lexical RoomConfig bytes with the configured real key, including multiline/escaped/duplicate occurrences; it does not JSON.parse/reencode the nested config. It calls real RoomService CreateRoom/DeleteRoom to establish an owned legal room, then sends all25 cases through both actual HTTP `/rtc/validate` routes using **both bearer and query authentication**:100 total requests across Go/Rust. Positive bodies must be real200 `success`; negative bodies must be401 through the real diagnostic wrapper. All rows require validate CORS`*`. It captures actual status, raw body bytes/base64/hash and content type and compares Go vsRust **exactly**, including the actual prefix. Every comparison executes before reporting PASS_SCOPED; no narrower run or skipped rejection counts. Owned cleanup failure fails the run.

Coordinator:

```
F109_OFFICIAL_URL=http://127.0.0.1:17880 F109_RUST_URL=http://127.0.0.1:17980 F109_OFFICIAL_BINARY=/ABS/actual-reference F109_RUST_BINARY=/ABS/actual-livecloud LIVEKIT_API_KEY=devkey LIVEKIT_API_SECRET=secret F109_OUTPUT=/owned/f109-http.json node livekit-rs-server/tests/interop/jwt-room-config-http/http.mjs
```

Parent must prove the binary paths correspond to the actual launched processes and archive effective server profile/ports. The script hashes those files, not an invented source-only identity. Config/API authentication setup must be actual and identical. Use trusted normal TLS environment for HTTPS; this script introduces no certificate bypass. No browser/SDK claim follows from this HTTP-only gate.

All25 original F103 cases are copied byte-exact, as are the actual Go25 direct/verify observations. Actual source Go results give admission expectations; **the running reference HTTP server supplies actual response bytes** because its executable prefix can differ from the Go component oracle. API positive controls would fail without a legal roomJoin grant, which the old component-only payload did not need; this new fixture includes it. The driver never persists JWTs, credentials, SDP or participant IDs. Captured error text contains only controlled public fixture literals. Private server logs are parent-owned temporary evidence.
