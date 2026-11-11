# F07 public SIP call/transfer source-order differential

Status: integrated real HTTP/SDK differential. Initial Redis82 and no-Redis84
campaigns passed, including real official browser room membership. Each final
candidate still requires fresh execution; see SPEC-0019 and the F07 delivery
record for exact source/binary-bound results. This fixture never synthesizes a
successful call or replaces a SIP worker.

## Purpose

Determine actual fixed-Go vs Rust public outcomes for CreateSIPParticipant and
TransferSIPParticipant, in particular the disputed old unit expectation that a
valid Transfer on an absent room always returns Internal/not_connected. This
script **does not prescribe NotFound, Internal or text for that case**. It records
both actual responses including HTTP status, content type, full raw response,
code, message and all error metadata, then compares their exact parsed semantics.
No diagnostic string is rewritten, no metadata is discarded and no error is
normalized into an expected result. JSON key ordering is immaterial; raw bodies
remain in evidence. Timing is recorded but never compared as compatibility.

Use two independently configured servers and actual stores. Each token is
created by the existing unmodified locked livekit-server-sdk AccessToken helper:
SIP admin is not SIP call; SIP call alone is not room admin; room admin on another
room does not pass. This does not broaden grants or bypass either auth gate.

## Corpus

Default run: 82 actual HTTP observations with Redis or 84 without Redis:
- malformed JSON, scalar JSON, malformed protobuf and genuine known-field
  invalid UTF-8, with absent and full grants, for each public method;
- empty Create vs Transfer's source-specific room/identity preflight;
- SIP call vs admin vs unrelated video grant;
- Create hard validators and a deliberately missing real trunk, all rejected
  before a backend RPC; a fully valid Create is tested only in no-Redis mode;
- Transfer missing/invalid URI, missing identity/room, wrong room admin and
  invalid header before actual room lookup;
- all four legacy valid URI cases (SIP/SIPS/TEL/angle-TEL) on an absent room,
  preserving the official result rather than guessing the old assertion;
- actual CreateRoom; valid Transfer to missing participant in that existing room;
- an **actual connected non-SIP RTC participant** using unchanged official browser
  client; actual RoomService.GetParticipant confirms absence of sip.callID;
  Transfer then tests no-session failure and independent wrong-room denial;
- actual DeleteRoom followed by the same lookup boundary again.

All typed cases run as official SDK protocol JSON and binary protobuf. The
script imports the actual @livekit/protocol request classes instead of inventing
binary field tags (only the explicit invalid-UTF8 scalar tags are handwritten,
locked by actual public schema). No call reaches a legitimate backend worker:
Create inputs with Redis are deliberately invalid or missing a trunk; every
valid Transfer stops at real missing room/participant/no-SIP-session lookup.
Unexpected HTTP200 or an accidental RPC wait (12s HTTP safety timeout) is FAIL,
not a fabricated pass. All compared mismatches remain recorded, without aborting
at the first semantic difference. A transport failure is not a Go error oracle.

The normal participant uses a small local HTTP page and official SDK connection,
with no published media. This is real room membership, not an API-only mock
participant. It requires the existing installed Playwright/Chromium dependencies.
Set SIP_HTTP_WITH_PARTICIPANT=false only for a preliminary non-browser diagnostic:
coverage is explicitly NOT_RUN and the overall result is PARTIAL/exit1, never
PASS. Disabling this coverage cannot close the requested functional gate.

## Coordinator execution

Prerequisites: build and start pinned unmodified Go + candidate Rust servers,
each with either no Redis or its own dedicated broker/database (same mode on
both). Do not run a SIP worker, and do not point at production. The coordinator
starts/stops all server processes and owns final evidence/cleanup.

```
REFERENCE_URL=http://127.0.0.1:<go-port> \
LIVEKIT_URL=http://127.0.0.1:<rust-port> \
SIP_HTTP_BACKEND=redis \
REFERENCE_API_KEY=<go-key> REFERENCE_API_SECRET=<go-secret> \
LIVEKIT_API_KEY=<rust-key> LIVEKIT_API_SECRET=<rust-secret> \
INTEROP_PACKAGE=/absolute/repo/livekit-rs-server/tests/interop/package.json \
SIP_HTTP_OUTPUT=/private/task/sip-http-redis.json \
node livekit-rs-server/tests/interop/sip-http/run.mjs
```

Run a separate actual campaign with SIP_HTTP_BACKEND=none and correctly configured
no-Redis servers. The flag documents setup; it does not modify server state or
pretend a broker is disconnected. URLs must be HTTP(S); browser URL derives the
matching WS(S). The script requires current interop locked versions and no new
npm dependency. It never edits the SDK. Defaults devkey/secret are local fixtures,
not deployment credentials; no JWT or credentials are stored in evidence.

Cleanup deletes only the two uniquely task-owned rooms, closes its own browser
contexts and local static HTTP server. It never deletes arbitrary Redis keys,
starts/stops a SIP worker or stops either external server. A cleanup error fails
the profile. SIP trunk/catalog state is not mutated.

## Source and acceptance boundaries

Fixed server service/sip.go: Create584-680, Transfer683-781. Transfer's actual
RoomService.GetParticipant precedes SIPInternal worker use; absent attribute key
is distinct from present-empty. The actual oracle, not this source reading,
decides final public error status/text/metadata. Source SHA/SDK lock hashes are
recorded in SOURCE.json.

This script does not validate SIP call/transfer backend success, media, providers,
PSTN, worker cancellation, or Twirp timeout header propagation. A future explicit
real deadline/cancellation campaign must record both servers' actual outcomes;
no header contract is guessed or implied by this error-order corpus. Those
boundaries remain OPEN and separate from the local SIP media acceptance profile.
