# Authenticated RTC compression parity

This corpus compares two actual local servers: the pinned Go LiveKit v1.13.10
binary and the candidate Rust binary. It uses the existing locked official
`livekit-server-sdk` to sign tokens and `@livekit/protocol` for signal messages.
No browser, modified SDK, mock WebSocket server or compression header shim is used.

The fixed source contract combines `rtcservice.go`'s wire `SetReadLimit` with
`wsprotocol.go`'s decoded `io.LimitReader(messageSizeLimit + 1)`. A compressed
message can therefore fit the wire budget and still fail the decoded budget.
The former closes with1009; the latter closes normally with1000. The fixed
`test/signal_message_size_limit_test.go` compression cases explicitly distinguish
these outcomes. Only positive configured limits apply;0 and-1 disable both.

Run from the repository root after the coordinator builds both binaries:

```sh
/usr/bin/python3 livekit-rs-server/tests/interop/websocket-compression/run-paired.py \
  --go /absolute/path/to/pinned-livekit \
  --rust /absolute/path/to/candidate-livecloud \
  --output /absolute/path/to/new-evidence-directory
```

The runner starts fresh server pairs for1024/0/-1 profiles, retains their identity,
configuration hash and exit status, and stops only its own processes. Ports17880
and17980 must be free. The base YAML inputs are shared with the existing
`websocket-message-size` corpus. Input messages are at most2048 bytes; decoding
server responses has a2MiB guard. This is a correctness test, not a load test.

The first eleven cases per side/profile exercise actual negotiated compressed Join frames,
valid compressed requests followed by correlated typed/control Pongs, one-byte
compressed continuation frames, fresh dictionaries for repeated messages, decoded
vs wire overflow, invalid DEFLATE and an uncompressed no-offer connection. Accepted
connections must complete a real close handshake. Rejected cases require an actual
close frame with the pinned code. Additive controls check Gorilla's exact
1002 reasons for RSV2 and unnegotiated RSV1, and successful signaling after
negotiated RSV1 on a continuation. A second message referencing bytes outside its
fresh dictionary must retire with Close1000. Six additional cases send only
the first two header bytes after an authenticated Join. They assert the
ordered Close1002 reason before any extended length, mask key or payload
can arrive. The report checks observations for equality,
not merely each process's exit code. Tokens are redacted from errors.

Native codec tests additionally exercise RFC7692 known-answer frames, masked and
unmasked roles, Ping interleaving, continuation/RSV errors and optional standalone
decoded caps. Axum tests check route opt-in and extension-offer negotiation. Native
default decoded cap remainsNone; the authenticated `/rtc` consumer explicitly
binds both limits to `limits.signal_message_size_limit`.

Status: implementation and test source only until coordinator-produced execution
receipts bind these commands to the candidate. This corpus does not certify media,
Agent WebSockets, tenant isolation, performance or global Go compatibility.

Actual first mixed campaign retained `FAIL_COMPLETE_CAMPAIGN` because raw TCP
post-Close tails differed. The independent unchanged Go/Go control also failed
raw equality: after the same valid rejection Close it alternated EPIPE and
ECONNRESET, and separate runs observed both clean EOF and reset. Fixed
`wsprotocol.go:99 CloseWithReason` writes Close then calls conn.Close without
awaiting ACK. The runner now keeps `strictPairEqual` and every raw tail, and
separately computes `protocolContractEqual`. Only rejected parser/limit cases
exclude that already-observed post-Close race from protocol equality. Actual
correct Close, server-owned EOF/reset, no forced test destruction, and known
transport error class remain mandatory. Accepted exchanges still require the
complete clean handshake. Old receipts are never relabeled.

Seven additional Close payload cases exercise empty and short payloads, invalid status before malformed UTF-8, exact invalid UTF-8 reason, and legal normal and application status acknowledgements.

The signed-length corpus sends authenticated masked RTC frames with a high-bit
64-bit length both with and without the masking key. It also sends a
continuation whose declared length overflows the signed aggregate after an
actual first fragment. All three cases require the normal LiveKit Close 1000
and server-initiated transport shutdown at each configured limit. Two positive
controls encode a small valid signal using non-minimal 16-bit and 64-bit length
forms and require both control Pong and correlated application Pong.
