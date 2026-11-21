# pion-rtp-padding — fixed-Pion RTP padding admission oracle

Paired differential for SPEC-0077 item C67 (malformed RTP padding at the
publication boundary). It pins one question: for a packet whose padding bit is
set but whose padding is malformed, does the official Go stack *accept* the
packet, and what does the receiver then see?

## What is compared

`main.go` calls `rtp.Packet.Unmarshal` from the exact `github.com/pion/rtp
v1.10.5` revision that the fixed livekit-server resolves through
`github.com/livekit/webrtc-pion/v4 v4.2.18-warp.6`, once per vector in
`cases.jsonl`, and prints one `SOURCE_PION_RTP_PADDING {json}` row per vector
with the actual verdict and the actual Pion struct state:

| field | meaning |
| --- | --- |
| `accepted` | `Unmarshal` returned nil |
| `error_text` | the sentinel error text, empty exactly when accepted |
| `payload_hex` | `packet.Payload` after unpadding |
| `padding_only` | upstream's own predicate `rp.Padding && len(rp.Payload) == 0` |
| `padding_bit` | `packet.Padding` left by `Header.Unmarshal` |
| `padding_size` | `packet.PaddingSize`, including the value Pion assigns before it rejects for `end < n` |

`vendor/str0m/src/rtp/header.rs` holds the same table as frozen Rust literals
(`FROZEN_PADDING_ADMISSION`), and `Session::handle_rtp_packet`'s real path —
`RtpHeader::parse` then `RtpHeader::unpad_payload` — must reproduce the three
decision fields for every vector.

## What is *not* claimed here

* This is the **parser admission decision only**. The upstream publication
  consequence — warp.6 `handleUnknownRTPPacket` returning the error before any
  receiver, RID/mid metadata or `RemoteTrack` publication exists
  (`rtptransceiver.go:427-458`, consumed at `peerconnection.go:1899`, with the
  padding-only probe skip at `:1929`) — is mirrored in the Rust receive path by
  `authenticated_malformed_padding_drops_before_publication` in
  `vendor/str0m/src/session.rs`, which is an SRTP-authenticated session test, not
  this oracle.
* **Neither sender can produce these bytes.** pion/rtp `MarshalTo` refuses a set
  padding bit whose padding size is zero (`packet.go:539-540`) and always writes
  the terminal count byte; str0m `create_padding_packet` refuses `pad_len == 0`.
  Every vector is therefore a crafted wire buffer, which is exactly what makes
  the boundary a parser question rather than a codec round-trip question.
* `livekit-stable` does not contain `handleUnknownRTPPacket` or
  `isPaddingOnlyPacket`; those identifiers live in the warp fork. In
  livekit-stable the equivalent padding-only classification is
  `RTPFlowUnhandledReasonPaddingOnly` guarded by `payloadSize == 0` at
  `pkg/sfu/rtpstats/rtpstats_receiver.go:277`, and `Buffer.Write` re-runs the
  same full `Unmarshal` on every packet of a started stream
  (`pkg/sfu/buffer/buffer.go:162,197`).
* Padding-only *valid* packets and RTX/repair first-packet publication are
  covered by C65/C66 (`tests/oracles/dd-padding-physical`,
  `tests/oracles/video-rid-publication`); this oracle covers only the malformed
  padding acceptance set.

## Run

From the repository root, with the cached Go toolchain and the module cache
already populated (offline by design: `GOPROXY=off`, `-mod=readonly`):

```sh
python3 tests/oracles/pion-rtp-padding/run.py --output "$(mktemp -d)"
```

`run.py` runs the real Pion observer over `cases.jsonl`, validates the row count
and every field against the committed expectations, then runs

```sh
cargo test --release --locked -p str0m \
  rtp::header::test::original_go_rtp_padding_admission_rows_match_native_parser \
  -- --ignored --exact
```

with `PION_RTP_PADDING_GO_CASES` and `PION_RTP_PADDING_GO_RESULTS` pointed at the
actual observer files. It finally re-runs the same test against a mutated results
file (a rejected vector reported as accepted) and requires that run to fail, so a
green report cannot come from a comparison that is unable to disagree.

`tests/oracles/build-oracles.py` compiles this module as part of the oracle
sweep; the ignored Rust test is only meaningful through `run.py`.
