# Fixed Go/Pion SPED peer

`go-peer/main.go` is an actual Pion PeerConnection built against the same immutable
module selections as fixed LiveKit Server v1.13.10: webrtc-pion4.2.18-warp.6,
ICE4.4.0-warp.3 and DTLS3.1.5-warp.3. Their published implementations are not
modified. The observer wraps a real loopback UDP socket and counts serialized
DTLS-in-STUN/ACK attributes; it can drop exactly the first outbound embedded
DTLS datagram. DataChannel.Send must send a real binary SCTP payload, and the
Rust media consumer must acknowledge receipt before the Go process closes.

The Rust acceptance test is ignored unless explicitly run with the helper path:

```sh
# Copy go-peer inputs into a NEW task-owned build directory first.
# Build there, preserving source/module/binary hashes and the resolved module graph.
go build -mod=mod -o /absolute/task-output/fixed-go-sped-peer .
GO_LIVEKIT_SPED_PEER=/absolute/task-output/fixed-go-sped-peer \
  cargo test -p rtc-media --lib warp::warp_sped_fixed_go_pion_peer_over_real_udp \
  -- --exact --ignored --nocapture
```

The coordinator owns compiler execution, process cleanup and final certification.
Do not compile in or alter reference checkouts. `-mod=mod` may fill indirect
requirements only in the fresh build copy; retain its final go.mod/go.sum/module
graph with execution receipts. Input go.sum is the pinned server's complete sum
closure, not a license to change any of the three explicit replacement versions.

Four actual peer combinations run: SPED both sides; SPED both sides with one
lost Go flight; SPED Rust with plain Go; plain Rust with SPED Go. Positive cases
require nonzero embedded flights in both directions, nonzero ACK observations,
and actual data delivery. Fallback cases require the disabling endpoint never
emit an embedded flight. Both helper and Rust transport must close; the Rust test
owns and kills/waits only its spawned helper if an assertion or deadline fails.

This checks media transport compatibility with fixed Go. It does not certify
browser video decode, global room routing, or the server's entire compatibility
surface. No receipt is implied by the existence of this test source.
