# Claimed cross-node PerformRpc during owner drain or hard stop

The runner starts two daemons and a private Redis for each case. Both
clients enter through the HTTP front. The owner is proven by Redis routing,
catalog, two live participants and the sole request subscriber. The unchanged
browser SDK receives actual decoded video and a 440 Hz tone from a second
client. The public request uses the generated official Go Twirp client from
the fixed server's protocol dependency. Its SDK callback records the exact
payload and waits for the coordinator to release it.

The fault is gated on one native Redis REQ, one claim announcement and one
grant, with a matching request ID and target peer. The genuine SDK handler
must have entered. The front, owner and held request must all be live before
the signal. Monitor output is reduced to safe correlation IDs and digests.

- Drain uses one SIGTERM. The node must publish SHUTTING_DOWN. Decoded media
  must keep advancing, and releasing the SDK handler must return its exact
  response through the front. The owner exits after both clients disconnect.
  Redis node, catalog, participants, routing and topic readbacks must clear.
- Hard stop uses SIGKILL. The owner must exit with signal 9 and release its
  three sockets. The original claimed call has no response publication.
  The normal three-attempt policy emits two further unclaimed requests and
  returns HTTP 503, Twirp unavailable, `no response from servers`.
  The surviving front must still serve. The stale owner record and its room
  state are read before cleanup. Browser/peer and fixture processes/ports
  must release. The owned Redis is discarded only after the readback.
  Owner reallocation and SDK media recovery are separate from this control.

The runner requires the established fixed Go v1.13.10 binary SHA-256 and
stores the Go, Rust, fixture and client identities. Exit 0 requires every
selected case to satisfy its observations and paired public outcomes to
match. Both expected signal exits and any forced cleanup stay visible.

With `--concurrency 2`, both real Twirp calls target the same SDK
participant with distinct payloads. The owner must grant each claim
before either callback can finish. During drain the coordinator releases
the second callback first and requires the first caller to stay pending
through that response. During SIGKILL it checks the retry and terminal
result independently for both calls, including distinct Redis request
identities. `--single-pc true` and `--single-pc false` require one or two
actually connected SDK peer connections respectively.

With `--recreate`, two overlapping official RoomServiceClient CreateRoom
HTTP requests use the surviving node after the immediate Redis readback.
The resulting SID must be new after drain and must retain the stored
catalog SID after SIGKILL and the five-second availability window. The
runner checks the shared public response, room listing, Redis owner and
stored SID, then deletes the room through the SDK and observes catalog,
participant and mapping cleanup.

```sh
root="$PWD"
out="$(mktemp -d)"
(
  cd references/livekit-stable
  GOTOOLCHAIN=go1.26.0 go build -mod=readonly -o "$out/rpc-client" \
    "$root/livekit-rs-server/tests/interop/owner-claimed-rpc/rpc-client.go"
)
python3.13 -B livekit-rs-server/tests/interop/owner-claimed-rpc/run.py \
  --go /absolute/pinned/go --rust /absolute/candidate/livekit-server \
  --client "$out/rpc-client" --output "$out/paired"
```
