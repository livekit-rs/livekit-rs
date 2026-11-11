# Three-participant public API functional consumer

Use the existing locked, unmodified browser SDK and server SDK. This additive
fixture performs actual publication, targeted/broadcast RoomService data,
video unsubscribe/resubscribe and public PerformRpc against three real peers.
It observes the SDK's existing native PC handles without creating connections
or changing SDK code. Requested topology alone does not establish negotiated
Single/dual topology.

Start the pinned official server separately, then run:

```sh
LIVEKIT_URL=ws://127.0.0.1:OFFICIAL_PORT SINGLE_PC=true RESULT=/private/path/go-single.json node run.mjs
LIVEKIT_URL=ws://127.0.0.1:RUST_PORT SINGLE_PC=true REFERENCE_RESULTS=/private/path/go-single.json RESULT=/private/path/rust-single.json node run.mjs
```

Repeat the entire pair with `SINGLE_PC=false` and distinct result files. Supply
credentials through the existing `LIVEKIT_API_KEY` and `LIVEKIT_API_SECRET`
environment variables if needed. The coordinator must independently record
source/binary/config identities; a file named REFERENCE_RESULTS does not itself
prove an official process produced it. Go must finish REFERENCE_CAPTURED/exit0;
Rust must finish PASS/exit0 with the same finite API/topology contract. Counts
are independent functional minima, not performance comparisons. Failed source
runs cannot be rewritten or used to relax the Rust assertions.

The runner closes its peers, browser and HTTP asset server and deletes its own
actual room; any cleanup failure fails the attempt. Reports exclude credentials,
room names, participant/track IDs, addresses and SDP. Servers are externally
owned and must be stopped by the coordinator. Check repository evidence for
actual run status; source installation alone is NOT functional acceptance.
