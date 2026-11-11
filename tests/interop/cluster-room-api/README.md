# Actual cross-node RoomService API consumers

Status: NOT RUN on the current input. This campaign uses the unchanged official
LiveKit browser SDK 2.23.0 and server SDK 2.19.2 from the existing interop lock.
No endpoint response or packet count alone is treated as successful media.

The private Redis broker and two independently owned server processes run all
four fixed-Go/Rust front/owner pairs. B starts first and its actual node ID is
observed from that isolated broker; the official CreateRoom call explicitly pins
this room to B. All browser signaling and subsequent public API calls use A.
Both local debug room maps must prove B owns the real sessions and A has no
shadow room.

Required consumer effects:

- Actual 440 Hz decoded audio and changing decoded VP8 video frames in the SDK.
- Non-owner UpdateParticipant metadata, name and attributes visible remotely.
- Non-owner MutePublishedTrack causes measured decoded silence. With explicit
  `room.enable_remote_unmute: true`, remote unmute restores decoded audio/video.
- Non-owner UpdateSubscriptions removes both actual SDK tracks, keeps them
  absent for one second, then restores decoded audio and video on resubscribe.
- Non-owner SendData reaches exactly the selected identity through the SDK,
  including topic and unchanged payload; the publisher must not receive it.
- Non-owner PerformRpc invokes the real registered browser SDK method and
  returns its exact response through the original public API.
- Non-owner RemoveParticipant causes actual SDK disconnection, closed peer
  connections, ended local tracks, and owner removal while the observer remains.

Peer connection observation wraps the browser's standard constructor before the
unchanged SDK loads. It only records actual object state; it does not implement
signaling, SDP, ICE, codecs or media behavior.

```sh
python3 tests/interop/cluster-room-api/run-paired.py \
  --go-server /absolute/fixed-livekit-v1.13.10 \
  --rust-server /absolute/fresh-candidate-livecloud \
  --evidence-dir /absolute/new-empty-evidence-directory
```

The optional `--pair rust-front-go-owner` is diagnostic; full acceptance needs all
four. The runner captures the exact binary, probe, fixture and runner hashes,
checks their identity at the end, owns broker/server/browser cleanup, records
exit codes, and treats forced cleanup as failure. Run it from the server project
with the existing interop dependencies available; it performs no compilation.

ForwardParticipant and MoveParticipant are intentionally outside this positive
matrix because the fixed OSS server does not implement their Cloud migration
behavior. Their explicit original error contract remains independently tested.
Successful rows here cannot close other SDK, Redis topology, cross-node media,
worker or global-regression gaps.

Cleanup records the unchanged SDK DeleteRoom request's original HTTP status/body
and SDK error. Only exact 404 / `not_found` / `requested room does not exist` may
mean natural departure already removed the room. That classification additionally
requires both official ListRooms entrances to report absent, both real node debug
maps to report absent, and the campaign's private Redis room-node mapping to be
absent. Any other error, stale mapping or failed confirmation remains a cleanup
failure. The original 404 observation remains in `cleanupDelete`, never erased.

Both browser close operations use the shared real-child exit observer. Exit
listeners are registered before closing; the finite five-second grace begins
only after close completion if the process is still alive. The actual exit0 is
read from the event or child state, never inferred from SDK disconnection.
`owned-browser-exit.test.mjs` covers the stale pre-close timer regression and
preserves failure for real timeouts, nonzero exits and termination signals.

Context disposal is explicitly awaited before owner BrowserServer shutdown and
client disconnection. Nonzero process exit remains failure. The runner captures
browser lifecycle diagnostics and a bounded owned-child stderr tail with hash and
truncation metadata, so shutdown failures retain actual diagnostic evidence.
