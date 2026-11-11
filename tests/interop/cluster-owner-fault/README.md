# Official SDK room-owner drain and hard stop

This fixture starts a private Redis and two server processes. Both unchanged
browser SDK clients use the surviving HTTP entry. RoomService creates the test
room on the other registered node explicitly. Redis `room_node_map` and the
browser's selected ICE remote ports independently prove that node owns media
before the fault. Neither engine receives a synthetic Redis edit.

- **Drain** sends SIGTERM and requires the owner to publish SHUTTING_DOWN while
  both clients remain connected. Closing each test client's signaling socket
  exercises the SDK's normal resume path. The fixture verifies a new signal
  socket, the actual SDK transition, and fresh decoded media on the same owner.
  Both clients leave, the owner exits zero and unregisters, then clients join
  the recreated room on the survivor.
- **Hard stop** sends SIGKILL to the media owner. Existing SDK clients must
  automatically reconnect through the unchanged entry and move their actual
  media to the surviving node. A separately labeled fresh SDK reconnect, if
  attempted after failure, cannot turn the automatic reconnect result green.

Every successful media phase requires frame-count growth, opposite known audio
tones and five ordered, reliable application packets in each direction.
The final receipt records the room SID, identities, current owner, client PC and
audio cleanup, Redis absence of the room, mapping and participants, and physical
port release. The dead node's registry row is recorded separately from room
cleanup. Claimed server RPC behavior is covered by an independent fixture.

From the repository root, with the locked interop dependencies and Chromium
already available, run the matrix on both binaries:

```sh
node livekit-rs-server/tests/interop/cluster-owner-fault/run.mjs \
  --binary /absolute/path/to/livekit-server --mode drain --single \
  --output /absolute/path/to/new-output
```

Repeat with `--mode kill` and with `--single` omitted for dual PC.

Add `--reliable-trace` to the single-PC hard-stop profile to locate
application-message loss. This opt-in records the actual SDK room
Reconnected event, current peer connection and data-channel generations,
signal response types, the reliable send backlog, and native data-channel
sends and incoming events. Only packet type, topic, sequence and
participant SID are retained. The SDK bundle is served byte-for-byte.
If the immediate reliable gate fails, the same recovered SDK rooms
then exercise later reliable and lossy messages and the public
RoomService SendData path. Those controls and the fresh SDK reconnect
cannot turn the initial failure into a pass.
