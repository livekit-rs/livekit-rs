# Two-entry clustered official SDK media acceptance

The existing SDK files are unchanged: livekit-client2.23.0, server-sdk2.19.2 and
protocol1.52.1 from the existing locked interop package. `run.mjs` now accepts
`LIVEKIT_PEER_URLS='["ws://node-a:port","ws://node-b:port"]'` and an absolute
`INTEROP_OUTPUT_DIR`. Its original one-URL/default-output behavior remains the
default. The explicit output directory must be new, preventing evidence overwrite.

`CLUSTER_MEDIA_PROBE=true` adds actual remote frequency measurements to ordinary
RMS audio, ordered data and VP8 decoded-frame checks. Client0 publishes440Hz and
must receive880Hz; client1 publishes880Hz and must receive440Hz. The analyser
measures decoded MediaStream audio, not the oscillator configuration. Both
assigned signaling entries must actually appear in browser WebSocket profiles.
Each peer reconnects through its assigned entry and must receive new ordered data,
new correct-frequency audio and new VP8 frames afterward. SDK PeerConnections,
AudioContexts and sampling timers must close, and both entries must report no
remaining participants before the room is deleted.

```sh
/usr/bin/python3 livekit-rs-server/tests/interop/cluster-sdk/run.py \
  --go /absolute/immutable/pinned-go-server \
  --rust /absolute/immutable/candidate-rust-server \
  --redis-server /absolute/redis-server \
  --output /absolute/new-evidence-directory
```

The coordinator runs this command; authoring this harness did not launch any
server, browser or compiler. Optional `--only go-go-single` provides an explicitly
labeled calibration subset. Only the default full run can report all8: Go/Go,
Rust/Rust, Go/Rust and Rust/Go, each in single-PC and dual-PC modes.

Every case starts its own Redis and two servers, using fresh generated JSON/YAML
configs, distinct HTTP/media ports and shared Redis. Ports20780/20790 (HTTP),
20781/20791 (media TCP),20782/20792 (media UDP),20799 (Redis) must be free. Existing
services are never reused or stopped. Binary/config/SDK harness hashes, exit
codes and diagnostics are preserved. Browser process groups created by this
runner are the only ones it may terminate on a deadline; forced cleanup fails
acceptance.

The runner discovers each node's actual registered ID from Redis during ordered
startup. It samples the real `room_node_map` while SDK peers are connected and
requires one unchanged owner belonging to this two-node cluster. Distinct assigned
entries mean at least one actual peer enters through a node other than the owner.
Both browsers' selected remote ICE ports must belong to that same owner's media
endpoint. This combines actual owner/routing proof with decoded media/data, rather
than using listParticipants as a media substitute.

After SDK close/delete, routing state must disappear; both servers must unregister
their nodes and exit normally. Redis then exits normally. Any failed case remains
failed, and the runner continues collecting the complete selected matrix. This
scope does not certify workers, global migration, media Mesh, billing or all API
semantics.
