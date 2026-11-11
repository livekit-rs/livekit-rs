# Original WHIP RPC across the actual room owner

Status: NOT RUN for this source input. This is an independent two-node extension
of the existing native-whip media fixture; it does not change the official SDK.

The campaign runs Go/Go, Rust/Rust, Go-front/Rust-owner and Rust-front/Go-owner,
each on its own Redis process. Node B starts first; its actual registered node ID
is read from that private broker. The official RoomService client creates a room
with that explicit node ID. The unchanged JS SDK connects through A, and the
standards-facing browser WHIP peer sends POST, real candidate PATCH, ICE restart
PATCH and DELETE to A. Every request must reach B's original WHIP participant
service. Positive acceptance includes decoded 440 Hz audio into the official SDK,
decoded 880 Hz audio in the WHIP peer, media progression after restart, the same
participant SID/track IDs after restart, and actual owner participant removal on
DELETE while the SDK observer remains.

Both nodes' real `/debug/rooms` are checked: B must contain both actual sessions;
A must have no room or shadow participant. This is a live owner observation,
independent of Redis room catalog/list APIs which are intentionally global.

```sh
python3 tests/interop/native-whip-cluster/run-paired.py \
  --go-server /absolute/fixed-livekit-v1.13.10 \
  --rust-server /absolute/fresh-candidate-livecloud \
  --evidence-dir /absolute/new-empty-campaign-directory
```

Use `--pair rust-front-go-owner` for diagnosis only; completion requires all four.
The fixed Go executable SHA is checked before execution. Executable and probe
identities are captured and rechecked after the campaign. Each probe records the
exact browser PID it owns; the runner only reclaims that PID if it survives a
failed probe, and treats such reclamation as failed cleanup. Broker and server
exit codes must be zero. No compilation is performed by this runner.

Source contracts: fixed protocol `rpc/whip_signal.proto` and
`rpc/whip_signal.psrpc.go` (`Create`, `ICETrickle`, `ICERestart`, `DeleteSession`
all claim=true/queue=true); server `pkg/service/whipservice.go`,
`roommanager_service.go`, `roommanager.go` participant topic lifecycle. This does
not certify external Ingress worker publication, proxy subprotocols or every
configuration combination. Those retain separate acceptance gates.

The child exit observer subscribes before either Playwright close call. Its
unchanged five-second grace starts only after both close operations complete and
only if the actual child has not already exited. A real nonzero exit or signal
remains failure. A close operation lasting longer than five seconds cannot turn
an already observed exit0 into a stale timer failure. Pure observer regression
coverage is `node --test tests/interop/owned-browser-exit.test.mjs`.

Cleanup now records the original unchanged-SDK DeleteRoom HTTP status/body and
SDK error. Only exact official 404/not_found/requested-room-does-not-exist may
mean natural departure already removed this room, and only after both API
ListRooms entrances, both actual node debug maps, and the private Redis room-node
mapping all prove absence. All other errors or stale state remain cleanup FAIL;
the original failure observation stays visible in cleanupDelete.

The fixture now closes each public browser context while the client connection
is still live, verifies zero contexts, closes the owning BrowserServer, then
releases the disconnected client. This avoids overlapping Playwright's
asynchronous context cleanup on client disconnect with process shutdown. Real
nonzero child exit still fails. `pw:browser` lifecycle diagnostics go to the
private probe stderr; a separate bounded 2 MiB child-stderr tail is retained in
browser.stderr with total-byte/hash/truncation metadata. Thus a subsequent Chrome
failure has actual process diagnostics instead of a bare exit-code guess.
