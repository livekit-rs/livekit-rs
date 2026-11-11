# Original Pion SCTP restart observer

This fixture invokes the byte-identical published Pion SCTP v1.12.0 implementation
selected by the fixed server. It does not copy a restart algorithm or substitute
a helper for cookie validation. `prepare.py` verifies every original input hash,
then copies original production and test files unchanged into a fresh isolated
stage. The original test files supply their existing real UDP fixture helpers.
No source time substitution is used for restart observations.

```sh
/usr/bin/python3 prepare.py --root /absolute/livekit-rs \
  --source-reference /absolute/livekit-rs/references/go-modules/pion-sctp-v1.12.0 \
  --out /absolute/new-task-stage
# In the new stage, coordinator executes:
GO_SCTP_RESTART_HISTORY=/absolute/new-history.jsonl \
  go test -mod=mod ./source \
  -run '^TestSourceRestart(AuthenticationWireObserver|PhysicalPeerObserver)$' \
  -count=1 -timeout=45s -v
```

The output must be a new file: the two tests append to one JSONL stream. Preserve
actual commands/exit codes, source and staged module graph hashes, and diagnostics.
The fixture source and source zip identity are separate from execution evidence.
The coordinator owns execution and process/output cleanup.

The six wire observations pass original serialized CRC32C INIT/COOKIE packets
through original `handleInbound`: current-handshake duplicate INIT has no
challenge; invalid MAC and wrong-port cookies do not commit or discard pending
data; valid authenticated COOKIE commits the same TCB and open Stream; duplicate
and competing cookies cannot roll back generation; ShutdownAckSent matching INIT
only schedules its SHUTDOWN_ACK retry.

The three physical observations use original `association(t, pipeDump)` with real
UDP sockets. The persistent Association stays alive while the other endpoint
closes and rebinds its same local UDP address. A real original `Stream.Write`
blocked on its prior generation must return `ErrAssociationRestarted`, buffered
amount callbacks must report zero, the original application Stream must stay
open, and new data must traverse both directions after each restart.

Native `association::restart_tests::fixed_pion_restart_wire_history_matches_native`
is explicitly ignored until `GO_SCTP_RESTART_HISTORY` points at the actual Go
output. It replays native real wire input and compares the six shared observation
records. Random tags, opaque cookie bytes and clock representation are not
compared as bytes. The physical Go records are required and validated but are
not mislabeled as a native physical/async writer test; native generation-owner
and real native packet lifecycle have separate existing execution gates.
