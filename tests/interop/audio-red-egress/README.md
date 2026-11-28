# Audio RED egress ground truth

Measures what the original browser SDK actually receives on the egress of a forwarded audio track: the
payload type, the RFC 2198 redundancy chain (block count, block PTs, timestamp offsets, primary length) and
the per-packet header/tail byte budget. The pinned Go binary defines the expected values; the Rust binary is
scored against them.

    python3 tests/interop/audio-red-egress/run.py \
      --go /tmp/c99-build/livekit-server-go \
      --rust target/release/livekit-server \
      --output /tmp/c109-red/campaign

Two profiles (`audio.active_red_encoding` true/false) × two phases (subscriber joined before the track
exists, and a track published after the join) × both binaries. `--strict` turns a Go-vs-Rust divergence into
a non-zero exit; without it the gate only fails on a broken capture.

## How a value becomes trustworthy

* `probe.mjs` taps cleartext RTP headers in the TURN proxy (a prototype override on `LossyUdpProxy.observe`,
  so the shared `tests/interop/media-recovery/lossy-proxy.mjs` stays byte-identical) and installs an
  insertable-stream transform on the sender and the receiver. The two views join by RTP timestamp.
* `red-observer.worker.mjs` parses the chain from the payload the endpoint put on the wire and FNV-1a hashes
  the whole payload and the primary block, so cross-leg equality proves verbatim forwarding rather than a
  matching shape.
* A chain is only parsed on a PT the SDP negotiated as `red`, because an Opus TOC octet with the top bit set
  (config ≥ 16) mimics an RFC 2198 first octet: a `red: false` payload parsed ungated returns
  `chainError: "block-length-overflow"`. `redRows` separates the gated frames from those and `phantomChains`
  counts any complete chain an unnegotiated parse would invent.
* `run.py` rejects an empty or self-referential capture: ≥ 15 frames and ≥ 15 wire-matched frames per leg,
  `matchedInWire == frames`, `parseErrors == 0`, per-packet overhead within 12–64 bytes, ≥ 15 timestamps
  shared between the uplink and the egress of the same track, and an observer worker that reported no
  `onerror`. Hook attachments and any superseded transform are recorded in the receipt for diagnosis.

Results and their interpretation live in the evidence slot
`livekit-docs/server/evidence/2026-11-28/spec0077-c109-audio-red-ground-truth/`.

## Arms: the removal and restoration comparison

    python3 tests/interop/audio-red-egress/arms.py --arm all --output /tmp/c109-red/arms --skip-build

`arms.py` drives the four states the RED pass-through retirement has to distinguish, and writes
`arms.json` with the digest of every file it touched before and after each arm (an arm that cannot
restore its tree aborts the run):

| arm | source state | what must happen |
| --- | --- | --- |
| `retired` | app-side gate, 72-patch vendor chain | suite green, reconstruction gate green, campaign matches Go |
| `chain-stub` | `let chain = None` in `forward.rs` | the campaign must diverge on the egress PT and chain rows |
| `gate-blind` | `remote_red_pts` conjunct dropped | `red_publication_falls_back_to_the_primary_for_a_subscriber_without_red` must fail |
| `patch-reapplied` | retired patch text replayed onto `vendor/str0m` and left on disk | the gate must refuse the orphan patch, the suite must not change |

The neutrality of the move is measured, not asserted: `campaign4` (vendor-hook binary
`871d9efe7d0599f71a8205d77504b5129d2f47f2bacbb5ed85cc72c40c831c7d`) and `campaign5` (app-side binary
`773adaafc1ddc334b00f400da01e25ee4515a7ca7578b98e9ea1ee7ebf04481f`) share the same pinned Go
(`4bc2378e…`), the same SDK 2.23.0 / Chrome 156.0.8078.4 and byte-identical harness fixtures, and all 43
`wirePts`/`dominantBlockCounts`/`dominantShapes`/`firstChains`/`forwardMode`/`negotiatedRedPts` rows agree
between them; the informative match count is 94/127 on both.

