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
