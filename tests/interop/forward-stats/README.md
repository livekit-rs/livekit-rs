# Forward stats source-bound acceptance

Status: independent candidate based on immutable21; no Cargo/Go/server run by implementers. Parent owns compilation, real execution, evidence, Git and cleanup. Source assembly reconstruction is not a runtime pass.

## Actual consumer chain

Official CLI/YAML/ENV duration fields normalize into one Arc<rtc_core::forward_stats::ForwardStats> shared by every room MediaEngine and RoomManager. Any zero of the three disables the source pipeline. Negative ticker interval rejects startup; negative nonzero report window creates the source minimum one summary slot. Defaults remain disabled.

Native metadata is opt-in for this config. Each authenticated original RTP packet carries its actual received Instant and source ordering classification through RtpPacket/RtpMeta/frame cohort. Synthetic RED recovery has no original sample. A dropped partial frame or retired source drops its owned cohort; no orphan side queue is created. Engine submits each eligible original arrival once only after at least one real Writer::write returned Ok, not once per subscriber. Buffered/out-of-order packets are filtered. Existing native has no Go prebind raw RTP replay, so normal depay/frame waiting is not mislabelled IsBuffered.

The success boundary matches fixed Go DownTrack.WriteRTP's pacer.Enqueue return, not physical network transmit completion. Rust's real frame assembly cost remains in its observed packet transit; performance differences are allowed, and this does not claim a packet-by-packet physical forwarder.

Core collector uses source 216-bucket observed-range interpolated p90, source window ceil/minimum, bounded16x8192 sample shards, cumulative10-boundary Prometheus ns histogram, overflow and spread logging, uint32 reported NodeStats latency. Runtime owns an independent periodic worker even when no rooms/traffic exist and stops+joins on close_all/Drop. Per-room p90 values are never summed. Prometheus scraping exports cumulative snapshots without replay/double-count. Node sampler copies actual shared p90 into official protobuf forward_latency.

## Required coordinator execution

```sh
cargo test -p rtc-core --lib forward_stats -- --nocapture
cargo test -p rtc-runtime --lib forward_stats -- --nocapture
cargo test -p lk-config --lib forward_stats -- --nocapture
cargo test -p livecloud --lib forward_ -- --nocapture
cargo test -p rtc-media --lib cohort -- --nocapture
cargo test -p rtc-media --lib authenticated_source_restart_config_discards_only_old_partial_window_and_keeps_transport -- --nocapture
cargo test --manifest-path vendor/str0m/Cargo.toml --lib authenticated_fragmented_frame_preserves_each_arrival_and_original_out_of_order_flag -- --nocapture
```

Require nonzero test counts, exact exit0 and unchanged assertions. Actual tests include: independent-arrival histogram count/sum; p90 interpolation/ceil/empty expiry/uint32 narrowing; actual shard overflow; owned independent worker lifecycle; real official config activation; actual metrics handler and NodeStatsProvider protobuf output; authenticated fragmented source cohort preserves three distinct arrivals plus OOO; real subscribed SRTP path contributes samples while actual publisher without subscriber contributes zero. Full fresh workspace/native and official-SDK matrix must still pass for final source identity.

Original Go observer invokes original forwardSummary, ForwardStats.Update/flush/GetStats, bounded sample buffer and report→real Prometheus gauge, not copied algorithms. It requires12 original observations and a nonzero test count:

```sh
/usr/bin/python3 tests/interop/forward-stats/run-go-observer.py --go-binary /ABS/GO1.26.3/bin/go --fixed-server-source /Users/zoujiaqing/projects/livekit-rs/references/livekit-stable --evidence-dir /ABS/NEW/OWNED/EVIDENCE
```

Runner privately copies original server, verifies original hashes before/after, inserts only a private test observer and invokes original package with fixed module graph offline. It grants directory write only in its private stage; references are never edited. No source-only receipt upgrades a row to accepted.

## Limits retained

This is source-compatible forwarding admission measurement, not timestamp/packet-forwarding architecture equivalence or performance certification. No inactive OSS metric.* pipeline is activated. High-latency callback uses actual dequeued Instant, actual successful write count and spatial layer, and records source fields latency/queuingLatency/writeCount/isOutOfOrder/layer per eligible packet; fields are signed nanoseconds. Raw duration field encoding against Go logger remains a separate log-format acceptance surface. uint32 nanoseconds wraps exactly as source output. Main SPEC parents and815 leaf remainder stay open until source-bound acceptance.
