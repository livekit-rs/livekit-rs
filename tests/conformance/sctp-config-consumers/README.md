# SCTP config consumer conformance

Proves the three `rtc.sctp_*` leaves change **actual encrypted SCTP behaviour**
after the public loader, without adding any new core/native setting getter:

```
lk_config::parse_with_probe(YAML)  ->  rtc_core::config::SctpConfig
  ->  rtc_media::MediaEngine        (crates/rtc-media/src/engine.rs positive_sctp_u32)
  ->  str0m SctpTransportOptions    (vendor/str0m/src/sctp_options.rs)
  ->  sctp_proto::TransportConfig   (vendor/sctp-proto/src/config.rs)
  ->  Association behaviour sites   (initial cwnd floor, CA increment, fast burst budget)
```

Every leg drives a real `MediaEngine` transport against a real str0m client over
ICE/DTLS/SCTP on 1 ms virtual time, so a leg either completes or fails loudly;
nothing here asserts a projected config value.

| Test | Knob | Actual observable | Discriminating mechanism |
| --- | --- | --- | --- |
| `source_local_receive_capability_is_actually_advertised_65535` | — | real local SDP answer | `a=max-message-size:65535` |
| `signed_positive_min_cwnd_changes_actual_encrypted_initial_flight_after_public_loader` | `sctp_min_cwnd` | encrypted bytes emitted before any feedback | `(4*mtu).min((2*mtu).max(4380)).max(min_cwnd)` |
| `scheduled_datagram_loss_is_actually_discarded_and_recovered_by_the_default_leg` | native defaults | drop counter, datagram counter, reassembled bytes | loss-control leg: the injection is real and the default leg still recovers |
| `positive_fast_rtx_wnd_bundles_actual_sctp_retransmission_bursts_under_real_loss` | `sctp_fast_rtx_wnd` | virtual ticks to deliver every byte under an identical datagram-drop schedule | burst budget `mtu.max(fast_rtx_wnd)`; with the default the loop stops after one ~1 KB chunk, a positive window retransmits all chunks already marked `miss_indicator >= 3` |
| `positive_cwnd_ca_step_actually_grows_the_congestion_window_in_congestion_avoidance` | `sctp_cwnd_ca_step` | virtual ticks to deliver 4 MiB after one early loss | one loss halves `ssthresh`, the run then stays in congestion avoidance where growth is `mtu.max(cwnd_ca_step)` per cwnd bytes acknowledged |

The two timing legs of a comparison differ in exactly one field, so a mutant
that ignores that field makes both legs identical and the stated margin
(`+4` ticks / `2x`) fails instead of passing vacuously.

## Rerun

```
cargo test --release --manifest-path tests/conformance/sctp-config-consumers/Cargo.toml
```

## Falsifiability control

Temporarily replace the handoff in `crates/rtc-media/src/engine.rs` so the field
under test is pinned to `0` (`positive_sctp_u32(rtc_cfg.sctp.fast_rtx_wnd)` ->
`0`, likewise `cwnd_ca_step`), rebuild, and require the corresponding test to
FAIL. Restore the file before using any result as evidence.

## Go side

`tests/oracles/sctp-source-knobs/` is the fixed-Pion source observer for the same
chain; `actual-controls-2026-10-27/knobs.tsv` (10 rows x 14 columns,
sha256 `02efe6af581c8fd8197f8cd934bdc108aa2ba721470b65c1a097fe04ce2821a2`)
is the committed actual output consumed by the ignored vendored differential
`actual_go_knob_rows_match_constructor_floor_ca_and_fast_packets`:

```
GO_SCTP_KNOB_TSV=$PWD/tests/oracles/sctp-source-knobs/actual-controls-2026-10-27/knobs.tsv \
  cargo test -p sctp-proto --lib actual_go_knob_rows_match_constructor_floor_ca_and_fast_packets -- --exact --ignored
```

That rerun belongs to the same-candidate acceptance pass; until it is run on the
bound candidate the official differential stays `NOT_RUN` and this suite alone
does not certify Go equality. See SPEC-0077 C69.
