# Original audio level duration and output semantics

Status: independent source candidate, not compiled or executed by its author.

Fixed Go v1.13.10 `pkg/sfu/buffer/buffer_base.go:982-985` only observes a
successfully parsed RFC6464 extension. `pkg/sfu/audio/audiolevel.go:146-162`
uses a zero duration for the first RTP observation. `GetLevel:164-176` returns
the actual smoothed level without clipping it to one.

The former Rust consumer invented a 20ms initial duration, advanced its
timestamp even for missing extensions, and clamped returned EMA to [0,1]. With
`audio.update_interval: 10` (ms), 20ms loud audio and default smoothing of two,
the fixed original level after the second RFC6464-bearing frame is 4/3. The
first sample is zero. With a 40ms window, the second sample stays inactive and
the third produces 2/3. The candidate follows those actual source semantics.

No packet timing is manufactured; the existing source RTP delta and active
speaker event chain remain the producer. The new regression drives a bound
native post-depacketizer frame through `MediaEngine::on_media_data` and verifies
actual `MediaEvent::AudioLevels`, missing extension handling, first/second/third
frames, duplicates/backwards timestamps, stale reset and public-command transport
release. It does not claim authenticated SRTP/SDK execution by itself.

Coordinator commands in a new candidate and owned target directory:

```sh
cargo test -p rtc-media --lib source_audio_rtp_first_sample_has_zero_age_and_short_windows_emit_unclipped_levels -- --nocapture
python3 tests/interop/audio-level-source/run-go-observer.py \
  --fixed-server-source /Users/zoujiaqing/projects/livekit-rs/references/livekit-stable \
  --go-binary /path/to/fixed/go1.26.3/bin/go \
  --evidence-dir /new/task-owned/evidence/audio-level-original-go-attempt01
```

The Go observer imports the original public audio package directly from the
fixed server module cwd. It calls `NewAudioLevel`, `SetConfig`,
`ObserveWithRTPTimestamp` and `GetLevel` and requires 17 actual observations.
No source algorithm is copied or modified. Full audio/workspace/SDK regression
must pass on the same final candidate before acceptance. This does not close
the separate transport-fallback consumer gaps or every audio config leaf.
