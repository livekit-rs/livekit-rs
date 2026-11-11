Local same-version fork of quinn-proto 0.11.19 (published crate checksum
0e750cca55fe4f0439a15d0bb529da9651e79993e8e72c61a899a36d462befbe) carrying one patch.

`stream-reset-partial-delivery.patch` — `RESET_STREAM_AT` partial delivery, per
draft-ietf-quic-reliable-stream-reset:

- `src/frame.rs`: new frame type `RESET_STREAM_AT = 0x24`, which carries a
  `reliable_size` next to the plain `RESET_STREAM` fields; a reliable size above the
  final offset is rejected as a malformed frame, like quic-go does.
- `src/connection/assembler.rs`: `contiguous_end()` and `truncate()`, so that a
  partially reset stream keeps exactly its reliable prefix and the contiguity of that
  prefix can be observed.
- `src/connection/streams/recv.rs`: the reliable prefix stays readable, a repeated
  reset may only shrink it (the first reset keeps the error code), and the prefix
  counts as complete only once the *contiguous* buffered end reaches the reliable
  size. Deciding that on the highest received offset instead would drop the very
  frames that fill a hole in an out-of-order prefix and strand the application.
- `src/connection/streams/state.rs`: connection-level flow control accounting for the
  retained prefix, and the reset interleaving tests (out-of-order prefix, prefix that
  arrives after the reset, partially read prefix, repeated resets, prefix beyond the
  reliable size, connection- and stream-level flow control).

The patch also carries the rustfmt reformatting that the workspace's newer rustfmt
applies to the touched files. Only the files recorded in `SOURCE.json` differ from the
published release; the original `Cargo.toml.orig`, `LICENSE-APACHE` and `LICENSE-MIT`
are retained unchanged.
