Local same-version fork of h3 0.0.8 (published crate checksum
10872b55cfb02a821b69dc7cf8dc6a71d6af25eb9a79662bec4a9d016056b3be) carrying one patch.

`webtransport-final-settings.patch` — final WebTransport HTTP/3 settings and protocol token:

- `src/proto/frame.rs`: adds `SettingId::WEBTRANSPORT_ENABLED = 0x2c7cf000`
  (`SETTINGS_WT_ENABLED`) and moves `WEBTRANSPORT_MAX_SESSIONS` from the draft-06
  codepoint `0x2b603743` to the final `SETTINGS_WT_MAX_SESSIONS = 0x14e9cd29`. The
  draft-06 `ENABLE_WEBTRANSPORT = 0x2B603742` is kept so that older peers stay usable.
  `SETTINGS_LEN` grows from 8 to 10 for the additional setting.
- `src/config.rs`: a peer that advertises only the final id is still accepted as a
  WebTransport endpoint, and a server that enables WebTransport also advertises the
  final id, which is what `webtransport-go` v0.12.0 requires from the server.
- `src/ext.rs`: accepts the final `:protocol` token `webtransport-h3` next to the draft
  `webtransport`, and adds `Protocol::is_webtransport()` covering both.
- `Cargo.toml`: silences the lints that upstream h3 0.0.8 triggers under the
  workspace's newer rustc, instead of editing the vendored sources for them.

Only the files recorded in `SOURCE.json` differ from the published release. The original
`Cargo.toml.orig`, `LICENSE` and `README.md` are retained unchanged.

- `webtransport-buffered-uni-varint.patch`: read a complete buffered type/ID
  varint without waiting for additional QUIC data or FIN, and reset its size
  before parsing the next varint. The fixed Go raw peer tests a live stream
  that keeps the send half open and would otherwise wait indefinitely.

- `webtransport-session-id-check.patch`: expose the CONNECT stream class check
  for the server's WebTransport routing. Other h3 users can continue to use
  `SessionId` values without a parsing policy change.
