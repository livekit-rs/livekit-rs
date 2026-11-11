# livekit-rs

[简体中文](README.zh-Hans.md) · [Website](https://livekit-rs.com/) · [User guides](https://livekit-rs.com/guides/) · [Compatibility](https://livekit-rs.com/compatibility/)

**livekit-rs is a single-node WebRTC server written in Rust, targeting compatibility with LiveKit Server v1.13.10.** It routes real-time audio, video, and data. The goal is full compatibility with the official Go server and more stable video delivery on the same hardware.

## Quick start

Build from the repository root with the pinned Rust toolchain and lockfile.

```sh
cargo build --locked --release -p livecloud --bin livekit-server
./target/release/livekit-server --dev --bind 127.0.0.1
```

In a separate terminal, check readiness and generate a development join token.

```sh
curl --fail http://127.0.0.1:7880/
./target/release/livekit-server --dev create-join-token --room demo --identity alice
```

Connect a LiveKit client to `ws://127.0.0.1:7880` with the returned token. Use development credentials only for local testing. For deployment, start with the [standalone configuration](examples/standalone.yaml) and follow the [deployment guide](https://livekit-rs.com/guides/#deploy-a-single-node).

## Features

- **WebRTC SFU** for audio, video, and reliable data, with tested simulcast and selected scalable-video profiles.
- **LiveKit interfaces** including client signaling, JWT authorization, Twirp Server APIs, and webhooks in the tested configurations.
- **Single-node deployment** with direct UDP media, ICE/TCP, built-in TURN/UDP, and HTTPS/WSS through a reverse proxy or native TLS.
- **Rust runtime** built on Tokio and Axum. A standalone deployment does not require a Go runtime, Redis, or a database.

## Compatibility with the official Go server

| Area | Evidence or current scope |
| --- | --- |
| CLI and configuration | The pinned v1.13.10 CLI differential passed. It covers the cases exercised by the checked-in test. |
| SDK and media | Verified SDK campaigns cover scoped signaling, media, data, and recovery scenarios. Check the exact profile before migrating. |
| Network recovery | In the published isolated IPv4 NAT campaign, Go passed 4 of 4 attempts and Rust passed 3 of 4. The first Rust single-PC attempt timed out before joining. Two Rust reruns passed. |
| Full parity | **100% compatibility is the goal.** Full Standalone acceptance remains open. |
| Video stability | The goal is more stable streams on equal hardware. The published recovery campaign does not measure an overall stability advantage. |

Read the [comparison methodology and results](https://livekit-rs.com/compatibility/) and [machine-readable evidence](https://livekit-rs.com/evidence/nat-recovery-2026-11-11.json).

## Verify locally

```sh
cargo test --locked --workspace
python3 scripts/check-protocol-schema.py
LIVEKIT_OFFICIAL_BIN=/absolute/path/to/official-v1.13.10/livekit-server \
  cargo test --locked -p lk-config --test official_cli -- --ignored
```

Licensed under the terms in [LICENSE](LICENSE). See [NOTICE](NOTICE) for third-party notices.
