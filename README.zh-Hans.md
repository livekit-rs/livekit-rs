# livekit-rs

[English](README.md) · [官方网站](https://livekit-rs.com/zh-hans/) · [使用指南](https://livekit-rs.com/zh-hans/guides/) · [兼容性](https://livekit-rs.com/zh-hans/compatibility/)

**livekit-rs 是用 Rust 实现的单节点 WebRTC 服务器，兼容目标为 LiveKit Server v1.13.10。** 它转发实时音频、视频和数据。我们的目标是与官方 Go 服务端完全兼容，并让同样硬件配置的节点提供更稳定的视频流服务。

## 快速开始

在仓库根目录使用锁定的 Rust 工具链和依赖构建。

```sh
cargo build --locked --release -p livecloud --bin livekit-server
./target/release/livekit-server --dev --bind 127.0.0.1
```

在另一个终端检查服务并生成开发环境入房令牌。

```sh
curl --fail http://127.0.0.1:7880/
./target/release/livekit-server --dev create-join-token --room demo --identity alice
```

用返回的令牌将 LiveKit 客户端连接到 `ws://127.0.0.1:7880`。开发凭据仅供本机测试。部署请从[单机配置模板](examples/standalone.yaml)开始，并参考[部署指南](https://livekit-rs.com/zh-hans/guides/#单机部署)。

## 功能

- **WebRTC SFU** 转发音频、视频和可靠数据。已测试 Simulcast 和指定的可伸缩视频配置。
- **LiveKit 接口**覆盖已验证配置中的客户端信令、JWT 鉴权、Twirp Server API 和 Webhook。
- **单节点部署**支持 UDP 媒体、ICE/TCP、内置 TURN/UDP，以及经反向代理或原生 TLS 提供的 HTTPS/WSS。
- **Rust 运行时**采用 Tokio 和 Axum。单机运行无需 Go 运行时、Redis 或数据库。

## 与官方 Go 版本的兼容性

| 范围 | 证据与现状 |
| --- | --- |
| CLI 和配置 | 已通过固定 v1.13.10 CLI 差分测试，结论覆盖仓库测试实际执行的用例。 |
| SDK 和媒体 | 已验证指定的信令、媒体、数据和恢复场景。迁移前请核对所需配置。 |
| 网络恢复 | 已公开的隔离 IPv4 NAT 对照中，Go 通过 4/4，Rust 通过 3/4。Rust 首轮单 PC 入房超时，后续两次复测通过。 |
| 完整兼容 | **100% 兼容是目标。** 完整单机验收仍在进行。 |
| 视频稳定性 | 目标是同等硬件上的视频流更稳定。现有恢复实验尚未测得整体稳定性优势。 |

查看[对照方法和结果](https://livekit-rs.com/zh-hans/compatibility/)及[机器可读证据](https://livekit-rs.com/evidence/nat-recovery-2026-11-11.json)。

## 本地验证

```sh
cargo test --locked --workspace
python3 scripts/check-protocol-schema.py
LIVEKIT_OFFICIAL_BIN=/absolute/path/to/official-v1.13.10/livekit-server \
  cargo test --locked -p lk-config --test official_cli -- --ignored
```

许可条款见 [LICENSE](LICENSE)，第三方声明见 [NOTICE](NOTICE)。
