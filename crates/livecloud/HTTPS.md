# 原生 HTTPS / WSS

锁定的官方 LiveKit Server v1.13.10 使用 `port` / `bind_addresses` 启动 HTTP 服务（`references/livekit-stable/pkg/service/server.go`）。官方 TURN/WebTransport 的证书字段不用于 HTTP。Rust 服务把原生 HTTPS 放在独立 `livecloud.https` 扩展中，官方 YAML、参数和帮助文本保持原有含义。

```yaml
port: 7880
bind_addresses: [127.0.0.1]
livecloud:
  schema_version: 1
  https:
    port: 8443
    cert_file: /etc/livecloud/fullchain.pem
    key_file: /etc/livecloud/privkey.pem
    http_enabled: true
    max_connections: 1024
    handshake_timeout_ms: 10000
```

执行 `livecloud --config config.yaml`。HTTP API/WS 继续在 `7880`，同一组 API/WSS 路由在 `8443`；房间、鉴权和媒体运行时共享。设置 `http_enabled: false` 可只启用 HTTPS/WSS。两者不进行隐式跳转；未配置该扩展时仅保留官方 HTTP 行为。HTTPS 监听地址默认继承官方 `bind_addresses`，也可设置 `livecloud.https.bind_addresses`。

证书文件是 PEM 完整链，叶证书在前，随后为中间证书；私钥通过统一 `rtc-tls::server_config` 加载。所有证书、密钥和监听地址在服务任务启动前验证或预留，证书/密钥不匹配或绑定失败会直接启动失败，不会悄悄降级到明文。TLS 算法和密钥格式由统一 provider 决定，不在 HTTP 模块重复定义。

证书在进程启动时加载。替换文件后需重启以应用；当前未提供证书热加载。生产部署使用自己的证书材料和客户端正常信任链；测试目录的短期证书及浏览器 SPKI 例外只用于本机验证。

## 配置优先级与扩展参数

扩展采用 `CLI > 环境变量 > YAML > 默认值`。以下参数都要求显式值（包括 `http-enabled` 的 `true` / `false`），支持 `--name=value` 或 `--name value`；它们不伪装为官方参数，官方 `--help` 输出保持兼容。

| YAML 字段 | CLI | 环境变量 | 默认值 |
| --- | --- | --- | --- |
| `port` | `--livecloud-https-port` | `LIVECLOUD_HTTPS_PORT` | `8443` |
| `cert_file` | `--livecloud-https-cert-file` | `LIVECLOUD_HTTPS_CERT_FILE` | 必填 |
| `key_file` | `--livecloud-https-key-file` | `LIVECLOUD_HTTPS_KEY_FILE` | 必填 |
| `bind_addresses` | `--livecloud-https-bind-addresses` | `LIVECLOUD_HTTPS_BIND_ADDRESSES` | 继承官方监听地址；CLI/ENV 用逗号分隔 |
| `http_enabled` | `--livecloud-https-http-enabled` | `LIVECLOUD_HTTPS_HTTP_ENABLED` | `true` |
| `max_connections` | `--livecloud-https-max-connections` | `LIVECLOUD_HTTPS_MAX_CONNECTIONS` | `1024`，范围 `1..65536` |
| `handshake_timeout_ms` | `--livecloud-https-handshake-timeout-ms` | `LIVECLOUD_HTTPS_HANDSHAKE_TIMEOUT_MS` | `10000`，范围 `1..300000` |

连接上限在本进程全部 HTTPS 地址间共享，覆盖握手、HTTP keepalive 和升级后的 WebSocket 生命周期。握手并发处理，一个慢握手不会阻塞其他客户端；超时释放连接名额。停止时先按官方房间语义排空，再停止接入并给予 HTTP 五秒完成时间，随后强制终止仍存活的 TLS/升级连接。

## 本机验证

```sh
# livekit-rs-server/tests/interop/
npm run tls:fixture

# livekit-rs-server/
livecloud --dev --config tests/interop/https-config.yaml
```

该 fixture 同时使用 HTTP `8380`、HTTPS/WSS `8443`、RTC TCP/UDP `8381`/`8382`，限制为 loopback。官方 SDK 可直接使用 `wss://localhost:8443`，服务端 SDK 使用 `https://localhost:8443`；测试客户端需要信任所生成的本地 CA。

传输回归执行 `cargo test --release -p livecloud --locked`，覆盖可信 TLS 上的 HTTP 和 WebSocket、真实对端地址、慢握手并发、超时/连接名额和升级连接强制关闭。配置回归使用 `cargo test --release -p lk-config --locked`。官方 SDK 的真实房间与媒体互操作由独立浏览器门禁验证；传输 echo 测试本身不作为 SDK 兼容证明。
