# 官方 SDK 浏览器互操作

`run.mjs` 使用锁定的 LiveKit JS/Server SDK、真实 Chrome 与本地合成媒体。安装和执行：

```sh
npm ci
npm test
```

`LIVEKIT_URL` 指定被测信令入口，`SERVER_LABEL` 区分证据文件，`SINGLE_PC=false` 切换双 PC，`VIDEO=true` 增加 VP8，`FORCE_RELAY=true` 要求浏览器采用 relay ICE policy。默认测试持续时间为 30 秒。输出写入本目录 `output/`；具体通过条件以测试脚本断言为准。

## 本机内置 TURN/UDP

先在服务端 workspace 根目录启动单独的开发实例：

```sh
target/debug/livecloud --dev --config tests/interop/turn-config.yaml
```

再在本目录运行：

```sh
LIVEKIT_URL=ws://127.0.0.1:8080 SERVER_LABEL=rust-turn FORCE_RELAY=true npm test
```

配置中的 HTTP、RTC TCP/UDP、TURN 分别使用 `8080`、`8081`/`8082`、`3488`，中继分配使用 `36000–36100`。所有服务监听或候选限制为 loopback；只放行 `127.0.0.1/32` 作为 TURN 对端。凭据为 `--dev` 的公开测试值，不包含生产密钥。

Join 返回的内置 ICE 服务应为 `turn:127.0.0.1:3488?transport=udp`，用户名/密码由服务端按当前参与者签发。TURN 收到 Binding 请求或监听成功不能证明媒体互通：通过结果需要 Chrome 在 relay policy 下实际连接并收到可解码媒体与可靠数据。此配置不覆盖 TURN/TLS、外网 NAT、防火墙或公网地址发现。

### 本轮实测证据（2026-10-23）

Chrome `155.0.8059.85` 使用锁定 JS SDK，在本机 Rust 服务执行了以下两轮测试：

| 模式 | 持续时间 | 实际选中路径 | DTLS | 双向音频与可靠数据 |
| --- | --- | --- | --- | --- |
| 单 PC + FORCE_RELAY | 30 秒 | 每端 1 条 UDP relay transport | 全部 connected | 每端 ≥310 个可听采样、100 条有序消息 |
| 双 PC + FORCE_RELAY | 30 秒 | 每端 2 条 UDP relay transport | 全部 connected | 每端 310 个可听采样、100 条有序消息 |

对应 `output/rust-turn-single-relay.json` 和 `output/rust-turn-dual-relay.json` 的成功报告包含选中 candidate 类型、双向传输字节、DTLS 状态及音频接收包数。每个 transport 均断言本地 candidate 为 `relay`；音频用实际解码后的 WebAudio RMS 验证。它们是本机 UDP TURN 互通结果，不代表公网可靠性、TURN/TLS 或容量验收。


## 原生 HTTPS/WSS、TLS、SVC 与拥塞恢复的复现

以下服务启动命令均在 `livekit-rs-server/` 执行；浏览器命令在 `tests/interop/` 执行。先构建服务并安装锁定的测试依赖：

```sh
# livekit-rs-server/
cargo build --locked --bins

# tests/interop/
npm ci
npx playwright install chromium
export BROWSER_CHANNEL=chromium
```

默认 `BROWSER_CHANNEL=chrome` 使用本机 Chrome；上述环境变量改用 Playwright 安装的 Chromium。各配置复用部分端口，请先停止上一组服务。所有配置及 `devkey / secret` 仅用于 loopback 开发测试。

### 内置 TURN/TLS 与 HTTPS Webhook

生成七天有效的本地 ECDSA 测试证书，需要测试机提供 OpenSSL。生成器使用的 OpenSSL 不属于服务器生产依赖，私钥写入忽略提交的 `output/tls/`：

```sh
# tests/interop/
npm run tls:fixture

# livekit-rs-server/，另一终端保持运行
./target/debug/livecloud --dev --config tests/interop/turn-tls-config.yaml

# tests/interop/
TLS_SPKI="$(cat output/tls/spki-sha256.txt)" \
LIVEKIT_URL=ws://127.0.0.1:8080 SERVER_LABEL=rust-turn-tls \
FORCE_RELAY=true TURN_TLS=true DURATION_MS=10000 npm test
```

此配置由 Rust 服务器直接终结 `5349` 的 TURN/TLS；浏览器必须实际选择 `relayProtocol=tls` 并收到音频和可靠数据。`TLS_SPKI` 仅向测试浏览器指定该本地证书的 SPKI 例外，不能据此宣称完成公网 CA、域名或系统信任库验收。信令入口仍为本机 HTTP/WebSocket；这里没有验证入站 HTTPS/WSS。

HTTPS Webhook 使用独立服务实例和官方 `WebhookReceiver`，不需要手动启动服务器：

```sh
# tests/interop/，先生成上述证书
node webhook-tls.mjs
```

脚本默认启动 `8280` 信令服务，自动向子进程设置 `SSL_CERT_FILE` 信任本地测试根，验证真实 HTTPS、证书校验、官方 SDK 签名/内容哈希、首次 `503` 后同事件重试及房间开始/结束通知。可用 `LIVEKIT_BIN` 指定二进制，`WEBHOOK_TEST_PORT` 更换端口。

### 原生 HTTPS/WSS：EC、RSA PKCS#1 与信任负例

先生成两套独立的七天测试材料；RSA 配置实际加载传统 PKCS#1 私钥，而不是生成后仍使用 PKCS#8：

```sh
# tests/interop/
npm run tls:fixture
TLS_KEY_TYPE=rsa TLS_FIXTURE_NAME=tls-rsa npm run tls:fixture

# livekit-rs-server/，各自保持运行；先停止上面的 TURN/TLS 实例，避免 5349 冲突
./target/debug/livecloud --dev --config tests/interop/native-https-config.yaml
# 另一终端，可与 EC 实例并行监听
./target/debug/livecloud --dev --config tests/interop/native-https-rsa-config.yaml
```

EC 实例使用 HTTPS/WSS `8443`、RTC `8441/8442`、TURN/TLS `5349`；RSA 实例对应 `9443`、`9441/9442`、`5350`。两套配置均设置 `livecloud.https.http_enabled: false`，分别保证明文 `8480`、`9480` 未监听。原生 HTTPS 是 `livecloud.https` 扩展，未给官方 LiveKit YAML 增加同名官方字段。默认为保留原 HTTP；这两份配置显式关闭它，不做隐式重定向。

先用 Node 的常规证书验证检查 HTTPS；探针不使用 SPKI 例外：

```sh
# tests/interop/
LIVEKIT_URL=wss://127.0.0.1:8443 PLAIN_HTTP_PORT=8480 PROBE_LABEL=ec \
node probe-native-https.mjs
LIVEKIT_URL=wss://127.0.0.1:9443 TLS_CA_FILE=output/tls-rsa/root.pem \
PLAIN_HTTP_PORT=9480 PROBE_LABEL=rsa node probe-native-https.mjs
```

每套都要求有效 CA 成功且证书获信任、错误主机名拒绝、不受信任 CA 拒绝、关闭的明文端口连接被拒绝。报告写入 `output/ec-native-https-probe.json` 和 `output/rsa-native-https-probe.json`。TCP readiness 仅用于等待监听，不能替代这些实际 TLS 断言。

然后使用未改动的官方 SDK 运行 HTTPS RoomService API、WSS 信令和 TURN/TLS 音视频。EC 场景同时验证 AV1 空间/时间层：

```sh
# tests/interop/
NODE_EXTRA_CA_CERTS="$PWD/output/tls/root.pem" \
TLS_SPKI="$(cat output/tls/spki-sha256.txt)" \
LIVEKIT_URL=wss://127.0.0.1:8443 SERVER_LABEL=rust-native-https-av1 \
FORCE_RELAY=true TURN_TLS=true VIDEO=true VIDEO_CODEC=av1 \
SCALABILITY_MODE=L3T3_KEY SVC_SWITCH=true SVC_TEMPORAL=true \
RUNS=1 DURATION_MS=10000 npm test

NODE_EXTRA_CA_CERTS="$PWD/output/tls-rsa/root.pem" \
TLS_SPKI="$(cat output/tls-rsa/spki-sha256.txt)" \
LIVEKIT_URL=wss://127.0.0.1:9443 SERVER_LABEL=rust-native-https-rsa \
FORCE_RELAY=true TURN_TLS=true RUNS=1 DURATION_MS=10000 npm test
```

`NODE_EXTRA_CA_CERTS` 为独立 Node HTTPS API 客户端追加测试根；`TLS_SPKI` 只向测试浏览器放行该生成叶证书，用于 WSS 和 TURN/TLS。没有关闭全局证书检查，也没有修改 LiveKit SDK。正常生产证书使用自己的完整 PEM 证书链与匹配私钥，不需要测试 SPKI 例外；当前证书续期后需要重启加载。此场景证明本机指定证书材料与端点组合，不据此声称已覆盖所有公网 CA、所有浏览器或所有操作系统信任策略。

Rust 出站 HTTPS 客户端从系统取根，仍由 WebPKI 验链。Linux 的 `SSL_CERT_FILE`、`SSL_CERT_DIR` 分别覆盖默认文件/目录来源；部分文件无效但其他来源有有效根时继续，全部无可用根时失败。macOS/Windows 使用系统枚举出的根，并保留显式 PEM 文件/目录追加私有 CA 的跨平台扩展；未使用 OS 在线链构建或 AIA 下载。

### 独立 OpenSSL TLS 与私钥格式互操作

TLS provider 支持 TLS 1.2/1.3、AES-128/256-GCM 与 ChaCha20-Poly1305；测试矩阵覆盖 X25519/P-256/P-384 ECDHE、RSA/ECDSA/Ed25519 端点及 RSA PKCS#1/PKCS#8 私钥。实际支持范围和断言以 `crates/rtc-tls/tests/expanded.rs` 为准。OpenSSL 仅是独立测试进程，不是 Rust 服务器运行时依赖：

```sh
# livekit-rs-server/，需要 OpenSSL 3
RTC_TLS_OPENSSL=openssl cargo test -p rtc-tls --locked --test handshake \
  independent_openssl_client_negotiates_tls13_and_verifies_certificate -- --ignored
RTC_TLS_OPENSSL=openssl cargo test -p rtc-tls --locked --test expanded -- --ignored
```

第二条会执行 opt-in 扩展测试：独立 OpenSSL 客户端验证 Rust 服务端的多算法组合、Rust HTTPS 客户端验证 OpenSSL RSA 服务端的 TLS 1.2/1.3，以及非字节对齐 RSA 签名的独立验证与篡改拒绝。默认 workspace 测试另外覆盖证书链、密钥匹配、错误签名和无效 key share。这里没有把“可握手”推广为完整官方所有 TLS 配置等价。

### VP9 空间层与时间层

先启动专用配置；该配置关闭拥塞分配，以便单独验证编解码和订阅层切换：

```sh
# livekit-rs-server/
./target/debug/livecloud --dev --config tests/interop/svc-config.yaml

# tests/interop/
LIVEKIT_URL=ws://127.0.0.1:7980 SERVER_LABEL=rust-vp9-spatial-temporal \
VIDEO=true VIDEO_CODEC=vp9 SCALABILITY_MODE=L3T3_KEY \
SVC_SWITCH=true SVC_TEMPORAL=true DURATION_MS=10000 npm test
```

`SVC_SWITCH=true` 请求低/高空间层并检查真实解码尺寸；`SVC_TEMPORAL=true` 通过官方订阅设置请求 `5`/`15` FPS，分别观测六秒，检查实际解码帧数随请求改变并恢复。它不是仅检查 SDP 中包含 VP9。将命令中的 `VIDEO_CODEC=vp9` 改为 `VIDEO_CODEC=av1`，并更换 `SERVER_LABEL`，可复现 AV1 `L3T3_KEY` 的同组空间/时间订阅门禁；上面的原生 EC WSS 命令已组合 AV1 与 TURN/TLS。AV1 场景需要浏览器实际支持该编码，检查真实解码尺寸、帧数与 dependency descriptor 驱动的层选择，不能用仅出现 SDP codec 名代替。当前覆盖 Chrome/Chromium 的 VP9/AV1 `L3T3_KEY`，没有据此覆盖全部编码模式、其他浏览器或客户端。结合拥塞的层选择使用下一组独立门禁，不从本组关闭拥塞的结果推导。

### 真实 TURN 限速、暂停与恢复

```sh
# livekit-rs-server/
./target/debug/livecloud --dev --config tests/interop/congestion-config.yaml

# tests/interop/，先验证代理本身
node --test network-proxy.test.mjs
LIVEKIT_URL=ws://127.0.0.1:8180 SERVER_LABEL=rust-congestion \
VIDEO=true COMPLEX_VIDEO=true FORCE_RELAY=true NETWORK_PROFILE=congest-recover \
METRICS_URL=http://127.0.0.1:9191/metrics RUNS=5 DURATION_MS=5000 npm test
```

`NETWORK_PROFILE=congest-recover` 要求 `FORCE_RELAY=true`。测试代理只替换浏览器原生 `RTCPeerConnection` 配置中的本机 TURN/UDP 端口，保留官方 SDK 代码、认证凭据、信令及 SDP；这是测试路由注入，不是客户端协议扩展。断言选中 relay 的 URL 指向代理，避免候选建立后实际绕过限速。

代理以每个浏览器 TURN UDP 客户端为单位，对服务端到浏览器的 IPv4 数据报计费（TURN UDP payload 加 28 字节 IP/UDP 头），执行 `4 Mbps → 120 kbps（18 秒）→ 4 Mbps`。反向流量不限速；它不是全双向公网网络模拟。队列有长度和等待时间上限，超限真实丢弃；每轮先等待连续三秒两端活跃解码，复杂视频还要求最近八帧宽度均至少 800（45 秒内未就绪则失败），再检查受限阶段新增丢包、SFU 暂停或降层，随后要求恢复到基线最近八帧的最低宽度并继续解码，额外观察五秒持续音视频推进。`TURN_UPSTREAM_PORT` 可更换代理连接的本机 TURN 端口，默认为 `3488`。

CI 对同一完整场景连续执行 `RUNS=5`，五轮均需独立通过；不会通过重试覆盖失败，也不更改每轮的速率、队列、解码阈值或时间上限。每秒 `networkTimeline` 保存阶段、monotonic 时间、Chrome 原生 RTP/RTCP 统计、BWE 指标与代理状态，失败时仍保留已采集轨迹。`flushTiming` 记录代理定时器延迟；每客户端统计区分过期、溢出及超长包，记录队列年龄、实际 IPv4 转发字节和持续积压期间的 `saturatedDiscardedRefillBytes`。总 `discardedRefillBytes` 包含空闲桶正常弃额，不能单独作为拥塞或容量损失证据；应结合持续积压、实际转发速率和浏览器反馈判断。诊断不会改变限速行为。

设置 `METRICS_URL` 后还检查有效 BWE 容量先降后升，并保存容量、建模媒体预算、暂停数和探测结果。指标按活跃房间聚合；音频/视频分配预算采用 payload 口径，有效容量为预留传输开销前的 bps，二者不是相同口径的原始流量计数。

2026-10-23 初期八秒探测窗口版本的本机 Chrome 历史实测通过：受限阶段新增丢包 `86` 次、两路视频暂停，有效总容量从 `639784` 降至 `150338` bps，恢复至 `1502331` bps并恢复解码。两个客户端受限前最后解码尺寸均为 `318×180`，其中一路已经在 `635×360` 与 `318×180` 间变化；恢复报告末帧为 `635×360` 和 `318×180`。该结果验证相对各自基线的恢复和后续五秒持续解码，未证明启动阶段已稳定到最高层，也未证明恢复至发布端最高 `960×544`。长时间满层稳定性、更多 RTT/丢包分布及公网容量仍需单独测量。

后续高基线 release 测试暴露了两个问题：启动阶段已经暂停的订阅不能作为“新增暂停”基线；固定按当前容量增加 200 kbps 的探测也不足以快速跨越高层所需成本。当前候选已改为下一可升级层的实际预算目标（包含音频与传输开销），并将探测目标保持 18 秒，覆盖原生 ALR 五秒和非 ALR 停滞探测十五秒的调度。请求探测速率仍不计作可分配容量，只有真实反馈才提升预算；高基线 release 的后续失败进一步定位到：音频继续发送时，按全部媒体字节判断“视频停滞”会阻止原生重新发现。当前候选按需求缺口、预算停滞与真实无拥塞反馈启动低起点原生指数探测，起点不超过已验证容量和 300 kbps；请求或重置本身均不作容量增加的证据。最终还修复了最高层精确需求裁掉探测余量的问题：padding 探测可超出精确需求 20% 或至少 200 kbps，但显式带宽 override 仍是硬上限；升级成功不能在尚未达到下一层完整 wire 成本时提前确认。

最终 release 版本在上述严格高基线门禁通过：两端基线均为 `960×544`，有效总容量 `4958962 → 65621 → 4532075` bps，两路暂停后恢复，受限阶段新增丢包 `1409` 次（累计 `29 → 1438`）。两端在恢复后的 30 秒内解码到原 `960×544` 的最近八帧，再连续观察五秒确认音视频继续前进。该额外观察结束时末帧分别为 `635×360`、`960×544`，说明动态选层仍继续；此通过不等于随后全程稳定保持最高分辨率。旧版本的低基线通过和高基线失败均保留于证据摘要，未用重跑覆盖。

### 独立 allocation-horizon 上界门禁（可选）

在上面完全相同的五轮拥塞命令中添加 `BANDWIDTH_ACCURACY=true`，原媒体、可靠数据、限速、队列、启动/恢复超时全部保留。缺少 `METRICS_URL` 时拒绝启动该门禁。额外门禁读取原有 `livekit_estimated_capacity_bps`，不裁剪生产值，也不换成新指标；路径数来自实际选中的 DTLS transports，并要求代理客户端数一致。

当前实现明确是**一秒 allocation horizon 的安全不变量**：假设当前配置速率保持一秒，采样的预算不能授予超过 `C*T/8+B` 的服务量。两客户端、每端 4 Mbps/4500 字节桶时上界为 `8,072,000` bps，120 kbps 阶段为 `312,000` bps；其他速率按实际 rate trace 和路径数计算，不是把生产固定硬限在 4 Mbps。它不是“实际观测到完整一秒持续吞吐”或 TWCC 容量下界证明。原恢复门禁也不因此变成完整带宽精度验收。

每次速率变化后先保留五秒历史过渡，再要求一秒同速资格历史；五秒来自现有 Robust ACK 最长窗口与探测结束历史上限，一秒来自现有最小窗口，均在候选运行前固定。变速调用前/完成时间和指标请求起止实际记录；指标读取跨变速、早期过渡及 relay identity 尚未验证的采样保留为 `WARMUP`。这是稳定阶段的不变量，不验收瞬时反应时间。每个初始/受限/恢复 epoch 都必须有有效稳定采样；原指标、时间或精确路径身份缺失为 `INSUFFICIENT`，不能通过。预算超上界为 `FAIL`。

证据另存 `bandwidthAccuracy`，逐 epoch 报告预算 min/max 和最大采样盲区。离散采样不能证明盲区中从未出现过冲；原始短 probe rate、持续服务速率和 committed budget 仍分别取证。新增元数据无 payload、SDP 或凭据；独立 evaluator 对超出有界输入上限的记录返回 `INSUFFICIENT`，不静默截断成通过。

独立计量回归可运行：

```sh
node --test bandwidth-accounting.test.mjs bandwidth-timeline-gate.test.mjs network-proxy.test.mjs
```

回归不仅测试自定义 oracle：还导入实际 `TokenBucket`，执行多包、refill 和变速 trace，由独立 BigInt 有理数账本审核真实接受结果。旧 18.331 Mbps 长期预算、低速 516 kbps 及额外超发字节的反例必须失败；短毫秒 probe 可合法消耗 burst，但不能据此称为持续容量。

### 证据与 CI 状态

`SERVER_LABEL` 区分 `output/` 下 JSON 报告，建议各场景使用不同标签避免覆盖。报告记录官方 SDK、浏览器版本、选中 ICE/DTLS 路径、实际媒体解码、信令和场景指标。普通测试还支持 `RUNS`（轮数）、`DURATION_MS`（基础观测时长）、`SINGLE_PC=false`（双 PC）和 `EXTENDED=true`（扩展房间操作）。

CI 已配置独立 OpenSSL 扩展 TLS、EC/RSA PKCS#1 原生 HTTPS 信任探针、原版 SDK WSS/HTTPS API 与 TURN/TLS、VP9/AV1 空间/时间层及 TURN 拥塞恢复门禁。原生 EC 与旧 TURN/TLS 共用 `5349`，工作流在切换前仅停止自己记录 PID 的旧实例。退出时停止并等待所有本任务实例，必要时终止；上传证据时排除私钥，再删除生成证书及原始输出，最后执行 `cargo clean`。远程 GitHub Actions 尚未执行，本机通过不能表述为远程 CI 已通过。

原始测试输出不纳入版本控制；精选、带来源哈希和范围说明的摘要位于 `livekit-rs-docs/evidence/2026-10-23/`。本机复现后同样应先保存必要的脱敏证据，再停止自己启动的实例、删除本目录 `output/`（含测试私钥），最后在服务端 workspace 执行 `cargo clean`；多智能体共享构建目录时由协调者统一清理。

### 可选 TURN 内层头部与 FIFO 观测

`OPTIN_PROXY_CLASS_TRACE=true` 仅在原网络 profile 中安装 `proxy-class-trace.mjs` 只读 wrapper。默认关闭且不导入；不改变 `network-proxy.mjs`、客户端、原5ms timer、4500B burst、速率、TTL、媒体和 accuracy 断言。无新增定时器。

原 backend 入队处理、flush 和 front.send 先执行；观测随后读取长度、时间和已有队列状态。send 的 receiver/参数/返回值/回调和原异常保持，诊断异常只标记覆盖无效。接收时间是原入队 handler 之后的观测时间；发送时间是 send 调用返回之后，并非发送完成或浏览器收到。

诊断只辨认 ChannelData 或 DataIndication 的 DATA 边界，以及内层 v2/PT 范围。SRTCP 首个明文控制头 PT/count-or-FMT 可区分可能的 PT205/FMT15 TWCC、SR/RR；不读取 SSRC，不解密或鉴权，不检查 compound 内部密文。RTP PT64–95 与 RTCP范围存在重叠，因此标记为 header-range 推断。没有 packet bytes、地址、通道号、参与者/packet IDs、SDP、凭据或 payload 被写入诊断。

每个 run 重建 WeakMap 并卸载旧 backend 监听，最多2048条环形事件保留最新 RTCP 接收/发送和超过10ms的 flush 返回间隔；溢出明确计数，`eventHistoryComplete=false`。各类 packet/bytes、队列年龄 histogram 和最大排队样本继续聚合。原 proxy 丢包只按原累计计数报告，TTL 丢包没有伪造逐类归因。诊断错误、未匹配发送、event overflow 与观测自身耗时单独保留；耗时不包括快照复制和全部事件分发开销，不能宣称观察零扰动或没有记录就不存在阻塞。原 gate 结果与诊断覆盖状态分别报告。

独立回归：`node --test *.test.mjs`，包括 wrapper exactly-once/回调/异常、TURN边界/畸形输入、实际队列中 RTCP 位于 RTP 之后、环形截断及跨run清理。该模块不调整任何原验收阈值。

Offline TWCC correlation can join the opt-in class trace with Chrome's selected
candidate-pair outbound estimate and the server's incoming-RTP TWCC metadata.
Capture the Node clock bridge in the **same process** that imports the unchanged
runner, retaining all the existing profile environment variables:

```sh
node --input-type=module -e 'console.log("OBS_NODE_TIME_ORIGIN_MS="+performance.timeOrigin); await import("./run.mjs")'
node correlate-twcc.mjs --input output/CAMPAIGN-single-video-relay.json \
  --server-log /tmp/CAMPAIGN-server.log --browser-log /tmp/CAMPAIGN-five.log \
  --run 4 --output /tmp/CAMPAIGN-run4-safe.json
```

The browser log must contain exactly one clock bridge for that campaign. Missing
or ambiguous bridges produce `INSUFFICIENT`; the script never substitutes
`startedAt` for the Node time origin. `--run` is zero based. Relative zero is that
run's first network sample, not its creation time. Each event includes signed
nearest-sample deltas; these are observations, not causal attribution or a
packet-identity match. Server snapshot log time can lag actual feedback emission
and repeated snapshots can refer to the same report. Chrome stats timestamps
show asynchronous sampling skew. Clear-header PT205/FMT15 inference does not
parse authenticated feedback or encrypted compound contents. Event-ring
truncation remains explicit. The offline output allowlists numeric counters,
durations, times and ordinals; addresses, credentials, payloads, SSRCs, candidate
IDs and transport IDs are excluded. Existing acceptance tests are unchanged.
