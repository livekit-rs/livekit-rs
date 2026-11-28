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

此配置由 Rust 服务器直接终结 `443` 的 TURN/TLS：只要设置了 `turn.tls_port`，Join 就固定下发 `turns:<domain>:443?transport=tcp`（与上游一致，见 `crates/lk-compat/src/signal/ice.rs`），监听其它端口等于浏览器联系不到中继。绑定 `443` 需要权限，Linux 用 `sudo sysctl -w net.ipv4.ip_unprivileged_port_start=0`，macOS 需以 root 启动该测试实例。浏览器必须实际选择 `relayProtocol=tls` 并收到音频和可靠数据。`TLS_SPKI` 仅向测试浏览器指定该本地证书的 SPKI 例外，不能据此宣称完成公网 CA、域名或系统信任库验收。信令入口仍为本机 HTTP/WebSocket；这里没有验证入站 HTTPS/WSS。

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
```

两套 fixture 的内置 TURN/TLS 都监听 `443`，所以只能依次运行；下面的 RSA 段之前先停掉 EC 实例。EC 实例使用 HTTPS/WSS `8443`、RTC `8441/8442`，RSA 实例对应 `9443`、`9441/9442`。两套配置均设置 `livecloud.https.http_enabled: false`，分别保证明文 `8480`、`9480` 未监听。原生 HTTPS 是 `livecloud.https` 扩展，未给官方 LiveKit YAML 增加同名官方字段。默认为保留原 HTTP；这两份配置显式关闭它，不做隐式重定向。

先跑 EC：用 Node 的常规证书验证检查 HTTPS，探针不使用 SPKI 例外，然后跑未改动的官方 SDK。EC 场景同时验证 AV1 空间/时间层。

```sh
# livekit-rs-server/，保持运行
sudo ./target/debug/livecloud --dev --config tests/interop/native-https-config.yaml

# tests/interop/
LIVEKIT_URL=wss://127.0.0.1:8443 PLAIN_HTTP_PORT=8480 PROBE_LABEL=ec \
node probe-native-https.mjs

NODE_EXTRA_CA_CERTS="$PWD/output/tls/root.pem" \
TLS_SPKI="$(cat output/tls/spki-sha256.txt)" \
LIVEKIT_URL=wss://127.0.0.1:8443 SERVER_LABEL=rust-native-https-av1 \
FORCE_RELAY=true TURN_TLS=true VIDEO=true VIDEO_CODEC=av1 \
SCALABILITY_MODE=L3T3_KEY SVC_SWITCH=true SVC_TEMPORAL=true \
RUNS=1 DURATION_MS=10000 npm test
```

停止 EC 实例后按同样顺序跑 RSA：

```sh
# livekit-rs-server/，保持运行
sudo ./target/debug/livecloud --dev --config tests/interop/native-https-rsa-config.yaml

# tests/interop/
LIVEKIT_URL=wss://127.0.0.1:9443 TLS_CA_FILE=output/tls-rsa/root.pem \
PLAIN_HTTP_PORT=9480 PROBE_LABEL=rsa node probe-native-https.mjs

NODE_EXTRA_CA_CERTS="$PWD/output/tls-rsa/root.pem" \
TLS_SPKI="$(cat output/tls-rsa/spki-sha256.txt)" \
LIVEKIT_URL=wss://127.0.0.1:9443 SERVER_LABEL=rust-native-https-rsa \
FORCE_RELAY=true TURN_TLS=true RUNS=1 DURATION_MS=10000 npm test
```

每套都要求有效 CA 成功且证书获信任、错误主机名拒绝、不受信任 CA 拒绝、关闭的明文端口连接被拒绝。报告写入 `output/ec-native-https-probe.json` 和 `output/rsa-native-https-probe.json`。TCP readiness 仅用于等待监听，不能替代这些实际 TLS 断言。

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

`SVC_SWITCH=true` 请求低/高空间层并检查真实解码尺寸；`SVC_TEMPORAL=true` 先把两路订阅切到空间层 `1`，再按 `0`/`2`/`15` FPS 各请求一次：每次请求后静置 2.5 秒，再取相隔 6 秒的两个采样，用订阅端 `inbound-rtp.framesDecoded`（无该字段时回退 `framesReceived`）增量作为服务端实际转发的视频帧数，要求低请求不足无请求的 60%、提高到 15 后至少是无请求限制的 1.5 倍且回到无请求值的 70% 以上。时间层上限读的是当前订阅空间层自己的帧率表，因此选两路发布都能稳定测得的空间层，而不是仅检查 SDP 中出现 VP9。发布端 `outbound-rtp.framesEncoded` 与 `videoFrames` 渲染回调一并写入证据但不作判定口径：VP9 SVC 一张图会按各空间层分别计一次编码帧，渲染回调只反映显示节奏。将命令中的 `VIDEO_CODEC=vp9` 改为 `VIDEO_CODEC=av1`，并更换 `SERVER_LABEL`，可复现 AV1 `L3T3_KEY` 的同组空间/时间订阅门禁；上面的原生 EC WSS 命令已组合 AV1 与 TURN/TLS。AV1 场景需要浏览器实际支持该编码，检查真实解码尺寸、帧数与 dependency descriptor 驱动的层选择，不能用仅出现 SDP codec 名代替。当前覆盖 Chrome/Chromium 的 VP9/AV1 `L3T3_KEY`，没有据此覆盖全部编码模式、其他浏览器或客户端。结合拥塞的层选择使用下一组独立门禁，不从本组关闭拥塞的结果推导。

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

设置 `METRICS_URL` 后还保存容量、建模媒体预算、暂停数和探测结果，并按本节点每路订阅的已承诺容量判定受限阶段下降、恢复阶段回升。指标按活跃房间聚合；音频/视频分配预算采用 payload 口径，有效容量为预留传输开销前的 bps，二者不是相同口径的原始流量计数。恢复进展本身按路判定，不读聚合值，理由见下节。

2026-10-23 初期八秒探测窗口版本的本机 Chrome 历史实测通过：受限阶段新增丢包 `86` 次、两路视频暂停，有效总容量从 `639784` 降至 `150338` bps，恢复至 `1502331` bps并恢复解码。两个客户端受限前最后解码尺寸均为 `318×180`，其中一路已经在 `635×360` 与 `318×180` 间变化；恢复报告末帧为 `635×360` 和 `318×180`。该结果验证相对各自基线的恢复和后续五秒持续解码，未证明启动阶段已稳定到最高层，也未证明恢复至发布端最高 `960×544`。长时间满层稳定性、更多 RTT/丢包分布及公网容量仍需单独测量。

后续高基线 release 测试暴露了两个问题：启动阶段已经暂停的订阅不能作为“新增暂停”基线；固定按当前容量增加 200 kbps 的探测也不足以快速跨越高层所需成本。当前候选已改为下一可升级层的实际预算目标（包含音频与传输开销），并将探测目标保持 18 秒，覆盖原生 ALR 五秒和非 ALR 停滞探测十五秒的调度。请求探测速率仍不计作可分配容量，只有真实反馈才提升预算；高基线 release 的后续失败进一步定位到：音频继续发送时，按全部媒体字节判断“视频停滞”会阻止原生重新发现。当前候选按需求缺口、预算停滞与真实无拥塞反馈启动低起点原生指数探测，起点不超过已验证容量和 300 kbps；请求或重置本身均不作容量增加的证据。最终还修复了最高层精确需求裁掉探测余量的问题：padding 探测可超出精确需求 20% 或至少 200 kbps，但显式带宽 override 仍是硬上限；升级成功不能在尚未达到下一层完整 wire 成本时提前确认。

最终 release 版本在上述严格高基线门禁通过：两端基线均为 `960×544`，有效总容量 `4958962 → 65621 → 4532075` bps，两路暂停后恢复，受限阶段新增丢包 `1409` 次（累计 `29 → 1438`）。两端在恢复后的 30 秒内解码到原 `960×544` 的最近八帧，再连续观察五秒确认音视频继续前进。该额外观察结束时末帧分别为 `635×360`、`960×544`，说明动态选层仍继续；此通过不等于随后全程稳定保持最高分辨率。旧版本的低基线通过和高基线失败均保留于证据摘要，未用重跑覆盖。

#### 恢复进展按订阅路判定

`livekit_estimated_capacity_bps` 是把每路订阅连接的已承诺容量相加得到的（`crates/rtc-runtime/src/manager.rs`），而两边实现都把每路容量初值为 100 Mbps，只在出现拥塞趋势或探测授信时才移动（Go `pkg/sfu/bwe/remotebwe/remote_bwe.go` 的 `Reset`/`estimateAvailableChannelCapacity`，Rust `crates/rtc-media/src/feedback.rs` 的 `committed`）。因此只要第二路从未拥塞，聚合值就始终带着它未被观测的 100 Mbps 先验：C108 实测中响应了限速的那一路从 `100,000,000` 跌到 `39,434`，随后按探测结算 climb 到 `68,525/83,694/129,481/163,244/205,713/362,104`，而聚合表每步只动 0.03–0.10%，26 个合格样本中没有一个到达过 8,000,000 bps 的开放路径聚合值——任何“在聚合值上要求相对阶梯或绝对下界”的判据对有一路未拥塞的服务器都不可能满足。

所以恢复判据改为逐路：在同一条订阅路上，既要在墙内出现低于自身墙前峰值的请求（≤ 0.7 倍）且这一削减能归因到服务器自己——被授予的层低于墙前层、该路出现 `IsDeficient`，或其已承诺容量离开了 100 Mbps 种子——也要在墙撤掉后的窗口内出现高于自身受限最低点的请求且层号同时升高。仅在带宽上回升而不交还任何管理层不算恢复；只有需求下降、而服务器既没减层也没报缺、容量仍钉在种子上的轨迹同样不算（C108 实测 `transport-1` 请求 `1,800,000 → 1,219,798`，层号 200 不变、0 条 deficient、容量恒为 `100,000,000`，这是编码器自身码率起伏而非对限速的反应）。判据源是两边都有的分配记录：固定 Go 的 `stream allocation:` 与本仓 `crates/rtc-media/src/forward.rs` 的 `alloc cost sample`。聚合容量的阶梯仍保留为备选判据，只在没有分配日志（例如未开 debug 级别）时生效；`congestion-config.yaml` 已固定 `logging.level: debug`，CI 的拥塞腿通过 `INTEROP_SERVER_LOG` 绑定服务端日志，使本地配对与 CI 走同一条判据。

#### 逐路接收端响应认证（`PASS_PER_PATH`）

聚合预算为 `INSUFFICIENT` 时，同一轮还可以走逐路判据，但换的是判据源，不是放松门槛：读取 `crates/rtc-media/src/feedback.rs` 的 `remote feedback sample`，它每路 REMB 一条，带接收端估算 `receiver_estimate_bps`、本路已承诺 `committed_capacity_bps`、观察者结论 `observation` 和转发的 `streams` SSRC。这些 SSRC 就是订阅端 `inbound-rtp.ssrc`，因此无需服务器加身份字段即可把每条记录绑到产生该估算的浏览器（实测 4 个 SSRC 精确绑定 2 路）。判定如下：

- 某路**有义务**反应——自己的接收端估算塌到墙前最大值的 50% 以下，或服务器为该路报告了 `Congesting/*` 趋势；
- 有义务却仍保持 `100,000,000` 种子 → `OBLIGATED_NOT_RESPONDED`，整轮 `FAIL`；
- 已承诺容量跌破种子 → `RESPONDED`，至少一路 `RESPONDED` 且无上述失败即 `PASS`；
- 接收端从未报任何拥塞 → `SEED_RETAINED`，只披露不打分：没有信号两边实现都不会动，固定 Go 正是更弱的一侧（它没有 `remote feedback sample` 这一行，实测 0 条，因此在 Go 侧该判据为 `INSUFFICIENT`）。

C108 实测两条腿都落在此判据上：`pair14rust` 中 `client-0` 估算 `597,516 → 17,862` 且容量 `100,000,000 → 17,862 → 2,179,520` 为 `RESPONDED`，`client-1` 估算墙前 187,560–281,078、墙内 286,406–309,318 从未下降、观察者也从未报 `Congesting`，故 `SEED_RETAINED`；`pair12rust` 两路的估算都没塌（发布端编码器先退让），义务完全来自 `Congesting/Loss nack_ratio=1.000`，两路各自承诺 `329,017`/`291,943` 而 `PASS`。逐轮结论记为该轮的 `runStatus`，`bandwidthAccuracy.status` 由所有轮次的 `runStatus` 汇总（任一轮 `FAIL` 即 `FAIL`，任一轮 `INSUFFICIENT` 即 `INSUFFICIENT`，全为 `PASS` 才是 `PASS`，否则为 `PASS_PER_PATH`）；聚合值不能靠整条腿跑完就改成通过——腿在别处失败会把结论降级为 `INCOMPLETE`，通过与否只由判据决定。逐路 verdict 与 `preWallEstimateMaxBps`/`underWallEstimateMinBps`/`underWallCapacityMinBps`/`afterWallCapacityMaxBps` 一并存档。

#### 独立 allocation-horizon 上界门禁（可选）

在上面完全相同的五轮拥塞命令中添加 `BANDWIDTH_ACCURACY=true`，原媒体、可靠数据、限速、队列、启动/恢复超时全部保留。缺少 `METRICS_URL` 时拒绝启动该门禁。额外门禁读取原有 `livekit_estimated_capacity_bps`，不裁剪生产值，也不换成新指标；路径数来自实际选中的 DTLS transports，并要求代理客户端数一致。

该门禁明确是**一秒 allocation horizon 的安全不变量**：假设当前速率保持一秒，采样的预算不能授予超过 `C*T/8+B` 的服务量。它不是“实际观测到完整一秒持续吞吐”或 TWCC 容量下界证明，原恢复门禁也不因此变成完整带宽精度验收。

上界里的 `C` 不是配置速率本身，而是 `max(配置速率 × 路数, 这些路已经真实送达过的最高速率)`，`B` 是每路声明的 4500 字节桶。理由来自固定 oracle：Go 在 LOSS 趋势下按需求推导承诺容量（`remote_bwe.go` `estimateAvailableChannelCapacity`），在 120 kbps 的墙上实测承诺过 `1,573,006` 与 `339,925` bps 每路，远高于 `240,000` bps 的名义聚合值，所以只按配置速率设界会把参考实现判成失败；相反，高于任何已送达速率的预算是无证据的凭空容量，仍为 `FAIL`。判定结果记录 `ceilingSource` 为 `delivered-peak` 或 `configured-rate`，送达速率由代理自己的逐路 `forwardedIpv4Bytes` 差分得出。

仍留在先验带里的聚合预算（≥ 每路 50 Mbps × 路数，即一半初值）不参与打分，单独记为 `PRIOR`：它是尚未被本路观测修正过的种子，不是本路的观测结果。某个 epoch 若全是 `PRIOR`，只有在该 epoch 的各路确实没有丢包（代理 `expiredPackets`/`queueLimitDrops`/`oversizedDrops` 增量为 0）时才算合理，否则说明限速生效而估算器从未交出观测预算，返回 `INSUFFICIENT`；一个 epoch 都没能合格的整轮同样返回 `INSUFFICIENT`。

每次速率变化后先保留五秒历史过渡，再要求一秒同速资格历史；五秒来自现有 Robust ACK 最长窗口与探测结束历史上限，一秒来自现有最小窗口，均在候选运行前固定。变速调用前/完成时间和指标请求起止实际记录；指标读取跨变速、早期过渡及 relay identity 尚未验证的采样保留为 `WARMUP`。每个初始/受限/恢复 epoch 都必须有有效稳定采样；原指标、时间或精确路径身份缺失为 `INSUFFICIENT`，不能通过。预算超上界为 `FAIL`。

证据另存 `bandwidthAccuracy`，逐 epoch 报告预算 min/max、合格/先验样本数、送达峰值、丢包计数和最大采样盲区。离散采样不能证明盲区中从未出现过冲；原始短 probe rate、持续服务速率和 committed budget 仍分别取证。新增元数据无 payload、SDP 或凭据；独立 evaluator 对超出有界输入上限的记录返回 `INSUFFICIENT`，不静默截断成通过。

独立计量回归可运行：

```sh
node --test bandwidth-accounting.test.mjs bandwidth-timeline-gate.test.mjs network-proxy.test.mjs
```

回归不仅测试自定义 oracle：还导入实际 `TokenBucket`，执行多包、refill 和变速 trace，由独立 BigInt 有理数账本审核真实接受结果。旧 18.331 Mbps 长期预算、低速 516 kbps 及额外超发字节的反例必须失败；短毫秒 probe 可合法消耗 burst，但不能据此称为持续容量。

### 证据与 CI 状态

`SERVER_LABEL` 区分 `output/` 下 JSON 报告，建议各场景使用不同标签避免覆盖。报告记录官方 SDK、浏览器版本、选中 ICE/DTLS 路径、实际媒体解码、信令和场景指标。普通测试还支持 `RUNS`（轮数）、`DURATION_MS`（基础观测时长）、`SINGLE_PC=false`（双 PC）和 `EXTENDED=true`（扩展房间操作）。

#### Remote CI leg status

Every browser-interoperability leg is configured in `.github/workflows/server.yml`.
Their actual execution state on the remote GitHub Actions runner differs per leg, and
only the following statements are supported by recorded run evidence. Step numbers in
this section are the pre-C88 workflow layout, which is what those run pages report.

Legs that have been green on a remote runner (observed identically in runs
`45257476589`, `45646502216`, `45660777186` and `45668976571` for the pre-C88 steps
1-17, and as recorded legs in `45689637824` and `45691070092`): dependency install,
Playwright Chromium install, TLS certificate generation, Rust binary build, standalone
server start, recorded fixture consistency and redaction, network impairment proxy
regression tests, single-PC audio and reliable data (two runs), reconnect /
republish / mute / metadata / RPC, VP8 video with extended operations, VP9 L3T3_KEY
spatial and temporal subscription switching, dual-PC audio and reliable data, the
HTTPS webhook signature and retry leg, and both native HTTPS trust probes.

Since run `45689637824` every leg executes and is recorded, so the matrix is complete
instead of truncated at step 18. Both `45689637824` (head `10b94c5`) and
`45691070092` (head `7494051`) recorded the same verdicts: 10 legs `success`, 4 legs
`failure` (`turn-tls`, `native-ec-av1`, `native-rsa`, `congestion`), 0 legs without a
recorded verdict. A local pass is still not evidence of a remote pass.

The three TURN TLS relay legs had one cause, now separated by recorded evidence rather
than inferred. In `45689637824` the served `iceServers` entry is
`turns:turn.localhost:443?transport=tcp`, the fixture listened on `5349`, and the server
log ends with `TURN server stopped {"allocations": 0}`: no Allocate ever reached the
server, which matches the browser report having zero `local-candidate`, `remote-candidate`
and `candidate-pair` rows. The deciding evidence is that the same log holds no
`TURN allocation created` row at all: `allocations` is a live gauge, so a clean stop also
prints `{"allocations": 0}` on a leg that did relay, as the `congestion` log shows after
two allocations were created and deleted by their clients. The same attribution dump shows
a harness TLS handshake to `127.0.0.1:5349` completing with TLS 1.3, the pinned SPKI and an
authorized certificate chain, so the certificate material was never the problem. Advertising
`443` regardless of
the configured port is exact upstream behavior (`pkg/service/roommanager.go` in the frozen
oracle, reproduced at `crates/lk-compat/src/signal/ice.rs` and asserted in that module's
tests), so the fix belongs to the fixtures and the runner, not the server: the TURN TLS and
native HTTPS fixtures now set `turn.tls_port: 443`, and the job lowers
`net.ipv4.ip_unprivileged_port_start` first. Because two fixtures now claim the same port,
the EC and RSA servers run one at a time, and each server-start step waits for its HTTPS
port and for `443`, so a refused privileged bind reddens that lifecycle step instead of
showing up as an ICE timeout.

The `congestion` leg is a different failure with no certificate or port involved. Its
recorded assertion is `client 0 had no SFU pause/layer reduction under congestion`, and the
`45691070092` timeline says why: across its 51 once-per-second samples
`livekit_estimated_capacity_bps` only ever holds its two initialization values (`100000000`
at the first sample, `200000000` at all 50 later ones) while
`livekit_probe_attempts`, `livekit_probe_successes`, `livekit_probe_failures`,
`livekit_congestion_events`, `livekit_paused_video_tracks` and
`livekit_allocation_overshoot_bps` stay at `0`. The impairment itself is real and measured in
the same file: the proxy forwarded `7,355,896` IPv4 bytes in `10,104` packets and dropped
`7,231`, and `livekit_allocated_bps` climbs from `2,735,272` at the moment the rate is cut to
`4,273,321` at its peak on a path the profile holds at 120 kbps. Both subscribers then freeze
for the remaining 17 s - `videoFrames` pinned at `145` and `147` from t=34.4 s to t=51.4 s
with `lastDimensions` still `960x544`, while `audibleSamples` keep rising `324→494` and
`326→496` - so audio continues and video stops, and the zero-valued `paused_video_tracks` means
the SFU never recorded a pause either. A zero `allocation_overshoot_bps` is not evidence of
accuracy: that gauge is computed against the server's own estimate, so it cannot see a path
the estimate ignores. The leg is left strict: an SFU that reacted to the
impairment would produce the pause or the layer change the assertion asks for.

To remove the masking, every media leg is now launched through `ci-leg.mjs`, which runs
the leg's own command, appends its id, exit code, signal, duration and a redacted output
tail to `$CI_STEP_RESULTS_FILE`, and re-raises that exit code so the step still shows
red on the run page. Each leg step also carries `continue-on-error: true`, so a red leg
no longer aborts the legs behind it. The last step, `Aggregate recorded interoperability
leg results`, runs `ci-leg-summary.mjs` against that file: it prints the leg matrix and
exits non-zero if any of the 14 legs is not recorded `success`, which includes a leg that
is absent because the job died before it. The gate reads the recorder file rather than the
workflow `steps` context on purpose: a step is only visible in that context when it has an
explicit `id`, and a `continue-on-error` step's recorded conclusion is ambiguous. Lifecycle
steps (server start/stop, evidence upload, TLS removal, `cargo clean`) stay ordinary steps
with `if: always()` where teardown matters, so if one of them fails the job is already red
without anything to re-assert. The leg ids, commands, environment values and assertions are
unchanged; what changed is which ports the fixtures listen on, one added sysctl step, and
`https-rsa-probe` moving after the EC leg because the RSA server can only start once the EC
server releases `443`. When the TURN TLS leg is red,
`Collect failed TURN TLS leg attribution evidence` runs `ci-leg-attribution.mjs`, which
dumps harness-side evidence (TURN TLS TCP/TLS probe with the recomputed SPKI pin,
WebSocket and HTTPS API probes, the recorded leg JSON diagnostics, network interfaces and
the Chromium launch args) into `output/step18-turn-tls-attribution.json`; that step checks
the recorded `turn-tls` conclusion through `ci-leg.mjs --failed turn-tls` instead of an
`if: failure()` expression, so a leg that passed cannot trigger it and a leg that never ran
cannot be mistaken for one that failed.

The leg recorder and the attribution dump both map RFC 1918 addresses to the RFC 5737
placeholder range `198.51.100.x` (last octet preserved); uploaded evidence excludes private
key material.

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
