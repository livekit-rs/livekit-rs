# 交付门禁

在服务端 workspace 根目录执行：

```sh
python3 scripts/test-check-dependencies.py
python3 scripts/check-dependencies.py
python3 crates/rtc-tls/scripts/check-rsa-boundary.py --self-test
cargo install cargo-deny --version 0.20.2 --locked
cargo deny --locked --exclude-dev check
```

`check-dependencies.py` 默认逐一查询 Linux `x86_64-unknown-linux-gnu` 和 `aarch64-unknown-linux-gnu` 的锁定 Cargo 图。可重复使用 `--target` 改目标，使用 `--output` 指定 JSON 证据文件。默认结果为 `target/evidence/dependency-graph.json`，包括锁文件 SHA-256、完整命令、各项门禁结果和失败依赖路径。

- A01：`rtc-core`、`rtc-media`、`rtc-runtime` 的 normal/build 传递依赖不含 `lk-*`。
- 生产构建依赖：从 `livecloud` 出发检查 normal/build 依赖，拒绝 ring、aws-lc、OpenSSL 和 rcgen；忽略仅存在于 dev 边的依赖。libc 等 OS FFI 不在密码实现禁用列表内。
- cargo-deny：检查安全公告、撤回版本、许可证、重复版本、通配版本和来源。重复版本报告为警告；安全公告逐条评审，禁止统一豁免。运行时必须保留 `--exclude-dev`，供未来独立 benchmark 使用的后端不进入生产门禁。

`deny.toml` 对 `RUSTSEC-2023-0071` 有单条、可撤销的不适用处置：`rsa 0.9.10` 的可达算术仅用于公钥验证，另使用其借用 DER 解析类型与公开摘要前缀；不调用该包受影响的私钥签名、解密或 CRT 算术。RSA 端点私钥签名由独立、固定公开精度的 `crypto-bigint` 适配层完成，强制盲化、签后公钥回算与敏感数据零化。这不是声明整个 `rsa` 包无风险，也不是其他公告的豁免。

`quality` CI 执行 `check-rsa-boundary.py --self-test`，扫描可达调用边界、关键签名不变量，并用恶意修改验证检查确实会拒绝越界；该源码检查不能证明机器码恒定时间。调用边界、负责人、独立测试和退出条件见 [RSA-PUBLIC-ONLY.md](../crates/rtc-tls/RSA-PUBLIC-ONLY.md)。每次相关依赖升级、调用修改及发行前须重新评审；受影响私钥 API 一旦可达即阻断发布，存在修复的稳定依赖或替代公钥验证器时移除该条处置。其余公告仍按正常门禁处理。

此工具不编译或交叉编译 Linux 二进制。Cargo metadata 的可选依赖边可能包含未激活项；脚本以每个目标、每个root的 `cargo tree --edges normal,build` 实际激活边构图，metadata仅提供包身份。证据保存命令、图与输出哈希；JSON不代表机器码、汇编或native link输入已被审计。最终发行仍需在目标平台验证实际构建及链接结果。

根目录 `.github/workflows/server.yml` 固定 Rust、cargo-deny 和 GitHub Action 版本，并运行格式、Clippy、测试与这些图检查。CI 上传依赖图 JSON；依赖审计的失败保持为发布阻断。

CI 的 `quality` job 还运行 `check-protocol-schema.py`，验证本仓库消费的 protobuf 清单和哈希。`official-cli` job 在 runner 临时目录获取 LiveKit 官方提交 `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`，用该提交声明的 Go 1.26.0 构建参考程序，再通过 `LIVEKIT_OFFICIAL_BIN` 显式运行 `lk-config` 的 opt-in CLI 差分测试。Go 仅用于测试参考程序；这些 CI 步骤已配置，不表示本地或远程已执行该 job。


`browser-interoperability` job 使用原版 JS SDK 和 Chromium，保留音频、可靠数据、重连和 VP8 门禁，并增加 VP9 `L3T3_KEY` 订阅层切换、网络限速代理单元回归、TURN/TLS 强制中继和 HTTPS Webhook 验证。TURN/TLS 使用独立的 8080 实例，浏览器须实际选中 `relayProtocol=tls`；Webhook 脚本另外启动临时服务端，通过官方 SDK 检查签名及正文摘要，并验证首次 503 后同一事件重试成功。

TLS 测试先运行 `npm run tls:fixture`，由 OpenSSL 生成仅供本地测试的七天 P-256 CA 和证书。OpenSSL 是测试工具，不是生产依赖。浏览器仅以生成证书的精确 SPKI 白名单信任本次证书；Webhook 客户端通过 `SSL_CERT_FILE` 添加测试 CA，并保持证书链、名称和有效期校验。测试凭据与私钥写入忽略的 `tests/interop/output/tls/`，上传证据时排除私钥。证书校验失败用例由 `rtc-tls` 的 Rust 测试覆盖。

这些步骤已写入工作流；本地通过的场景与远程 CI 执行状态须分别记录，配置完成不表示 GitHub Actions 已通过。

TLS新增 ISC 与 CDLA-Permissive-2.0 许可已核对并纳入 cargo-deny allow；对应原文和版权声明保存在 [LICENSES](../LICENSES/README.md)，发行物须随附。许可门禁仍检查其余所有依赖，不用统一豁免替代。

B0 的 str0m 补丁来源可单独离线重建核验：

```sh
python3 scripts/check-str0m-reconstruction.py
```

脚本使用本地 `../references/crates/str0m-0.24.1`，先校验发布包的 257 个文件及整树摘要，再按 `vendor/str0m/SOURCE.json` 最新重建记录的顺序应用独立补丁，并逐字节比较最新重建记录列出的 33 个源代码/manifest 文件（当前 19 个顺序补丁；先前 18 个补丁/29 文件、先前 17 个补丁/28 文件、先前 15 个补丁/26 文件及 14 个补丁/25 文件检查点保留为历史）。临时目录自动清除，JSON 证据写入标准输出；不运行 Cargo、不联网，也不改变 reference 或 vendor 文件。这项核验只证明源码可重建，运行时及原版 SDK 验收仍各自独立。

A1.5安全迁移的dimpl发布包及窄补丁可独立离线重建：

```sh
python3 scripts/check-dimpl-reconstruction.py
```

先核对`../references/crates/dimpl-0.7.6`的148文件发布整树，再严格应用`vendor/dimpl/migration.patch`，逐文件比较全部重建字节与vendor，并校验原manifest/两许可证未改。该脚本包含补丁解析拒绝错误上下文等自检，不运行Cargo/Git、不联网或写临时文件。source身份通过不替代安全门禁、加密算法或SDK互操作；默认需要先按来源锁恢复reference输入，未在CI运行则不宣称CI已通过。

F51 adds scripts/check-tungstenite-reconstruction.py. Its default mode is offline
and verifies the real original39-file package and exact2-file patch, sharing
the existing tested exact unified-diff parser. Explicit --restore-reference
restores only a missing checksum-pinned registry snapshot using bounded archive
validation and refuses overwrite. CI runs this source gate and both isolated
feature profiles in tests/conformance/websocket-close-policy. These configured
jobs are not a claim that remote CI has run. Actual local results are in SPEC0027.

Network ICE source reconstruction (same-version is0.11.1 fork):

```sh
python3 scripts/check-is-reconstruction.py
```

Requires restored `../references/crates/is-0.11.1` from its locked registry archive. The read-only checker validates all14 original files, strictly applies the2-file mapped-host patch, compares all14 reconstructed files and metadata/license byte hashes, and rejects undeclared vendor files. It uses the existing strict Python patch parser; no Git/Cargo/network or temporary writes. Source identity is separate from actual socket/ICE/DTLS/SDK acceptance.

`python3 scripts/check-smallvec-wrapper-reconstruction.py` replays the exact published14-file package plus its one-file maintained macro-provider manifest patch; no advisory ignore or library source change. Cargo/source/runtime gates are separate.

C15 h3 0.0.8 WebTransport final-settings source reconstruction:

```sh
python3 scripts/check-h3-reconstruction.py
```

Requires restored `../references/crates/h3-0.0.8` from its locked registry archive. The read-only checker validates all57 original files, strictly applies the4-file webtransport-final-settings patch, compares all57 reconstructed files and metadata/license byte hashes, and rejects undeclared vendor files. It uses the existing strict Python patch parser; no Git/Cargo/network or temporary writes. Source identity is separate from actual HTTP/3 settings negotiation or SDK acceptance.

C15 quinn-proto 0.11.19 RESET_STREAM_AT partial-delivery source reconstruction:

```sh
python3 scripts/check-quinn-proto-reconstruction.py
```

Requires restored `../references/crates/quinn-proto-0.11.19` from its locked registry archive. The read-only checker validates all58 original files, strictly applies the5-file stream-reset-partial-delivery patch, compares all58 reconstructed files and metadata/license byte hashes, and rejects undeclared vendor files. It uses the existing strict Python patch parser; no Git/Cargo/network or temporary writes. Source identity is separate from actual QUIC stream reset delivery or runtime acceptance.
