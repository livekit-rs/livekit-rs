# PERF-CRYPTO-01：独立 SRTP provider 比较

本目录是独立 Cargo workspace，具有单独的 `Cargo.lock`。仅比较二进制引入
`str0m-aws-lc-rs = 0.6.0`；生产 workspace 未增加该依赖。该发布版与生产
`str0m-proto = 0.7.0` 的 crypto trait 相容，其实际 aws-lc、dimpl 等版本见本目录 lock。
上游 provider 元数据：[crates.io](https://crates.io/crates/str0m-aws-lc-rs/0.6.0)。

## 运行

首次解析依赖后提交本目录 lock，后续运行固定依赖：

```sh
cd livekit-rs-server/benchmarks/crypto
cargo build --release --locked
./target/release/livekit-rs-crypto-benchmark --self-test
python3 run.py
```

`run.py` 保存 `results/macos-aarch64-software.json`，包含所有原始样本、汇总、编译器、
CPU、完整系统信息、构建前源文件 SHA-256、构建/运行期间源码差异和有效编译flags。
若未显式提供RUSTFLAGS/CARGO_ENCODED_RUSTFLAGS，驱动会向构建/运行进程传入
RUSTFLAGS=''，覆盖父级Cargo的ARM cfg，以保留aarch64软件基线；显式flags则原样记录并使用。
x86_64的crate默认可能运行时使用AES-NI，不能将其默认标签解读成软件后端。
文件名表示本次开发机样本，
Linux 门禁应保存另名产物并补充核绑定/调频策略。

快速开发检查（只能生成 `SMOKE_ONLY`，不满足 PERF-CRYPTO-01 工作量）：

```sh
./target/release/livekit-rs-crypto-benchmark --smoke --packets 10000 --rounds 1 --output results/smoke.json
```

## 测量边界

- 两个 provider 共用完全相同的 RTP 包封装和 `str0m-proto` IV/HMAC helper；
  输入包括 12 字节 RTP header、无扩展、160/1200 字节 payload，以及 10/16 字节 tag。
- 套件：AES128_CM_SHA1_80、AEAD_AES_128_GCM；加密和认证解密分别测量。
- 每场景预热 10,000 包；正式每轮 1,000,000 包，至少 5 轮。
- packet index 连续递增并跨越 16 位序号回绕，ROC 随 index 增长；套件、负载、
  operation 和轮次采用独立 key 域，同 key 下不同明文不复用 nonce。
- 开始前校验双方 ciphertext 完全一致、交叉解密、序号/ROC 回绕及篡改拒绝。
- 每 1024 包形成一个测量批次。解密 ciphertext 预先生成于该批次计时之外，
  内存上界约 1.3 MiB，不把预生成加密成本计入解密。
- CPU 为 `getrusage(RUSAGE_SELF)` 的 user+system CPU，逐批次相加；同时报告
  实测 wall time、pps、有效 payload Gbps、批次时延 p50/p95/p99 和每轮样本。
- 全局 allocator 统计 Rust 分配/重分配调用；计数用 relaxed atomic，本身存在
  少量共同测量开销。aws-lc 的 C 内部分配不经过 Rust allocator，**不会计入**；
  因此该指标仅代表 Rust allocator 调用，不能用它推断两者总分配量。
- 两个 provider 的初始化、DTLS 握手、session key KDF 不计入稳态成本。

这是一致的 SRTP RTP-protection 封装与 provider 比较，**不是**生产 str0m 私有
`SrtpContext` 的完整状态机基准，更不是完整 SFU 吞吐。包解析、重放窗口、乱序、
传输和排队都未包含。代码直接使用会话 key，未测完整密钥派生路径。

macOS aarch64 样本用于发现实现成本；尚不能满足规范要求的 Linux x86_64/aarch64
门禁。未控制核绑定与 CPU 调频，可能存在其他工作负载；精确硬件加速分派未检查。
AEAD_AES_256_GCM、SRTCP、fan-out 1/4/16、cycles/packet、SFU 容量和端到端排队 p99 均为 **NOT_RUN**。
报告中保留这些缺口，不据当前数字推断商业容量或宣称完整性能门禁通过。

## 隔离检查

在生产 workspace 执行 `cargo tree -p livecloud -e features --locked`，确认其中没有
`aws-lc-rs` / `aws-lc-sys`。独立 benchmark 本身包含它们是有意的；不要把本目录
加入生产 workspace members，也不要给生产 str0m/dimpl 开启 aws-lc feature。

## ARMv8 Rust intrinsics 对照

aes 0.8.4 与 polyval 0.6.2 的 crate 默认在 aarch64 使用软件实现，需要分别设置
`aes_armv8` / `polyval_armv8` cfg 才启用可用的 ARM 加速实现。可运行第二份隔离产物：

```sh
RUSTFLAGS='--cfg aes_armv8 --cfg polyval_armv8' \
CARGO_TARGET_DIR=target-armv8 CRYPTO_BENCH_LABEL=armv8 python3 run.py
```

本次软件基线是在生产 aarch64 配置加入前编译的；保留为 `software` 结果。
生产 `.cargo/config.toml` 现在已启用这两个 cfg。直接调用cargo会继承加速配置；
本目录run.py在未提供显式flags时会传入空RUSTFLAGS覆盖父配置，保证aarch64软件基线。
需要重现软件路径时显式覆盖 flags（使用不同输出目录）：

```sh
RUSTFLAGS='--cfg aes_force_soft --cfg polyval_force_soft' \
CARGO_TARGET_DIR=target-software CRYPTO_BENCH_LABEL=software python3 run.py
```

对照写入不同结果文件。不使用 `target-cpu=native`。
源码依据（均为已锁定发布版）：

- [aes 0.8.4 source](https://docs.rs/crate/aes/0.8.4/source/src/autodetect.rs)：`src/lib.rs` 选择后端；`src/autodetect.rs` 中
  `cpufeatures::new!(aes_intrinsics, "aes")` 与 `init_get()` 在运行时选择
  ARM `core::arch::aarch64` 实现，检测不支持时选择 `soft`。
- [polyval 0.6.2 source](https://docs.rs/crate/polyval/0.6.2/source/src/backend/autodetect.rs)：`src/backend.rs` 选择后端；`src/backend/autodetect.rs` 中
  `cpufeatures::new!(mul_intrinsics, "aes")`（aes implies PMULL）与
  `init_get()` 在运行时选择 PMULL 或 `soft`。实现为 Rust arch intrinsics。

因此软件默认结果不能被解释成 Rust 语言本身的性能上限，加速版也不能被解释成
所有 ARM 硬件的保证。实际分派、频率、温度、背景负载会影响样本；正式平台门禁
仍需固定 Linux 硬件并记录相应条件。
