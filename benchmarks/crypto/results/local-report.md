# 本机 SRTP provider 对照快照

状态：**LOCAL_EVIDENCE；PERF-CRYPTO-01 完整门禁尚未通过。**

平台 Apple M1 Pro / macOS aarch64，Rust 1.99.0，release + thin LTO + codegen-units=1。每个场景预热10,000包，正式1,000,000包×5轮。两轮共160个百万包样本。两种provider使用同一RFC RTP封装和str0m-proto 0.7.0 trait；生产私有SrtpContext、解析/重放窗口、传输不在测量范围。

下表为**CPU秒/百万包中位数**；stdev、分配、批次时延、pps及原始每轮数据保留在JSON。

| 套件 | 操作 | Payload | RustCrypto 软件 | RustCrypto ARM intrinsics | AWS-LC（ARM轮） |
| --- | --- | ---: | ---: | ---: | ---: |
| AES128_CM_SHA1_80 | encrypt | 160 B | 2.0548 | 0.6071 | 0.7090 |
| AES128_CM_SHA1_80 | encrypt | 1200 B | 8.5756 | 1.9595 | 1.3815 |
| AES128_CM_SHA1_80 | decrypt | 160 B | 2.0322 | 0.5959 | 0.7317 |
| AES128_CM_SHA1_80 | decrypt | 1200 B | 8.7495 | 1.8931 | 1.3145 |
| AEAD_AES_128_GCM | encrypt | 160 B | 1.7984 | 0.1256 | 0.0975 |
| AEAD_AES_128_GCM | encrypt | 1200 B | 8.3253 | 0.7144 | 0.2370 |
| AEAD_AES_128_GCM | decrypt | 160 B | 1.8297 | 0.1489 | 0.0939 |
| AEAD_AES_128_GCM | decrypt | 1200 B | 8.3479 | 0.7184 | 0.2104 |

软件基线在生产ARM配置加入前构建；加速轮只增加`--cfg aes_armv8 --cfg polyval_armv8`，不使用target-cpu=native。相应实现使用Rust arch intrinsics，并保留cpufeatures探测及软件回退。生产配置已按本机对照启用；media/crypto聚焦测试65/65通过，strict clippy全绿。

本机测量显示，启用ARM指令显著降低RustCrypto成本；不同套件/负载的相对结果不同。GCM仍有进一步优化空间，现实现每包经过一次Rust分配；AES-CM小包在该包装下也并非统一慢于AWS。不能把这些数字外推为Rust/Go语言差异、完整SFU容量或云成本。

主机同时存在其他工作负载，未固定核绑定/频率。分配指标仅统计Rust allocator调用，**不含AWS-LC C内部malloc**，不能比较为双方总分配量。Cycles、AEAD_AES_256_GCM、SRTCP、fan-out 1/4/16、端到端排队，以及Linux x86_64/aarch64门禁均为NOT_RUN。

首轮驱动在运行结束后才采集源码SHA-256；期间存在格式化/注释调整。因此JSON中的源码哈希仅为运行后观察快照，**不是构建时精确源码身份凭证**。算法实现未改动。驱动现已改为构建前快照，并分别记录构建期间/运行期间差异；本轮未为补此元数据重复长基准。

- [软件轮完整数据](macos-aarch64-software.json)
- [ARM轮完整数据](macos-aarch64-armv8.json)
- [可重复运行说明及隔离边界](../README.md)
- [原始规范](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0001-p0-plan.md)
