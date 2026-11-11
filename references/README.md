# 本地参考源码索引

本索引最初核查日期为 2026-10-22；截至 2026-10-27，参考锁记录 67 个 Git checkout，另有独立发布包和 Go module 源码归档。每个输入的用途、精确 commit/版本、选读文件与后续核查以锁文件和 evidence 为准；仓库数量不代表功能验收进度。

用途、版本政策和阻断见[依赖规范](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0001-dependencies.md)；完整 commit、选读文件 SHA-256 见[参考锁](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0001-references-lock.json)，已发布 crate 版本/校验值见[版本目录](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0001-dependency-catalog.json)。

参考代码不属于本项目实现；不要把这些目录作为 production path dependency。这里保留原始上游许可。当前未编译或运行上游服务/测试，不意味着全部代码已经逐行审阅。

## 协议与架构参考

| 本地目录 | 上游 | checkout commit |
| --- | --- | --- |
| [client-sdk-js](client-sdk-js/) | [livekit/client-sdk-js](https://github.com/livekit/client-sdk-js) | `db8d49c69950` |
| [livekit](livekit/) | [livekit/livekit](https://github.com/livekit/livekit) | `49414fe10068` |
| [livekit-stable](livekit-stable/) | [livekit/livekit](https://github.com/livekit/livekit) | `e5655ac20267` |
| [mediasoup](mediasoup/) | [versatica/mediasoup](https://github.com/versatica/mediasoup) | `f6b640cb1f73` |
| [protocol](protocol/) | [livekit/protocol](https://github.com/livekit/protocol) | `f09f87aab29b` |
| [protocol-stable](protocol-stable/) | [livekit/protocol](https://github.com/livekit/protocol) | `9bb708cdb819` |
| [pulsebeam](pulsebeam/) | [PulseBeamDev/pulsebeam](https://github.com/PulseBeamDev/pulsebeam) | `ac3d579444f2` |
| [rheomesh](rheomesh/) | [h3poteto/rheomesh](https://github.com/h3poteto/rheomesh) | `9ad474ab685f` |
| [rtc](rtc/) | [webrtc-rs/rtc](https://github.com/webrtc-rs/rtc) | `07423df27fbb` |
| [server-sdk-js](server-sdk-js/) | [livekit/server-sdk-js](https://github.com/livekit/server-sdk-js) | `08dc11d30a28` |
| [sfu](sfu/) | [webrtc-rs/sfu](https://github.com/webrtc-rs/sfu) | `81883686343f` |
| [str0m](str0m/) | [algesten/str0m](https://github.com/algesten/str0m) | `aba3c2799be5` |
| [voice](voice/) | [n1snt/livekit-rs-voice](https://github.com/n1snt/livekit-rs-voice) | `28189c188975` |
| [waterbus](waterbus/) | [waterbustech/waterbus-rs](https://github.com/waterbustech/waterbus-rs) | `17464f6e5f92` |
| [webrtc](webrtc/) | [webrtc-rs/webrtc](https://github.com/webrtc-rs/webrtc) | `f6b017b0b8f3` |

## 底层依赖 Git clone

表内 checkout 是该仓库本次研究入口。多 crate 的发布版本可能来自其他 commit；对应 Git object 已抓取，实际发布文件另存 crates/，不能把 checkout 版本混称为所有 crate 的发布版本。

| 本地目录 | 对应候选 | checkout commit |
| --- | --- | --- |
| [Keats--jsonwebtoken](deps/Keats--jsonwebtoken/) | jsonwebtoken | `4c0ae752e9ac` |
| [RustCrypto--AEADs](deps/RustCrypto--AEADs/) | aes-gcm | `2eff6ccb1378` |
| [RustCrypto--MACs](deps/RustCrypto--MACs/) | hmac | `0236c8eb5009` |
| [RustCrypto--elliptic-curves](deps/RustCrypto--elliptic-curves/) | p256 | `27e62fdf973e` |
| [RustCrypto--formats](deps/RustCrypto--formats/) | x509-cert | `c478c6bb0db6` |
| [RustCrypto--hashes](deps/RustCrypto--hashes/) | sha2 | `138dc5a05b93` |
| [RustCrypto--rustls-rustcrypto](deps/RustCrypto--rustls-rustcrypto/) | rustls-rustcrypto | `70f76c039e58` |
| [RustCrypto--utils](deps/RustCrypto--utils/) | zeroize | `34a6ebcb9b92` |
| [algesten--dimpl](deps/algesten--dimpl/) | dimpl | `f68b0099eebc` |
| [algesten--sctp-proto](deps/algesten--sctp-proto/) | sctp-proto | `a23184979e36` |
| [andrewhickman--protox](deps/andrewhickman--protox/) | protox | `847875cd44a3` |
| [bourumir-wyngs--serde-saphyr](deps/bourumir-wyngs--serde-saphyr/) | serde-saphyr | `e3a4566ef269` |
| [clap-rs--clap](deps/clap-rs--clap/) | clap | `13f2db5072d6` |
| [dtolnay--thiserror](deps/dtolnay--thiserror/) | thiserror | `b1827ee06f81` |
| [hyperium--hyper](deps/hyperium--hyper/) | hyper | `7f67699ce32d` |
| [hyperium--hyper-util](deps/hyperium--hyper-util/) | hyper-util | `23a868965964` |
| [influxdata--pbjson](deps/influxdata--pbjson/) | pbjson, pbjson-build | `48ee5989564e` |
| [marshallpierce--rust-base64](deps/marshallpierce--rust-base64/) | base64 | `069bf7067b94` |
| [prometheus--client_rust](deps/prometheus--client_rust/) | prometheus-client | `3e3d55cda6d1` |
| [redis-rs--redis-rs](deps/redis-rs--redis-rs/) | redis | `9e9e62acb94a` |
| [rust-lang--flate2-rs](deps/rust-lang--flate2-rs/) | flate2 | `ed93d4fc60ea` |
| [rust-lang--socket2](deps/rust-lang--socket2/) | socket2 | `239dd83a4ced` |
| [rust-random--getrandom](deps/rust-random--getrandom/) | getrandom | `5e7cd5733536` |
| [rust-random--rand](deps/rust-random--rand/) | rand | `9e7d328f60d6` |
| [rustls--rcgen](deps/rustls--rcgen/) | rcgen | `f4a3b165c467` |
| [rustls--rustls](deps/rustls--rustls/) | rustls | `f04804fa1334` |
| [rustls--tokio-rustls](deps/rustls--tokio-rustls/) | tokio-rustls | `8aebb7c9556e` |
| [seanmonstar--reqwest](deps/seanmonstar--reqwest/) | reqwest | `de55373434f0` |
| [serde-rs--json](deps/serde-rs--json/) | serde_json | `20572a6d0669` |
| [serde-rs--serde](deps/serde-rs--serde/) | serde | `7fc3b4c30c94` |
| [tokio-rs--axum](deps/tokio-rs--axum/) | axum | `c59208c86fde` |
| [tokio-rs--bytes](deps/tokio-rs--bytes/) | bytes | `76c0fbb54ed4` |
| [tokio-rs--prost](deps/tokio-rs--prost/) | prost, prost-build | `13646cde7eab` |
| [tokio-rs--tokio](deps/tokio-rs--tokio/) | tokio, tokio-util | `b53b2001ed2c` |
| [tokio-rs--tracing](deps/tokio-rs--tracing/) | tracing, tracing-subscriber | `2d55f6faf9be` |
| [tower-rs--tower](deps/tower-rs--tower/) | tower | `4b0a6b0e688b` |
| [tower-rs--tower-http](deps/tower-rs--tower-http/) | tower-http | `c9414514a421` |
| [uuid-rs--uuid](deps/uuid-rs--uuid/) | uuid | `276c23628a66` |
| [webrtc--dcsctp](deps/webrtc--dcsctp/) | dcsctp | `b38fe5cdf5b4` |

## 发布源码

`crates/` 包含 49 个最新稳定 crate 的发布源码，以及用于追踪 str0m 约束的 dimpl 0.7.6（共 50 个包目录）。下载内容已核对注册表 SHA-256；目录中的 Cargo.toml 是 Cargo 实际发布的归一化依赖声明。`rustls-rustcrypto` 没有稳定发布，只有 Git 研究副本，不列为稳定生产候选。

## 更新约束

克隆已固定 commit，更新时先记录新证据再调整锁；保留旧来源以便审查。不得重置使用者修改、隐式更新成 HEAD，或用新源码覆盖旧 commit 的哈希。克隆采用浅历史；需要更早版本时显式 fetch 对应 commit。

## 恢复与校验

在项目根运行 `sh references/fetch.sh`：从文档中的 lock 恢复缺失仓库和发布包，固定 commit，验证下载包 SHA-256。已有目录只检查，不重置、覆盖或自动切换版本。重复运行可继续补齐缺失项；工作树修改或锁不匹配会明确失败并保留文件。

运行 `sh references/fetch.sh --verify` 离线检查 checkout 身份、干净状态、发布 commit 对象、包 manifest 与选读文件哈希。它不验证未记录文件的内容，也不编译代码；恢复时的下载包完整性由注册表 checksum 校验。

脚本需要 Git 和 Python 3.9+；可通过 `LIVEKIT_REFS_PYTHON` 指定解释器。根 `.gitignore` 忽略参考源码目录，保留本索引和 fetch 脚本；版本锁在文档目录。当前尚未初始化项目 Git 仓库，忽略规则会在初始化后生效。


## TLS / HTTPS 源码补充（2026-10-23）

[0005 TLS 依赖锁](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0005-tls-dependency-lock.json) 单独记录 `rtc-tls` 的 18 个直接依赖及证书校验关键传递依赖 `rustls-webpki`：官方注册表最新稳定版本、发布 checksum、真实缓存包 SHA-256、发布 Git commit、目录和选读源码哈希。既有 0001 锁保持不变。现有仓库 checkout 不切换；需要的其他发布 commit 仅补抓 Git object。

新增克隆如下，均固定为对应发布版本的 commit：

| 本地目录 | 发布版本 | 精确 commit | 用途 |
| --- | --- | --- | --- |
| [rustls--pki-types](deps/rustls--pki-types/) | rustls-pki-types 1.15.1 | `e29ad15465126acc089b39611bcec85e27ce0817` | DER/PEM certificate and PKCS8/SEC1 key containers; signature verification trait boundary. |
| [rustls--webpki-roots](deps/rustls--webpki-roots/) | webpki-roots 1.0.10 | `70760478b9d3ea729b22faa0b5324da38a297c1c` | Frozen Mozilla public trust anchors; explicitly configured SSL_CERT_FILE appends roots. |
| [rustls--hyper-rustls](deps/rustls--hyper-rustls/) | hyper-rustls 0.27.10 | `ce45b6b0dfdf238f9058b89feb1e3b8eaebbc5b0` | Explicitly configured HTTPS connector; default provider/platform verifier features disabled. |
| [hyperium--http-body](deps/hyperium--http-body/) | http-body-util 0.1.5 | `07838bd97b714b95bd783cd695ebf211b67545c4` | Full byte request bodies and body handling for HTTPS delivery. |
| [servo--rust-url](deps/servo--rust-url/) | url 2.5.8 | `d6ea13c5f8e7e6e627f6390161b3e185bda5e5ce` | Strict HTTPS URL and redirect destination/origin parsing. |
| [rustls--webpki](deps/rustls--webpki/) | rustls-webpki 0.103.16 | `8ecdcc76f56b68fda67a6adf7976f5eed5006a4c` | Transitive verifier implementation: trust chain, validity, EKU, server name and signature verification via supplied RustCrypto algorithms. |

`rustls`、`tokio-rustls`、RustCrypto 椭圆曲线/AEAD/MAC/hash、Tokio、Hyper 及其他已克隆来源均复用。`RustCrypto/rustls-rustcrypto` 仅作可追溯的 provider 胶水参考，其 alpha 包不是生产依赖。生产构建依赖注册表中的稳定库，禁止通过 `path` 引用本目录。该补充锁中的新增来源尚未接入历史 `fetch.sh` 恢复逻辑；可按 JSON 的 repository + release_commit 使用 `git fetch --depth 1 origin <commit>` 恢复，并对照选读文件哈希校验。

本次所有新增 TLS/HTTPS 库采用核查时最新稳定版。复用的既有 `aes-gcm 0.10.3`、`hmac 0.12.1`、`sha2 0.10.9` 仍低于注册表最新版本；锁文件明确记录升级跟踪，不能将整个依赖图宣称为全部最新版。本索引的历史仓库数和初始核查状态不包含此补充。


## TLS 算法与证书来源补充（2026-10-23）

[0006 TLS 依赖锁](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0006-tls-dependency-lock.json) 记录当前 `rtc-tls` 的全部 30 个直接依赖声明（含平台声明和版本别名），以及注册表 checksum、实际发布包核验、精确发布 commit、官方仓库和选读源码哈希。既有 0001/0005 锁保留为历史。稳定版本核查在冻结时进行；复用旧系列有单独升级跟踪，不能将传递闭包称为全部最新。

该补充涉及的来源如下（已存在的仓库复用；需要的发布 commit 以 Git object 补抓，不强制切换 checkout）：

| 来源目录 | 对应包与版本 | 发布 commit |
| --- | --- | --- |
| `references/deps/rustls--rustls` | rustls 0.23.46 | `f04804fa13341a9b1dc02b5d1dd8a746f2c4b7fb` |
| `references/deps/rustls--tokio-rustls` | tokio-rustls 0.26.6 | `8aebb7c9556ea8024c1b974b84a4a6f2e306889a` |
| `references/deps/rustls--pki-types` | rustls-pki-types 1.15.1 | `e29ad15465126acc089b39611bcec85e27ce0817` |
| `references/deps/rustls--webpki-roots` | webpki-roots 1.0.10 | `70760478b9d3ea729b22faa0b5324da38a297c1c` |
| `references/deps/RustCrypto--elliptic-curves` | p256 0.14.0 | `27e62fdf973ef5f1b0a81d4577bec0aa323cde5f` |
| `references/deps/RustCrypto--elliptic-curves` | p384 0.14.0 | `24b65d148ceeb20f1412efed9f3588cf02bddc47` |
| `references/deps/RustCrypto--AEADs` | aes-gcm 0.10.3 | `fa8197f11d79a079fcb1f6ef67fa9119ce6939b9` |
| `references/deps/RustCrypto--MACs` | hmac 0.12.1 | `46797e3b44973a30edb9d7f3a3ebb41810061d90` |
| `references/deps/RustCrypto--hashes` | sha2 0.10.9 | `82c36a428f8d6f05f3bfccdedb243e9d1f85359d` |
| `references/deps/rust-random--getrandom` | getrandom 0.4.3 | `5e7cd5733536844a9856dc7259bd4696bbe5e3ae` |
| `references/deps/dtolnay--thiserror` | thiserror 2.0.21 | `b1827ee06f81a7d7f954e676771e19a97f9f8e3b` |
| `references/deps/tokio-rs--tokio` | tokio 1.54.1 | `b53b2001ed2ccfd9b87e32a0c6da1a1eebdc80b8` |
| `references/deps/rustls--hyper-rustls` | hyper-rustls 0.27.10 | `ce45b6b0dfdf238f9058b89feb1e3b8eaebbc5b0` |
| `references/deps/hyperium--hyper` | hyper 1.12.0 | `7f67699ce32d717081deb7018b4da7d679277ac0` |
| `references/deps/hyperium--hyper-util` | hyper-util 0.1.21 | `23a868965964c1d6bb1b94f30ba4c420a4bebe7c` |
| `references/deps/hyperium--http-body` | http-body-util 0.1.5 | `07838bd97b714b95bd783cd695ebf211b67545c4` |
| `references/deps/tokio-rs--bytes` | bytes 1.12.1 | `76c0fbb54ed4336caf9d2311658a2f4a5627c21d` |
| `references/deps/servo--rust-url` | url 2.5.8 | `d6ea13c5f8e7e6e627f6390161b3e185bda5e5ce` |
| `references/deps/RustCrypto--traits` | aead 0.6.1 | `3d2d90045bffc402af4edb6a5a4dbb1e217329d1` |
| `references/deps/RustCrypto--AEADs` | aes-gcm 0.11.1 | `2eff6ccb137850202ad581afb6e947f302ccbbcf` |
| `references/deps/RustCrypto--AEADs` | chacha20poly1305 0.11.0 | `e37a978ccf0992d9053fbc039470d6527108e393` |
| `references/deps/dalek-cryptography--curve25519-dalek` | ed25519-dalek 3.0.0 | `07bef73ff85998a206cd2cea7f2605c801d0d1c9` |
| `references/deps/dalek-cryptography--curve25519-dalek` | x25519-dalek 3.0.0 | `07bef73ff85998a206cd2cea7f2605c801d0d1c9` |
| `references/deps/RustCrypto--RSA` | rsa 0.9.10 | `da2af9a0ff814762957c428460e4098720f394a6` |
| `references/deps/RustCrypto--crypto-bigint` | crypto-bigint 0.7.5 | `2b54d248cce00457e3afb5650d9b14632ef4a116` |
| `references/deps/RustCrypto--utils` | zeroize 1.9.1 | `34a6ebcb9b920ec7ae5809e470fce94fe7c0cb55` |
| `references/deps/RustCrypto--elliptic-curves` | p521 0.14.0 | `e6b9f475e83b627abca7ef4ef17791540845522f` |
| `references/deps/rustls--rustls-native-certs` | rustls-native-certs 0.8.5 | `4bcc4bb91d0b0bdffa49a9b3c85bb0fc03d49899` |
| `references/deps/kornelski--rust-security-framework` | security-framework 3.7.0 | `5f6e65114b77d5bc161d2b099cad09f2a67609d2` |
| `references/deps/steffengy--schannel-rs` | schannel 0.1.30 | `91c925d987e6b1180bfb21fb29959ab3677adc8a` |

另保留 `RustCrypto/RSA` 的 `0.10.0-rc.19` 源码（commit `db103005d1c9ae550efc5f6daa1b9cfe150e549a`），仅供窄范围签名编码/盲化/故障检查来源审计，**不是生产依赖**。生产私钥乘方由稳定 `crypto-bigint 0.7.5` 的固定公开宽度接口实现；`rsa 0.9.10` 仅承担公钥验证与借用 ASN.1 数据结构。精确 RustSec 例外、私钥调用边界和退出条件见 [RSA 审计说明](../crates/rtc-tls/RSA-PUBLIC-ONLY.md)，来源哈希与许可证见 provider 的 `SOURCE.json`、`THIRD_PARTY.md`。生产不能 path 引用 references。新增锁尚不由历史 fetch.sh 自动恢复，可按 lock 的 repository/release_commit 恢复，核 checksum 与源码哈希。

## 2026-10-23：ctutils 安全升级来源

[0007 依赖锁](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0007-dependency-lock.json) 单独冻结 `ctutils 0.4.5`：核查时官方 sparse registry 最新、未撤回的稳定版。原锁的 `0.4.2` 命中 [RUSTSEC-2026-0385](https://github.com/RustSec/advisory-db/blob/main/crates/ctutils/RUSTSEC-2026-0385.md)，修复范围为 `>=0.4.4`；`0.4.4` 修复 mask 生成和 32 位 `u128` 比较的恒时性问题，`0.4.5` 又补充 Thumb-1 比较及 `Choice` 组合加固。采用冻结时最新版 `0.4.5`，不新增公告豁免。

复用现有真实 clone `deps/RustCrypto--utils`，保留其历史 checkout `34a6ebcb9b920ec7ae5809e470fce94fe7c0cb55`，另取发布包 `.cargo_vcs_info.json` 指定的 commit `88d6e90d4cfd2fc3333d00e13c99241507d2138f`。可在该 clone 执行 `git show 88d6e90d4cfd2fc3333d00e13c99241507d2138f:ctutils/src/choice.rs` 研究固定版本；恢复时执行 `git fetch --depth 1 origin 88d6e90d4cfd2fc3333d00e13c99241507d2138f`，不切换旧 checkout。新锁记录注册表 checksum、真实下载包 SHA-256、Cargo.lock 一致性及选读源码和许可证哈希。0006 等历史锁保持原状；生产仍从注册表解析，不引用 references 的 path。此记录核对来源及上游修复说明，不等于独立证明所有目标上的机器码恒时性。

## Google GoogCC ACK estimator reference (2026-10-23)

`libwebrtc-googcc/` is a shallow `blob:none` partial clone of the official
`https://webrtc.googlesource.com/src`, sparsely checked out to
GoogCC and its sent-packet/ALR call paths at commit
`202acee882062f7ca632472e4810f25938828f6a` (sparse paths and reproducible commands are fixed in the source lock).
It retains LICENSE, PATENTS and AUTHORS. Exact source hashes and reproducible
commands are recorded in
`livekit-rs-docs/evidence/2026-10-23/libwebrtc-googcc-source-lock.json`.
This is a source-review input, not a production dependency or a compiled C++ port.

## 2026-10-25：CLI 与配套服务 RPC 参考

新增真实 shallow clones：`urfave-cli/` 固定 tag `v3.11.0`、commit
`138d3b34001ba0ddd59f6f29474788ba3d2f8ea7`；`psrpc/` 固定 tag `v0.8.2`、commit
`e9701ee1b5a981cfa3c7c69503d4775ddff13729`。它们是冻结 LiveKit/Protocol
Go 模块声明的参考版本，仅用于解析器和 Redis Pub/Sub 协议审查、独立差分；不是 Rust
产品的 Go 运行时依赖。逐文件 hash 见
[参考锁](https://github.com/livekit-rs/livekit-docs/blob/main/server/evidence/2026-10-25/service-cli-reference-clones.json)。
恢复可分别执行 `git clone --depth 1 --branch v3.11.0 https://github.com/urfave/cli.git references/urfave-cli`
和 `git clone --depth 1 --branch v0.8.2 https://github.com/livekit/psrpc.git references/psrpc`，
再核对上述 commit 和文件 hash。保留早期 `go-modules/urfave-cli-v3.11.0` 发布模块快照；不覆盖旧审计输入。
当前 TLS 私钥 bigint 的安全迁移以 [SPEC-0010](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0010-crypto-bigint-security-migration.md)
的 `0.7.6` 为准，上表 `0.7.5` 是历史冻结记录。

## 2026-10-25：功能兼容依赖增量

`redis-rs/` clone固定`fec5ef0127a34a3e2096bbc014f18305859223e3`（redis1.8.0），
`prost-reflect/` clone固定`2d0e1847a07d2582ae3edf84dbb80f2684961fef`（0.16.5）。
实际发布包分别保留在`crates/redis-1.8.0`与`crates/prost-reflect-0.16.5`，
下载包checksum和逐文件hash见[功能增量锁](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0015-functional-dependency-lock.json)。
恢复时从锁中的remote clone后按commit detached checkout，下载对应crates.io包并核checksum；
Git checkout和归一化发布manifest分别保留。早期`deps/redis-rs--redis-rs`及旧1.7.1源码保持历史身份。
生产从注册表精确锁定，不path引用references，也不以新客户端库已安装证明完整Redis/worker功能。

## 2026-10-25：SIP 功能差分与 Go 标准库语义参考

新增真实 sparse clone `go/`，固定 tag `go1.26.0`，commit `d90b98e65320778f3b1f99a6951ab20f04d218b3`，remote `https://github.com/golang/go.git`。只选取 LICENSE、strconv、slices、net/netip；独立核对其文件与先前工具链快照逐字节一致。用于 Rust 的 Unicode 引号/IsPrint 与重复 ID 排序语义移植，不是生产 Go 运行时。文件身份见 `../livekit-rs-docs/evidence/2026-10-25/f04-go-reference-clone.json`，BSD 许可随发行保存在 `../livekit-rs-server/LICENSES/Go-BSD-3-Clause.txt`。

恢复：clone `--filter=blob:none --no-checkout --depth 1 --branch go1.26.0` 后启用 sparse checkout（LICENSE、src/strconv、src/slices、src/net/netip），detached checkout 并核对 commit/hash。`go-stdlib-1.26.0/`、`go-slices-go1.26.0/` 是历史选读文件快照，继续保留。2026-10-22 索引的“未运行”仅是当日初审状态；后续真实官方 Server、Go RedisStore/ProtoJSON 与 Rust 执行范围以每批 evidence 为准。

## 2026-10-25：F05 CLI、二进制兼容与官方 Agent SDK 来源

来源和直接依赖边见 [F05 来源锁](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0017-dependency-source-lock.json)。CLI 新增直接使用 chrono`0.4.45`（禁默认特性，仅std）和 unicode-width`0.2.2`（默认特性）；SIP 侧车新增 lk-compat→prost-reflect`0.16.5`，lk-protocol 的该依赖边已经存在。协调者实际注册表查询确认这三个选择是冻结时最新稳定版；只增加已锁版本的直接边，未升级 Cargo.lock 包版本。旧消费者仍可能保留兼容约束内的 unicode-width`0.1.14`，不能为了“全闭包最新”改写其上游约束。此处不宣称每个新配置/CLI分支已验证。

- `agents-js/`：真实 shallow checkout，remote `https://github.com/livekit/agents-js`，tag `@livekit/agents@1.9.4`，detached HEAD `48224e24d95afa1ce95bb801284083c1d2014b04`；agents/package.json 版本实际为1.9.4。它是测试源码参考，不是 Rust 产品的 Node 运行时。
- 官方未修改 npm 测试包由 `livekit-rs-server/tests/agent-sdk/package-lock.json` 冻结：`@livekit/agents1.9.4`、peer `@livekit/rtc-node1.2.1`、传递 `@livekit/protocol1.53.0`。实际 resolved 地址（npmmirror）及原 SHA512 integrity 已逐项保留到上述锁，不替换成推测的 npmjs 下载地址。安装/import 只证明加载，不等于完整 worker SDK 或 RTC 兼容通过；实际注册/分配/RTC/退出的执行证据单独审阅。
- `go-cli/github.com/jxskiss/base62@v1.1.0/`、`go-cli/github.com/dustin/go-humanize@v1.0.1/` 和 `go-cli/github.com/olekukonko/tablewriter@v1.1.4/`：固定官方 Go 模块原始来源输入，分别用于精确 secret 编码、数值格式及14列 node 表差分。Go 工具和 module graph 仅属于开发 oracle。
- `go-cli/google.golang.org/protobuf@v1.36.12/`：固定 generated-Go decoder 的源语义参考，特别是 known-number/wrong-wire 作为 unknown 的规则。`go-cli/github.com/livekit/mediatransportutil@v0.0.0-20260821083140-f234b534b095/`：网络配置真实消费者审计输入，不是当前 Rust/Pion 实现。

已有 `prost-reflect/` clone与注册表包继续按 SPEC0015 保留。chrono/unicode-width 的专用真实 clone 现也已落地：`chrono/` detached HEAD `170338250e836976a211e64728ec956e45e78a39`，`unicode-width/` detached HEAD `9d98411769fe13c7c18cab0b3fbbab29ba8350ea`。来源锁同时记录真实 crates.io package checksum、published VCS commit、发布 manifest hash 与 checkout 的 manifest/许可 hash。Git checkout 不等于归一化发布包，不能以 HEAD 相同声称 archive 逐字节相同。生产移植的原始 MIT 许可现随发行保存在 `livekit-rs-server/LICENSES/jxskiss-base62-MIT.txt` 与 `go-humanize-MIT.txt`；不能因早期测试 oracle 也带副本而省略发行物许可。

F05 source restoration (verify against the lock, never follow moving HEAD):

```sh
git clone --filter=blob:none https://github.com/chronotope/chrono.git references/chrono
git -C references/chrono checkout --detach 170338250e836976a211e64728ec956e45e78a39
git clone --filter=blob:none https://github.com/unicode-rs/unicode-width.git references/unicode-width
git -C references/unicode-width checkout --detach 9d98411769fe13c7c18cab0b3fbbab29ba8350ea
```

Registry archive identities remain chrono0.4.45 checksum `1aa79e62e7697b8e29b513a68abacf485adcd1fe8284a4316c5ae868e6633327` and unicode-width0.2.2 checksum `b4ac048d71ede7ee76d585517add45da530660ef4390e49b098733c6e897f254`. Independently check downloaded archive bytes against those values; a Git tree hash is not a substitute.

## Functional close-reply compatibility reference (2026-10-25)

snapview--tungstenite-rs is cloned under deps/ at published commit
c2921718b76f0ed8c69a895397ed981e4a321f6f. The original0.29.0 registry package
is retained under crates/, exact39 files and archive checksum locked by the
reference/catalog and vendor SOURCE. It is Axum0.8.9's existing compatible
transport version, not a claim that0.29 is globally latest. Runtime proof is
recorded separately in SPEC0027; cloning/byte checks alone are not execution.

## 2026-10-26 supplementary source

`deps/mitchellh--go-homedir` is a real clone at v1.1.0 / af06845cf3004701891bf4fdb884bfe4920b3727 for fixed-Go POSIX expansion and the independent test oracle. Exact selected source/license hashes are in the reference lock. This does not add Go to production dependencies.

`deps/go-yaml--yaml-v3` is a real v3.0.1 clone at f6f7691b1fdeb513f56608cd2c32c51f8194bf51. Its selected decoder/reader/license files are byte-identical to the fixed upstream module used in the actual peer.

F73 receiver oracle retains `deps/livekit--webrtc-pion` v4.2.18-warp.6 at `e5f5c3cafda19d16683c5f33e84d6cbd1f8f25f4`; selected source and MIT license hashes are in the reference lock. This is an actual Go test peer, not a production dependency.

F69/F83 POSIX source work retains exact `go1.26.3` at `deps/golang--go-1.26.3` (`2dc996f71b0ebafb77e64433e58333e049488a3c`), including OS/exec, path, syscall and BSD license. Selected Go sources match the actual pinned toolchain bytes; Darwin oracle capture does not certify Rust or Linux parity.

`deps/urfave--cli-v3` is fixed target dependency v3.11.0 (`138d3b34001ba0ddd59f6f29474788ba3d2f8ea7`): genuine `exec.ExitError` satisfies CLI ExitCoder and prints stderr with the child exit status. Ordinary errors still reach main stdout/1. Source identity does not imply actual consumer acceptance.

The fixed ProtoYAML configuration consumer now has reproducible module inputs in
`go-modules/buf-protoyaml-v0.7.0` and `go-modules/go-yaml-v3-v3.0.5`.
SPEC-0018 locks the immutable proxy archives and every original file, including
licenses. Independently computed Go dirhash values match the pinned server's
`go.sum`. These are test/source-review inputs; they add no Go production runtime.

The fixed interceptor dependency `go-modules/pion-interceptor-v0.1.49` retains
all178 original published files and MIT licenses for the real Pion GCC port.
SPEC-0018 locks the archive; root independently recomputed its Go dirhash against
the fixed server go.sum. The source reference alone does not establish provider
algorithm or runtime compatibility.

## Fixed Go standard-library address reference

`go-stdlib-go1.26.3/` is the separately cloned official `golang/go` release tag `go1.26.3`, commit `2dc996f71b0ebafb77e64433e58333e049488a3c`. Selected original network/address/service/zone files and BSD license are byte-verified against the isolated Redis literal-address candidate. It is a development/source reference, never a production Go dependency. Exact identities are in the reference lock; functional transport acceptance remains separate.

## Ordinary data writer source input

`go-modules/pion-transport-v5.1.1/` retains the original published source used by the fixed server data-channel writer. The archive Go dirhash was independently recomputed and matched fixed `go.sum`; file identities and the immutable archive checksum are in [the handoff reference receipt](https://github.com/livekit-rs/livekit-docs/blob/main/server/evidence/2026-10-28/claude-code-handoff/pion-transport-reference.json). This is a reference input, not a new production dependency or a passed writer implementation.
