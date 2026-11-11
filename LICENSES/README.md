# 第三方依赖与移植许可文本

保留新增 TLS 依赖的 ISC 版权/许可声明与 Mozilla 根数据 CDLA-Permissive-2.0 协议全文。发行物应随附这些文本；注册表源码原有版权、NOTICE 和其他依赖许可同样需要保留。这不是整个依赖图的 SBOM。

- rustls-webpki 0.103.16：原始 LICENSE（含文件例外说明）。
- untrusted 0.9.0：原始 LICENSE.txt。
- webpki-roots 1.0.10：crate 内 LICENSE；共享根数据时附协议全文。

来源与发布包 checksum 见 [TLS 锁](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0005-tls-dependency-lock.json)。

Go Authors BSD-3-Clause: fixed Go1.26 strconv print classification/quoting and slices comparator sorting ports in SIP compatibility. Full attribution/license: [Go-BSD-3-Clause.txt](Go-BSD-3-Clause.txt). Original reference file hashes are locked by SIP domain/catalog SOURCE manifests; these are Rust ports, no production Go runtime.

## F05 CLI 生产移植

- [jxskiss-base62-MIT.txt](jxskiss-base62-MIT.txt)：原 jxskiss/base62 v1.1.0 MIT全文，Copyright2019 Shawn Wang。`lk-config/src/keys.rs` 的精确compact bit-packing编码是Rust移植，保留原版权和许可；不引入生产Go运行时。
- [go-humanize-MIT.txt](go-humanize-MIT.txt)：原 go-humanize v1.0.1 MIT全文，Copyright2005–2008 Dustin Sallings。`livecloud/src/cli_nodes.rs` 的Bytes/SI格式辅助按固定源码移植；发行必须保留此声明。

上述文件逐字节复制原许可，不是测试限定授权。来源与SHA见[F05依赖/来源锁](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0017-dependency-source-lock.json)；测试oracle目录中的历史副本继续保留。CLI主体的LiveKit Apache-2.0来源归属与仓库原有NOTICE同样适用。本目录仍不是整个闭包的完整SBOM。

## F07 SIP gateway and worker error contracts

SIP source validation/mapping follows pinned LiveKit Server/Protocol Apache-2.0;
source hashes and full Apache license are retained in
`crates/lk-compat/src/workers/sip_gateway/`. The Go Authors x/textv0.42.0 country
classification data retains its BSD license and PATENTS grant there; selected
source/table hashes and exact Go oracle inputs are preserved in
`tests/oracles/sip-gateway/`. The reference checkout is locked in the repository
restore manifest. These Rust adaptations do not introduce a production Go
runtime. Private PSRPC error adapters retain request-owned wrapper provenance;
source metadata in `tests/oracles/worker-errors` and `rpc-error-provenance`
records the exact independent Go expectations.


`pion-sdp-MIT.txt`: retained Pion community MIT notice for the validation-only Rust grammar port in `crates/lk-compat/src/whip/sdp_syntax.rs`, derived from github.com/pion/sdp/v3 v3.0.20 `base_lexer.go`, `unmarshal.go`, and `util.go`. Exact reference archive/file identities and registry retrieval are locked in `livekit-rs-docs/sources/0028-pion-sdp-grammar-source.json`. No Pion runtime is linked.

F51 CLI quoting ports also use the existing Go Authors BSD-3-Clause license;
source hashes and full Unicode digest are in the F51 evidence. The vendored
tungstenite0.29.0 retains unchanged upstream MIT/Apache-2.0 license files in
vendor/tungstenite. Its narrow source patch and provenance are documented there.

F53 finite CLI stage control-flow port retains urfave/cli v3.11.0 MIT notice in `urfave-cli-MIT.txt`. Fixed primary references and command graph accompany the actual CLI differential fixture. No Go runtime is linked.

`go-homedir-MIT.txt` preserves the MIT notice for the pinned go-homedir1.1.0 POSIX home-expansion reference and test oracle. The Go module is test-only; production expansion is implemented in Rust. Source identity is recorded in `crates/lk-config/SOURCE-F60-F61.json` and the reference lock.

`go-yaml-MIT-Apache.txt` preserves the upstream dual-license notice for the fixed v3.0.1 YAML decoder/encoding reference. Source clone and exact hashes are in the reference lock; the Go package is not a production dependency.

`livekit-NOTICE.txt` retains the fixed upstream Apache-2.0 NOTICE for source-derived supervision/limit semantics and exact Go oracle extraction. The Apache license text is retained in the server LICENSE; source copyright notices remain in the derived modules.

RoomConfiguration ProtoYAML compatibility uses the fixed buf.build/go/protoyaml
v0.7.0 (Apache-2.0), go.yaml.in/yaml/v3 v3.0.5 (MIT/Apache-2.0),
and Go1.26.3 internal/strconv float lexical/hex conversion (Go BSD-3-Clause)
as source references. Rust ports retain source copyright notices; the existing
Go-BSD-3-Clause.txt applies to the float port. The exact module archive and Go
source identities are retained in SPEC-0018, the references lock, and the public
room-config-yaml/room-config-float-bits oracle manifests. Go is a test reference,
not a server runtime dependency.

The same-version vendored prost-reflect0.16.5 and prost-derive0.14.4 retain their
upstream license texts in their vendor directories. Narrow signed-zero patches,
original package/file hashes and reconstruction instructions are recorded in
those directories' SOURCE.json/PATCHES.md and in the references lock.
`scripts/check-protobuf-reconstruction.py` verifies all259 original source files
and exact patched trees. These are Rust production dependencies; the independent
Google Protobuf/Go observers are test references only.

F110 CPU/heap profile sampling uses rpprof0.18.3 (MIT OR Apache-2.0), default
features disabled, framehop-unwinder and prost-codec enabled. The unchanged full
license files and all original package sources are reproducibly retained under
references/crates/rpprof-0.18.3 and the published Git commit checkout. The source
lock includes all28 new resolved package originals, licenses and VCS metadata
where published. OS signal/timer/unwind FFI is explicit; no Swift or C++ sampler
feature is selected for the declared64bit Mac/Linux targets. Go pprof is a test
consumer, not a production dependency.

Pion feedback-stage/adapter and typed RTCP Rust ports retain the original2026 Pion community MIT texts in [pion-interceptor-0.1.49-MIT.txt](pion-interceptor-0.1.49-MIT.txt) and [pion-rtcp-1.2.19-MIT.txt](pion-rtcp-1.2.19-MIT.txt). Both files match their respective full retained upstream module LICENSE byte-for-byte, even though the two license texts are identical. Source archives/files are locked in SPEC0018; application source/oracle profiles record finite tested scope. No production Go runtime is linked.

`vendor/is/LICENSE-MIT.txt` retains the original Martin Algesten license for the same-version is0.11.1 source fork. Only mapped-host construction/Host input-base selection are patched; `scripts/check-is-reconstruction.py` replays the exact patch over all14 original published files and compares the entire reconstructed tree. The network Go observer MIT/Apache source notices are preserved under `tests/oracles/network-interfaces`; they are test references, not runtime dependencies.

- smallvec-wrapper0.4.1: original MIT OR Apache-2.0 licenses retained in [vendor/smallvec-wrapper](../vendor/smallvec-wrapper); manifest-only maintained pastey alias patch.
