# Real Linux qdisc production-function acceptance

PREPARED ONLY, NOT RUN. Run only inside the coordinator-owned disposable ARM64
Linux guest after other task compilers/workers are idle. No Cargo, service,
installation, interface mutation or Go command was executed by this author.

Minimal coordinator entry (absolute paths; a genuinely new output directory):

```sh
python3 tests/interop/linux-qdisc/run-owned-qdisc.py \
 --rust-source /absolute/exact-candidate10/crates/livecloud/src/node_tc.rs \
 --libc-rlib /absolute/already-built/target/debug/deps/liblibc-ACTUAL.rlib \
 --rustc /home/fixture/fixture/toolchain/rust/bin/rustc \
 --go-source /absolute/fixed-source/references/livekit-stable \
 --go /absolute/installed-go1.26.3/bin/go \
 --output /home/fixture/fixture/output/tc-NEW
```

Transfer this entire small fixture directory and the exact production node_tc.rs
whose SHA is pinned in SOURCE.json. Its identity is checked before use; a Mac
parser result or a different candidate cannot pass. The already-built Linux
libc rlib and rustc identities are recorded. Rust is compiled directly with
rustc and `#[path="production.rs"]`; the included production source is unchanged.

The fixed Go server is copied into a new owned staging tree. Its module files
and node_linux.go are checked against the fixed source hashes, the entire original
inventory is recorded and checked unchanged, and only one same-package observer
_test.go is added. That observer calls the actual unexported getTCStats function.
The original function is never copied into a replacement algorithm or rewritten.
`GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOWORK=off go test -mod=readonly -c`
uses only the source/module cache already populated by the real Go server build.
Missing modules/toolchains fail explicitly; the runtime nft lock is not relaxed.
The original prometheus package has no existing _test.go files, so this adds no
third-party test dependency to its production module graph.

Two fresh random task namespaces and one fresh random veth pair are created.
Only those new interfaces receive addresses or qdiscs. No lo, eth0, existing
interface, host route, host namespace or host firewall is modified. Both test
namespaces have only a direct veth route, no uplink. The nonroot fixture user
runs both observers after privileged ip-netns entry. sudo is used only for the
named network setup/teardown, never compilation or source extraction.

A real receiver proves all 100 deterministic UDP payloads arrived with netem
loss0%. Both actual production samplers must equal `tc -s -j qdisc` kernel
packet/drop counters in a quiescent snapshot, with successful packets>0 and
drops0. The same qdisc is changed to loss100%, real UDP sends produce positive
kernel drops, and all three views must again match exactly with packets>0 and
drops>0. No fabricated netlink messages or expected counter table are supplied.

Each observer performs 257 actual NETLINK dumps per phase. Rust checks the exact
FD set after every call; Go checks actual /proc/self/fd counts. Any error, partial
snapshot, changed traffic count, mismatch or FD leak fails. Every subprocess
exit, raw stdout/stderr, tc JSON, receiver count, original/staged inventory and
probe input/binary hash remains in the new evidence directory.

Cleanup deletes only namespace/interface names this invocation successfully
created. A failed/partial setup never authorizes deleting some other namespace.
Namespace release and original source identity remain mandatory. All failure
records persist; no apt/network fallback or weaker test is substituted. Root
must ensure guest iproute2/tc, sudo/runuser and the real sch_netem kernel support
are already available. This proves the kernel TC consumer function, not every
Prometheus gauge or whole-server lifecycle counter.
