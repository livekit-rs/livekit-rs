# SPEC-0078 WSS control interop

This fixture checks the Rust control component against the pinned Kotlin/Native
MsgTrans transport. It does not run the Neton application or authorize RTC
traffic. The fixture requires the first request to register and verifies the
registration, snapshot, acknowledgement and heartbeat payloads. It sends a
notification during a heartbeat and checks the same-version renewal. The Rust
peer also attempts WSS with an unrelated CA and requires verification to fail.

1. Archive Kotlin MsgTrans commit `f99388ca7660d07068261f8228c7f67c5f7d7ad5`
   into a temporary directory. Copy `CloudControlFixture.kt` into its
   `msgtrans-websocket/src/nativeTest/kotlin/msgtrans/transport/` directory.
2. From that directory's repository root, run
   `./gradlew :msgtrans-websocket:linkDebugTestMacosArm64 --console=plain`.
   The fixture executable is
   `msgtrans-websocket/build/bin/macosArm64/debugTest/test.kexe`.
3. In a separate temporary working directory run the pinned Kotlin checkout's
   `interop/gen-certs.sh`. It generates an ephemeral `certs/` directory with
   the server certificate and both trusted and unrelated CAs.
4. From the Rust workspace run
   `cargo test --locked -p livecloud --features cloud --lib --no-run`.
   Use the resulting `livecloud-…` libtest path from Cargo's output.
5. Run `python3 tests/interop/cloud-control/run.py <test.kexe> <libtest> <certs> <new-output-dir>`.
   Both test executables must exit 0 and the runner checks their positive
   assertion markers. Stop the task-owned Gradle daemon and remove the
   temporary checkout, certificates and outputs after preserving receipts.

The feature-free Rust MsgTrans packet codec is pinned to
`b6d13f658b368b676d61d650c7803476a266ce6a`. The verified TLS connection uses
the same `rtc-tls` provider as the existing Rust server.
