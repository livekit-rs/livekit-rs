# Verified Go certificate-role and alert comparison

Coordinator builds the standard-library Go witness with the pinned Go1.26.3 toolchain and the Rust `rtc-tls --test go_profile` executable. The runner never builds or cleans either toolchain:

```sh
python3 tests/interop/tls-go-certificate-profile/run-owned.py \
  --test-binary /absolute/path/to/target/debug/deps/go_profile-TEST_EXECUTABLE \
  --go-client /absolute/path/to/prebuilt/go-client \
  --output /absolute/path/to/new/evidence/directory
```

It compares twelve case/version pairs (TLS1.2 and1.3): trusted CA:TRUE end entity, wrong name, wrong root, expired verification time, wrong server EKU and corrupt certificate signature. Accepted connections must exchange actual ping/pong data. Rejected Go-profile connections must produce actual peer `bad_certificate` alerts; raw reason strings and original client errors are retained. The trusted CA-leaf additionally must fail under the ordinary strict WebPKI profile, demonstrating that the default library policy was not relaxed.

Python/OpenSSL is an external fixture peer only. The Rust client uses the actual production `rtc_tls::verified_client_config` consumer. Peer sockets/threads and subprocesses are owned and bounded. Existing public test fixture files remain project inputs; generated temporary files are removed. Receipts bind binaries, witness source, runner and certificate/CA hashes. They do not replace the coordinator's build-input manifest or final global regression.

The selected Python must report `ssl.HAS_TLSv1_3 == True`. macOS system Python linked to LibreSSL2.8.3 cannot run the TLS1.3 half. Select a supported OpenSSL-linked Python (for example Homebrew Python on this workstation); the runner now rejects that missing capability before creating fixture/output state. Both protocol versions remain mandatory.
