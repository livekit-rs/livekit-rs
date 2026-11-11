#!/usr/bin/env python3
"""Run the locked Kotlin/Native WSS fixture against the Rust libtest binary."""

import argparse
import os
from pathlib import Path
import socket
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kotlin_test", type=Path)
    parser.add_argument("rust_test", type=Path)
    parser.add_argument("certificates", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    certificate = args.certificates.resolve()
    kotlin_log = args.output / "kotlin.log"
    kotlin_env = dict(
        os.environ,
        LK_CLOUD_FIXTURE_PORT=str(port),
        LK_CLOUD_FIXTURE_CERT=str(certificate / "server.pem"),
        LK_CLOUD_FIXTURE_KEY=str(certificate / "server.key"),
    )
    rust_env = dict(
        os.environ,
        LK_CLOUD_TEST_WSS=f"wss://localhost:{port}/",
        LK_CLOUD_TEST_CA=str(certificate / "ca.pem"),
        LK_CLOUD_TEST_OTHER_CA=str(certificate / "other-ca.pem"),
    )
    with kotlin_log.open("wb") as server_output:
        server = subprocess.Popen(
            [str(args.kotlin_test.resolve()), "--ktest_filter=msgtrans.transport.CloudControlFixture.*"],
            stdout=server_output,
            stderr=subprocess.STDOUT,
            env=kotlin_env,
        )
        try:
            deadline = time.monotonic() + 5
            while "SPEC0078 fixture listening" not in kotlin_log.read_text():
                if time.monotonic() >= deadline or server.poll() is not None:
                    raise RuntimeError("Kotlin WSS fixture did not start")
                time.sleep(0.05)
            with (args.output / "rust.log").open("wb") as node_output:
                node = subprocess.run(
                    [
                        str(args.rust_test.resolve()),
                        "cloud_control::tests::kotlin_native_wss_registration_snapshot_ack_and_heartbeat",
                        "--ignored",
                        "--exact",
                        "--nocapture",
                    ],
                    stdout=node_output,
                    stderr=subprocess.STDOUT,
                    timeout=15,
                    env=rust_env,
                )
            kotlin_exit = server.wait(timeout=18)
            print(f"rust_exit={node.returncode} kotlin_exit={kotlin_exit}")
            if "1 passed" not in (args.output / "rust.log").read_text():
                return 1
            if "SPEC0078 fixture verified 1,3,4,2,3,4,2,1,3,4,2" not in kotlin_log.read_text():
                return 1
            return int(node.returncode != 0 or kotlin_exit != 0)
        finally:
            if server.poll() is None:
                server.kill()
                server.wait()


if __name__ == "__main__":
    raise SystemExit(main())
