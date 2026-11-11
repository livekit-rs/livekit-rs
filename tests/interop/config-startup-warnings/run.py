#!/usr/bin/env python3
"""Check whether a live daemon misreports configured RPC options as ignored."""
import argparse
from contextlib import ExitStack
from hashlib import file_digest
from http.client import HTTPConnection
import json
from pathlib import Path
import signal
import socket
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--go", type=Path, required=True)
parser.add_argument("--rust", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
out = args.output.resolve()
out.mkdir()
report = {}
for side, binary in (("go", args.go.resolve(strict=True)), ("rust", args.rust.resolve(strict=True))):
    with ExitStack() as stack:
        ports = []
        for protocol in (socket.SOCK_STREAM, socket.SOCK_STREAM, socket.SOCK_DGRAM):
            s = stack.enter_context(socket.socket(type=protocol))
            s.bind(("127.0.0.1", 0))
            ports.append(s.getsockname()[1])
    http, tcp, udp = ports
    config = out / f"{side}.yaml"
    config.write_text(f"""port: {http}
bind_addresses: [127.0.0.1]
rtc:
  tcp_port: {tcp}
  udp_port: {udp}
  node_ip: 127.0.0.1
  use_external_ip: false
  enable_loopback_candidate: true
keys:
  devkey: public-psrpc-config-secret-0123456789
signal_relay:
  retry_timeout: 4s
psrpc:
  max_attempts: 5
""")
    with (out / f"{side}.log").open("w+") as log:
        process = subprocess.Popen([binary, "--config", str(config)], stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 10
            for _ in range(200):
                assert process.poll() is None
                conn = HTTPConnection("127.0.0.1", http, timeout=1)
                try:
                    conn.request("GET", "/")
                    ready = conn.getresponse().status == 200
                    if ready:
                        break
                except OSError:
                    pass
                finally:
                    conn.close()
                if time.monotonic() > deadline:
                    raise TimeoutError(side)
                time.sleep(.05)
            else:
                raise TimeoutError(side)
            time.sleep(.2)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                code = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                code = process.wait(timeout=5)
        log.seek(0)
        log_text = log.read()
        with open(binary, "rb") as binary_file:
            report[side] = {
                "binary_sha256": file_digest(binary_file, "sha256").hexdigest(),
                "daemon_exit": code,
                "ready": ready,
                "warnings": [line for line in log_text.splitlines() if "accepted but ignored" in line],
            }
(out / "execution.json").write_text(json.dumps(report, indent=2) + "\n")
raise SystemExit(int(not all(
    row["ready"] and row["daemon_exit"] == 0 and not row["warnings"]
    for row in report.values()
)))
