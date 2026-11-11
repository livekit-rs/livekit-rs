#!/usr/bin/env python3
"""Compare raw HTTP/3 CONNECT capsule outcomes with the fixed Go daemon."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time


def port(socket_type):
    with socket.socket(socket.AF_INET, socket_type) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def sha256(path):
    with Path(path).open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def load_auth():
    spec = importlib.util.spec_from_file_location("wt_diff", Path(__file__).with_name("run.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.API_KEY, module.SECRET, module.mint_token()


def run(binary, probe, output):
    output.mkdir()
    api_key, secret, token = load_auth()
    http_port, wt_port, rtc_port = port(socket.SOCK_STREAM), port(socket.SOCK_DGRAM), port(socket.SOCK_DGRAM)
    while wt_port == rtc_port:
        rtc_port = port(socket.SOCK_DGRAM)
    config = {
        "development": True,
        "port": http_port,
        "bind_addresses": ["127.0.0.1"],
        "rtc": {"tcp_port": 0, "udp_port": rtc_port, "use_external_ip": False, "node_ip": "127.0.0.1"},
        "webtransport": {"port": wt_port},
        "keys": {api_key: secret},
    }
    config_path = output / "server.yaml"
    config_path.write_text(json.dumps(config))
    probe_exit = None
    setup_error = None
    with (output / "server.log").open("wb") as daemon_log:
        daemon = subprocess.Popen([binary, "--config", config_path], stdout=daemon_log, stderr=daemon_log)
        try:
            deadline = time.monotonic() + 15
            while daemon.poll() is None:
                try:
                    with socket.create_connection(("127.0.0.1", http_port), timeout=0.1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("daemon HTTP readiness timed out")
                    time.sleep(0.1)
            else:
                raise RuntimeError("daemon stopped before HTTP readiness")
            with (output / "wire.jsonl").open("wb") as wire, (output / "probe.log").open("wb") as errors:
                probe_exit = subprocess.run(
                    [probe, "--addr", f"127.0.0.1:{wt_port}"],
                    env={**os.environ, "LIVEKIT_WT_PROBE_TOKEN": token},
                    stdout=wire,
                    stderr=errors,
                    timeout=65,
                ).returncode
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            setup_error = str(error)
        finally:
            if daemon.poll() is None:
                daemon.send_signal(signal.SIGINT)
            try:
                daemon_exit = daemon.wait(timeout=15)
            except subprocess.TimeoutExpired:
                daemon.kill()
                daemon_exit = daemon.wait()
    wire = output / "wire.jsonl"
    rows = [json.loads(line) for line in wire.read_text().splitlines()] if wire.exists() else []
    return {
        "binary_sha256": sha256(binary),
        "config_sha256": sha256(config_path),
        "probe_exit": probe_exit,
        "daemon_exit": daemon_exit,
        "setup_error": setup_error,
        "wire": rows,
    }


def complete(run):
    return run["probe_exit"] == 0 and run["daemon_exit"] == 0 and len(run["wire"]) == 12


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--rust", type=Path)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir()
    expected_close = 0x170D7B68
    go = run(args.go.resolve(), args.probe.resolve(), args.output / "go")
    go_expected = complete(go) and all(
        row["registered"] == (index != 0)
        and row["connect_reset"] == (0x33 if index < 9 else expected_close)
        and row["owned_reset"] == (None if index == 0 else expected_close)
        for index, row in enumerate(go["wire"])
    )
    receipt = {"probe_sha256": sha256(args.probe), "go": go, "go_expected": go_expected}
    success = go_expected
    if args.rust:
        rust = run(args.rust.resolve(), args.probe.resolve(), args.output / "rust")
        success = success and complete(rust) and rust["wire"] == go["wire"]
        receipt.update(rust=rust, same_wire=rust["wire"] == go["wire"])
    (args.output / "execution.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return int(not success)


if __name__ == "__main__":
    raise SystemExit(main())
