#!/usr/bin/env python3
"""Run real browser backup-codec publication against owned Go and Rust nodes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request

PROFILES = {
    "regression": -1, "simulcast": 0, "prefer-below": 4, "prefer-equal": 3,
    "unknown-positive": 0, "unknown-negative": 0,
}


def free_port(kind, used):
    # Keep the released port outside the Darwin client ephemeral range.
    for _ in range(100):
        port = random.randrange(27000, 34000)
        if port in used:
            continue
        with socket.socket(socket.AF_INET, kind) as sock:
            try:
                sock.bind(("0.0.0.0", port))
            except OSError:
                continue
        used.add(port)
        return port
    raise RuntimeError("no free probe port")


def run(binary, output, profile, single):
    output.mkdir()
    used = set()
    http = free_port(socket.SOCK_STREAM, used)
    tcp = free_port(socket.SOCK_STREAM, used)
    udp = free_port(socket.SOCK_DGRAM, used)
    config = {
        "port": http, "bind_addresses": ["127.0.0.1"], "keys": {"devkey": "secret"},
        "rtc": {"node_ip": "127.0.0.1", "use_external_ip": False,
                "enable_loopback_candidate": True, "tcp_port": tcp, "udp_port": udp,
                "ips": {"includes": ["127.0.0.1/32"]}},
        "video": {"codec_regression_threshold": PROFILES[profile]},
    }
    url = f"http://127.0.0.1:{http}"
    receipt = {"profile": profile, "single": single, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
    with tempfile.TemporaryDirectory(prefix="lk-backup-codec-") as temp:
        conf = Path(temp) / "server.json"
        conf.write_text(json.dumps(config))
        with (output / "server.log").open("wb") as log:
            proc = subprocess.Popen([str(binary), "--config", str(conf)], stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 20
                while True:
                    if proc.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("server did not start")
                    try:
                        with urllib.request.urlopen(url, timeout=1):
                            break
                    except OSError:
                        time.sleep(0.1)
                env = dict(os.environ, LIVEKIT_URL=url.replace("http:", "ws:", 1),
                           CODEC_OUTPUT=str(output / "sdk.json"), CODEC_PROFILE=profile, SINGLE_PC=single)
                with (output / "probe.log").open("wb") as probe_log:
                    probe = subprocess.run(["node", str(Path(__file__).with_name("run.mjs"))],
                                           env=env, stdout=probe_log, stderr=subprocess.STDOUT, timeout=90)
                receipt["probe_exit"] = probe.returncode
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                receipt["error"] = str(error)
            finally:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGINT)
                try:
                    receipt["server_exit"] = proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    receipt["server_exit"] = proc.wait()
                    receipt["forced_kill"] = True
    try:
        receipt["sdk"] = json.loads((output / "sdk.json").read_text())
    except (OSError, ValueError):
        receipt["sdk"] = {}
    receipt["pass"] = (receipt.get("probe_exit") == receipt["server_exit"] == 0
                       and receipt["sdk"].get("status") == "PASS")
    return receipt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--go", required=True, type=Path)
    parser.add_argument("--rust", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--single", choices=["true", "false"], default="false")
    parser.add_argument("--profile", choices=PROFILES, action="append")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir()
    results = []
    for profile in args.profile or PROFILES:
        row = {"profile": profile}
        for kind in ("go", "rust"):
            row[kind] = run(getattr(args, kind).resolve(), args.output / f"{profile}-{kind}", profile, args.single)
        row["pass"] = all(row[kind]["pass"] for kind in ("go", "rust")) and all(
            row["go"]["sdk"].get(key) == row["rust"]["sdk"].get(key)
            for key in ("sdkSha256", "lockSha256", "browser")
        )
        results.append(row)
        (args.output / "comparison.json").write_text(json.dumps(results, indent=2) + "\n")
        print(profile, "PASS" if row["pass"] else "DIFF", flush=True)
    return 0 if all(row["pass"] for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
