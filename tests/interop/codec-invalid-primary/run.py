#!/usr/bin/env python3
"""Compare a negotiated VP9-to-VP8 payload change on the original publisher SSRC."""
import argparse
import hashlib
import json
from pathlib import Path
import random
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request


def ports():
    selected = set()
    for kind in (socket.SOCK_STREAM, socket.SOCK_STREAM, socket.SOCK_DGRAM):
        for _ in range(100):
            port = random.randrange(27000, 34000)
            if port in selected:
                continue
            with socket.socket(socket.AF_INET, kind) as probe:
                try:
                    probe.bind(("0.0.0.0", port))
                except OSError:
                    continue
            selected.add(port)
            yield port
            break
        else:
            raise RuntimeError("no free probe port")


def digest(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def run(binary, args, policy, single, output):
    output.mkdir()
    http, tcp, udp = ports()
    url = f"http://127.0.0.1:{http}"
    config = {
        "port": http, "bind_addresses": ["127.0.0.1"], "keys": {"devkey": "secret"},
        "rtc": {"node_ip": "127.0.0.1", "use_external_ip": False,
                "enable_loopback_candidate": True, "tcp_port": tcp, "udp_port": udp,
                "ips": {"includes": ["127.0.0.1/32"]}},
        "video": {"codec_regression_threshold": -1},
        "logging": {"level": "debug"},
    }
    receipt = {"binary_sha256": digest(binary), "policy": policy, "single": single}
    with tempfile.TemporaryDirectory(prefix="lk-invalid-codec-") as temp:
        path = Path(temp) / "server.json"
        path.write_text(json.dumps(config))
        with (output / "server.log").open("wb") as log:
            server = subprocess.Popen([binary, "--config", path], stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 15
                while True:
                    if server.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("server did not become ready")
                    try:
                        with urllib.request.urlopen(url, timeout=1):
                            break
                    except OSError:
                        time.sleep(.1)
                with (output / "probe.json").open("wb") as out, (output / "probe.stderr").open("wb") as err:
                    result = subprocess.run([args.probe, "--url", url, "--vp8", args.vp8, "--vp9", args.vp9,
                                             "--injected", args.injected, "--policy", policy, f"--single={str(single).lower()}"],
                                            stdout=out, stderr=err, timeout=45)
                receipt["probe_exit"] = result.returncode
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                receipt["error"] = str(error)
            finally:
                if server.poll() is None:
                    server.send_signal(signal.SIGINT)
                try:
                    receipt["server_exit"] = server.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    server.kill()
                    receipt["server_exit"] = server.wait()
    try:
        receipt["probe"] = json.loads((output / "probe.json").read_text())
    except (OSError, ValueError):
        receipt["probe"] = {}
    receipt["pass"] = (receipt.get("probe_exit") == receipt["server_exit"] == 0
                       and receipt["probe"].get("status") == "PASS")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("go", "rust", "probe", "vp8", "vp9", "injected", "output"):
        parser.add_argument(f"--{name}", required=True, type=lambda path: Path(path).resolve())
    parser.add_argument("--policy", choices=("prefer", "unknown", "simulcast"), action="append")
    parser.add_argument("--single", choices=("true", "false"), action="append")
    args = parser.parse_args()
    args.output.mkdir()
    results = []
    for single in args.single or ("false", "true"):
        for policy in args.policy or ("prefer", "unknown", "simulcast"):
            row = {"policy": policy, "single": single == "true"}
            for engine in ("go", "rust"):
                row[engine] = run(getattr(args, engine), args, policy, single == "true",
                                  args.output / f"{single}-{policy}-{engine}")
            row["pass"] = row["go"]["pass"] and row["rust"]["pass"]
            results.append(row)
            (args.output / "execution.json").write_text(json.dumps({
                "probe_sha256": digest(args.probe), "vp8_sha256": digest(args.vp8),
                "vp9_sha256": digest(args.vp9), "injected_sha256": digest(args.injected), "cases": results,
            }, indent=2) + "\n")
            print(single, policy, row["go"]["pass"], row["rust"]["pass"], flush=True)
    return int(not all(row["pass"] for row in results))


if __name__ == "__main__":
    raise SystemExit(main())
