#!/usr/bin/env python3
"""Launch one owned exact server and one pinned browser SDK probe."""
import argparse
import hashlib
import http.client
import json
import os
import signal
import socket
import subprocess
import time
from pathlib import Path

KEY = "devkey"
SECRET = "owned-rtc-query-fixture-secret-0123456789"


def sha(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def reservation(kind):
    sock = socket.socket(socket.AF_INET, kind)
    sock.bind(("127.0.0.1", 0))
    return sock


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "probe", "interop", "browser", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--engine", choices=("go", "rust-before", "rust-after"), required=True)
    args = parser.parse_args()
    args.output.mkdir(mode=0o700)
    with reservation(socket.SOCK_STREAM) as h, reservation(socket.SOCK_STREAM) as t, reservation(socket.SOCK_DGRAM) as u, reservation(socket.SOCK_DGRAM) as stun:
        http_port, tcp, udp, stun_port = (s.getsockname()[1] for s in (h, t, u, stun))
    config = args.output / "server.json"
    config.write_text(json.dumps({"port": http_port, "keys": {KEY: SECRET}, "bind_addresses": ["127.0.0.1"],
        "rtc": {"tcp_port": tcp, "udp_port": udp, "node_ip": "127.0.0.1", "use_external_ip": False,
                "enable_loopback_candidate": True, "stun_servers": [f"127.0.0.1:{stun_port}"],
                "ips": {"includes": ["127.0.0.1/32"]}},
        "turn": {"enabled": False}, "logging": {"level": "info"}}))
    config.chmod(0o600)
    env = {k: v for k, v in os.environ.items() if not k.startswith("LIVEKIT_")}
    result = {"status": "FAIL", "engine": args.engine, "processes": {},
              "source_sha256": {str(p): sha(p) for p in (
                  args.binary, args.probe, args.browser, Path(__file__),
                  args.interop / "package-lock.json", args.interop / "owned-browser-exit.mjs")}}
    children = []

    def launch(name, command, settings=env):
        log = (args.output / (name + ".log")).open("wb")
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, env=settings)
        children.append((name, child, log))
        result["processes"][name] = {"pid": child.pid}
        return child

    try:
        server = launch("server", [str(args.binary), "--config", str(config)])
        end = time.monotonic() + 10
        while True:
            assert server.poll() is None
            assert time.monotonic() < end, "server health deadline"
            conn = http.client.HTTPConnection("127.0.0.1", http_port, timeout=.2)
            try:
                conn.request("GET", "/")
                assert conn.getresponse().status == 200
                break
            except OSError:
                time.sleep(.05)
            finally:
                conn.close()
        p = launch("node", ["node", str(args.probe)], dict(env, RTC_URL=f"http://127.0.0.1:{http_port}",
            INTEROP_DIR=str(args.interop), OWNED_CHROMIUM=str(args.browser),
            LIVEKIT_API_KEY=KEY, LIVEKIT_API_SECRET=SECRET, OUTPUT=str(args.output / "probe.json")))
        code = p.wait(timeout=115)
        result["probe"] = json.loads((args.output / "probe.json").read_text())
        if code == 0 and result["probe"]["status"] == "PASS":
            result["status"] = "PASS"
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        for name, child, log in reversed(children):
            if child.poll() is None:
                child.send_signal(signal.SIGINT if name == "server" else signal.SIGTERM)
            try:
                code = child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                child.kill()
                code = child.wait()
                result["processes"][name]["forced"] = True
            result["processes"][name]["exit"] = code
            if code != 0:
                result["status"] = "FAIL"
            log.close()
        try:
            for kind, port in ((socket.SOCK_STREAM, http_port), (socket.SOCK_STREAM, tcp), (socket.SOCK_DGRAM, udp)):
                with socket.socket(socket.AF_INET, kind) as sock:
                    if kind == socket.SOCK_STREAM:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock.bind(("127.0.0.1", port))
            result["ports_released"] = True
        except OSError as error:
            result["status"] = "FAIL"
            result["release_error"] = str(error)
        result["source_identity_stable"] = all(sha(Path(p)) == digest for p, digest in result["source_sha256"].items())
        if not result["source_identity_stable"]:
            result["status"] = "FAIL"
        (args.output / "execution.json").write_text(json.dumps(result, indent=2) + "\n")
    return result["status"] != "PASS"


if __name__ == "__main__":
    raise SystemExit(main())
