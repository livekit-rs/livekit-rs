#!/usr/bin/env python3
"""Validate quic-relay.py against a known-good exchange and capture forensics.

Starts a server binary, puts the relay in front of its WebTransport listener and
runs one probe through the relay, so the relay's own Initial/ClientHello decoding
is exercised on a path that is *expected to work* (Rust client -> Rust server) as
well as on the failing cross-stack path.

Usage: relay-check.py --server BIN --probe BIN [--label LABEL] [--out DIR]
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import run as diff  # noqa: E402  (config/token/port helpers)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--probe", required=True, action="append")
    ap.add_argument("--label", default="rust")
    ap.add_argument("--out", default="/tmp/lk-wt-xnet")
    ap.add_argument("--wt-port", type=int, default=0,
                    help="fixed listener port (0 = pick a free one)")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    token = diff.mint_token()
    home = tempfile.mkdtemp(prefix=f"c15relay-{a.label}-")
    wt_port = a.wt_port or diff.free_udp()
    http_port = diff.free_udp()
    relay_port = diff.free_udp()
    cfg = diff.write_config(home, wt_port, http_port)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("LIVEKIT_", "LIVECLOUD_", "REDIS_"))}
    env.update(HOME=home, NO_COLOR="1")
    srv_log = open(os.path.join(a.out, f"server-{a.label}.log"), "wb")
    proc = subprocess.Popen([a.server, "--config", cfg,
                             "--keys", f"{diff.API_KEY}: {diff.SECRET}"],
                            stdin=subprocess.DEVNULL, stdout=srv_log,
                            stderr=subprocess.STDOUT, env=env, cwd=home,
                            start_new_session=True)
    deadline = time.time() + 30
    ready = False
    while time.time() < deadline and not ready:
        time.sleep(0.3)
        try:
            with open(os.path.join(a.out, f"server-{a.label}.log"), "rb") as f:
                ready = b"starting LiveKit server" in f.read()
        except OSError:
            pass
    print(f"server ready={ready} wt_port={wt_port} http_port={http_port}")
    if not ready:
        proc.kill()
        sys.exit(1)

    relay_out = os.path.join(a.out, f"relay-{a.label}.log")
    relay_args = [sys.executable, os.path.join(HERE, "quic-relay.py"),
                  "--listen", f"127.0.0.1:{relay_port}",
                  "--target", f"127.0.0.1:{wt_port}",
                  "--out", relay_out,
                  "--ch-out", os.path.join(a.out, f"clienthello-{a.label}.bin"),
                  "--dump", os.path.join(a.out, f"dump-{a.label}"),
                  "--budget", "90"]
    relay = subprocess.Popen(relay_args, stdout=subprocess.DEVNULL,
                             stderr=open(os.path.join(a.out, f"relay-{a.label}.stderr"), "w"))
    time.sleep(0.4)
    addr = f"127.0.0.1:{relay_port}"
    results = []
    for probe in a.probe:
        argv = [probe, "--addr", addr, "--token", token]
        start = time.time()
        try:
            p = subprocess.run(argv, capture_output=True, text=True, timeout=40)
            results.append({"probe": probe, "exit": p.returncode,
                            "stdout": p.stdout.strip()[-1500:],
                            "stderr": p.stderr.strip()[-1500:],
                            "secs": round(time.time() - start, 1)})
        except subprocess.TimeoutExpired:
            results.append({"probe": probe, "exit": "timeout", "secs":
                            round(time.time() - start, 1)})
    # Give the relay a moment to flush the reply direction.
    time.sleep(1.0)
    with open(os.path.join(a.out, f"probes-{a.label}.json"), "w") as f:
        json.dump(results, f, indent=1)
    for r in results:
        print(f"\n$ {os.path.basename(r['probe'])} exit={r['exit']} {r['secs']}s")
        print(r.get("stdout") or r.get("stderr"))
    relay.terminate()
    try:
        relay.wait(timeout=10)
    except subprocess.TimeoutExpired:  # the dump is best-effort evidence
        relay.kill()
        relay.wait(timeout=10)
    os.killpg(proc.pid, signal.SIGKILL)
    proc.wait(timeout=10)
    srv_log.close()
    print(f"\n--- relay log ({relay_out}) ---")
    with open(relay_out) as f:
        print(f.read()[:12000])


if __name__ == "__main__":
    main()
