#!/usr/bin/env python3
"""C15 cross-stack layered root-cause driver (SPEC-0074 §2.4 C15).

Starts one server binary (Go oracle or Rust) with the shared agent-wt config and
runs the layered probes against its WebTransport listener, streaming every probe
line plus the server log so the failing layer (QUIC handshake -> h3 SETTINGS ->
WebTransport negotiation -> agent control stream) is unambiguous.

Usage: run-xnet.py --server BIN --label go|rust [--out FILE] [--relay]

With --relay the probes are pointed at `quic-relay.py` instead of the listener
directly, so the same run also captures the decoded Initial/ClientHello wire
evidence and whether the server answers at all.
"""
import argparse
import os
import signal
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import run as diff  # noqa: E402  (reuse config/token/port helpers)

WTSTACK = "/tmp/lk-wt-xnet/wtstack"
WTDIFF = "/tmp/lk-wt-xnet/wtdiff"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--out")
    ap.add_argument("--ready-timeout", type=float, default=30)
    ap.add_argument("--relay", action="store_true",
                    help="put quic-relay.py between the probes and the listener")
    ap.add_argument("--qlog", default="/tmp/lk-wt-xnet/qlog",
                    help="directory for quic-go qlog traces (empty disables)")
    ap.add_argument("--quinn-trace", action="store_true",
                    help="log quinn/quinn_proto at trace level (packet decode decisions)")
    a = ap.parse_args()

    token = diff.mint_token()
    http_port = diff.free_udp()
    wt_port = diff.free_udp()
    lines = []
    with tempfile.TemporaryDirectory(prefix=f"c15xnet-{a.label}-") as home:
        cfg = diff.write_config(home, wt_port, http_port)
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("LIVEKIT_", "LIVECLOUD_", "REDIS_"))}
        rust_log = "livecloud=debug,lk_webtransport=debug,rustls=debug,warn"
        if a.quinn_trace:
            rust_log = "quinn=trace,quinn_proto=trace," + rust_log
        env.update(HOME=home, NO_COLOR="1", RUST_LOG=rust_log)
        proc = subprocess.Popen([a.server, "--config", cfg,
                                 "--keys", f"{diff.API_KEY}: {diff.SECRET}"],
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, env=env, cwd=home,
                                start_new_session=True)
        os.set_blocking(proc.stdout.fileno(), False)
        ready, log = diff.wait_ready(proc, {proc.stdout.fileno(): "out"},
                                     time.time() + a.ready_timeout)
        lines.append(f"# label={a.label} ready={ready} wt_port={wt_port}")
        addr = f"127.0.0.1:{wt_port}"
        relay = None
        if a.relay and ready:
            relay_port = diff.free_udp()
            relay_args = [sys.executable, os.path.join(HERE, "quic-relay.py"),
                          "--listen", f"127.0.0.1:{relay_port}",
                          "--target", f"127.0.0.1:{wt_port}", "--budget", "60"]
            if a.out:
                relay_args += ["--out", a.out + ".relay.log",
                               "--ch-out", a.out + ".clienthello.bin",
                               "--dump", a.out + ".dump"]
            relay = subprocess.Popen(relay_args,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            addr = f"127.0.0.1:{relay_port}"
            lines.append(f"# relay={addr} -> {wt_port}")
            time.sleep(0.3)
        for mode in ("raw", "wt"):
            argv = [WTSTACK, "--addr", addr, "--token", token,
                    "--mode", mode, "--timeout", "12s"]
            if a.qlog:
                os.makedirs(a.qlog, exist_ok=True)
                argv += ["--qlog", a.qlog]
            try:
                p = subprocess.run(argv, capture_output=True, text=True, timeout=30)
                lines.append(f"$ wtstack --mode {mode} -> exit={p.returncode}")
                lines.append(p.stdout.strip() or "(no stdout)")
                if p.stderr.strip():
                    lines.append(f"  stderr: {p.stderr.strip()[:800]}")
            except subprocess.TimeoutExpired:
                lines.append(f"$ wtstack --mode {mode} -> WALL CLOCK TIMEOUT")
        try:
            p = subprocess.run([WTDIFF, "--addr", addr, "--token", token],
                               capture_output=True, text=True, timeout=30)
            lines.append(f"$ wtdiff -> exit={p.returncode}")
            lines.append((p.stdout.strip() or "(no stdout)")[:2000])
            if p.stderr.strip():
                lines.append(f"  stderr: {p.stderr.strip()[:1500]}")
        except subprocess.TimeoutExpired:
            lines.append("$ wtdiff -> WALL CLOCK TIMEOUT")

        # Drain whatever the server logged during the probes.
        time.sleep(0.5)
        tail = b""
        try:
            while True:
                chunk = os.read(proc.stdout.fileno(), 65536)
                if not chunk:
                    break
                tail += chunk
        except BlockingIOError:
            pass
        lines.append("# server log (tail 6000)")
        lines.append((log + tail).decode("utf-8", "replace")[-6000:])
        fulllog = (log + tail).decode("utf-8", "replace")
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10)
        if relay:
            relay.terminate()
            relay.wait(timeout=15)

    text = "\n".join(lines)
    print(text)
    if a.out:
        with open(a.out, "w") as f:
            f.write(text + "\n")
        with open(a.out + ".server.log", "w") as f:
            f.write(fulllog)


if __name__ == "__main__":
    main()
