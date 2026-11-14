#!/usr/bin/env python3
"""Paired Go-vs-Rust acceptance that the launched MAIN server consumes rtc.force_tcp.

SPEC-0077 C13. Three ledger items share this consumer and are closed together:
  YAML:rtc.force_tcp
  generated_flags:--rtc.force_tcp
  CONFIG_ENABLED_CAPABILITY:rtc.force_tcp

What upstream says, from the locked sources (v1.13.10, e5655ac2):
  references/go-modules/mediatransportutil-f234b534b095/pkg/rtcconfig/webrtc_config.go
    :123  if !rtcConf.ForceTCP { ...add UDP network types + bind UDP mux... }
    :180  if rtcConf.TCPPort != 0 { ...add TCP network types + net.ListenTCP... }
    :201  if len(networkTypes) == 0 { return nil, errors.New("TCP is forced but not configured") }
  references/livekit-stable/pkg/service/roommanager.go:126-128 returns that error UNWRAPPED;
  references/livekit-stable/cmd/server/main.go:190-192 prints `fmt.Println(err)` to STDOUT and
  exits 1. So force_tcp with no tcp_port is a startup refusal whose sentence is byte-identical
  to the Rust ServerError::ForcedTcpWithoutPort (crates/livecloud/src/server.rs:34), printed by
  crates/livecloud/src/lib.rs:193 `println!("{e}")` to STDOUT, exit 1.
  references/livekit-stable/pkg/service/server.go:287-296 logs rtc.portTCP when TCPPort!=0, then
  rtc.portUDP={Start,End} when !ForceTCP && UDPPort.Valid(), else rtc.portICERange=[start,end].
  The Rust reproduces this field-for-field (server.rs:1154-1180) through the zap console encoder.

The Rust consumer sites under test (production, not test-only):
  crates/livecloud/src/server.rs:332-335      force_tcp && tcp_port==0 -> ForcedTcpWithoutPort
  crates/livecloud/src/interfaces.rs:343      udp_enabled = !Disabled && !force_tcp
  crates/livecloud/src/interfaces.rs:267      mux_ports_for_cpu returns [] when force_tcp
  crates/livecloud/src/interfaces.rs:275      standalone_stun_mux_port None when force_tcp
  crates/rtc-runtime/src/net/mod.rs:363,396   UDP muxes bound only if udp_enabled; TCP wildcard
                                              listener bound only if tcp_port is Some
  crates/livecloud/src/server.rs:1160         startup-log portUDP-vs-portICERange branch

Observables, each compared Go-vs-Rust on identical config bytes / argv / ENV:
  1. startup refusal sentence + exit code + HTTP never ready (negative vector)
  2. the actual owned-PID socket topology via lsof: is the configured TCP media port LISTENing,
     is the configured UDP media port bound (force_tcp must drop UDP, keep TCP)
  3. the startup-log port keys (portTCP / portUDP / portICERange) parsed from the zap context
  4. the *selected* ICE candidate pair from a real official-SDK browser media session
     (probe.mjs): local protocol udp/tcp and remote port — the negotiated transport, not a
     parse of offered candidates. Run for the two vectors whose protocol is forced.

Vectors:
  force-tcp-no-port                 force_tcp:true, tcp_port:0          -> startup refusal, byte parity
  force-tcp-with-port               force_tcp:true, tcp_port:T, udp:U   -> TCP-only listeners, portICERange, browser TCP
  force-tcp-cli-flag                config force_tcp unset (default false), argv --rtc.force_tcp=true, tcp_port:T, udp:U
                                    -> TCP-only listeners, portICERange, browser TCP (proves the CLI flag, not just the
                                       YAML leaf, reaches the launched MAIN consumer on this same candidate)
  default-udp                       force_tcp:false, tcp_port:0, udp:U  -> UDP listener, portUDP, browser UDP
  force-tcp-false-with-tcp-port     force_tcp:false, tcp_port:T, udp:U  -> BOTH listeners, portTCP+portUDP

Each side must independently satisfy the vector's `expect` contract AND match the other side's
comparable shape; a pair that is merely equal-but-wrong does not pass. The falsifiability probe
(mutating the force_tcp UDP-drop consumer) is driven separately by the operator and recorded as
a positive control; this launcher only runs the honest contract.

Usage:
  python3 run-paired.py --official /abs/livekit-server-go --rust /abs/livecloud \
      --browser "/abs/Google Chrome for Testing" --interop tests/interop --output DIR
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import re
import signal
import socket
import subprocess
import time
from pathlib import Path

KEY = "devkey"
SECRET = "owned-force-tcp-fixture-secret-0123456789"
ERROR_SENTENCE = "TCP is forced but not configured"
ITEMS = ["YAML:rtc.force_tcp", "generated_flags:--rtc.force_tcp", "CONFIG_ENABLED_CAPABILITY:rtc.force_tcp"]
HERE = Path(__file__).resolve().parent


def sha(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def reservation(kind: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, kind)
    sock.bind(("127.0.0.1", 0))
    return sock


def lsof_ports(pid: int, proto: str) -> set[int]:
    """Ports of `proto` (TCP-LISTEN or UDP) owned by exactly this PID."""
    args = ["/usr/sbin/lsof", "-a", "-nP", "-p", str(pid), "-i" + proto]
    if proto == "TCP":
        args.append("-sTCP:LISTEN")
    done = subprocess.run(args, capture_output=True, timeout=5)
    ports: set[int] = set()
    if done.returncode != 0:
        return ports
    for line in done.stdout.decode().splitlines()[1:]:
        match = re.search(r"(?:\d+\.\d+\.\d+\.\d+|\*|\[[0-9a-fA-F:]+\]):(\d+)", line)
        if match:
            ports.add(int(match.group(1)))
    return ports


def startup_log_ports(stderr_text: str) -> dict:
    """Extract rtc.portTCP / rtc.portUDP / rtc.portICERange from the zap startup line."""
    out = {"portTCP": None, "portUDP": None, "portICERange": None}
    for line in stderr_text.splitlines():
        if "starting LiveKit server" not in line:
            continue
        brace = line.find("{")
        if brace < 0:
            continue
        try:
            context = json.loads(line[brace:])
        except json.JSONDecodeError:
            continue
        out["portTCP"] = context.get("rtc.portTCP")
        out["portUDP"] = context.get("rtc.portUDP")
        out["portICERange"] = context.get("rtc.portICERange")
        break
    return out


def error_line(stdout_text: str) -> str | None:
    for line in stdout_text.splitlines():
        if line.strip() == ERROR_SENTENCE:
            return line.strip()
    return None


def http_ready(port: int) -> bool:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=0.3)
    try:
        conn.request("GET", "/")
        return conn.getresponse().status == 200
    except OSError:
        return False
    finally:
        conn.close()


def build_config(http_port: int, tcp: int, udp: int, vector: dict) -> str:
    rtc = {"node_ip": "127.0.0.1", "use_external_ip": False, "enable_loopback_candidate": True,
           "udp_port": udp, "ips": {"includes": ["127.0.0.1/32"]}}
    rtc.update(vector["rtc"])
    rtc.setdefault("tcp_port", tcp)
    return json.dumps({"port": http_port, "keys": {KEY: SECRET}, "bind_addresses": ["127.0.0.1"],
                       "rtc": rtc, "turn": {"enabled": False}, "logging": {"level": "info"}}, indent=2)


def run_side(side: str, binary: Path, vector: dict, ports: dict, outdir: Path,
             interop: Path, browser: Path, probe: Path) -> dict:
    http_port, tcp, udp = ports["http"], ports["tcp"], ports["udp"]
    side_dir = outdir / vector["id"] / side
    side_dir.mkdir(parents=True, exist_ok=True)
    config_text = build_config(http_port, tcp, udp, vector)
    config_path = side_dir / "config.json"
    config_path.write_text(config_text)
    config_path.chmod(0o600)
    stdout_path = side_dir / "stdout.log"
    stderr_path = side_dir / "stderr.log"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("LIVEKIT_", "LIVECLOUD_"))}
    obs: dict = {"side": side, "binary_sha256": sha(binary), "config_sha256": hashlib.sha256(config_text.encode()).hexdigest(),
                 "ports": ports, "cli": list(vector.get("cli", [])), "reached_http": False, "exit_code": None, "forced": False}
    child = None
    try:
        with stdout_path.open("wb") as so, stderr_path.open("wb") as se:
            argv = [str(binary.resolve()), "--config", str(config_path)] + list(vector.get("cli", []))
            obs["argv"] = argv[1:]
            child = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=so, stderr=se, env=env)
            obs["pid"] = child.pid
            if vector["expect"]["kind"] == "startup-failure":
                # The refusal happens before HTTP; wait for the process to exit on its own.
                deadline = time.monotonic() + 20
                while child.poll() is None and time.monotonic() < deadline:
                    if http_ready(http_port):
                        obs["reached_http"] = True
                    time.sleep(0.05)
                if child.poll() is None:
                    child.send_signal(signal.SIGINT)
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        child.kill(); child.wait(); obs["forced"] = True
                obs["exit_code"] = child.returncode
            else:
                deadline = time.monotonic() + 30
                stable = None
                while time.monotonic() < deadline:
                    assert child.poll() is None, f"{side} main exited before ready"
                    if http_ready(http_port):
                        obs["reached_http"] = True
                        now = time.monotonic()
                        if stable is None:
                            stable = now
                        if now - stable >= 1.0:
                            break
                    else:
                        stable = None
                    time.sleep(0.05)
                else:
                    raise AssertionError(f"{side} 30s startup budget exhausted")
                # Socket topology owned by exactly this PID, before any browser sockets exist.
                tcp_listen = lsof_ports(child.pid, "TCP")
                udp_bound = lsof_ports(child.pid, "UDP")
                obs["tcp_listen_ports"] = sorted(tcp_listen)
                obs["udp_bound_ports"] = sorted(udp_bound)
                obs["tcp_media"] = tcp in tcp_listen
                obs["udp_media"] = udp in udp_bound
                obs["http_listening"] = http_port in tcp_listen
                if vector.get("browser"):
                    probe_out = side_dir / "probe.json"
                    probe_env = dict(env, RTC_URL=f"http://127.0.0.1:{http_port}", INTEROP_DIR=str(interop),
                                     OWNED_CHROMIUM=str(browser), LIVEKIT_API_KEY=KEY, LIVEKIT_API_SECRET=SECRET,
                                     OUTPUT=str(probe_out), EXPECT_PROTOCOL=vector["expect_protocol"],
                                     EXPECT_REMOTE_PORT=str(ports[vector["media_port_key"]]))
                    done = subprocess.run(["node", str(probe)], env=probe_env, capture_output=True, timeout=150)
                    obs["probe_exit"] = done.returncode
                    probe_json = json.loads(probe_out.read_text()) if probe_out.exists() else {"status": "MISSING"}
                    obs["probe_status"] = probe_json.get("status")
                    obs["probe"] = {"protocol": (probe_json.get("transports") or [{}])[0].get("protocol"),
                                    "remote_port": (probe_json.get("transports") or [{}])[0].get("remotePort"),
                                    "dtlsState": (probe_json.get("transports") or [{}])[0].get("dtlsState"),
                                    "transports": probe_json.get("transports", []),
                                    "sdk_sha256": probe_json.get("sdk_sha256"),
                                    "errors": probe_json.get("errors", []),
                                    "cleanup_errors": probe_json.get("cleanup_errors", [])}
                # Clean shutdown of the owned main.
                child.send_signal(signal.SIGINT)
                try:
                    child.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    obs["forced"] = True
                    child.terminate()
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        child.kill(); child.wait()
                obs["exit_code"] = child.returncode
    except Exception as error:
        obs["failure"] = f"{type(error).__name__}: {error}"
        if child and child.poll() is None:
            child.kill(); child.wait()
            obs["exit_code"] = child.returncode
    finally:
        obs["error_line"] = error_line(stdout_path.read_text()) if stdout_path.exists() else None
        obs["log_ports"] = startup_log_ports(stderr_path.read_text()) if stderr_path.exists() else {}
        obs["stdout_sha256"] = sha(stdout_path) if stdout_path.exists() else None
        obs["stderr_sha256"] = sha(stderr_path) if stderr_path.exists() else None
    return obs


def expected_shape(vector: dict, ports: dict) -> dict:
    """The force_tcp contract this vector must satisfy, with concrete port numbers."""
    tcp, udp = ports["tcp"], ports["udp"]
    exp = vector["expect"]
    shape: dict = {"kind": exp["kind"]}
    if exp["kind"] == "startup-failure":
        shape.update(exit_code=exp["exit_code"], error_line=exp["error_line"], reached_http=False)
        return shape
    shape["reached_http"] = True
    shape["tcp_media"] = exp["listeners"]["tcp_media"]
    shape["udp_media"] = exp["listeners"]["udp_media"]
    lp = {}
    for key, val in exp["log_ports"].items():
        if val == "<T>":
            lp[key] = tcp
        elif isinstance(val, dict):
            lp[key] = {k: (udp if v == "<U>" else v) for k, v in val.items()}
        else:
            lp[key] = val
    shape["log_ports"] = lp
    if vector.get("browser"):
        shape["probe"] = {"protocol": exp["probe"]["protocol"],
                          "remote_port": ports[vector["media_port_key"]],
                          "dtlsState": "connected"}
    return shape


def observed_shape(obs: dict) -> dict:
    shape: dict = {"kind": "startup-failure" if obs.get("error_line") else "running",
                   "reached_http": obs.get("reached_http", False)}
    if shape["kind"] == "startup-failure":
        shape.update(exit_code=obs.get("exit_code"), error_line=obs.get("error_line"))
        return shape
    shape["tcp_media"] = obs.get("tcp_media")
    shape["udp_media"] = obs.get("udp_media")
    shape["log_ports"] = obs.get("log_ports", {})
    if "probe" in obs and obs.get("probe_status") is not None:
        p = obs["probe"]
        shape["probe"] = {"protocol": p.get("protocol"), "remote_port": p.get("remote_port"), "dtlsState": p.get("dtlsState")}
    return shape


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("official", "rust", "browser", "interop", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--probe", type=Path, default=HERE / "probe.mjs")
    parser.add_argument("--vectors", nargs="*", default=None, help="subset of vector ids to run")
    args = parser.parse_args()
    args.output.mkdir(mode=0o700, parents=True, exist_ok=True)

    vectors = [
        {"id": "force-tcp-no-port", "rtc": {"force_tcp": True, "tcp_port": 0}, "browser": False,
         "expect": {"kind": "startup-failure", "exit_code": 1, "error_line": ERROR_SENTENCE}},
        {"id": "force-tcp-with-port", "rtc": {"force_tcp": True}, "browser": True,
         "expect_protocol": "tcp", "media_port_key": "tcp",
         "expect": {"kind": "running", "listeners": {"tcp_media": True, "udp_media": False},
                    "log_ports": {"portTCP": "<T>", "portUDP": None, "portICERange": [0, 0]},
                    "probe": {"protocol": "tcp"}}},
        {"id": "force-tcp-cli-flag", "rtc": {}, "cli": ["--rtc.force_tcp=true"], "browser": True,
         "expect_protocol": "tcp", "media_port_key": "tcp",
         "expect": {"kind": "running", "listeners": {"tcp_media": True, "udp_media": False},
                    "log_ports": {"portTCP": "<T>", "portUDP": None, "portICERange": [0, 0]},
                    "probe": {"protocol": "tcp"}}},
        {"id": "default-udp", "rtc": {"force_tcp": False, "tcp_port": 0}, "browser": True,
         "expect_protocol": "udp", "media_port_key": "udp",
         "expect": {"kind": "running", "listeners": {"tcp_media": False, "udp_media": True},
                    "log_ports": {"portTCP": None, "portUDP": {"Start": "<U>", "End": 0}, "portICERange": None},
                    "probe": {"protocol": "udp"}}},
        {"id": "force-tcp-false-with-tcp-port", "rtc": {"force_tcp": False}, "browser": False,
         "expect": {"kind": "running", "listeners": {"tcp_media": True, "udp_media": True},
                    "log_ports": {"portTCP": "<T>", "portUDP": {"Start": "<U>", "End": 0}, "portICERange": None}}},
    ]
    if args.vectors:
        vectors = [v for v in vectors if v["id"] in args.vectors]

    go_sha, rust_sha = sha(args.official), sha(args.rust)
    report = {"status": "RUNNING", "go_binary_sha256": go_sha, "rust_binary_sha256": rust_sha,
              "binaries": {"official_go": str(args.official), "rust": str(args.rust)},
              "items": ITEMS, "probe_sha256": sha(args.probe), "launcher_sha256": sha(Path(__file__)),
              "vectors": [], "failures": []}
    for vector in vectors:
        with reservation(socket.SOCK_STREAM) as h, reservation(socket.SOCK_STREAM) as t, reservation(socket.SOCK_DGRAM) as u:
            ports = {"http": h.getsockname()[1], "tcp": t.getsockname()[1], "udp": u.getsockname()[1]}
        expect = expected_shape(vector, ports)
        go_obs = run_side("go", args.official, vector, ports, args.output, args.interop, args.browser, args.probe)
        rust_obs = run_side("rust", args.rust, vector, ports, args.output, args.interop, args.browser, args.probe)
        go_shape, rust_shape = observed_shape(go_obs), observed_shape(rust_obs)
        go_ok, rust_ok = go_shape == expect, rust_shape == expect
        parity = go_shape == rust_shape
        # The owned main must exit cleanly on first SIGINT for running vectors.
        clean = all(o.get("exit_code") == 0 and not o.get("forced") for o in (go_obs, rust_obs)) \
            if expect["kind"] == "running" else True
        passed = go_ok and rust_ok and parity and clean
        entry = {"id": vector["id"], "ports": ports, "expect": expect,
                 "go": go_obs, "rust": rust_obs, "go_shape": go_shape, "rust_shape": rust_shape,
                 "go_matches_expect": go_ok, "rust_matches_expect": rust_ok, "parity": parity,
                 "clean_exit": clean, "pass": passed}
        report["vectors"].append(entry)
        if not passed:
            report["failures"].append({"id": vector["id"], "go_ok": go_ok, "rust_ok": rust_ok,
                                       "parity": parity, "clean_exit": clean,
                                       "expect": expect, "go_shape": go_shape, "rust_shape": rust_shape})
        print(f"[{vector['id']}] go_ok={go_ok} rust_ok={rust_ok} parity={parity} clean={clean} -> {'PASS' if passed else 'FAIL'}")

    report["status"] = "PASS" if not report["failures"] and report["vectors"] else "FAIL"
    (args.output / "BATCH.json").write_text(json.dumps(report, indent=2) + "\n")
    (args.output / "execution.json").write_text(json.dumps(
        {"status": report["status"], "go_binary_sha256": go_sha, "rust_binary_sha256": rust_sha,
         "vector_count": len(report["vectors"]), "failures": report["failures"]}, indent=2) + "\n")
    print(f"BATCH status={report['status']} vectors={len(report['vectors'])} failures={len(report['failures'])}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())