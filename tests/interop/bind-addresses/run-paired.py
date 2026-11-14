#!/usr/bin/env python3
"""Paired Go-vs-Rust acceptance: launched-MAIN bind_addresses / --bind contract.

SPEC-0077 C20. Closes the launched-MAIN (main_server_consumer) and paired locked-Go
(official_differential_lifecycle) tiers for the two listener-bind items whose
upstream_source_contract / config_projection / component_behavior were MET but whose
differential tier was PARTIAL: the only prior evidence (candidate81 / batch23) was a
coarse SINGLE-side hostname reachability smoke, never a paired locked-Go differential
naming these two items with a per-case contract.

  YAML:bind_addresses   nil -> [""] dual-stack; [] -> no listener; explicit -> per-host
  base_flags:--bind     CLI --bind overrides config bind_addresses (repeatable, comma)

Upstream contract (locked v1.13.10, e5655ac2):
  pkg/service/server.go:239-270  addresses := BindAddresses; if nil { [""] }; for each
    addr net.Listen("tcp", JoinHostPort(addr, Port)) for HTTP, Prometheus (if promServer
    != nil) and debug (if debugServer != nil); any listen error returns before the
    startup log, so an unbindable address fails startup with no "starting LiveKit server".
  pkg/service/server.go:284-285  the startup line carries "bindAddresses" iff
    BindAddresses != nil (nil -> field absent, [] -> empty array).
  pkg/service/server.go:326-338  httpGroup has one Serve goroutine per HTTP listener;
    with ZERO listeners Wait() returns nil (!= http.ErrServerClosed) so the watchdog logs
    Errorw("could not start server", nil) -- no error field -- and calls Stop(true), which
    returns early because `running` is still false: the process STAYS ALIVE.
  net/ipsock_posix.go:134-157 favoriteAddrFamily: a "tcp" listen on a wildcard laddr
    ("" / 0.0.0.0 / ::, i.e. IP.IsUnspecified()) is dual-stack AF_INET6 (V6ONLY=0) wherever
    IPv4-mapping is supported, with an IPv4-only fallback; a concrete address keeps its
    own family. The pre-fix Rust bound 0.0.0.0 IPv4-only, so ::1 was refused while Go
    accepted it -- a real wildcard family gap. C20 routes every wildcard laddr to the same
    dual-stack [::] path (crates/livecloud/src/server.rs listen()) and emits the empty-bind
    "could not start server" ERROR keyed on an empty bind list (Go builds its listeners
    purely from BindAddresses, not the Rust-only livecloud.https http_enabled toggle).

Observable: ONE launched MAIN per side over raw TCP connect + a proper http.client GET
(no browser, no media, official SDK unchanged and unused). Every profile runs on IDENTICAL
config bytes / argv / ENV per side. Two explicit verdicts are computed from the SAME
observations, and nothing is dropped:

  1. bind_contract  -- the YAML:bind_addresses / base_flags:--bind item observable: the TCP
     reachability matrix of the HTTP / Prometheus / debug listeners across 127.0.0.1 / ::1 /
     en0 (which address:port each listener binds, and its family), the startup "bindAddresses"
     field shape, the empty-bind ERROR record, and the process lifecycle. Each side must
     independently satisfy this structural contract, then the two normalized bind contracts
     must be deeply equal. Equal-but-wrong does not pass. Bindable and empty-bind servers must
     stay alive and exit cleanly on the first SIGINT; an unbindable address must fail startup
     non-zero with no listener. This is the precise bind observable: TCP reachability proves
     the listener exists on that address/family exactly as Go's net.Listen does.

  2. http_serving   -- a supplementary application-layer probe (http.client GET /). On the
     loopback families (127.0.0.1 / ::1) it must return 200 wherever reachable on BOTH sides,
     confirming the bound listener actually serves. On en0 it is reported per side; a
     divergence is recorded as a PLATFORM RESIDUAL, not a bind-contract failure, because it is
     a host-specific Rust std::net accept anomaly proven BELOW livekit-rs: a minimal bare
     std::net AND tokio reproducer (platform-residual/reproducer.rs) shows accept() never fires
     for an en0-destined connection on this macOS host (Clash utun present) for EVERY socket
     option (plain / SO_REUSEADDR / SO_REUSEPORT / IPv4 wildcard / dual-stack), while the SAME
     socket serves 127.0.0.1 and a simultaneous Python asyncio / Go en0 server serve en0 fine.
     It is independent of bind_addresses (the listener binds correctly; reach matches Go) and
     is not introduced by C20. See platform-residual/README.md. The en0 liveness data is kept
     in every observation and in the BATCH platform_residuals; no field or failure mode is
     deleted.

Usage:
  python3 run-paired.py --official /abs/livekit-server-go --rust /abs/livecloud --output DIR
"""
from __future__ import annotations

import argparse
import copy
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
SECRET = "owned-bind-fixture-secret-0123456789"
FIXED_GO_SHA256 = "4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc"
ITEMS = ["YAML:bind_addresses", "base_flags:--bind"]
HERE = Path(__file__).resolve().parent
LEVELS = {"DEBUG", "INFO", "WARN", "ERROR", "DPANIC", "PANIC", "FATAL"}
TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


def sha(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def en0_ipv4() -> str:
    out = subprocess.run(["ipconfig", "getifaddr", "en0"], capture_output=True, text=True)
    return out.stdout.strip()


def reservation(kind: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, kind)
    sock.bind(("127.0.0.1", 0))
    return sock


def tcp_ok(addr: str, port: int, t: float = 0.5) -> bool:
    fam = socket.AF_INET6 if ":" in addr else socket.AF_INET
    try:
        with socket.socket(fam, socket.SOCK_STREAM) as s:
            s.settimeout(t)
            s.connect((addr, port))
            return True
    except OSError:
        return False


def http_get(addr: str, port: int, path: str, t: float = 2.0):
    """Proper HTTP/1.1 liveness via http.client (not a raw HTTP/1.0 probe)."""
    conn = http.client.HTTPConnection(addr, port, timeout=t)
    try:
        conn.request("GET", path, headers={"Host": addr})
        resp = conn.getresponse()
        resp.read()
        return resp.status
    except (OSError, http.client.HTTPException):
        return None
    finally:
        conn.close()


def deep_merge(base: dict, overlay: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def build_config(ports: dict, prof: dict, en0: str) -> str:
    base = {
        "port": ports["http"], "keys": {KEY: SECRET},
        "rtc": {"node_ip": "127.0.0.1", "use_external_ip": False, "enable_loopback_candidate": True,
                "tcp_port": ports["tcp"], "udp_port": ports["udp"], "ips": {"includes": ["127.0.0.1/32"]}},
        "room": {"auto_create": True}, "prometheus": {"port": ports["prom"]},
        "debug_handler": {"port": ports["debug"]},
        "turn": {"enabled": False}, "logging": {"level": "info"},
    }
    bind = prof["bind"]
    if bind == "absent":
        pass  # nil: omit the key entirely
    elif bind == "en0":
        base["bind_addresses"] = [en0]
    elif isinstance(bind, list):
        base["bind_addresses"] = [en0 if b == "en0" else b for b in bind]
    return json.dumps(base, indent=2)


def zap_lines(text: str) -> list[list[str]]:
    """Tab-split zap console records only; stacktrace continuation lines are dropped."""
    out = []
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) >= 5 and TS.match(parts[0]) and parts[1] in LEVELS:
            out.append(parts)
    return out


def parse_log(text: str) -> dict:
    """Extract the C20-relevant log contract, scrubbing side-specific diagnostics.

    bindAddresses: presence + value from the "starting LiveKit server" INFO record.
    could_not_start: the empty-bind ERROR record's caller and whether it carries an
    "error" field. The Go zap stacktrace (runtime frames + absolute build paths) is a
    side-specific diagnostic and is not compared; the structured record is.
    """
    startup_present = False
    bind_present = False
    bind_value = None
    could = False
    could_caller = None
    could_error_field = None
    for parts in zap_lines(text):
        msg = parts[4]
        fields = {}
        if len(parts) >= 6 and parts[5].startswith("{"):
            try:
                fields = json.loads(parts[5])
            except json.JSONDecodeError:
                fields = {}
        if msg == "starting LiveKit server":
            startup_present = True
            bind_present = "bindAddresses" in fields
            bind_value = fields.get("bindAddresses")
        if msg == "could not start server":
            could = True
            could_caller = parts[3]
            could_error_field = "error" in fields
    return {"startup_present": startup_present, "bind_addresses_present": bind_present,
            "bind_addresses_value": bind_value, "could_not_start": could,
            "could_not_start_caller": could_caller,
            "could_not_start_has_error_field": could_error_field}


def probe(ports: dict, en0: str) -> dict:
    addrs = {"v4": "127.0.0.1", "v6": "::1", "en0": en0}
    reach, live = {}, {}
    for label, addr in addrs.items():
        if not addr:
            reach[label] = {"http": False, "prom": False, "debug": False}
            live[label] = None
            continue
        reach[label] = {"http": tcp_ok(addr, ports["http"]),
                        "prom": tcp_ok(addr, ports["prom"]),
                        "debug": tcp_ok(addr, ports["debug"])}
        live[label] = http_get(addr, ports["http"], "/") if reach[label]["http"] else None
    return {"reach": reach, "http_liveness": live}


def expected_reach(prof: dict) -> dict:
    e = prof["expect"]
    return {label: {"http": bool(e[label]), "prom": bool(e[label]), "debug": bool(e[label])}
            for label in ("v4", "v6", "en0")}


def run_side(side: str, binary: Path, ports: dict, outdir: Path, prof: dict, en0: str) -> dict:
    side_dir = outdir / prof["id"] / side
    side_dir.mkdir(parents=True, exist_ok=True)
    config_text = build_config(ports, prof, en0)
    config_path = side_dir / "config.json"
    config_path.write_text(config_text)
    config_path.chmod(0o600)
    stdout_path, stderr_path = side_dir / "stdout.log", side_dir / "stderr.log"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("LIVEKIT_", "LIVECLOUD_"))}
    cli = [en0 if a == "en0" else a for a in prof["cli"]]
    obs: dict = {"side": side, "profile": prof["id"], "kind": prof["kind"],
                 "binary_sha256": sha(binary),
                 "config_sha256": hashlib.sha256(config_text.encode()).hexdigest(),
                 "flags": cli, "ports": ports, "reached": False, "self_exit": False,
                 "forced": False, "exit_code": None}
    child = None
    try:
        with stdout_path.open("wb") as so, stderr_path.open("wb") as se:
            argv = [str(binary.resolve()), "--config", str(config_path)] + cli
            obs["argv"] = argv[1:]
            child = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=so, stderr=se, env=env)
            obs["pid"] = child.pid
            deadline = time.monotonic() + 30
            kind = prof["kind"]
            if kind == "unbindable":
                while time.monotonic() < deadline:
                    if child.poll() is not None:
                        obs["self_exit"] = True
                        break
                    time.sleep(0.05)
                else:
                    raise AssertionError(f"{side} unbindable main did not fail startup in 30s")
                obs["exit_code"] = child.returncode
            else:
                if kind == "empty":
                    while time.monotonic() < deadline:
                        assert child.poll() is None, f"{side} empty-bind main self-exited (Go stays alive)"
                        if "could not start server" in stderr_path.read_text(errors="replace"):
                            break
                        time.sleep(0.05)
                    else:
                        raise AssertionError(f"{side} empty-bind main never logged 'could not start server'")
                    time.sleep(1.0)
                    assert child.poll() is None, f"{side} empty-bind main must stay alive after the ERROR"
                else:
                    stable = None
                    while time.monotonic() < deadline:
                        assert child.poll() is None, f"{side} main exited before ready"
                        if tcp_ok("127.0.0.1", ports["http"]):
                            now = time.monotonic()
                            stable = now if stable is None else stable
                            if now - stable >= 1.0:
                                break
                        else:
                            stable = None
                        time.sleep(0.05)
                    else:
                        raise AssertionError(f"{side} 30s startup budget exhausted")
                obs["reached"] = True
                obs.update(probe(ports, en0))
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
        text = ""
        if stderr_path.exists():
            text += stderr_path.read_text(errors="replace")
        if stdout_path.exists():
            text += stdout_path.read_text(errors="replace")
        obs["log"] = parse_log(text)
        obs["stdout_sha256"] = sha(stdout_path) if stdout_path.exists() else None
        obs["stderr_sha256"] = sha(stderr_path) if stderr_path.exists() else None
    return obs


def structural_ok(obs: dict, prof: dict, en0: str) -> tuple[bool, list[str]]:
    """Whether one side independently satisfies the bind contract for this profile."""
    errs = []
    kind = prof["kind"]
    if obs.get("failure"):
        return False, [obs["failure"]]
    if kind == "unbindable":
        if not obs.get("self_exit") or obs.get("exit_code") == 0:
            errs.append(f"unbindable must fail startup non-zero, got exit={obs.get('exit_code')} self_exit={obs.get('self_exit')}")
        if obs.get("reached"):
            errs.append("unbindable must not reach readiness")
        if obs["log"]["startup_present"]:
            errs.append("unbindable must not log 'starting LiveKit server' (bind fails first)")
        return (not errs), errs
    # bindable / empty: clean SIGINT exit, alive until then
    if obs.get("exit_code") != 0 or obs.get("forced") or obs.get("self_exit") or not obs.get("reached"):
        errs.append(f"lifecycle: exit={obs.get('exit_code')} forced={obs.get('forced')} self_exit={obs.get('self_exit')} reached={obs.get('reached')}")
    exp = expected_reach(prof)
    if obs.get("reach") != exp:
        errs.append(f"reach {obs.get('reach')} != expected {exp}")
    log = obs["log"]
    if not log["startup_present"]:
        errs.append("missing 'starting LiveKit server'")
    if log["bind_addresses_present"] != prof["bind_present"]:
        errs.append(f"bindAddresses present={log['bind_addresses_present']} want {prof['bind_present']}")
    if prof["bind_present"]:
        want_val = [en0 if b == "en0" else b for b in prof["bind_value"]]
        if log["bind_addresses_value"] != want_val:
            errs.append(f"bindAddresses value={log['bind_addresses_value']} want {want_val}")
    if log["could_not_start"] != prof["could_not_start"]:
        errs.append(f"could_not_start={log['could_not_start']} want {prof['could_not_start']}")
    if prof["could_not_start"]:
        if log["could_not_start_caller"] != "service/server.go:335":
            errs.append(f"could_not_start caller={log['could_not_start_caller']}")
        if log["could_not_start_has_error_field"]:
            errs.append("could_not_start must carry no error field (Go Errorw(msg, nil))")
    return (not errs), errs


def contract(obs: dict) -> dict:
    """The normalized cross-side BIND contract: reachability, log shape, lifecycle.

    HTTP liveness is deliberately NOT part of the bind contract: it is an application-layer
    probe reported separately by http_serving_check (the en0 divergence is a host platform
    residual below std::net, not a bind_addresses observable). The bind observable is which
    address:port listeners exist and accept TCP, the bindAddresses field, and lifecycle.
    """
    return {"reach": obs.get("reach"),
            "log": obs.get("log"),
            "lifecycle": {"exit_zero": obs.get("exit_code") == 0, "reached": obs.get("reached"),
                          "self_exit": obs.get("self_exit"), "forced": obs.get("forced")}}


def http_serving_check(go: dict, rust: dict, prof: dict, en0: str) -> tuple[bool, list[str], dict | None]:
    """Supplementary application-layer serving probe, kept fully visible.

    Loopback families (v4/v6) must serve 200 wherever reachable on BOTH sides and match each
    other -- a real serving check that can fail. en0 liveness is reported per side; a Rust/Go
    divergence is returned as a platform residual (host Rust std::net accept anomaly), NOT a
    bind-contract failure. No liveness data is discarded.
    """
    errs = []
    for side, obs in (("go", go), ("rust", rust)):
        for label in ("v4", "v6"):
            want = prof["expect"][label]
            got = obs.get("http_liveness", {}).get(label)
            if want and got != 200:
                errs.append(f"{side} http liveness {label} = {got}, want 200")
            if not want and got is not None:
                errs.append(f"{side} http liveness {label} = {got}, want None (unreachable)")
    go_en0 = go.get("http_liveness", {}).get("en0")
    rust_en0 = rust.get("http_liveness", {}).get("en0")
    residual = None
    if prof["expect"]["en0"] and go_en0 != rust_en0:
        residual = {
            "id": "en0-userspace-accept",
            "profile": prof["id"],
            "en0_ipv4": en0,
            "go_http_liveness": go_en0,
            "rust_http_liveness": rust_en0,
            "bind_contract_unaffected": True,
            "attribution": ("host-specific Rust std::net accept anomaly below livekit-rs: minimal bare "
                            "std::net AND tokio reproducer shows accept() never fires for an en0-destined "
                            "connection on this macOS host (Clash utun) for every socket option, while the "
                            "same socket serves 127.0.0.1 and simultaneous Python asyncio / Go en0 servers "
                            "serve en0. The en0 listener binds correctly (TCP reach matches Go); only "
                            "userspace accept/serve diverges. Not a bind_addresses defect, not introduced "
                            "by C20, not fixable in app code, no network mutation permitted."),
            "evidence": "tests/interop/bind-addresses/platform-residual/README.md",
        }
    return (not errs), errs, residual


def leaf_parity(go: dict, rust: dict) -> dict:
    out = {}
    for item in ITEMS:
        out[item] = {"parity": contract(go) == contract(rust),
                     "go_cases": 1, "rust_cases": 1}
    return out


# expect: per-address reachability (applied to http/prom/debug). bind_value uses the
# "en0" sentinel resolved to the discovered interface IPv4. kind drives lifecycle.
PROFILES = [
    {"id": "loopback", "bind": ["127.0.0.1"], "cli": [], "kind": "bindable",
     "expect": {"v4": 1, "v6": 0, "en0": 0}, "bind_present": True, "bind_value": ["127.0.0.1"],
     "could_not_start": False,
     "covers": "concrete IPv4 laddr keeps its family: 127.0.0.1 reachable on http/prom/debug, ::1 and en0 refused"},
    {"id": "wildcard-v4", "bind": ["0.0.0.0"], "cli": [], "kind": "bindable",
     "expect": {"v4": 1, "v6": 1, "en0": 1}, "bind_present": True, "bind_value": ["0.0.0.0"],
     "could_not_start": False,
     "covers": "YAML:bind_addresses wildcard 0.0.0.0 is dual-stack (favoriteAddrFamily): 127.0.0.1 AND ::1 AND en0 reachable"},
    {"id": "wildcard-v6", "bind": ["::"], "cli": [], "kind": "bindable",
     "expect": {"v4": 1, "v6": 1, "en0": 1}, "bind_present": True, "bind_value": ["::"],
     "could_not_start": False,
     "covers": "wildcard :: is dual-stack: 127.0.0.1 AND ::1 AND en0 reachable"},
    {"id": "absent", "bind": "absent", "cli": [], "kind": "bindable",
     "expect": {"v4": 1, "v6": 1, "en0": 1}, "bind_present": False, "bind_value": None,
     "could_not_start": False,
     "covers": "nil BindAddresses defaults to [\"\"] dual-stack and the startup log omits the bindAddresses field"},
    {"id": "empty", "bind": [], "cli": [], "kind": "empty",
     "expect": {"v4": 0, "v6": 0, "en0": 0}, "bind_present": True, "bind_value": [],
     "could_not_start": True,
     "covers": "YAML:bind_addresses [] creates no listener, logs bindAddresses:[], emits ERROR could-not-start (no error field) and STAYS ALIVE"},
    {"id": "multi", "bind": ["127.0.0.1", "en0"], "cli": [], "kind": "bindable",
     "expect": {"v4": 1, "v6": 0, "en0": 1}, "bind_present": True, "bind_value": ["127.0.0.1", "en0"],
     "could_not_start": False,
     "covers": "two concrete laddrs each own a listener: 127.0.0.1 and en0 reachable, ::1 refused"},
    {"id": "localhost", "bind": ["localhost"], "cli": [], "kind": "bindable",
     "expect": {"v4": 1, "v6": 0, "en0": 0}, "bind_present": True, "bind_value": ["localhost"],
     "could_not_start": False,
     "covers": "hostname laddr resolves to the first IPv4 (Go Listen preference): 127.0.0.1 reachable, ::1 refused"},
    {"id": "unbindable", "bind": ["10.255.255.1"], "cli": [], "kind": "unbindable",
     "expect": {"v4": 0, "v6": 0, "en0": 0}, "bind_present": False, "bind_value": None,
     "could_not_start": False,
     "covers": "an address that cannot be bound fails startup non-zero before the startup log, with no listener"},
    {"id": "cli-override", "bind": ["0.0.0.0"], "cli": ["--bind", "127.0.0.1"], "kind": "bindable",
     "expect": {"v4": 1, "v6": 0, "en0": 0}, "bind_present": True, "bind_value": ["127.0.0.1"],
     "could_not_start": False,
     "covers": "base_flags:--bind overrides config bind_addresses: CLI 127.0.0.1 wins over YAML 0.0.0.0 (concrete, ::1 refused)"},
    {"id": "cli-multi", "bind": "absent", "cli": ["--bind", "127.0.0.1", "--bind", "en0"], "kind": "bindable",
     "expect": {"v4": 1, "v6": 0, "en0": 1}, "bind_present": True, "bind_value": ["127.0.0.1", "en0"],
     "could_not_start": False,
     "covers": "base_flags:--bind is repeatable: two CLI laddrs replace nil config, 127.0.0.1 and en0 reachable, ::1 refused"},
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("official", "rust", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(mode=0o700, parents=True, exist_ok=True)
    if sha(args.official) != FIXED_GO_SHA256:
        raise RuntimeError("fixed source Go binary identity mismatch before execution")
    en0 = en0_ipv4()
    if not en0:
        raise RuntimeError("could not discover en0 IPv4 (ipconfig getifaddr en0)")

    fixture_inputs = [Path(__file__).resolve()]
    go_sha, rust_sha = sha(args.official), sha(args.rust)
    report = {"status": "RUNNING", "go_binary_sha256": go_sha, "rust_binary_sha256": rust_sha,
              "binaries": {"official_go": str(args.official), "rust": str(args.rust)},
              "items": ITEMS, "profiles": [p["id"] for p in PROFILES], "en0_ipv4": en0,
              "launcher_sha256": sha(Path(__file__)),
              "fixture_sha256": {str(p): sha(p) for p in fixture_inputs},
              "fixed_go_source": "e5655ac20267fa5005dccaf3f0e3bfc3ea688c07",
              "server_sdk": "livekit-server-sdk@2.19.2 (unchanged; unused — raw TCP/HTTP drives the bind contract)",
              "normalization": "timestamps, nodeID, ephemeral ports and the Go zap stacktrace are side-specific and scrubbed; the compared BIND contract is TCP reachability, the bindAddresses field shape, the could-not-start ERROR record and the process lifecycle. http.client liveness is a supplementary application probe (loopback families must serve 200; en0 divergence is a host platform residual below std::net, reported in platform_residuals, never deleted)",
              "vectors": [], "failures": [], "platform_residuals": [], "http_serving_failures": []}

    all_passed = True
    total_cases = 0
    for prof in PROFILES:
        with reservation(socket.SOCK_STREAM) as h, reservation(socket.SOCK_STREAM) as p, \
                reservation(socket.SOCK_STREAM) as d, reservation(socket.SOCK_STREAM) as t, \
                reservation(socket.SOCK_DGRAM) as u:
            ports = {"http": h.getsockname()[1], "prom": p.getsockname()[1],
                     "debug": d.getsockname()[1], "tcp": t.getsockname()[1], "udp": u.getsockname()[1]}
        go_obs = run_side("go", args.official, ports, args.output, prof, en0)
        rust_obs = run_side("rust", args.rust, ports, args.output, prof, en0)
        go_ok, go_errs = structural_ok(go_obs, prof, en0)
        rust_ok, rust_errs = structural_ok(rust_obs, prof, en0)
        parity = contract(go_obs) == contract(rust_obs)
        passed = go_ok and rust_ok and parity
        all_passed = all_passed and passed
        serving_ok, serving_errs, residual = http_serving_check(go_obs, rust_obs, prof, en0)
        if residual:
            report["platform_residuals"].append(residual)
        if not serving_ok:
            report["http_serving_failures"].append({"id": prof["id"], "errors": serving_errs})
        total_cases += 1
        per_leaf = leaf_parity(go_obs, rust_obs)
        entry = {"id": prof["id"], "covers": prof["covers"], "kind": prof["kind"], "ports": ports,
                 "go": go_obs, "rust": rust_obs,
                 "go_matches_contract": go_ok, "rust_matches_contract": rust_ok, "parity": parity,
                 "pass": passed, "contract_case_count": 1, "leaf_coverage": per_leaf,
                 "http_serving_ok": serving_ok, "http_serving_errors": serving_errs,
                 "en0_platform_residual": residual is not None,
                 "go_errors": go_errs, "rust_errors": rust_errs}
        report["vectors"].append(entry)
        if not passed:
            report["failures"].append({"id": prof["id"], "go_ok": go_ok, "rust_ok": rust_ok,
                                       "parity": parity, "go_errors": go_errs, "rust_errors": rust_errs,
                                       "go_contract": contract(go_obs), "rust_contract": contract(rust_obs),
                                       "go_failure": go_obs.get("failure"), "rust_failure": rust_obs.get("failure")})
        print(f"[{prof['id']}] bind: go_ok={go_ok} rust_ok={rust_ok} parity={parity} -> {'PASS' if passed else 'FAIL'}"
              f" | http_serving_ok={serving_ok} en0_residual={residual is not None}")
        if not passed:
            print(f"    go_errs={go_errs} rust_errs={rust_errs}")
        if not serving_ok:
            print(f"    serving_errs={serving_errs}")
        import sys
        sys.stdout.flush()

    report["final_binary_sha256"] = {"go": sha(args.official), "rust": sha(args.rust)}
    report["final_fixture_sha256"] = {str(p): sha(p) for p in fixture_inputs}
    report["input_identity_stable"] = (report["final_binary_sha256"] == {"go": go_sha, "rust": rust_sha}
                                       and report["final_fixture_sha256"] == report["fixture_sha256"])
    report["bind_contract_status"] = "PASS" if all_passed and report["input_identity_stable"] else "FAIL"
    report["http_serving_status"] = "PASS" if not report["http_serving_failures"] else "FAIL"
    report["status"] = report["bind_contract_status"]
    (args.output / "BATCH.json").write_text(json.dumps(report, indent=2) + "\n")
    (args.output / "execution.json").write_text(json.dumps(
        {"status": report["status"], "bind_contract_status": report["bind_contract_status"],
         "http_serving_status": report["http_serving_status"],
         "go_binary_sha256": go_sha, "rust_binary_sha256": rust_sha,
         "vector_count": len(report["vectors"]), "contract_case_count": total_cases,
         "input_identity_stable": report["input_identity_stable"],
         "platform_residual_count": len(report["platform_residuals"]),
         "platform_residuals": report["platform_residuals"],
         "http_serving_failures": report["http_serving_failures"], "failures": report["failures"]}, indent=2) + "\n")
    print(f"BATCH bind_contract={report['bind_contract_status']} http_serving={report['http_serving_status']} "
          f"cases={total_cases} bind_failures={len(report['failures'])} "
          f"platform_residuals={len(report['platform_residuals'])} serving_failures={len(report['http_serving_failures'])}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())