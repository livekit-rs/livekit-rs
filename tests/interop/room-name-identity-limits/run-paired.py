#!/usr/bin/env python3
"""Paired Go-vs-Rust acceptance: launched MAIN room-name / participant-identity length gate.

SPEC-0077 C16. Closes the launched-MAIN (main_server_consumer) and paired locked-Go
(official_differential_lifecycle) tiers for the room.*/limit.* name/identity length family
whose upstream_source_contract / config_projection / component_behavior were MET but whose
MAIN/differential tiers were NOT_RUN: the C09-b campaign exercised limit.* via YAML only, not
the legacy room.* alias projection nor the CLI-flag projection.

Upstream contract (locked v1.13.10, e5655ac2):
  pkg/config/config.go:401/405 Check*Length = limit==0 || len(s)<=limit  (len = UTF-8 BYTES;
  zero unlimited; the default is 256). config.go:743-747 a NONZERO legacy room.max_* overrides
  limit.max_*; a ZERO room.max_* preserves limit.max_*. NewConfig order: defaults -> YAML ->
  updateFromCLI (flags override) -> legacy merge, so CLI flags feed the same merge.
  pkg/service/roomservice.go:83 CreateRoom CheckRoomNameLength -> psrpc.InvalidArgument
  "room name length exceeds limits: max length N".
  pkg/service/utils.go:369/387 join ValidateConnectRequest identity/room-name -> HTTP 400
  "participant identity length exceeds limits: max length N" / "room name length exceeds ...".

Rust reproduces this in crates/lk-config/src/load.rs:664-673 (legacy merge, same ordering),
crates/rtc-core/src/config.rs signed_byte_length_allowed (byte-count predicate),
crates/lk-compat/src/twirp/room_service.rs:322 (CreateRoom) and crates/lk-compat/src/signal/
params.rs:219/230 (join identity/room-name), surfaced via /rtc/validate and the real /rtc join.

Observable: probe.mjs drives ONE launched MAIN per side with the UNCHANGED official
livekit-server-sdk 2.19.2 RoomServiceClient (CreateRoom) plus the server's /rtc/validate preflight
and a real /rtc WebSocket join, at the ASCII and UTF-8 multibyte BYTE boundary on BOTH sides of
each configured limit. Six profiles cover every projection path: default(256), YAML room.*
nonzero-override(10), YAML room.* zero-preserve + limit.*(12), CLI --room.*(10), CLI --limit.*(12),
YAML limit.*=0 unlimited. The launcher runs the locked Go oracle and the Rust candidate
sequentially on IDENTICAL config bytes / argv / ENV (same reserved ports) per profile, requires
each side to independently satisfy the structural boundary contract (probe PASS), then requires
the two normalized contracts to be deeply equal. Equal-but-wrong does not pass. Both sides must
exit cleanly on the first SIGINT.

Usage:
  python3 run-paired.py --official /abs/livekit-server-go --rust /abs/livecloud \
      --interop tests/interop --output DIR
"""
from __future__ import annotations

import argparse
import copy
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
SECRET = "owned-room-service-fixture-secret-0123456789"
FIXED_GO_SHA256 = "4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc"
ITEMS = [
    "YAML:room.max_room_name_length",
    "YAML:room.max_participant_identity_length",
    "generated_flags:--room.max_room_name_length",
    "generated_flags:--room.max_participant_identity_length",
    "generated_flags:--limit.max_room_name_length",
    "generated_flags:--limit.max_participant_identity_length",
    "CONFIG_CONSUMPTION:room.max_room_name_length",
    "CONFIG_CONSUMPTION:room.max_participant_identity_length",
]
HERE = Path(__file__).resolve().parent

# Each profile delivers the effective limit through ONE projection path. name_limit/identity_limit
# are the EXPECTED effective limits the probe asserts structurally (None -> "unlimited").
PROFILES = [
    {"id": "default", "overlay": {}, "flags": [],
     "name_limit": 256, "identity_limit": 256,
     "covers": "default 256 + UTF-8 byte counting anchor"},
    {"id": "yaml-room-nonzero", "overlay": {"room": {"max_room_name_length": 10, "max_participant_identity_length": 10}},
     "flags": [], "name_limit": 10, "identity_limit": 10,
     "covers": "YAML room.* legacy alias NONZERO overrides limit.* (YAML:room.*, CONFIG_CONSUMPTION:room.*)"},
    {"id": "yaml-room-zero", "overlay": {"room": {"max_room_name_length": 0, "max_participant_identity_length": 0},
                                          "limit": {"max_room_name_length": 12, "max_participant_identity_length": 12}},
     "flags": [], "name_limit": 12, "identity_limit": 12,
     "covers": "YAML room.* ZERO preserves limit.* (CONFIG_CONSUMPTION:room.* zero-preserve)"},
    {"id": "cli-room", "overlay": {}, "flags": ["--room.max_room_name_length=10", "--room.max_participant_identity_length=10"],
     "name_limit": 10, "identity_limit": 10,
     "covers": "CLI --room.* flags feed legacy merge (generated_flags:--room.*)"},
    {"id": "cli-limit", "overlay": {}, "flags": ["--limit.max_room_name_length=12", "--limit.max_participant_identity_length=12"],
     "name_limit": 12, "identity_limit": 12,
     "covers": "CLI --limit.* flags (generated_flags:--limit.*)"},
    {"id": "yaml-limit-zero", "overlay": {"limit": {"max_room_name_length": 0, "max_participant_identity_length": 0}},
     "flags": [], "name_limit": None, "identity_limit": None,
     "covers": "YAML limit.*=0 unlimited (zero-unlimited boundary)"},
]


def sha(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def reservation(kind: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, kind)
    sock.bind(("127.0.0.1", 0))
    return sock


def http_ready(port: int) -> bool:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=0.3)
    try:
        conn.request("GET", "/")
        return conn.getresponse().status == 200
    except OSError:
        return False
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


def build_config(http_port: int, tcp: int, udp: int, debug: int, overlay: dict) -> str:
    base = {
        "port": http_port, "keys": {KEY: SECRET}, "bind_addresses": ["127.0.0.1"],
        "rtc": {"node_ip": "127.0.0.1", "use_external_ip": False, "enable_loopback_candidate": True,
                "tcp_port": tcp, "udp_port": udp, "ips": {"includes": ["127.0.0.1/32"]}},
        "room": {"auto_create": True},
        "debug_handler": {"port": debug},
        "turn": {"enabled": False}, "logging": {"level": "info"},
    }
    return json.dumps(deep_merge(base, overlay), indent=2)


def run_side(side: str, binary: Path, ports: dict, outdir: Path, interop: Path,
             probe: Path, prof: dict) -> dict:
    http_port = ports["http"]
    side_dir = outdir / prof["id"] / side
    side_dir.mkdir(parents=True, exist_ok=True)
    config_text = build_config(http_port, ports["tcp"], ports["udp"], ports["debug"], prof["overlay"])
    config_path = side_dir / "config.json"
    config_path.write_text(config_text)
    config_path.chmod(0o600)
    stdout_path, stderr_path = side_dir / "stdout.log", side_dir / "stderr.log"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("LIVEKIT_", "LIVECLOUD_"))}
    obs: dict = {"side": side, "profile": prof["id"], "binary_sha256": sha(binary),
                 "config_sha256": hashlib.sha256(config_text.encode()).hexdigest(),
                 "flags": prof["flags"], "ports": ports, "reached_http": False,
                 "exit_code": None, "forced": False}
    child = None
    try:
        with stdout_path.open("wb") as so, stderr_path.open("wb") as se:
            argv = [str(binary.resolve()), "--config", str(config_path)] + prof["flags"]
            obs["argv"] = argv[1:]
            child = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=so, stderr=se, env=env)
            obs["pid"] = child.pid
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
            probe_out = side_dir / "probe.json"
            nl = "unlimited" if prof["name_limit"] is None else str(prof["name_limit"])
            il = "unlimited" if prof["identity_limit"] is None else str(prof["identity_limit"])
            probe_env = dict(env, RTC_URL=f"http://127.0.0.1:{http_port}", INTEROP_DIR=str(interop),
                             LIVEKIT_API_KEY=KEY, LIVEKIT_API_SECRET=SECRET, OUTPUT=str(probe_out),
                             SIDE=side, PROFILE=prof["id"], NAME_LIMIT=nl, IDENTITY_LIMIT=il)
            done = subprocess.run(["node", str(probe)], env=probe_env, capture_output=True, timeout=180)
            obs["probe_exit"] = done.returncode
            (side_dir / "probe.stdout").write_bytes(done.stdout)
            (side_dir / "probe.stderr").write_bytes(done.stderr)
            probe_json = json.loads(probe_out.read_text()) if probe_out.exists() else {"status": "MISSING"}
            obs["probe_status"] = probe_json.get("status")
            obs["contract"] = probe_json.get("contract", [])
            obs["probe_errors"] = probe_json.get("errors", [])
            obs["diagnostics"] = probe_json.get("diagnostics", {})
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
        obs["stdout_sha256"] = sha(stdout_path) if stdout_path.exists() else None
        obs["stderr_sha256"] = sha(stderr_path) if stderr_path.exists() else None
    return obs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("official", "rust", "interop", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--probe", type=Path, default=HERE / "probe.mjs")
    args = parser.parse_args()
    args.output.mkdir(mode=0o700, parents=True, exist_ok=True)
    if sha(args.official) != FIXED_GO_SHA256:
        raise RuntimeError("fixed source Go binary identity mismatch before execution")

    fixture_inputs = [args.probe, Path(__file__).resolve(),
                      args.interop / "package.json", args.interop / "package-lock.json"]
    go_sha, rust_sha = sha(args.official), sha(args.rust)
    report = {"status": "RUNNING", "go_binary_sha256": go_sha, "rust_binary_sha256": rust_sha,
              "binaries": {"official_go": str(args.official), "rust": str(args.rust)},
              "items": ITEMS, "profiles": [p["id"] for p in PROFILES],
              "probe_sha256": sha(args.probe), "launcher_sha256": sha(Path(__file__)),
              "fixture_sha256": {str(p): sha(p) for p in fixture_inputs},
              "fixed_go_source": "e5655ac20267fa5005dccaf3f0e3bfc3ea688c07",
              "server_sdk": "livekit-server-sdk@2.19.2 (unchanged)",
              "vectors": [], "failures": []}

    all_passed = True
    total_cases = 0
    for prof in PROFILES:
        with reservation(socket.SOCK_STREAM) as h, reservation(socket.SOCK_STREAM) as t, \
                reservation(socket.SOCK_DGRAM) as u, reservation(socket.SOCK_STREAM) as d:
            ports = {"http": h.getsockname()[1], "tcp": t.getsockname()[1],
                     "udp": u.getsockname()[1], "debug": d.getsockname()[1]}
        go_obs = run_side("go", args.official, ports, args.output, args.interop, args.probe, prof)
        rust_obs = run_side("rust", args.rust, ports, args.output, args.interop, args.probe, prof)
        parity = go_obs.get("contract") == rust_obs.get("contract")
        go_ok = go_obs.get("probe_status") == "PASS" and go_obs.get("probe_exit") == 0
        rust_ok = rust_obs.get("probe_status") == "PASS" and rust_obs.get("probe_exit") == 0
        clean = all(o.get("exit_code") == 0 and not o.get("forced") and o.get("reached_http") for o in (go_obs, rust_obs))
        passed = go_ok and rust_ok and parity and clean
        all_passed = all_passed and passed
        total_cases += len(go_obs.get("contract", []))
        entry = {"id": prof["id"], "covers": prof["covers"], "ports": ports,
                 "expected_name_limit": prof["name_limit"], "expected_identity_limit": prof["identity_limit"],
                 "go": go_obs, "rust": rust_obs,
                 "go_matches_contract": go_ok, "rust_matches_contract": rust_ok, "parity": parity,
                 "clean_exit": clean, "pass": passed, "contract_case_count": len(go_obs.get("contract", []))}
        report["vectors"].append(entry)
        if not passed:
            diff = []
            gc, rc = go_obs.get("contract", []), rust_obs.get("contract", [])
            for i in range(max(len(gc), len(rc))):
                g = gc[i] if i < len(gc) else None
                r = rc[i] if i < len(rc) else None
                if g != r:
                    diff.append({"index": i, "case": (g or r or {}).get("case"),
                                 "endpoint": (g or r or {}).get("endpoint"), "go": g, "rust": r})
            report["failures"].append({"id": prof["id"], "go_ok": go_ok, "rust_ok": rust_ok,
                                       "parity": parity, "clean_exit": clean, "contract_diff": diff,
                                       "go_errors": go_obs.get("probe_errors"), "rust_errors": rust_obs.get("probe_errors"),
                                       "go_failure": go_obs.get("failure"), "rust_failure": rust_obs.get("failure")})
        print(f"[{prof['id']}] go_ok={go_ok} rust_ok={rust_ok} parity={parity} clean={clean} "
              f"cases={len(go_obs.get('contract', []))} -> {'PASS' if passed else 'FAIL'}")

    report["final_binary_sha256"] = {"go": sha(args.official), "rust": sha(args.rust)}
    report["final_fixture_sha256"] = {str(p): sha(p) for p in fixture_inputs}
    report["input_identity_stable"] = (report["final_binary_sha256"] == {"go": go_sha, "rust": rust_sha}
                                       and report["final_fixture_sha256"] == report["fixture_sha256"])
    report["status"] = "PASS" if all_passed and report["input_identity_stable"] else "FAIL"
    (args.output / "BATCH.json").write_text(json.dumps(report, indent=2) + "\n")
    (args.output / "execution.json").write_text(json.dumps(
        {"status": report["status"], "go_binary_sha256": go_sha, "rust_binary_sha256": rust_sha,
         "vector_count": len(report["vectors"]), "contract_case_count": total_cases,
         "input_identity_stable": report["input_identity_stable"], "failures": report["failures"]}, indent=2) + "\n")
    print(f"BATCH status={report['status']} cases={total_cases} failures={len(report['failures'])}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())