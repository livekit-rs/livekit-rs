#!/usr/bin/env python3
"""Paired Go-vs-Rust acceptance: launched-MAIN twirp ProtoJSON request codec.

SPEC-0077 C18. Closes the launched-MAIN (main_server_consumer) and paired locked-Go
(official_differential_lifecycle) tiers for the five twirp_json CODEC leaves whose
upstream_source_contract / config_projection / component_behavior were already MET
(component tests compare decode_dynamic against pinned Go wire bytes for the 22/49/246
official vectors) but whose MAIN/differential tiers were NOT_RUN: the official 22-case
oracle had only been run at the typed-component level, never through a launched MAIN's
real twirp HTTP boundary paired against the locked Go server.

  CODEC:known_null_and_duplicate   null acceptance + field-number seen before null + alias dup + oneof
  CODEC:unknown_enum               unknown numeric preserved / symbolic dropped without bypassing dup
  CODEC:integer_lexical            exact decimal/exponent integers, no f64 loss; fraction/whitespace/overflow reject
  CODEC:wkt_bounds                 Duration/Timestamp grammar + ranges
  CODEC:duplicate_known_json_field cross-method known/alias duplicate rejection BEFORE auth

Upstream contract (locked v1.13.10, e5655ac2): the generated twirp handler decodes the JSON
request with protojson BEFORE the service impl permission check. Decode failure ->
twirp.Malformed "the json request could not be decoded" (400). Decode success with no token ->
auth gate -> twirp.Unauthenticated "permissions denied" (401, pkg/service/auth.go:263). Rust
reproduces the ordering (room_service.rs:46 unary!, other_services.rs:109/121/160,
sip_calls.rs:37) and the codec (lk-protocol/src/json/{mod,builder}.rs). The codec accept/reject
decision is therefore observable at HTTP with NO worker/Redis/room business logic.

Observable: probe.mjs drives ONE launched MAIN per side over the real /twirp HTTP boundary with
hand-crafted raw JSON edge bodies (the official SDK is unchanged and not used for these bodies;
raw HTTP is the only way to put duplicate tokens on the wire), plus a small authenticated
CreateRoom value-echo group proving the launched MAIN decodes integer lexical VALUES. The launcher
runs the locked Go oracle and the Rust candidate sequentially on IDENTICAL config bytes / argv /
ENV (same reserved ports), requires each side to independently satisfy the structural codec
contract (probe PASS), then requires the two normalized contracts to be deeply equal.
Equal-but-wrong does not pass. Both sides must exit cleanly on the first SIGINT.

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
    "CODEC:known_null_and_duplicate",
    "CODEC:unknown_enum",
    "CODEC:integer_lexical",
    "CODEC:wkt_bounds",
    "CODEC:duplicate_known_json_field",
]
HERE = Path(__file__).resolve().parent

# The codec is config-independent: a single default config exercises every leaf. The probe
# tags each case with its group/leaf so the receipt attributes coverage per leaf.
PROFILES = [
    {"id": "twirp-json-codec", "overlay": {}, "flags": [],
     "covers": "launched-MAIN twirp ProtoJSON request codec: known null/duplicate/oneof, unknown enum, "
               "integer lexical (decision + authenticated value echo), WKT Duration/Timestamp bounds, "
               "cross-method duplicate-before-auth dispatch"},
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
            probe_env = dict(env, RTC_URL=f"http://127.0.0.1:{http_port}", INTEROP_DIR=str(interop),
                             LIVEKIT_API_KEY=KEY, LIVEKIT_API_SECRET=SECRET, OUTPUT=str(probe_out),
                             SIDE=side, PROFILE=prof["id"])
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


def leaf_parity(go_contract: list, rust_contract: list) -> dict:
    out = {}
    for leaf in ITEMS:
        g = [r for r in go_contract if r.get("leaf") == leaf]
        r = [r for r in rust_contract if r.get("leaf") == leaf]
        out[leaf] = {"go_cases": len(g), "rust_cases": len(r), "parity": g == r,
                     "cases": [x.get("case") for x in g]}
    return out


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
              "server_sdk": "livekit-server-sdk@2.19.2 (unchanged; raw HTTP used for edge bodies)",
              "protocol": "@livekit/protocol@1.52.1 (unchanged)",
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
        gc, rc = go_obs.get("contract", []), rust_obs.get("contract", [])
        parity = gc == rc
        go_ok = go_obs.get("probe_status") == "PASS" and go_obs.get("probe_exit") == 0
        rust_ok = rust_obs.get("probe_status") == "PASS" and rust_obs.get("probe_exit") == 0
        clean = all(o.get("exit_code") == 0 and not o.get("forced") and o.get("reached_http") for o in (go_obs, rust_obs))
        per_leaf = leaf_parity(gc, rc)
        leaves_ok = all(v["parity"] and v["go_cases"] > 0 for v in per_leaf.values())
        passed = go_ok and rust_ok and parity and clean and leaves_ok
        all_passed = all_passed and passed
        total_cases += len(gc)
        entry = {"id": prof["id"], "covers": prof["covers"], "ports": ports,
                 "go": go_obs, "rust": rust_obs,
                 "go_matches_contract": go_ok, "rust_matches_contract": rust_ok, "parity": parity,
                 "clean_exit": clean, "pass": passed, "contract_case_count": len(gc),
                 "leaf_coverage": per_leaf}
        report["vectors"].append(entry)
        if not passed:
            diff = []
            for i in range(max(len(gc), len(rc))):
                g = gc[i] if i < len(gc) else None
                r = rc[i] if i < len(rc) else None
                if g != r:
                    diff.append({"index": i, "case": (g or r or {}).get("case"),
                                 "endpoint": (g or r or {}).get("endpoint"), "go": g, "rust": r})
            report["failures"].append({"id": prof["id"], "go_ok": go_ok, "rust_ok": rust_ok,
                                       "parity": parity, "clean_exit": clean, "leaves_ok": leaves_ok,
                                       "contract_diff": diff, "leaf_coverage": per_leaf,
                                       "go_errors": go_obs.get("probe_errors"), "rust_errors": rust_obs.get("probe_errors"),
                                       "go_failure": go_obs.get("failure"), "rust_failure": rust_obs.get("failure")})
        print(f"[{prof['id']}] go_ok={go_ok} rust_ok={rust_ok} parity={parity} clean={clean} "
              f"leaves_ok={leaves_ok} cases={len(gc)} -> {'PASS' if passed else 'FAIL'}")

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