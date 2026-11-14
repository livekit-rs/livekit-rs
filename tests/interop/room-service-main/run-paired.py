#!/usr/bin/env python3
"""Paired Go-vs-Rust acceptance that the launched MAIN server serves livekit.RoomService.

SPEC-0077 C14. Closes the W2-SERVER-API RoomService family whose component tier was
already MET but whose launched-MAIN consumer and paired official differential were
NOT_RUN:
  API:RoomService.ListRooms / DeleteRoom / ListParticipants / GetParticipant /
  RemoveParticipant / MutePublishedTrack / UpdateParticipant / UpdateSubscriptions /
  SendData / UpdateRoomMetadata / ForwardParticipant / MoveParticipant / PerformRpc
  (and API:RoomService.CreateRoom, the control-plane parent).

What upstream says, from the locked sources (v1.13.10, e5655ac2):
  references/livekit-stable/pkg/service/roomservice.go is the public Twirp adapter;
  pkg/service/roommanager.go owns the single-node local room/participant operations.
  Forward/Move on the OSS node answer errors.New("not implemented") once the source
  participant exists, and psrpc "no response from servers" when nobody owns it
  (roommanager.go:912-918); same-source/destination is InvalidArgument. DeleteRoom /
  UpdateRoomMetadata on an absent room is NotFound "requested room does not exist".
  The Rust reproduces these in crates/lk-compat/src/twirp/room_service.rs (create_room
  :310, list_rooms :433, delete_room :452, list_participants :478, get_participant :506,
  remove_participant :533, mute_published_track :556, update_participant :576,
  update_subscriptions :603, send_data :640, update_room_metadata :668,
  forward_or_move :697, perform_rpc :752). Single node (no Redis) takes the local path.

Observable: probe.mjs drives the public Twirp surface of ONE launched MAIN with the
unchanged official server SDK AccessToken for auth, capturing the exact decoded HTTP
envelope for control-plane semantics/error contracts, and witnesses the participant-
dependent positive side-effects through two real unchanged official browser SDK peers
(publisher of 440 Hz audio + 640x360 VP8 video, subscriber). Nondeterministic IDs and
timestamps are mapped, list order canonicalised; every semantic field (codes, messages,
metadata, permissions, mute state, disconnect reasons, topics, payloads) is kept. The
launcher runs the locked Go oracle and the Rust candidate sequentially on IDENTICAL
config bytes / argv / ENV (same reserved ports), requires each side to independently
satisfy the structural contract (probe status PASS), then requires the two normalized
contracts to be deeply equal. Equal-but-wrong does not pass.

The falsifiability probe (mutating a Rust RoomService consumer) is driven separately by
the operator and recorded as a positive control; this launcher only runs the honest contract.

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
import signal
import socket
import subprocess
import time
from pathlib import Path

KEY = "devkey"
SECRET = "owned-room-service-fixture-secret-0123456789"
FIXED_GO_SHA256 = "4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc"
ITEMS = [
    "API:RoomService.CreateRoom",
    "API:RoomService.ListRooms",
    "API:RoomService.DeleteRoom",
    "API:RoomService.ListParticipants",
    "API:RoomService.GetParticipant",
    "API:RoomService.RemoveParticipant",
    "API:RoomService.MutePublishedTrack",
    "API:RoomService.UpdateParticipant",
    "API:RoomService.UpdateSubscriptions",
    "API:RoomService.SendData",
    "API:RoomService.UpdateRoomMetadata",
    "API:RoomService.ForwardParticipant",
    "API:RoomService.MoveParticipant",
    "API:RoomService.PerformRpc",
]
HERE = Path(__file__).resolve().parent


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


def build_config(http_port: int, tcp: int, udp: int, debug: int) -> str:
    return json.dumps({
        "port": http_port, "keys": {KEY: SECRET}, "bind_addresses": ["127.0.0.1"],
        "rtc": {"node_ip": "127.0.0.1", "use_external_ip": False, "enable_loopback_candidate": True,
                "tcp_port": tcp, "udp_port": udp, "ips": {"includes": ["127.0.0.1/32"]}},
        "room": {"enable_remote_unmute": True},
        "debug_handler": {"port": debug},
        "turn": {"enabled": False}, "logging": {"level": "info"},
    }, indent=2)


def run_side(side: str, binary: Path, ports: dict, outdir: Path, interop: Path,
             browser: Path, probe: Path, client: Path) -> dict:
    http_port = ports["http"]
    side_dir = outdir / side
    side_dir.mkdir(parents=True, exist_ok=True)
    config_text = build_config(http_port, ports["tcp"], ports["udp"], ports["debug"])
    config_path = side_dir / "config.json"
    config_path.write_text(config_text)
    config_path.chmod(0o600)
    stdout_path, stderr_path = side_dir / "stdout.log", side_dir / "stderr.log"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("LIVEKIT_", "LIVECLOUD_"))}
    obs: dict = {"side": side, "binary_sha256": sha(binary),
                 "config_sha256": hashlib.sha256(config_text.encode()).hexdigest(),
                 "ports": ports, "reached_http": False, "exit_code": None, "forced": False}
    child = None
    try:
        with stdout_path.open("wb") as so, stderr_path.open("wb") as se:
            argv = [str(binary.resolve()), "--config", str(config_path)]
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
                             OWNED_CHROMIUM=str(browser), LIVEKIT_API_KEY=KEY, LIVEKIT_API_SECRET=SECRET,
                             OUTPUT=str(probe_out), CLIENT_HTML=str(client), SIDE=side)
            done = subprocess.run(["node", str(probe)], env=probe_env, capture_output=True, timeout=240)
            obs["probe_exit"] = done.returncode
            (side_dir / "probe.stdout").write_bytes(done.stdout)
            (side_dir / "probe.stderr").write_bytes(done.stderr)
            probe_json = json.loads(probe_out.read_text()) if probe_out.exists() else {"status": "MISSING"}
            obs["probe_status"] = probe_json.get("status")
            obs["contract"] = probe_json.get("contract", [])
            obs["probe_errors"] = probe_json.get("errors", [])
            obs["cleanup_errors"] = probe_json.get("cleanup_errors", [])
            obs["sdk_sha256"] = probe_json.get("sdk_sha256")
            obs["browser"] = probe_json.get("browser")
            obs["browser_stderr"] = probe_json.get("browser_stderr")
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
    for name in ("official", "rust", "browser", "interop", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--probe", type=Path, default=HERE / "probe.mjs")
    parser.add_argument("--client", type=Path, default=HERE / "client.html")
    args = parser.parse_args()
    args.output.mkdir(mode=0o700, parents=True, exist_ok=True)
    if sha(args.official) != FIXED_GO_SHA256:
        raise RuntimeError("fixed source Go binary identity mismatch before execution")

    fixture_inputs = [args.probe, args.client, Path(__file__).resolve(),
                      args.interop / "owned-browser-exit.mjs", args.interop / "package.json",
                      args.interop / "package-lock.json"]
    go_sha, rust_sha = sha(args.official), sha(args.rust)
    report = {"status": "RUNNING", "go_binary_sha256": go_sha, "rust_binary_sha256": rust_sha,
              "binaries": {"official_go": str(args.official), "rust": str(args.rust)},
              "items": ITEMS, "probe_sha256": sha(args.probe), "client_sha256": sha(args.client),
              "launcher_sha256": sha(Path(__file__)),
              "fixture_sha256": {str(p): sha(p) for p in fixture_inputs},
              "fixed_go_source": "e5655ac20267fa5005dccaf3f0e3bfc3ea688c07",
              "vectors": [], "failures": []}

    with reservation(socket.SOCK_STREAM) as h, reservation(socket.SOCK_STREAM) as t, \
            reservation(socket.SOCK_DGRAM) as u, reservation(socket.SOCK_STREAM) as d:
        ports = {"http": h.getsockname()[1], "tcp": t.getsockname()[1],
                 "udp": u.getsockname()[1], "debug": d.getsockname()[1]}
    go_obs = run_side("go", args.official, ports, args.output, args.interop, args.browser, args.probe, args.client)
    rust_obs = run_side("rust", args.rust, ports, args.output, args.interop, args.browser, args.probe, args.client)
    parity = go_obs.get("contract") == rust_obs.get("contract")
    go_ok = go_obs.get("probe_status") == "PASS" and go_obs.get("probe_exit") == 0
    rust_ok = rust_obs.get("probe_status") == "PASS" and rust_obs.get("probe_exit") == 0
    clean = all(o.get("exit_code") == 0 and not o.get("forced") and o.get("reached_http") for o in (go_obs, rust_obs))
    passed = go_ok and rust_ok and parity and clean
    entry = {"id": "room-service-contract", "ports": ports, "go": go_obs, "rust": rust_obs,
             "go_matches_contract": go_ok, "rust_matches_contract": rust_ok, "parity": parity,
             "clean_exit": clean, "pass": passed,
             "contract_case_count": len(go_obs.get("contract", []))}
    report["vectors"].append(entry)
    if not passed:
        diff = []
        gc, rc = go_obs.get("contract", []), rust_obs.get("contract", [])
        for i in range(max(len(gc), len(rc))):
            g = gc[i] if i < len(gc) else None
            r = rc[i] if i < len(rc) else None
            if g != r:
                diff.append({"index": i, "case": (g or r or {}).get("case"), "go": g, "rust": r})
        report["failures"].append({"id": "room-service-contract", "go_ok": go_ok, "rust_ok": rust_ok,
                                   "parity": parity, "clean_exit": clean, "contract_diff": diff,
                                   "go_errors": go_obs.get("probe_errors"), "rust_errors": rust_obs.get("probe_errors"),
                                   "go_failure": go_obs.get("failure"), "rust_failure": rust_obs.get("failure")})
    print(f"[room-service-contract] go_ok={go_ok} rust_ok={rust_ok} parity={parity} clean={clean} "
          f"cases={len(go_obs.get('contract', []))} -> {'PASS' if passed else 'FAIL'}")

    report["final_binary_sha256"] = {"go": sha(args.official), "rust": sha(args.rust)}
    report["final_fixture_sha256"] = {str(p): sha(p) for p in fixture_inputs}
    report["input_identity_stable"] = (report["final_binary_sha256"] == {"go": go_sha, "rust": rust_sha}
                                       and report["final_fixture_sha256"] == report["fixture_sha256"])
    report["status"] = "PASS" if passed and report["input_identity_stable"] else "FAIL"
    (args.output / "BATCH.json").write_text(json.dumps(report, indent=2) + "\n")
    (args.output / "execution.json").write_text(json.dumps(
        {"status": report["status"], "go_binary_sha256": go_sha, "rust_binary_sha256": rust_sha,
         "vector_count": len(report["vectors"]), "contract_case_count": len(go_obs.get("contract", [])),
         "input_identity_stable": report["input_identity_stable"], "failures": report["failures"]}, indent=2) + "\n")
    print(f"BATCH status={report['status']} cases={len(go_obs.get('contract', []))} failures={len(report['failures'])}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())