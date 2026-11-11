#!/usr/bin/env python3
"""Run the official SDK simulateScenario campaign against pinned Go and Rust and compare outcomes.

  python3 run.py --go <pinned-go-livekit-server> --rust <livecloud> --deps <tests/interop> --output <new-dir> [--single false]

Each implementation runs as its own owned single-node server (loopback, TCP and UDP media ports, no Redis).
The comparison uses the normalized observable fields; raw event timelines are kept in each evidence.json.
"""
import argparse, hashlib, json, os, signal, socket, subprocess, tempfile, time
from pathlib import Path

FIELDS = ("finalState", "sidChanged", "aStates", "aEvents", "disconnectReason", "bSawLeave", "bSawRejoin",
          "audioAfter", "dataAfter", "protocols", "bSubscribedAfter")
DEFAULT_SCENARIOS = ("speaker,signal-reconnect,resume-reconnect,full-reconnect,node-failure,migration,server-leave,"
                     "disconnect-signal-on-resume,disconnect-signal-on-resume-no-messages,leave-full-reconnect,force-tcp,force-tls")
# Speaker fields are compared only for the `speaker` scenario. Elsewhere they reflect whether the real
# 440 Hz tone's audio-level detection happened to fire inside the observation window (timing), and
# are kept in the raw timelines instead of being asserted.
SPEAKER_FIELDS = ("speakerOnA", "speakerOnB", "speakerClearedOnB")


def fields(name):
    return FIELDS + SPEAKER_FIELDS if name == "speaker" else FIELDS


def free_ports(turn_tls, turn_udp):
    held, ports = [], []
    kinds = [socket.SOCK_STREAM, socket.SOCK_STREAM, socket.SOCK_DGRAM]
    if turn_tls:
        kinds.append(socket.SOCK_STREAM)
    if turn_udp:
        kinds.append(socket.SOCK_DGRAM)
    for kind in kinds:
        s = socket.socket(socket.AF_INET, kind); s.bind(("127.0.0.1", 0)); ports.append(s.getsockname()[1]); held.append(s)
    for s in held:
        s.close()
    return ports


def released_ports(endpoints):
    for port, kind in endpoints:
        with socket.socket(socket.AF_INET, kind) as sock:
            if kind == socket.SOCK_STREAM:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                return False
    return True


def run_one(kind, binary, deps, out, single, scenarios, turn_tls, coalesce_rejoin_sdp=False):
    out.mkdir()
    binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
    turn_udp = "ice-network-change" in scenarios.split(",")
    http, tcp, udp, *turn = free_ports(turn_tls, turn_udp)
    cfg = {"port": http, "bind_addresses": ["127.0.0.1"], "keys": {"devkey": "secret"},
           "rtc": {"node_ip": "127.0.0.1", "use_external_ip": False, "enable_loopback_candidate": True,
                   "ips": {"includes": ["127.0.0.1/32"]}, "tcp_port": tcp, "udp_port": udp}}
    if os.environ.get("LK_SIM_LOG_LEVEL"):
        cfg["logging"] = {"level": os.environ["LK_SIM_LOG_LEVEL"]}
    turn_env = {}
    if turn_tls:
        cfg["turn"] = {"enabled": True, "domain": "turn.localhost", "tls_port": turn[0], "udp_port": 0,
                       "cert_file": str(turn_tls / "server.pem"), "key_file": str(turn_tls / "server-key.pem"),
                       "bind_addresses": ["127.0.0.1"], "allow_restricted_peer_cidrs": ["127.0.0.1/32"]}
        turn_env = {"LK_SIM_TURN_TLS_PORT": str(turn[0]),
                    "LK_SIM_TURN_TLS_SPKI": (turn_tls / "spki-sha256.txt").read_text().strip()}
    if turn_udp:
        cfg.setdefault("turn", {"enabled": True, "bind_addresses": ["127.0.0.1"],
                                "allow_restricted_peer_cidrs": ["127.0.0.1/32"]})["udp_port"] = turn[-1]
        turn_env["LK_SIM_TURN_UDP_PORT"] = str(turn[-1])
    with tempfile.TemporaryDirectory(prefix="lk-sim-") as tmp:
        config = Path(tmp) / "server.yaml"; config.write_text(json.dumps(cfg))
        with (out / "server.log").open("wb") as log:
            server = subprocess.Popen([str(binary), "--config", str(config)], stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                deadline = time.monotonic() + 30
                while True:
                    assert server.poll() is None, f"{kind} server exited during startup"
                    try:
                        socket.create_connection(("127.0.0.1", http), timeout=.5).close(); break
                    except OSError:
                        assert time.monotonic() < deadline, f"{kind} server did not listen"; time.sleep(.1)
                env = dict(os.environ, INTEROP_DEPENDENCY_ROOT=str(deps), INTEROP_OUTPUT_DIR=str(out / "sdk"),
                           LIVEKIT_URL=f"ws://127.0.0.1:{http}", SINGLE_PC=single, SCENARIOS=scenarios,
                           LK_SIM_COALESCE_REJOIN_SDP=str(coalesce_rejoin_sdp).lower(), **turn_env)
                with (out / "probe.log").open("wb") as plog:
                    probe = subprocess.run(["node", str(Path(__file__).with_name("run.mjs"))], env=env, stdout=plog,
                                           stderr=subprocess.STDOUT, timeout=1500)
            finally:
                server.send_signal(signal.SIGINT)
                try:
                    server_exit = server.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(server.pid, signal.SIGKILL); server_exit = server.wait()
    evidence_path = out / "sdk" / "evidence.json"
    evidence = json.loads(evidence_path.read_text()) if evidence_path.is_file() else {}
    if hashlib.sha256(binary.read_bytes()).hexdigest() != binary_sha256:
        raise RuntimeError(f"{kind} server binary changed during the test")
    endpoints = [(http, socket.SOCK_STREAM), (tcp, socket.SOCK_STREAM), (udp, socket.SOCK_DGRAM)]
    if turn_tls:
        endpoints.append((turn[0], socket.SOCK_STREAM))
    if turn_udp:
        endpoints.append((turn[-1], socket.SOCK_DGRAM))
    receipt = {"probe_exit": probe.returncode, "server_exit": server_exit, "ports_released": released_ports(endpoints),
               "binary": str(binary), "binary_sha256": binary_sha256, "config": cfg}
    (out / "execution.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return {**receipt, "evidence": evidence}


def row_errors(row):
    errors = [row[field] for field in ("error", "simulateError", "cleanupError") if row.get(field)]
    if row.get("pageErrors"):
        errors.append(row["pageErrors"])
    for field in ("audioBefore", "dataBefore", "roomDeleted"):
        if row.get(field) is not True:
            errors.append(f"{field} not proven")
    if row.get("finalState") not in ("connected", "disconnected", "reconnecting", "signalReconnecting"):
        errors.append("final connection state not observed")
    if row.get("finalState") == "connected":
        for field in ("audioAfter", "dataAfter"):
            if row.get(field) is not True:
                errors.append(f"{field} not proven after reconnect")
    return errors


def compare(runs, scenarios, single, coalesce_rejoin_sdp=False):
    report = {"single_pc": single, "requested_scenarios": scenarios,
              "coalesce_rejoin_sdp": coalesce_rejoin_sdp,
              "binaries": {k: v["binary_sha256"] for k, v in runs.items()},
              "exits": {k: {"probe": v["probe_exit"], "server": v["server_exit"]} for k, v in runs.items()},
              "sdk_sha256": {k: v["evidence"].get("sdkSha256") for k, v in runs.items()}, "scenarios": {}}
    sdk_ids = report["sdk_sha256"]
    report["inputs_match"] = bool(scenarios and all(scenarios) and len(set(scenarios)) == len(scenarios)
                                  and sdk_ids["go"] and sdk_ids["go"] == sdk_ids["rust"]) and all(
        set(v["evidence"].get("scenarios", {})) == set(scenarios) for v in runs.values())
    for name in scenarios:
        rows = {k: v["evidence"].get("scenarios", {}).get(name, {}) for k, v in runs.items()}
        g, r = ({f: rows[k].get(f) for f in fields(name)} for k in ("go", "rust"))
        diff = {f: {"go": g[f], "rust": r[f]} for f in fields(name) if g[f] != r[f]}
        errors = {k: row_errors(row) for k, row in rows.items()}
        if coalesce_rejoin_sdp:
            for k, row in rows.items():
                delivery = row.get("signalDelivery", [])
                batch = delivery[1] if len(delivery) >= 2 else {}
                if (batch.get("connection") != 2 or batch.get("flush") != "pair"
                        or batch.get("descriptionKinds") != ["answer", "offer"]
                        or batch.get("decodeError") or batch.get("closeError")):
                    errors[k].append("replacement offer and answer delivery burst not proven")
        if name == "transport-switch":
            transitions = {}
            for k, row in rows.items():
                steps = row.get("transportSteps", [])
                if ([step.get("name") for step in steps] != ["udp", "tcp", "tls"]
                        or not all(step.get(field) is True for step in steps[1:]
                                   for field in ("audioA", "audioB", "dataA", "dataB"))):
                    errors[k].append("complete transport switch not proven")
                transitions[k] = [{"replacedPcs": step.get("replacedPcs"), "sidChanged": step.get("sidChanged")}
                                  for step in steps[1:]]
            if transitions["go"] != transitions["rust"]:
                diff["transportTransitions"] = transitions
        if name == "ice-network-change":
            for k, row in rows.items():
                change = row.get("networkChange", {})
                if (change.get("sidRetained") is not True or len(change.get("proxies", [])) != 2
                        or not all(change.get(step, {}).get(field) is True for step in ("before", "after")
                                   for field in ("audioA", "audioB", "dataA", "dataB"))):
                    errors[k].append("ICE restart across replaced network path not proven")
        report["scenarios"][name] = {"match": not diff and not any(errors.values()),
                                     "go": g, "rust": r, "diff": diff, "errors": errors}
    report["match"] = (report["inputs_match"] and all(
        code == 0 for exits in report["exits"].values() for code in exits.values()
    ) and all(v.get("ports_released") is True for v in runs.values())
        and all(v["match"] for v in report["scenarios"].values()))
    report["ports_released"] = {k: v.get("ports_released") for k, v in runs.items()}
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--go", type=Path, required=True)
    p.add_argument("--rust", type=Path, required=True)
    p.add_argument("--deps", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--single", default="true", choices=("true", "false"))
    p.add_argument("--scenarios")
    p.add_argument("--turn-tls-fixture", type=Path)
    p.add_argument("--coalesce-rejoin-sdp", action="store_true",
                   help="coalesce the real replacement answer and offer in dual-PC force-tls without TURN")
    p.add_argument("--recompare", action="store_true", help="recompute comparison.json from an existing output directory")
    a = p.parse_args()
    if a.recompare:
        old = json.loads((a.output / "comparison.json").read_text())
        runs = {k: {"binary_sha256": old["binaries"][k], "probe_exit": old["exits"][k]["probe"], "server_exit": old["exits"][k]["server"],
                    "ports_released": old.get("ports_released", {}).get(k),
                    "evidence": json.loads((a.output / k / "sdk" / "evidence.json").read_text())} for k in ("go", "rust")}
        scenarios = a.scenarios or ",".join(old.get("requested_scenarios", [])) or DEFAULT_SCENARIOS
        coalesce_rejoin_sdp = old.get("coalesce_rejoin_sdp", False)
    else:
        scenarios = a.scenarios or DEFAULT_SCENARIOS
        coalesce_rejoin_sdp = a.coalesce_rejoin_sdp
        if coalesce_rejoin_sdp and (a.single != "false" or scenarios != "force-tls" or a.turn_tls_fixture):
            p.error("--coalesce-rejoin-sdp requires only dual-PC force-tls without a TURN/TLS fixture")
        a.output.mkdir()
        turn_tls = a.turn_tls_fixture.resolve() if a.turn_tls_fixture else None
        runs = {k: run_one(k, b.resolve(), a.deps.resolve(), a.output / k, a.single, scenarios, turn_tls, coalesce_rejoin_sdp)
                for k, b in (("go", a.go), ("rust", a.rust))}
    report = compare(runs, scenarios.split(","), a.single, coalesce_rejoin_sdp)
    (a.output / "comparison.json").write_text(json.dumps(report, indent=2) + "\n")
    for name, v in report["scenarios"].items():
        print(("MATCH " if v["match"] else "DIFF  ") + name + ("" if v["match"] else "  " + json.dumps(v["diff"] or v["errors"])))
    return 0 if report["match"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
