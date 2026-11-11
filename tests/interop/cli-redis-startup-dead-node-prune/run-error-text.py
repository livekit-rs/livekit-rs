#!/usr/bin/env python3
"""Compare fatal node-sweep diagnostics using the real server and an owned Redis."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("prune_fixture", Path(__file__).with_name("run-paired.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
GO_REV = "e5655ac20267fa5005dccaf3f0e3bfc3ea688c07"


def ports():
    # Keep the reservations simultaneous to avoid selecting the same port twice.
    with (socket.socket() as redis, socket.socket() as http, socket.socket() as tcp,
          socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp, socket.socket() as metrics):
        for listener in (redis, http, tcp, udp, metrics):
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in (redis, http, tcp, udp, metrics)]


def readback(redis, key_kind):
    if key_kind == "string":
        return redis.call(b"GET", b"nodes").hex()
    entries = iter(redis.call(b"HGETALL", b"nodes"))
    return {name.decode(): value.hex() for name, value in zip(entries, entries, strict=True)}


def configure(redis, case):
    assert redis.call(b"DEL", b"nodes") in (0, 1)
    if case == "wrongtype":
        assert redis.call(b"SET", b"nodes", b"wrong Redis key kind") == b"OK"
        return "string"
    records = {
        b"ND_keep": fixture.node_proto("ND_keep"),
    }
    if case != "occupied-metrics":
        records[b"ND_stale"] = fixture.node_proto("ND_stale", int(time.time()) - 3600)
    if case.startswith("malformed-wire"):
        records[b"ND_invalid"] = b"\x80"
    elif case == "invalid-utf8":
        records[b"ND_invalid"] = b"\x0a\x01\xff"
    for name, value in records.items():
        assert redis.call(b"HSET", b"nodes", name, value) == 1
    return "hash"


def run_server(binary, config, tag, output, server_ports):
    http, tcp, udp = server_ports
    argv = [str(binary), "--dev", "--config", str(config), "--port", str(http)]
    reached_http = False
    timed_out = False
    with (output / f"{tag}.stdout").open("wb") as stdout, (output / f"{tag}.stderr").open("wb") as stderr:
        proc = subprocess.Popen(argv, stdout=stdout, stderr=stderr, start_new_session=True)
        try:
            deadline = time.monotonic() + 10
            while proc.poll() is None and time.monotonic() < deadline:
                reached_http |= not fixture.free(http)
                time.sleep(.025)
            timed_out = proc.poll() is None
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(("127.0.0.1", udp))
    # The fatal CLI line is stdout. Keep the full streams, including logs.
    raw = (output / f"{tag}.stdout").read_bytes()
    diagnostic = raw.decode("utf-8").splitlines()[-1] if raw else None
    return {
        "argv": argv, "exit": proc.returncode, "http_bound": reached_http, "timed_out": timed_out,
        "ports_released": fixture.free(http) and fixture.free(tcp), "fatal_stdout_line": diagnostic,
        "stdout_sha256": fixture.sha(raw),
        "stderr_sha256": fixture.sha((output / f"{tag}.stderr").read_bytes()),
    }


def node_mutations(monitor):
    # All daemon commands have finished by the time we read the monitor.
    monitor.sock.settimeout(.15)
    mutations = []
    while True:
        try:
            line = monitor.read()
        except TimeoutError:
            return mutations
        matched = re.search(rb'"(hset|hdel)" "nodes" "([^"]+)"', line, re.I)
        if matched:
            command, node = matched.groups()
            mutations.append([command.decode().lower(), node.decode()])


def redis_cleanup(redis):
    deadline = time.monotonic() + 3
    while True:
        clients = len(redis.call(b"CLIENT", b"LIST").splitlines())
        channels = redis.call(b"PUBSUB", b"CHANNELS")
        patterns = redis.call(b"PUBSUB", b"NUMPAT")
        released = clients == 1 and channels == [] and patterns == 0
        if released or time.monotonic() >= deadline:
            return {
                "released": released, "client_count_including_coordinator": clients,
                "channels": [channel.decode() for channel in channels], "patterns": patterns,
            }
        time.sleep(.025)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--official", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True)
    go, rust = args.official.resolve(), args.rust.resolve()
    version = fixture.run_checked([str(go), "--version"]).stdout.strip()
    vcs = fixture.run_checked(["go", "version", "-m", str(go)]).stdout
    assert version == fixture.OFFICIAL_VERSION
    assert f"vcs.revision={GO_REV}" in vcs and "vcs.modified=false" in vcs
    (output / "go-build-info.txt").write_text(vcs)
    report = {"go": {"version": version, "sha256": fixture.sha(go.read_bytes())},
              "rust": {"sha256": fixture.sha(rust.read_bytes())}, "cases": []}
    cases = {
        "malformed-wire": "proto: cannot parse invalid wire-format data",
        "invalid-utf8": "string field contains invalid UTF-8",
        "wrongtype": "could not list nodes: WRONGTYPE Operation against a key holding the wrong kind of value",
        "deny-hvals": "could not list nodes: NOPERM ",
        "deny-hdel": "NOPERM ",
        "malformed-wire-and-occupied-metrics": "proto: cannot parse invalid wire-format data",
        "occupied-metrics": "listen tcp ",
    }
    redis_port, http, tcp, udp, metrics = ports()
    with tempfile.TemporaryDirectory(prefix="lk-prune-text-") as directory:
        staging = Path(directory)
        broker = fixture.redis_server(redis_port, staging)
        try:
            redis = fixture.Resp("127.0.0.1", redis_port)
            try:
                for case, expected in cases.items():
                    case_out = output / case
                    case_out.mkdir()
                    kind = configure(redis, case)
                    deny = {"deny-hvals": [b"-hvals"], "deny-hdel": [b"-hdel"]}.get(case, [])
                    assert redis.call(
                        b"ACL", b"SETUSER", b"livekit", b"reset", b"on",
                        b">fixture-only", b"~*", b"&*", b"+@all", *deny
                    ) == b"OK"
                    prior = readback(redis, kind)
                    config = staging / f"{case}.yaml"
                    body = ("keys: {devkey: secret}\nlogging: {level: info, json: true}\n"
                            "bind_addresses: [127.0.0.1]\n"
                            "rtc: {node_ip: 127.0.0.1, use_external_ip: false, "
                            f"tcp_port: {tcp}, udp_port: {udp}, enable_loopback_candidate: true}}\n"
                            "turn: {enabled: false}\n"
                            f"redis: {{address: '127.0.0.1:{redis_port}', "
                            "username: livekit, password: fixture-only}\n")
                    with socket.socket() as held:
                        if case.endswith("occupied-metrics"):
                            held.bind(("127.0.0.1", metrics))
                            held.listen()
                            body += f"prometheus: {{port: {metrics}}}\n"
                        config.write_text(body)
                        result = {"case": case, "expected": expected, "keyspace": prior,
                                  "config": body, "config_sha256": fixture.sha(config.read_bytes())}
                        for tag, binary in (("go", go), ("rust", rust)):
                            monitor = fixture.Resp("127.0.0.1", redis_port)
                            try:
                                assert monitor.call(b"MONITOR") == b"OK"
                                side = run_server(binary, config, tag, case_out, (http, tcp, udp))
                                side["node_mutations"] = node_mutations(monitor)
                            finally:
                                monitor.close()
                            side["keyspace_preserved"] = readback(redis, kind) == prior
                            side["redis_cleanup"] = redis_cleanup(redis)
                            result[tag] = side
                    go_line = result["go"]["fatal_stdout_line"] or ""
                    go_matches = go_line.startswith(expected) if expected.endswith(" ") else go_line == expected
                    if case == "occupied-metrics":
                        go_matches &= go_line == f"listen tcp 127.0.0.1:{metrics}: bind: address already in use"
                    result["fatal_line_equal"] = result["rust"]["fatal_stdout_line"] == go_line
                    result["accepted"] = go_matches and result["fatal_line_equal"] and all(
                        side["exit"] == 1 and not side["timed_out"]
                        and side["ports_released"] and side["keyspace_preserved"]
                        and side["redis_cleanup"]["released"]
                        and accepted_startup(side, case)
                        for side in (result["go"], result["rust"]))
                    report["cases"].append(result)
            finally:
                redis.close()
        finally:
            fixture.stop_process(broker)
            report["redis_exit"] = broker.returncode
            report["redis_port_released"] = fixture.free(redis_port)
            (output / "redis.log").write_bytes((staging / f"redis-{redis_port}.log").read_bytes())
    report["status"] = "PASS" if report["redis_exit"] == 0 and report["redis_port_released"] and all(
        case["accepted"] for case in report["cases"]) else "FAIL"
    (output / "execution.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["status"] == "PASS" else 1


def accepted_startup(side, case):
    if case != "occupied-metrics":
        return not side["http_bound"] and not side["node_mutations"]
    mutations = side["node_mutations"]
    return bool(
        len(mutations) == 2
        and [command for command, _ in mutations] == ["hset", "hdel"]
        and mutations[0][1] == mutations[1][1]
    )


if __name__ == "__main__":
    raise SystemExit(main())
