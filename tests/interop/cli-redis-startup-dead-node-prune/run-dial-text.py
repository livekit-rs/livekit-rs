#!/usr/bin/env python3
"""C43: the server-start refused-dial fatal line, paired across topologies.

Item: CONFIG_BEHAVIOR:Redis.NodePruneFailureText (the server-produced fatal
startup line, NOT the go-redis library stderr).

Upstream builds the Redis client and runs the dead-node sweep during startup
(`server.go:197` `router.RemoveDeadNodes`). When the startup Redis dial is refused
the daemon prints a fatal line to STDOUT and exits 1 before any listener answers.
The line tracks the real dial address and the endpoint topology:

  SIMPLE  address refused:
    unable to connect to redis: dial tcp 127.0.0.1:<port>: connect: connection refused
  CLUSTER seed refused (first seed's net.OpError, osscluster.go loadState firstErr):
    unable to connect to redis: dial tcp 127.0.0.1:<port>: connect: connection refused
  SENTINEL unreachable (aggregate of the per-sentinel dial attempts):
    unable to connect to redis: redis: all sentinels specified in configuration are
    unreachable: dial tcp 127.0.0.1:<port>: connect: connection refused

This harness pins each line BYTE-EQUAL between the locked Go daemon and the Rust
candidate. The two SIMPLE cases use distinct refused ports so the line must track
the real dial address rather than a fixed string; the CLUSTER and SENTINEL cases
add the topology branches that route through the discovery Ping (`ping_existing`)
instead of the bus pool's `connect_endpoint`. Both binaries are fed identical
config bytes per case.

Scope boundary: ONLY the server-produced fatal STDOUT line is compared. The
go-redis library stderr (`pool.go:762` "connection pool: failed to dial after 5
attempts") is recorded per side but NOT compared -- it belongs to
CONFIG_BEHAVIOR:Redis.SentinelDiscoveryDiagnostics, not this leaf. The Rust
candidate dials eagerly and fails on the first refused attempt, so it emits no
pool-retry stderr; that library-diagnostic difference is deliberately out of scope
here and is never read as a fatal-line parity failure.

Owned loopback fixture: each refused port is reserved then released so nothing
listens; no broker is started. The physical http/tcp/udp ports are reserved free
and must stay released (the daemon exits before binding them).
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "prune_fixture", Path(__file__).with_name("run-paired.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
GO_REV = "e5655ac20267fa5005dccaf3f0e3bfc3ea688c07"
ITEM = "CONFIG_BEHAVIOR:Redis.NodePruneFailureText"
CASES = ("simple-refused-dial", "simple-refused-dial-alt-port",
         "cluster-refused-dial", "sentinel-unreachable")


def ports():
    # Reserve simultaneously so no two slots pick the same port; the `with` closes
    # them all, leaving the redis slot refused (nothing listens) and http/tcp/udp free.
    with (socket.socket() as redis, socket.socket() as http, socket.socket() as tcp,
          socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp):
        for listener in (redis, http, tcp, udp):
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in (redis, http, tcp, udp)]


def case_config(case, redis_port, tcp, udp):
    common = ("keys: {devkey: secret}\nlogging: {level: info, json: true}\n"
              "bind_addresses: [127.0.0.1]\n"
              "rtc: {node_ip: 127.0.0.1, use_external_ip: false, "
              f"tcp_port: {tcp}, udp_port: {udp}, enable_loopback_candidate: true}}\n"
              "turn: {enabled: false}\n")
    if case.startswith("cluster"):
        # A single refused seed makes the reported first-seed address unambiguous.
        return common + f"redis:\n  cluster_addresses: [127.0.0.1:{redis_port}]\n"
    if case.startswith("sentinel"):
        return common + (f"redis:\n  sentinel_addresses: [127.0.0.1:{redis_port}]\n"
                         "  sentinel_master_name: mymaster\n")
    return common + f"redis:\n  address: 127.0.0.1:{redis_port}\n"


def case_expected(case, redis_port):
    if case.startswith("sentinel"):
        return ("unable to connect to redis: redis: all sentinels specified in "
                "configuration are unreachable: "
                f"dial tcp 127.0.0.1:{redis_port}: connect: connection refused")
    return (f"unable to connect to redis: dial tcp 127.0.0.1:{redis_port}: "
            "connect: connection refused")


def run_server(binary, config, tag, output, server_ports):
    http, tcp, udp = server_ports
    argv = [str(binary), "--dev", "--config", str(config), "--port", str(http)]
    reached_http = False
    timed_out = False
    with (output / f"{tag}.stdout").open("wb") as stdout, \
            (output / f"{tag}.stderr").open("wb") as stderr:
        proc = subprocess.Popen(argv, stdout=stdout, stderr=stderr, start_new_session=True)
        try:
            # The Sentinel branch resolves the master per dial on its own budget,
            # so allow more headroom than the instant SIMPLE/CLUSTER refusal.
            deadline = time.monotonic() + 40
            while proc.poll() is None and time.monotonic() < deadline:
                reached_http |= not fixture.free(http)
                time.sleep(.025)
            timed_out = proc.poll() is None
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)
    # The daemon must never have bound the media udp port either.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(("127.0.0.1", udp))
    raw = (output / f"{tag}.stdout").read_bytes()
    err = (output / f"{tag}.stderr").read_bytes()
    # The fatal CLI line is the last STDOUT line. Keep the full streams beside it.
    diagnostic = raw.decode("utf-8").splitlines()[-1] if raw else None
    return {
        "argv": argv, "exit": proc.returncode, "http_bound": reached_http,
        "timed_out": timed_out,
        "ports_released": fixture.free(http) and fixture.free(tcp),
        "fatal_stdout_line": diagnostic,
        "stdout_sha256": fixture.sha(raw),
        "stderr_sha256": fixture.sha(err),
        # Recorded, NOT compared: go-redis library retry diagnostics (out of scope).
        "stderr_library_dial_lines": [
            line for line in err.decode("utf-8", "replace").splitlines()
            if "pool.go" in line or "failed to dial" in line],
    }


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
    assert version == fixture.OFFICIAL_VERSION, ("locked reference binary required", version)
    assert f"vcs.revision={GO_REV}" in vcs and "vcs.modified=false" in vcs
    (output / "go-build-info.txt").write_text(vcs)
    report = {
        "items": [ITEM],
        "go": {"version": version, "sha256": fixture.sha(go.read_bytes())},
        "rust": {"sha256": fixture.sha(rust.read_bytes())},
        "go_binary_sha256": fixture.sha(go.read_bytes()),
        "rust_binary_sha256": fixture.sha(rust.read_bytes()),
        "scope": (
            "the server-produced fatal STDOUT line printed when the startup Redis "
            "dial is refused, compared byte-equal between the locked Go daemon and "
            "the Rust candidate across all three endpoint topologies: SIMPLE address "
            "(two distinct refused ports so the line must track the real dial "
            "address), CLUSTER first seed, and SENTINEL aggregate-unreachable. The "
            "CLUSTER/SENTINEL branches route through the discovery Ping "
            "(ping_existing), the SIMPLE branch through the bus pool "
            "(connect_endpoint). exit status, no HTTP listener, no timeout and "
            "released physical ports are asserted on both sides. The go-redis "
            "library stderr (pool.go:762 retry diagnostics) is recorded per side but "
            "NOT compared: it belongs to "
            "CONFIG_BEHAVIOR:Redis.SentinelDiscoveryDiagnostics."),
        "normalization": (
            "fatal_stdout_line is compared exactly (and against the expected Go "
            "sentence built from the actual refused port and topology); exit==1, "
            "http_bound==False, timed_out==False and ports_released==True are "
            "required per side. stderr_library_dial_lines and the stream shas are "
            "recorded, not compared. Both sides are fed the identical config file "
            "bytes per case."),
        "cases": [],
    }
    refused_ports = []
    with tempfile.TemporaryDirectory(prefix="lk-dial-text-") as directory:
        staging = Path(directory)
        for case in CASES:
            redis_port, http, tcp, udp = ports()
            refused_ports.append(redis_port)
            assert fixture.free(redis_port), (case, "redis port answers, not refused", redis_port)
            config = staging / f"{case}.yaml"
            body = case_config(case, redis_port, tcp, udp)
            config.write_text(body)
            expected = case_expected(case, redis_port)
            case_out = output / case
            case_out.mkdir()
            result = {"case": case, "expected": expected, "redis_port": redis_port,
                      "redis_port_refused": fixture.free(redis_port),
                      "config": body, "config_sha256": fixture.sha(config.read_bytes())}
            for tag, binary in (("go", go), ("rust", rust)):
                result[tag] = run_server(binary, config, tag, case_out, (http, tcp, udp))
            go_line = result["go"]["fatal_stdout_line"] or ""
            result["go_matches_expected"] = go_line == expected
            result["fatal_line_equal"] = result["rust"]["fatal_stdout_line"] == go_line
            result["accepted"] = (
                result["go_matches_expected"] and result["fatal_line_equal"]
                and result["redis_port_refused"]
                and all(side["exit"] == 1 and not side["timed_out"]
                        and not side["http_bound"] and side["ports_released"]
                        for side in (result["go"], result["rust"])))
            report["cases"].append(result)
    # Every refused port must differ, otherwise the address-tracking control is vacuous.
    report["distinct_refused_ports"] = len(set(refused_ports)) == len(refused_ports)
    report["status"] = "PASS" if report["distinct_refused_ports"] and all(
        case["accepted"] for case in report["cases"]) else "FAIL"
    (output / "execution.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({
        "status": report["status"], "cases": len(report["cases"]),
        "distinct_refused_ports": report["distinct_refused_ports"],
        "lines": [{"case": c["case"], "go": c["go"]["fatal_stdout_line"],
                   "rust": c["rust"]["fatal_stdout_line"],
                   "equal": c["fatal_line_equal"]} for c in report["cases"]],
    }, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())