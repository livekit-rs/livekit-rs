#!/usr/bin/env python3
"""Compare original Ingress transaction effects through real Redis Cluster and Sentinel."""
import argparse
from contextlib import contextmanager, ExitStack
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import socket
import ssl
import subprocess
import tempfile
import threading
import time


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HERE = Path(__file__).resolve().parent
wire = load(HERE.parent / "redis-command-retry" / "run.py", "wire")
case = load(HERE / "run.py", "case")
FIXTURES = HERE.parents[2] / "crates/rtc-tls/tests/fixtures"


def resp(value):
    if isinstance(value, bytes):
        return b"$%d\r\n" % len(value) + value + b"\r\n"
    if isinstance(value, int):
        return b":%d\r\n" % value
    return b"*%d\r\n" % len(value) + b"".join(map(resp, value))


def port_pair(used):
    for port in random.sample(range(27000, 43000), 300):
        if {port, port + 10000}.intersection(used):
            continue
        with socket.socket() as client, socket.socket() as bus:
            try:
                client.bind(("127.0.0.1", port))
                bus.bind(("127.0.0.1", port + 10000))
            except OSError:
                continue
        used.update((port, port + 10000))
        return port
    raise RuntimeError("no local Redis port pair")


def cli(port, *args):
    return subprocess.check_output(
        ["redis-cli", "-h", "127.0.0.1", "-p", str(port), "--raw", *args],
        stderr=subprocess.PIPE, timeout=15,
    ).strip()


def wait_for(check):
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except subprocess.CalledProcessError:
            pass
        time.sleep(.1)
    raise RuntimeError("owned Redis readiness timed out")


@contextmanager
def broker(root, port, log, *args):
    root.mkdir()
    command = ["redis-server", "--bind", "127.0.0.1", "--port", str(port),
               "--dir", str(root), "--save", "", "--appendonly", "no", *args]
    with log.open("wb") as stream:
        proc = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT)
        try:
            wait_for(lambda: proc.poll() is None and cli(port, "PING") == b"PONG")
            yield
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=6)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                    raise
            assert proc.returncode == 0


@contextmanager
def running_tap(tap):
    with tap:
        thread = threading.Thread(target=tap.serve_forever)
        thread.start()
        try:
            yield tap
        finally:
            tap.shutdown()
            thread.join(timeout=4)
            assert not thread.is_alive() and tap.errors == 0


@contextmanager
def cluster(work, output, profile, tls):
    used = set()
    ports = [port_pair(used) for _ in range(3)]
    with ExitStack() as stack:
        for index, port in enumerate(ports):
            stack.enter_context(broker(
                work / str(index), port, output / f"redis-{index}.log",
                "--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf",
                "--cluster-node-timeout", "1000", "--cluster-announce-ip", "127.0.0.1",
            ))
        subprocess.run(
            ["redis-cli", "--cluster", "create",
             *(f"127.0.0.1:{port}" for port in ports),
             "--cluster-replicas", "0", "--cluster-yes"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=True,
        )
        for port in ports:
            wait_for(lambda: b"cluster_state:ok" in cli(port, "CLUSTER", "INFO"))
        _, slots = case.command(ports[0], wire.frame, b"CLUSTER", b"SLOTS")

        def owner(key):
            slot = int(cli(ports[0], "CLUSTER", "KEYSLOT", key))
            return next(int(node[1]) for low, high, node, *_ in slots
                        if int(low) <= slot <= int(high))

        def rewrite(args, raw, parsed):
            if [word.upper() for word in args[:2]] != [b"CLUSTER", b"SLOTS"]:
                return raw
            translated = []
            for low, high, *nodes in parsed:
                translated.append([int(low), int(high), *[
                    [b"127.0.0.1", redirects[int(node[1])], node[2]] for node in nodes
                ]])
            return resp(translated)

        ingress = owner("ingress")
        taps = {port: stack.enter_context(running_tap(case.Tap(
            port, wire.frame, profile if port == ingress else "normal", rewrite, tls,
        ))) for port in ports}
        redirects = {port: tap.server_address[1] for port, tap in taps.items()}
        address = ",".join(f"127.0.0.1:{redirects[port]}" for port in ports[:2])
        yield address, taps[ingress], ingress, owner("egress")


@contextmanager
def sentinel(work, output, profile, tls):
    with wire.owned_redis(output / "redis.log") as primary:
        with running_tap(case.Tap(primary, wire.frame, profile, tls_context=tls)) as tap:
            port = port_pair({primary, tap.server_address[1]})
            home = work / "sentinel"
            home.mkdir()
            conf = home / "sentinel.conf"
            conf.write_text(
                f"bind 127.0.0.1\nport {port}\ndir {home}\n"
                f"sentinel monitor transaction-fixture 127.0.0.1 {primary} 1\n"
            )
            with (output / "sentinel.log").open("wb") as log:
                process = subprocess.Popen(["redis-server", conf, "--sentinel"],
                                           stdout=log, stderr=subprocess.STDOUT)
                try:
                    wait_for(lambda: process.poll() is None and cli(
                        port, "SENTINEL", "get-master-addr-by-name", "transaction-fixture",
                    ).endswith(str(primary).encode()))

                    def rewrite(args, raw, parsed):
                        if [word.upper() for word in args[:2]] == [
                            b"SENTINEL", b"GET-MASTER-ADDR-BY-NAME",
                        ]:
                            assert int(parsed[1]) == primary
                            return resp([b"127.0.0.1", str(tap.server_address[1]).encode()])
                        return raw

                    with running_tap(case.Tap(port, wire.frame, "normal", rewrite, tls)) as discovery:
                        yield f"127.0.0.1:{discovery.server_address[1]}", tap, primary, primary
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=6)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                        raise
                    assert process.returncode == 0


def run(binary, kind, topology, profile, output, tls_enabled=False):
    output.mkdir()
    handshakes = []
    tls = None
    if tls_enabled:
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(FIXTURES / "expanded/rsa.pem", FIXTURES / "expanded/rsa-key.pem")
        tls.set_servername_callback(lambda _, name, __: handshakes.append(name))
    with tempfile.TemporaryDirectory(prefix="lk-transaction-topology-") as name:
        with {"cluster": cluster, "sentinel": sentinel}[topology](
            Path(name), output, profile, tls,
        ) as (address, tap, ingress, egress):
            case.seed(ingress, wire.frame, tap.room, b"original")
            if topology == "cluster" and kind == "rust":
                scheme = "rediss://" if tls_enabled else "redis://"
                address = ",".join(scheme + addr for addr in address.split(","))
            extra = [str(FIXTURES / "expanded/root-rsa.pem")] if tls_enabled else []
            child = subprocess.run(
                [binary, address, str(len(tap.room)), topology, *extra], capture_output=True, timeout=20,
            )
            (output / "stdout").write_bytes(child.stdout)
            (output / "stderr").write_bytes(child.stderr)
            values = [json.loads(line) for line in child.stdout.splitlines() if line.startswith(b"{")]
            assert child.returncode == 0 and len(values) == 1, (child.returncode, child.stderr)

            def read(port, *args):
                _, value = case.command(port, wire.frame, *args)
                return value.decode()

            if tls_enabled:
                assert handshakes and set(handshakes) == {"localhost"}, handshakes
            return {
                **values[0], "kind": kind, "topology": topology, "profile": profile,
                "entry": read(ingress, b"HGET", b"ingress", b"IN_fixture"),
                "room_member": read(ingress, b"SISMEMBER", b"room_{ingress}:" + tap.room, b"IN_fixture"),
                "recovery_entry": read(egress, b"HEXISTS", b"egress", b"EG_recovery"),
                "stream_key": read(ingress, b"HGET", b"{ingress}_stream_key", b"stream"),
                "state": read(ingress, b"GET", b"{ingress}_state:IN_fixture"),
                "events": tap.events, "exit": child.returncode, "verified_sni": handshakes,
            }


def behavior(row):
    return {key: row[key] for key in (
        "success", "entry", "room_member", "stream_key", "state", "recovery", "recovery_entry",
    )} | {"attempts": [(event["command"], event["forwarded"]) for event in row["events"]
                     if event["command"] in ("MULTI", "PARTIAL_SREM", "EXEC")]}


def check_oracle(row):
    profile = row["profile"]
    result = behavior(row)
    attempts = result["attempts"]
    partial = [("MULTI", True), ("PARTIAL_SREM", False)]
    full = [("MULTI", True), ("EXEC", True)]
    assert result["recovery"] and result["recovery_entry"] == "1"
    if profile == "confirmed-incomplete-write":
        assert result["success"] == (row["topology"] == "cluster")
        assert attempts == partial * 3 + full
        expected = "-1"
    elif profile == "confirmed-incomplete-write-exhausted":
        assert not result["success"] and attempts == partial * 4
        expected = "original"
    elif profile == "normal":
        assert result["success"] and attempts == full
        expected = "-1"
    elif profile == "after-real-exec-before-reply":
        assert not result["success"] and attempts == full
        assert any(event["command"] == "RECREATE" for event in row["events"])
        expected = "recreated"
    else:
        assert not result["success"] and attempts == [("MULTI", True), ("EXEC", False)]
        expected = "original"
    assert result["entry"] == result["state"] == expected
    assert result["room_member"] == ("0" if expected == "-1" else "1")
    assert result["stream_key"] == ("-1" if expected == "-1" else "IN_fixture")
    sockets = [event["socket"] for event in row["events"] if event["command"] == "MULTI"]
    assert len(sockets) == len(set(sockets))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--topology", choices=("cluster", "sentinel"), action="append")
    parser.add_argument("--profile", choices=tuple(case.PROFILES), action="append")
    parser.add_argument("--tls", action="store_true", help="require a verified TLS socket to each tap")
    args = parser.parse_args()
    assert __debug__
    args.output.mkdir()
    report = {"tls": args.tls, "binaries": {kind: hashlib.sha256(binary.read_bytes()).hexdigest()
                          for kind, binary in (("go", args.go), ("rust", args.rust))}, "pairs": []}
    for topology in args.topology or ("cluster", "sentinel"):
        for profile in args.profile or case.PROFILES:
            rows = [run(binary, kind, topology, profile,
                        args.output / f"{topology}-{profile}-{kind}", args.tls)
                    for kind, binary in (("go", args.go), ("rust", args.rust))]
            check_oracle(rows[0])
            equal = behavior(rows[0]) == behavior(rows[1])
            if equal:
                check_oracle(rows[1])
            report["pairs"].append({"topology": topology, "profile": profile,
                                    "match": equal, "rows": rows})
            print(topology, profile, equal, [behavior(row) for row in rows], flush=True)
            (args.output / "execution.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if all(row["match"] for row in report["pairs"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
