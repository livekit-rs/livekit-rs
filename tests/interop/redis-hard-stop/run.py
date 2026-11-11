#!/usr/bin/env python3
"""Isolate the server command client's cached-slot behavior from original PSRPC workers."""
import argparse
import importlib.util
import json
from pathlib import Path
import select
import subprocess
import sys
import time

sys.dont_write_bytecode = True
helper_path = Path(__file__).resolve().parents[1] / "sip-redis-failover/run.py"
spec = importlib.util.spec_from_file_location("redis_fixture_helpers", helper_path)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)

PROFILES = (("egress-read", "HGET", "egress"), ("ingress-read", "HGET", "ingress"),
            ("room-cleanup", "HDEL", "rooms"))
HOST = helpers.HOST


def redis(port, *args, input_text=None):
    return subprocess.run(["redis-cli", "--raw", "-h", HOST, "-p", str(port), *args],
                          input=input_text, capture_output=True, text=True, check=True, timeout=20).stdout.strip()


def slot_owner(port, slot):
    slots = subprocess.run(["redis-cli", "--json", "-h", HOST, "-p", str(port), "CLUSTER", "SLOTS"],
                           capture_output=True, text=True, check=True, timeout=3)
    return next((row[2][1] for row in json.loads(slots.stdout) if row[0] <= slot <= row[1]), None)


def read_warm(client):
    end = time.monotonic() + 20
    while time.monotonic() < end:
        if select.select([client.stdout], [], [], .2)[0]:
            line = client.stdout.readline()
            if not line:
                raise RuntimeError("observer exited before warming cache")
            try:
                row = json.loads(line)
            except ValueError:
                continue
            assert row == {"phase": "warm", "value": "fixture"}, row
            return
    raise TimeoutError("observer warm HGET")


def observe(args, profile, operation, key, output):
    output.mkdir()
    data = {"profile": profile, "operation": operation, "key": key, "status": "FAIL"}
    brokers, clients = {}, []
    selected = helpers.ports(12)
    redis_ports, bus_ports = selected[:6], selected[6:]
    observed = False

    def start_observer(kind, phase, addresses):
        field = f"{kind}-{phase}"
        binary = getattr(args, kind)
        with (output / f"{field}.stderr").open("wb") as stderr:
            child = subprocess.Popen([str(binary), addresses, operation, key, field],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr,
                                     text=True, bufsize=1)
        clients.append(child)
        read_warm(child)
        return child

    def request(cohort):
        for client in cohort.values():
            client.stdin.write("run\n")
            client.stdin.flush()
        results = {}
        for kind, client in cohort.items():
            stdout, _ = client.communicate(timeout=25)
            rows = [json.loads(line) for line in stdout.splitlines() if line.startswith("{")]
            assert len(rows) == 1 and rows[0]["phase"] == "request", stdout
            results[kind] = {**rows[0], "exit": client.returncode}
            assert client.returncode == 0
        return results

    try:
        for index, port in enumerate(redis_ports):
            directory = output / f"redis-{index}"
            directory.mkdir()
            with (output / f"redis-{index}.log").open("wb") as log:
                brokers[port] = subprocess.Popen([
                    "redis-server", "--bind", HOST, "--port", str(port), "--dir", str(directory),
                    "--save", "", "--appendonly", "no", "--cluster-enabled", "yes",
                    "--cluster-port", str(bus_ports[index]), "--cluster-config-file", "nodes.conf",
                    "--cluster-node-timeout", "1000",
                ], stdout=log, stderr=subprocess.STDOUT)
            helpers.wait(lambda: helpers.redis_cli(port, "PING") == "PONG", 8, "Redis ready", brokers[port])
        addresses = ",".join(f"{HOST}:{p}" for p in redis_ports)
        create = subprocess.run(["redis-cli", "--cluster", "create", *addresses.split(","),
                                 "--cluster-replicas", "1", "--cluster-yes"],
                                capture_output=True, text=True, check=True, timeout=25)
        assert "[OK] All 16384 slots covered" in create.stdout
        (output / "cluster-create.log").write_text(create.stdout)
        helpers.wait(lambda: all("cluster_state:ok" in redis(p, "CLUSTER", "INFO") for p in redis_ports),
                     15, "all brokers healthy")
        slot = int(redis(redis_ports[0], "CLUSTER", "KEYSLOT", key))
        old = slot_owner(redis_ports[0], slot)
        assert old in brokers
        seed = f"HSET {key} go-warm fixture rust-warm fixture go-fresh fixture rust-fresh fixture"
        # WAIT must use the same connection and write offset as HSET.
        fields = redis(old, input_text=seed + "\nWAIT 1 15000\n").splitlines()
        assert fields == ["4", "1"], fields
        data.update(slot=slot, old_primary=old, seed_replica_acks=int(fields[1]))
        warm = {kind: start_observer(kind, "warm", addresses) for kind in ("go", "rust")}
        started = time.monotonic()
        brokers[old].kill()
        data["old_primary_exit"] = brokers[old].wait(timeout=5)
        assert data["old_primary_exit"] == -9
        alive = [p for p in redis_ports if p != old]
        assert helpers.redis_cli(old, "PING") != "PONG"

        def elected():
            owners = [slot_owner(p, slot) for p in alive]
            if len(set(owners)) == 1 and owners[0] in alive and all(
                "cluster_state:ok" in redis(p, "CLUSTER", "INFO") for p in alive
            ):
                return owners[0]
            return None

        data["new_primary"] = helpers.wait(elected, 25, "automatic election and full slot agreement")
        data["ms_since_stop_at_request"] = round((time.monotonic() - started) * 1000)
        assert data["ms_since_stop_at_request"] < 30000
        data["warm"] = request(warm)
        fresh_addrs = ",".join(f"{HOST}:{p}" for p in alive)
        data["fresh"] = request({kind: start_observer(kind, "fresh", fresh_addrs) for kind in ("go", "rust")})
        expected = "fixture" if operation == "HGET" else 1
        assert all(row.get("value") == expected and "error" not in row for row in data["fresh"].values())
        for kind, row in data["warm"].items():
            if "error" in row:
                assert "refused" in row["error"].lower() and str(old) in row["error"], row
            else:
                assert row["value"] == expected, row
            if operation == "HDEL":
                remaining = redis(data["new_primary"], "HGET", key, f"{kind}-warm")
                assert remaining == ("fixture" if "error" in row else "")
                assert redis(data["new_primary"], "HGET", key, f"{kind}-fresh") == ""
        data["same_warm_outcome"] = ("error" in data["warm"]["go"]) == ("error" in data["warm"]["rust"])
        observed = True
    finally:
        for client in clients:
            if client.poll() is None:
                client.kill()
            client.wait(timeout=5)
        for child in brokers.values():
            if child.poll() is None:
                child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)
        data["broker_exits"] = {p: child.returncode for p, child in brokers.items()}
        data["ports_released"] = all(helpers.released(p) for p in selected)
        if observed and data["ports_released"] and all(
            code == (-9 if p == data["old_primary"] else 0)
            for p, code in data["broker_exits"].items()
        ):
            data["status"] = "OBSERVED"
        (output / "execution.json").write_text(json.dumps(data, indent=2) + "\n")
    assert data["status"] == "OBSERVED"
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir()
    rows = [observe(args, *profile, args.output / profile[0]) for profile in PROFILES]
    (args.output / "execution.json").write_text(json.dumps({
        "binary_sha256": {kind: helpers.sha(getattr(args, kind)) for kind in ("go", "rust")},
        "cases": rows,
    }, indent=2) + "\n")


if __name__ == "__main__":
    main()
