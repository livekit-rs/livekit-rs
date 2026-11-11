#!/usr/bin/env python3
"""Observe Go and Rust Egress and Ingress RedisStore reply boundaries."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import socket
import socketserver
import subprocess
import threading

PROFILES = {
    "egress-first-loading": ("egress", "HSET", b"-LOADING fixture is temporarily unavailable\r\n", 3),
    "egress-first-loading-exhausted": ("egress", "HSET", b"-LOADING fixture is temporarily unavailable\r\n", 4),
    "egress-first-ordinary-error": ("egress", "HSET", b"-ERR ordinary error mentioning LOADING\r\n", 4),
    "egress-later-loading": ("egress", "SADD", b"-LOADING fixture is temporarily unavailable\r\n", 4),
    "egress-committed-later-reply-lost": ("egress", "SADD", "after", 1),
    "egress-no-fault": ("egress", "", None, 0),
    "ingress-exec-not-forwarded": ("ingress", "EXEC", "before", 1),
    "ingress-committed-exec-reply-lost": ("ingress", "EXEC", "after", 1),
    "ingress-no-fault": ("ingress", "", None, 0),
}

def cli(port, *command):
    binary = shutil.which("redis-cli")
    result = subprocess.run(
        [binary, "-h", "127.0.0.1", "-p", str(port), "--raw", *command],
        capture_output=True, check=True, timeout=3,
    )
    return result.stdout.strip()

def seed_ingress(port, state):
    assert cli(port, "SADD", "room_{ingress}:fixture", "IN_fixture") == b"1"
    assert cli(port, "HSET", "{ingress}_stream_key", "stream", "IN_fixture") == b"1"
    assert cli(port, "HSET", "ingress", "IN_fixture", state) == b"1"
    assert cli(port, "SET", "{ingress}_state:IN_fixture", state) == b"OK"

class Tap(socketserver.ThreadingTCPServer):
    daemon_threads = False

    def __init__(self, backend, profile, frame):
        self.backend = backend
        self.op, self.target, self.fault, self.remaining = PROFILES[profile]
        self.frame = frame
        self.events = []
        self.sockets = 0
        self.errors = []
        self.lock = threading.Lock()
        super().__init__(("127.0.0.1", 0), Handler)

    def handle_error(self, request, client_address):
        with self.lock:
            self.errors.append("RESP relay failed")
        super().handle_error(request, client_address)

    def classify(self, ident, command):
        with self.lock:
            fault = self.fault if command == self.target and self.remaining else None
            if fault is not None:
                self.remaining -= 1
            if command in ("MULTI", "EXEC", "HSET", "SADD", "SREM", "HDEL", "DEL"):
                row = {"socket": ident, "command": command}
                self.events.append(row)
            else:
                row = None
            return fault, row

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        tap = self.server
        with tap.lock:
            tap.sockets += 1
            ident = tap.sockets
        with socket.create_connection(("127.0.0.1", tap.backend), timeout=8) as upstream:
            with self.request.makefile("rb") as client, upstream.makefile("rb") as redis:
                self.request.settimeout(8)
                while True:
                    try:
                        raw, argv = tap.frame(client)
                    except (EOFError, ConnectionError):
                        return
                    command = argv[0].decode().upper()
                    fault, event = tap.classify(ident, command)
                    if fault == "before":
                        event.update(forwarded=False, client_reply="EOF before forwarding to Redis")
                        return
                    if isinstance(fault, bytes):
                        reply = fault
                        event.update(forwarded=False)
                    else:
                        upstream.sendall(raw)
                        reply, _ = tap.frame(redis)
                        if event is not None:
                            event.update(forwarded=True, redis_reply=reply.decode())
                    if fault == "after":
                        event["client_reply"] = "EOF after actual Redis reply"
                        if tap.op == "ingress":
                            assert reply == b"*4\r\n:1\r\n:1\r\n:1\r\n:1\r\n"
                            seed_ingress(tap.backend, "recreated-after-first-exec")
                            event["concurrent_recreation"] = True
                        return
                    if event is not None:
                        event["client_reply"] = reply.decode()
                    try:
                        self.request.sendall(reply)
                    except ConnectionError:
                        return

def run_one(binary, kind, profile, helpers, output):
    with helpers.owned_redis(output / "redis.log") as port, Tap(port, profile, helpers.frame) as tap:
        if tap.op == "ingress":
            seed_ingress(port, "original")
        thread = threading.Thread(target=tap.serve_forever)
        thread.start()
        try:
            host = f"127.0.0.1:{tap.server_address[1]}"
            endpoint = host if kind == "go" else "redis://" + host
            result = subprocess.run([binary, endpoint, tap.op], capture_output=True, timeout=15)
            (output / "stdout").write_bytes(result.stdout)
            (output / "stderr").write_bytes(result.stderr)
            parsed = [json.loads(line) for line in result.stdout.splitlines() if line.startswith(b"{")]
            assert result.returncode == 0 and len(parsed) == 1
            if tap.op == "egress":
                final = {
                    "entry_exists": int(cli(port, "HEXISTS", "egress", "EG_fixture")),
                    "room_index": int(cli(port, "SISMEMBER", "egress:room:fixture", "EG_fixture")),
                }
            else:
                final = {
                    "recovery_entry": int(cli(port, "HEXISTS", "egress", "EG_recovery")),
                    "recovery_index": int(cli(port, "SISMEMBER", "egress:room:recovery", "EG_recovery")),
                    "entry": cli(port, "HGET", "ingress", "IN_fixture").decode(),
                    "state": cli(port, "GET", "{ingress}_state:IN_fixture").decode(),
                    "stream": cli(port, "HGET", "{ingress}_stream_key", "stream").decode(),
                    "room_index": int(cli(port, "SISMEMBER", "room_{ingress}:fixture", "IN_fixture")),
                }
            return {"exit": result.returncode, **parsed[0], "events": tap.events, "redis": final}
        finally:
            tap.shutdown()
            thread.join(timeout=4)
            assert not thread.is_alive() and not tap.errors

def check(row, profile, go_or_fixed, tx_closed):
    events = row["events"]
    if profile.startswith("egress"):
        n = 1
        expected_success = profile in ("egress-no-fault", "egress-committed-later-reply-lost")
        if profile == "egress-first-loading":
            n = 4 if go_or_fixed else 1
            expected_success = go_or_fixed
        if profile == "egress-first-loading-exhausted":
            n = 4 if go_or_fixed else 1
        if profile == "egress-committed-later-reply-lost":
            n = 2
            assert len({e["socket"] for e in events}) == 2
        if profile.startswith("egress-first-loading"):
            assert len({e["socket"] for e in events}) == 1
        assert [e["command"] for e in events] == ["HSET", "SADD"] * n
        entry = int(expected_success or profile == "egress-later-loading")
        index = int(profile != "egress-later-loading")
        assert row["redis"] == {"entry_exists": entry, "room_index": index}
        assert row["success"] == expected_success
        return
    execs = [e for e in events if e["command"] == "EXEC"]
    if profile == "ingress-no-fault":
        assert row["success"] and len(execs) == 1
        expected = ""
    else:
        assert execs[0]["forwarded"] == (profile == "ingress-committed-exec-reply-lost")
        if row["kind"] == "go" or tx_closed:
            assert not row["success"] and len(execs) == 1
            expected = "original" if profile == "ingress-exec-not-forwarded" else "recreated-after-first-exec"
        else:
            # Track separately. The scoped ordinary pipeline repair does not
            # infer transaction write completion from a generic driver error.
            assert row["success"] and len(execs) == 2
            expected = ""
    assert row["recovery"]
    assert row["redis"] == {"entry": expected, "state": expected,
                            "stream": "IN_fixture" if expected else "",
                            "room_index": int(bool(expected)), "recovery_entry": 1, "recovery_index": 1}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--go", type=Path, required=True)
    p.add_argument("--rust", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--expect", choices=("before", "after", "transaction-closed"), required=True)
    p.add_argument("--wire-helper", type=Path,
                   default=Path(__file__).resolve().parents[1] / "redis-command-retry/run.py")
    args = p.parse_args()
    spec = importlib.util.spec_from_file_location("wire", args.wire_helper)
    helpers = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helpers)
    args.output.mkdir()
    report = {"expect": args.expect, "scope": "ordinary Egress pipeline and Ingress transaction reply-loss handling", "rows": []}
    for kind, binary in (("go", args.go), ("rust", args.rust)):
        report[kind + "_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
        for profile in PROFILES:
            output = args.output / (kind + "-" + profile)
            output.mkdir()
            row = run_one(binary, kind, profile, helpers, output)
            row.update(kind=kind, profile=profile)
            report["rows"].append(row)
            (args.output / "execution.json").write_text(json.dumps(report, indent=2) + "\n")
            check(row, profile, kind == "go" or args.expect != "before", args.expect == "transaction-closed")
    report["status"] = ("ORDINARY_PIPELINE_DIFFERENCE_REPRODUCED" if args.expect == "before"
                        else "ORDINARY_PIPELINE_SCOPED_MATCH")
    report["ingress_transaction_reply_loss"] = ("SCOPED_WIRE_MATCH" if args.expect == "transaction-closed" else "GAP_REMAINS")
    (args.output / "execution.json").write_text(json.dumps(report, indent=2) + "\n")

if __name__ == "__main__":
    main()
