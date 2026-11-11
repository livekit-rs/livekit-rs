#!/usr/bin/env python3
"""Observe safe Redis atomic write retry and ambiguous reply failures."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import socketserver
import subprocess
import threading
import time

LARGE_ROOM_BYTES = 12 * 1024 * 1024
PROFILES = {
    "confirmed-incomplete-write": (LARGE_ROOM_BYTES, "write", 3),
    "confirmed-incomplete-write-exhausted": (LARGE_ROOM_BYTES, "write", 4),
    "before-forwarding-exec": (8, "before-exec", 1),
    "after-real-exec-before-reply": (8, "after-exec", 1),
    "normal": (8, None, 0),
}

def command(port, frame, *args):
    if not all(isinstance(x, bytes) for x in args):
        raise TypeError("Redis command takes bytes")
    request = b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n" % len(arg) + arg + b"\r\n" for arg in args)
    with socket.create_connection(("127.0.0.1", port), timeout=5) as stream, stream.makefile("rb") as reader:
        stream.sendall(request)
        return frame(reader)

def seed(port, frame, room, state):
    def send(*args):
        return command(port, frame, *args)[0]
    assert send(b"SADD", b"room_{ingress}:" + room, b"IN_fixture") == b":1\r\n"
    assert send(b"HSET", b"{ingress}_stream_key", b"stream", b"IN_fixture") == b":1\r\n"
    assert send(b"HSET", b"ingress", b"IN_fixture", state) == b":1\r\n"
    assert send(b"SET", b"{ingress}_state:IN_fixture", state) == b"+OK\r\n"

class Tap(socketserver.ThreadingTCPServer):
    daemon_threads = False

    def __init__(self, port, frame, profile, rewrite_reply=None, tls_context=None):
        self.port, self.frame = port, frame
        self.rewrite_reply = rewrite_reply
        self.tls_context = tls_context
        room_bytes, self.fault, self.remaining = PROFILES[profile]
        self.room = b"p" * room_bytes
        self.lock = threading.Lock()
        self.serial = 0
        self.events = []
        self.errors = 0
        super().__init__(("127.0.0.1", 0), Handler)

    def get_request(self):
        conn, addr = super().get_request()
        if self.tls_context:
            conn.settimeout(3)
            try:
                conn = self.tls_context.wrap_socket(conn, server_side=True)
            except OSError:
                conn.close()
                raise
        return conn, addr

    def handle_error(self, request, client_address):
        with self.lock:
            self.errors += 1
        super().handle_error(request, client_address)

    def record(self, serial, command, **detail):
        with self.lock:
            self.events.append({"socket": serial, "command": command, **detail})

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        tap = self.server
        with tap.lock:
            tap.serial += 1
            serial = tap.serial
        if tap.fault == "write":
            self.request.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        self.request.settimeout(8)
        with socket.create_connection(("127.0.0.1", tap.port), timeout=8) as upstream:
            with self.request.makefile("rb") as client, upstream.makefile("rb") as redis:
                while True:
                    try:
                        raw, args = tap.frame(client)
                    except (EOFError, ConnectionError):
                        return
                    op = args[0].decode().upper()
                    if op == "MULTI":
                        with tap.lock:
                            write_fault = tap.fault == "write" and tap.remaining > 0
                            if write_fault:
                                tap.remaining -= 1
                    if op == "EXEC":
                        with tap.lock:
                            fault = tap.fault if tap.remaining else None
                            if fault:
                                tap.remaining -= 1
                        if fault == "before-exec":
                            tap.record(serial, "EXEC", forwarded=False)
                            return
                    else:
                        fault = None
                    upstream.sendall(raw)
                    reply, parsed = tap.frame(redis)
                    if tap.rewrite_reply:
                        reply = tap.rewrite_reply(args, reply, parsed)
                    if op in ("MULTI", "EXEC", "HSET", "SADD"):
                        tap.record(serial, op, forwarded=True, reply=reply.decode())
                    if fault == "after-exec":
                        assert reply == b"*4\r\n:1\r\n:1\r\n:1\r\n:1\r\n"
                        seed(tap.port, tap.frame, tap.room, b"recreated")
                        tap.record(serial, "RECREATE", after_actual_exec=True)
                        return
                    try:
                        self.request.sendall(reply)
                    except ConnectionError:
                        return
                    if op == "MULTI" and write_fault:
                        assert reply == b"+OK\r\n"
                        assert client.readline() == b"*3\r\n"
                        assert client.readline() == b"$4\r\n"
                        assert client.read(6).upper() == b"SREM\r\n"
                        declared = int(client.readline()[1:])
                        assert declared == len(b"room_{ingress}:" + tap.room)
                        partial = client.read(2048)
                        assert partial.startswith(b"room_{ingress}:")
                        tap.record(serial, "PARTIAL_SREM", forwarded=False, declared=declared,
                                   fixture_bytes_read=len(partial), receive_buffer=self.request.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF))
                        # Keep the peer open and stop reading. The observed
                        # application write deadline occurs before this close.
                        time.sleep(.55)
                        return

def run_one(binary, kind, profile, helpers, destination):
    with helpers.owned_redis(destination / "redis.log") as port, Tap(port, helpers.frame, profile) as tap:
        seed(port, helpers.frame, tap.room, b"original")
        thread = threading.Thread(target=tap.serve_forever)
        thread.start()
        try:
            host = f"127.0.0.1:{tap.server_address[1]}"
            endpoint = host if kind == "go" else "redis://" + host
            started = time.monotonic()
            proc = subprocess.run([binary, endpoint, str(len(tap.room))], capture_output=True, timeout=15)
            seconds = time.monotonic() - started
            (destination / "stdout").write_bytes(proc.stdout)
            (destination / "stderr").write_bytes(proc.stderr)
            values = [json.loads(line) for line in proc.stdout.splitlines() if line.startswith(b"{")]
            assert proc.returncode == 0 and len(values) == 1
            def read(*args):
                _, value = command(port, helpers.frame, *args)
                return value.decode()
            final = {
                "entry": read(b"HGET", b"ingress", b"IN_fixture"),
                "room_member": read(b"SISMEMBER", b"room_{ingress}:" + tap.room, b"IN_fixture"),
                "recovery_entry": read(b"HEXISTS", b"egress", b"EG_recovery"),
            }
            return {**values[0], "redis": final, "events": tap.events, "exit": proc.returncode, "seconds": seconds}
        finally:
            tap.shutdown()
            thread.join(timeout=4)
            assert not thread.is_alive() and tap.errors == 0

def check(kind, profile, row, expect):
    txs = [event for event in row["events"] if event["command"] == "MULTI"]
    execs = [event for event in row["events"] if event["command"] == "EXEC"]
    partials = [event for event in row["events"] if event["command"] == "PARTIAL_SREM"]
    fixed = kind == "go" or expect == "after"
    assert row["recovery"] and row["redis"]["recovery_entry"] == "1"
    if profile == "confirmed-incomplete-write":
        assert len(partials) == (3 if fixed else 1)
        assert len(txs) == (4 if fixed else 1)
        assert len({e["socket"] for e in txs}) == len(txs)
        assert not row["success"] and len(execs) == int(fixed)
        assert row["redis"]["entry"] == ("-1" if fixed else "original")
        if not fixed:
            assert "Redis socket write timed out" in row["error"]
    elif profile == "confirmed-incomplete-write-exhausted":
        assert not row["success"] and len(txs) == (4 if fixed else 1) and len(partials) == len(txs)
        assert not execs and row["redis"]["entry"] == "original"
        assert "timeout" in row["error"] or "timed out" in row["error"]
    elif profile == "normal":
        assert row["success"] and len(txs) == len(execs) == 1 and row["redis"]["entry"] == "-1"
    else:
        assert not row["success"] and len(txs) == len(execs) == 1
        assert row["redis"]["entry"] == ("recreated" if profile == "after-real-exec-before-reply" else "original")
    expected_member = "0" if row["redis"]["entry"] == "-1" else "1"
    assert row["redis"]["room_member"] == expected_member
    assert not any(e["forwarded"] for e in partials)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expect", choices=("before", "after"), required=True)
    parser.add_argument("--wire-helper", type=Path,
                        default=Path(__file__).resolve().parents[1] / "redis-command-retry/run.py")
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("wire", args.wire_helper)
    helpers = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helpers)
    args.output.mkdir()
    receipt = {"expect": args.expect, "rows": []}
    for kind, binary in (("go", args.go), ("rust", args.rust)):
        receipt[kind + "_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
        for profile in PROFILES:
            out = args.output / (kind + "-" + profile)
            out.mkdir()
            row = run_one(binary, kind, profile, helpers, out)
            row.update(kind=kind, profile=profile)
            receipt["rows"].append(row)
            (args.output / "execution.json").write_text(json.dumps(receipt, indent=2) + "\n")
            check(kind, profile, row, args.expect)
    receipt["status"] = "PREWRITE_WIRE_MATCH" if args.expect == "after" else "PREWRITE_GAP_REPRODUCED"
    (args.output / "execution.json").write_text(json.dumps(receipt, indent=2) + "\n")

if __name__ == "__main__":
    main()
