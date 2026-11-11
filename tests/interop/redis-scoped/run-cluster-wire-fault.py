#!/usr/bin/env python3
"""Run the owned TCP fault profiles against a real Cluster client."""

import argparse
import binascii
import contextlib
import hashlib
import json
import socket
import ssl
import subprocess
import threading
import time
from pathlib import Path

SIZE = 16 << 20
PREFIX = "LIVEKIT_CLUSTER_WIRE_FAULT_"
TEST = (
    "redis_tls::tests::empty_master_timeout_tests::"
    "empty_master_cluster_write_and_verified_discovered_tls_dial_use_wire_deadlines"
)
CASES = ("write_short", "write_long", "tls_short", "tls_long")
FIXTURES = Path(__file__).resolve().parents[3] / "crates/rtc-tls/tests/fixtures"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def resp(value):
    if isinstance(value, list):
        return f"*{len(value)}\r\n".encode() + b"".join(map(resp, value))
    if isinstance(value, int):
        return f":{value}\r\n".encode()
    return f"${len(value)}\r\n".encode() + value + b"\r\n"


def length(stream, prefix):
    line = stream.readline(256)
    if not line:
        raise EOFError
    if not line.startswith(prefix) or not line.endswith(b"\r\n"):
        raise ValueError(f"invalid RESP header {line!r}")
    return int(line[1:-2])


def recv_exact(sock, count):
    result = bytearray()
    while len(result) < count:
        chunk = sock.recv(count - len(result))
        if not chunk:
            raise EOFError
        result.extend(chunk)
    return bytes(result)


class Cluster:
    def __init__(self, case, key):
        self.case = case
        self.key = key.encode()
        self.observations = []
        self.stored = threading.Event()
        self.lock = threading.Lock()
        self.closed = threading.Event()
        self.listeners = []
        self.connections = set()
        self.peers = []
        self.handlers = []
        for _ in range(3):
            listener = socket.socket()
            if case.startswith("write_"):
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32768)
            listener.bind(("127.0.0.1", 0))
            listener.listen(16)
            listener.settimeout(0.1)
            self.listeners.append(listener)
        self.ports = [sock.getsockname()[1] for sock in self.listeners]

    def record(self, node, event, **detail):
        with self.lock:
            self.observations.append(dict(node=node, event=event, **detail))

    def tls(self, node, sock):
        if node == 2 and self.case == "tls_short":
            start = time.monotonic()
            header = recv_exact(sock, 5)
            hello = recv_exact(sock, int.from_bytes(header[3:5], "big"))
            assert header[0] == 22 and hello[0] == 1, "expected actual TLS ClientHello"
            assert sock.recv(1) == b"", "unexpected application bytes before TLS handshake"
            self.record(node, "stalled_client_hello_eof", seconds=time.monotonic() - start,
                        hello_bytes=len(hello) + 5)
            return None
        if node == 2 and self.case == "tls_long":
            time.sleep(0.35)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(FIXTURES / "server.pem", FIXTURES / "server-key.pem")
        context.set_servername_callback(lambda _, name, __: self.record(node, "sni", name=name))
        return context.wrap_socket(sock, server_side=True)

    def handle(self, node, sock):
        sock.settimeout(7)
        connection = sock
        try:
            if self.case.startswith("tls_"):
                connection = self.tls(node, sock)
                if connection is None:
                    return
                with self.lock:
                    self.connections.add(connection)
            with connection.makefile("rb") as stream:
                self.commands(node, connection, stream)
        except EOFError:
            pass
        except (ConnectionError, ssl.SSLError, OSError) as error:
            self.record(node, "socket_end", error=str(error))
        except Exception as error:
            self.record(node, "fixture_error", error=repr(error))
        finally:
            if connection is not None:
                connection.close()
            sock.close()
            with self.lock:
                self.connections.discard(connection)
                self.connections.discard(sock)

    def commands(self, node, connection, stream):
        while not self.closed.is_set():
            count = length(stream, b"*")
            parts = []
            for index in range(count):
                size = length(stream, b"$")
                if index == 2 and parts == [b"SET", self.key]:
                    if node != 2:
                        raise AssertionError(f"SET incorrectly routed to seed {node}")
                    if self.case.startswith("write_") and size == SIZE:
                        self.record(node, "set_header", payload_bytes=size)
                        time.sleep(0.45 if self.case == "write_short" else 0.15)
                        received = 0
                        while received < size:
                            try:
                                chunk = stream.read(min(65536, size - received))
                            except ConnectionResetError:
                                chunk = b""
                            if not chunk:
                                self.record(node, "partial_set_eof", bytes=received,
                                            declared=size)
                                return
                            assert chunk == b"x" * len(chunk), "unexpected payload bytes"
                            received += len(chunk)
                        body = b"x"  # Already validated incrementally.
                    else:
                        body = stream.read(size)
                        expected = (b"recover-after-timeout" if self.case == "write_short"
                                    else b"verified-peer")
                        assert body == expected
                    assert stream.read(2) == b"\r\n"
                    event = ("recovery_set" if body == b"recover-after-timeout"
                             else "complete_set")
                    self.record(node, event, bytes=size)
                    self.stored.set()
                    parts.append(body)
                else:
                    assert 0 <= size <= 65536, "unexpected control command size"
                    parts.append(stream.read(size))
                    assert stream.read(2) == b"\r\n"
                if index == 0:
                    parts[0] = parts[0].upper()
            command = parts[0]
            if command in (b"CLIENT", b"SET"):
                response = b"+OK\r\n"
            elif command == b"PING":
                response = b"+PONG\r\n"
            elif command == b"HELLO":
                response = b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n"
            elif command == b"EXISTS" and parts[1] == self.key and node == 2:
                stored = self.stored.is_set()
                self.record(node, "exists", stored=stored)
                response = resp(int(stored))
            elif command == b"CLUSTER" and parts[1].upper() == b"SLOTS":
                self.record(node, "cluster_slots")
                response = resp([
                    [first, last, [b"127.0.0.1", port, f"{index + 1:040x}".encode()]]
                    for index, ((first, last), port) in enumerate(zip(
                        [(0, 5460), (5461, 10922), (10923, 16383)], self.ports
                    ))
                ])
            else:
                self.record(node, "unexpected_command", command=repr(parts))
                response = b"-ERR fixture does not implement this command\r\n"
            connection.sendall(response)

    def accept(self, node, listener):
        while not self.closed.is_set():
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                if not self.closed.is_set():
                    raise
                return
            handler = threading.Thread(target=self.handle, args=(node, connection), daemon=True)
            with self.lock:
                self.connections.add(connection)
                self.handlers.append(handler)
            handler.start()

    def __enter__(self):
        for node, listener in enumerate(self.listeners):
            peer = threading.Thread(target=self.accept, args=(node, listener), daemon=True)
            peer.start()
            self.peers.append(peer)
        return self

    def __exit__(self, *_):
        self.closed.set()
        for listener in self.listeners:
            listener.close()
        for peer in self.peers:
            peer.join(timeout=2)
        for sock in list(self.connections):
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
        for handler in self.handlers:
            handler.join(timeout=8)
        assert not any(t.is_alive() for t in self.peers + self.handlers)
        assert not self.connections
        self.record(None, "owned_sockets_retired", listeners=3, handlers=len(self.handlers))


def config(cluster):
    ports = ", ".join(f"'127.0.0.1:{port}'" for port in cluster.ports[:2])
    dial = 150 if cluster.case == "tls_short" else 1500
    write = 35 if cluster.case == "write_short" else 3500
    tls = ""
    if cluster.case.startswith("tls_"):
        tls = (
            "  tls:\n    enabled: true\n    server_name: localhost\n"
            f"    ca_cert_file: '{FIXTURES / 'root.pem'}'\n"
        )
    return (
        "rtc:\n  node_ip: 127.0.0.1\nredis:\n"
        f"  sentinel_addresses: [{ports}]\n  sentinel_master_name: ''\n"
        f"  read_timeout: -1\n  dial_timeout: {dial}\n  write_timeout: {write}\n{tls}"
    )


def acceptance(case, observations):
    def events(name):
        return [event for event in observations if event["event"] == name]

    assert events("cluster_slots"), "client never discovered topology"
    assert not events("fixture_error"), events("fixture_error")
    assert events("owned_sockets_retired"), "fixture leaked sockets"
    if case == "write_short":
        assert events("set_header"), "no real write reached the discovered owner"
        assert not events("complete_set")
        assert events("recovery_set") and any(event["stored"] for event in events("exists"))
        assert events("partial_set_eof") and all(
            0 < event["bytes"] < SIZE for event in events("partial_set_eof")
        ), "owner did not receive a physically partial write before EOF"
    elif case == "tls_short":
        assert not events("complete_set")
        assert events("stalled_client_hello_eof") and all(
            event["hello_bytes"] > 5 and 0.08 <= event["seconds"] < 1.0
            for event in events("stalled_client_hello_eof")
        ), "no real TLS ClientHello closed on the shorter budget"
    else:
        assert events("complete_set") and events("exists")
        assert all(event["stored"] for event in events("exists"))
        if case == "tls_long":
            assert any(event["node"] == 2 and event["name"] == "localhost"
                       for event in events("sni")), "no verified discovered-peer SNI"


def main():
    import os

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--kind", choices=["go", "rust"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", action="append", choices=CASES)
    args = parser.parse_args()
    args.output.mkdir()
    binary = args.binary.resolve(strict=True)
    binary_sha = digest(binary)
    results = []
    for case in args.case or CASES:
        key = next(f"cluster-wire-{i}" for i in range(1000)
                   if binascii.crc_hqx(f"cluster-wire-{i}".encode(), 0) % 16384 >= 10923)
        cluster = Cluster(case, key)
        yaml = config(cluster)
        (args.output / f"{case}.yaml").write_text(yaml)
        env = dict(os.environ, **{PREFIX + "YAML": yaml, PREFIX + "CASE": case, PREFIX + "KEY": key})
        command = [str(binary)]
        if args.kind == "rust":
            command += ["--ignored", "--exact", TEST, "--nocapture", "--test-threads=1"]
        with (args.output / f"{case}.log").open("wb") as log:
            row = dict(case=case, accepted=False, key=key, ports=cluster.ports, command=command)
            try:
                with cluster:
                    row["exit_code"] = subprocess.run(command, env=env, stdout=log,
                                                       stderr=subprocess.STDOUT, timeout=25).returncode
                    # A short-write observer waits for the client to close; it must finish
                    # before fixture shutdown, so forced shutdown cannot count as peer EOF.
                    limit = time.monotonic() + 2
                    target = {"write_short": "partial_set_eof",
                              "tls_short": "stalled_client_hello_eof"}.get(case)
                    while target and time.monotonic() < limit:
                        if any(event["event"] == target for event in cluster.observations):
                            break
                        time.sleep(0.025)
                    row["pre_shutdown_observations"] = len(cluster.observations)
                assert row["exit_code"] == 0, f"client exit {row['exit_code']}"
                if args.kind == "rust":
                    assert "1 passed; 0 failed" in (args.output / f"{case}.log").read_text()
                acceptance(case, cluster.observations[:row["pre_shutdown_observations"]] +
                           [event for event in cluster.observations
                            if event["event"] in ("owned_sockets_retired", "fixture_error")])
                row["accepted"] = True
            except Exception as error:
                row["failure"] = repr(error)
            row["observations"] = cluster.observations
            results.append(row)
        print(case, "PASS" if row["accepted"] else row.get("failure"), flush=True)
    unchanged = digest(binary) == binary_sha
    receipt = dict(kind=args.kind, binary=str(binary), sha256=binary_sha, binary_unchanged=unchanged,
                   runner_sha256=digest(Path(__file__)), results=results)
    (args.output / "result.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return 0 if unchanged and all(row["accepted"] for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
