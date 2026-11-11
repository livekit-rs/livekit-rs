#!/usr/bin/env python3
"""Observe the daemon-owned TCP socket after rejecting a trusted PROXY header."""

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import ssl
import struct
import subprocess
import time
import urllib.request


LOOPBACK = "127.0.0.1"
STUN_COOKIE = 0x2112A442
INVALID_PROXY = b"\r\n\r\n\0\r\nQUIT\n\x21\x13\0\0"


def reserve(kind):
    stream = socket.socket(type=kind)
    try:
        stream.bind((LOOPBACK, 0))
        return stream
    except BaseException:
        stream.close()
        raise


def exact(stream, length):
    data = bytearray()
    while len(data) < length:
        part = stream.recv(length - len(data))
        if not part:
            raise EOFError("truncated STUN message")
        data.extend(part)
    return data


def binding_request():
    return struct.pack("!HHI12s", 1, 0, STUN_COOKIE, os.urandom(12))


def healthy_binding(turn, context):
    with socket.create_connection((LOOPBACK, turn), timeout=2) as raw:
        expected_port = raw.getsockname()[1]
        raw.sendall(b"PROXY UNKNOWN\r\n")
        stream = raw if context is None else context.wrap_socket(raw, server_hostname="localhost")
        try:
            request = binding_request()
            stream.sendall(request)
            head = exact(stream, 20)
            msg_type, length, cookie, txn = struct.unpack("!HHI12s", head)
            if msg_type != 0x0101 or cookie != STUN_COOKIE or txn != request[8:]:
                raise AssertionError("unrelated or unsuccessful Binding response")
            attrs = exact(stream, length)
            for offset in attributes(attrs):
                kind, size = struct.unpack_from("!HH", attrs, offset)
                if kind != 0x0020 or size != 8:
                    continue
                family, port, ip = struct.unpack_from("!xBHI", attrs, offset + 4)
                mapped = socket.inet_ntoa(struct.pack("!I", ip ^ STUN_COOKIE))
                if family != 1 or mapped != LOOPBACK or port ^ (STUN_COOKIE >> 16) != expected_port:
                    raise AssertionError("Binding did not map the raw loopback peer")
                return {"mapped": f"{mapped}:{expected_port}", "verified_tls": context is not None}
            raise AssertionError("missing XOR-MAPPED-ADDRESS")
        finally:
            stream.close()


def attributes(data):
    offset = 0
    while offset < len(data):
        yield offset
        size = struct.unpack_from("!H", data, offset + 2)[0]
        offset += 4 + (size + 3) // 4 * 4


def client_hello(context):
    incoming = ssl.MemoryBIO()
    outgoing = ssl.MemoryBIO()
    conn = context.wrap_bio(incoming, outgoing, server_hostname="localhost")
    try:
        conn.do_handshake()
    except ssl.SSLWantReadError:
        packet = outgoing.read()
        if packet.startswith(b"\x16\x03"):
            return packet
    raise AssertionError("failed to obtain a genuine ClientHello")


def bad_peer(turn, payload):
    stream = socket.create_connection((LOOPBACK, turn), timeout=2)
    port = stream.getsockname()[1]
    stream.sendall(INVALID_PROXY + payload)
    return stream, port


def read_outcome(stream):
    stream.settimeout(0.25)
    try:
        data = stream.recv(1)
        return "unexpected_response" if data else "eof"
    except socket.timeout:
        return "quiet"
    except ConnectionResetError:
        return "reset"


def descriptors(pid, turn, ports, output):
    # A server-side port in lsof must belong to this PID and to the exact
    # client port. An unrelated host-wide TCP state cannot satisfy the probe.
    result = subprocess.run(
        ["lsof", "-nP", "-a", "-p", str(pid), "-iTCP", "-F", "fnT"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode or result.stderr:
        raise RuntimeError(f"lsof failed: {result.returncode}, {result.stderr}")
    output.write_text(result.stdout)
    states = {str(port): None for port in ports}
    connection = None
    for line in result.stdout.splitlines():
        if line.startswith("f"):
            connection = None
        elif line.startswith(f"n{LOOPBACK}:{turn}->{LOOPBACK}:"):
            connection = line.rsplit(":", 1)[1]
            if connection in states:
                states[connection] = "unknown"
        elif line.startswith("TST=") and connection in states:
            states[connection] = line.removeprefix("TST=")
    return states


def wait_http(server, port):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline and server.poll() is None:
        try:
            with urllib.request.urlopen(f"http://{LOOPBACK}:{port}/", timeout=0.4) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.02)
    raise RuntimeError(f"daemon readiness timeout, exit {server.poll()}")


def snapshot(server, turn, observations, name, peers, start, output):
    row = {"seconds_after_first_write": round(time.monotonic() - start, 3)}
    states = descriptors(server.pid, turn, peers.values(), output / f"{name}.lsof.txt")
    row["daemon_tcp_states"] = {label: states[str(port)] for label, port in peers.items()}
    observations[name] = row
    return row


def stop(server):
    if server.poll() is None:
        server.send_signal(signal.SIGTERM)
    try:
        return server.wait(timeout=12)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait(timeout=5)
        raise


def run(args, report):
    with ExitStack() as stack:
        ports = [stack.enter_context(reserve(socket.SOCK_STREAM)) for _ in range(3)]
        relay = stack.enter_context(reserve(socket.SOCK_DGRAM))
        http, debug, turn = [stream.getsockname()[1] for stream in ports]
        relay_port = relay.getsockname()[1]
    config = {
        "port": http,
        "debug_handler": {"port": debug},
        "bind_addresses": [LOOPBACK],
        "keys": {"devkey": "public-turn-proxy-retirement-fixture"},
        "rtc": {"node_ip": LOOPBACK, "tcp_port": 0},
        "turn": {
            "enabled": True,
            "bind_addresses": [LOOPBACK],
            "domain": "turn.example.com",
            "tls_port": turn,
            "cert_file": str(args.fixtures / "server.pem"),
            "key_file": str(args.fixtures / "server-key.pem"),
            "external_tls": args.plain,
            "proxy_protocol": True,
            "proxy_protocol_trusted_cidrs": ["127.0.0.1/32"],
            "relay_range_start": relay_port,
            "relay_range_end": relay_port,
        },
    }
    config_path = args.output / "server.yaml"
    config_path.write_text(json.dumps(config, indent=2) + "\n")
    env = dict(os.environ)
    env.pop("GOGC", None)
    env.pop("GOMEMLIMIT", None)
    if args.force_gc:
        env["GOGC"] = "off"
    context = None if args.plain else ssl.create_default_context(cafile=args.fixtures / "root.pem")
    payload = binding_request() if context is None else client_hello(context)
    observations = report["observations"]
    with ExitStack() as peers, (args.output / "server.log").open("wb") as log:
        server = subprocess.Popen([args.binary, "--config", config_path], env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            wait_http(server, http)
            time.sleep(0.25)
            report["fresh_before"] = healthy_binding(turn, context)
            start = time.monotonic()
            first, first_port = bad_peer(turn, payload)
            peers.enter_context(first)
            second, second_port = bad_peer(turn, payload)
            peers.enter_context(second)
            named = {"held": first_port, "peer_closed": second_port}
            report["client_ports"] = named.copy()
            report["initial_peer_reads"] = [read_outcome(stream) for stream in (first, second)]
            snapshot(server, turn, observations, "before_peer_close", named, start, args.output)
            second.close()
            time.sleep(0.25)
            snapshot(server, turn, observations, "after_peer_close", named, start, args.output)
            report["fresh_after_bad_headers"] = healthy_binding(turn, context)
            remaining = args.wait - (time.monotonic() - start)
            if remaining > 0:
                time.sleep(remaining)
            snapshot(server, turn, observations, "after_handshake_deadline", named, start, args.output)
            report["held_after_deadline"] = read_outcome(first)
            if args.force_gc:
                for _ in range(2):
                    with urllib.request.urlopen(f"http://{LOOPBACK}:{debug}/debug/pprof/heap?gc=1", timeout=5) as response:
                        response.read()
                        if response.status != 200:
                            raise AssertionError("forced Go heap GC endpoint failed")
                time.sleep(0.25)
                snapshot(server, turn, observations, "after_forced_gc", named, start, args.output)
                report["held_after_gc"] = read_outcome(first)
            third, third_port = bad_peer(turn, payload)
            peers.enter_context(third)
            report["third_initial_read"] = read_outcome(third)
            snapshot(server, turn, observations, "before_shutdown", {"newly_invalid": third_port}, start, args.output)
            report["daemon_exit"] = stop(server)
            report["newly_invalid_after_shutdown"] = read_outcome(third)
        finally:
            if server.poll() is None:
                report["daemon_exit"] = stop(server)
    with socket.socket() as tcp, socket.socket(type=socket.SOCK_DGRAM) as udp:
        tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp.bind((LOOPBACK, turn))
        tcp.listen()
        udp.bind((LOOPBACK, relay_port))
    report["listener_and_relay_rebind"] = True


def verify(report, expected):
    closed = {"eof", "reset"}
    reads = [*report["initial_peer_reads"], report["held_after_deadline"], report["third_initial_read"]]
    states = {label: row["daemon_tcp_states"] for label, row in report["observations"].items()}
    if report["daemon_exit"] or report["newly_invalid_after_shutdown"] not in closed:
        raise AssertionError("daemon or invalid peer failed to retire at shutdown")
    if "unexpected_response" in reads:
        raise AssertionError("malformed PROXY peer received a response")
    if expected == "early":
        if any(state is not None for row in states.values() for state in row.values()) or any(
            outcome not in closed for outcome in reads
        ):
            raise AssertionError("malformed PROXY peer retained a descriptor or lacked EOF/reset")
    elif expected == "forced-gc":
        live = {"held": "ESTABLISHED", "peer_closed": "CLOSE_WAIT"}
        if (
            states["before_peer_close"] != {"held": "ESTABLISHED", "peer_closed": "ESTABLISHED"}
            or states["after_peer_close"] != live
            or states["after_handshake_deadline"] != live
            or states["after_forced_gc"] != {"held": None, "peer_closed": None}
            or states["before_shutdown"] != {"newly_invalid": "ESTABLISHED"}
            or any(outcome != "quiet" for outcome in reads)
            or report["held_after_gc"] not in closed
        ):
            raise AssertionError("expected retained malformed descriptors to close on forced GC or daemon stop")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--plain", action="store_true")
    parser.add_argument("--force-gc", action="store_true", help="fixed Go only, disables automatic GC then invokes pprof GC")
    parser.add_argument("--expect", choices=("observe", "early", "forced-gc"), default="observe")
    parser.add_argument("--wait", type=float, default=11.25)
    args = parser.parse_args()
    if args.force_gc != (args.expect == "forced-gc") or args.force_gc and args.plain:
        parser.error("--force-gc requires --expect forced-gc and internal TLS")
    args.binary = args.binary.resolve()
    args.fixtures = args.fixtures.resolve()
    args.output = args.output.resolve()
    args.output.mkdir()
    with args.binary.open("rb") as binary:
        report = {
            "binary_sha256": hashlib.file_digest(binary, "sha256").hexdigest(),
            "observer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "external_tls": args.plain,
            "forced_gc": args.force_gc,
            "expected": args.expect,
            "wait_seconds": args.wait,
            "observations": {},
        }
    try:
        run(args, report)
        verify(report, args.expect)
        report["passed"] = True
    finally:
        (args.output / "result.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
