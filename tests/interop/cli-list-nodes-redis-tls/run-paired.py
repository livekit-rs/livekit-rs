#!/usr/bin/env python3
"""Paired Go-vs-Rust acceptance of the Redis TLS dial consumed by `list-nodes`
(SPEC-0076 workline A target 2; items CLI:list-nodes.RedisTlsConsumer and the
YAML:redis.tls.* / generated_flags:--redis.tls.* leaves).

Why a relay instead of a TLS Redis: the reference host has a `redis-server` built
without TLS support (it rejects `--tls-port`), so the harness terminates TLS itself
with a throwaway openssl CA. The server side of the handshake is a test double; the
*client* behaviour under test — `xtls.Config.ClientTLSConfig()` in
`references/protocol-stable/xtls/tls.go:34` mapped onto the YAML leaves — is exercised
by both shipped binaries over a real TLS connection to a real Redis behind it.

Certificate material is generated into a scratch directory and never committed: the
receipt records the openssl command, the leaf/common names and the SHA-256 fingerprint
of each DER, which is enough to reproduce the run.

Vectors (both binaries, same config file, same client):
  tls-ca-verified          enabled + ca_cert_file, address 127.0.0.1:<relay>  -> lists the seeded node
  tls-insecure             enabled + insecure: true, no CA                   -> lists the seeded node
  tls-server-name-ok       enabled + server_name: localhost                  -> lists the seeded node
  tls-client-cert          enabled + client_cert_file/client_key_file mTLS   -> lists the seeded node
  tls-wrong-ca             enabled + ca_cert_file from a second CA           -> verification failure
  tls-server-name-mismatch enabled + server_name: not.example               -> name mismatch failure
  tls-against-plaintext    enabled + address points at the plain Redis        -> handshake failure
  tls-missing-ca-file      enabled + ca_cert_file that does not exist        -> config-load failure
  tls-unparsable-ca-file   enabled + ca_cert_file with junk bytes            -> CA load failure
  tls-disabled-to-relay    enabled: false + address points at the TLS relay  -> plaintext record
                            reaches the relay (no TLS handshake is ever emitted) and the relay's
                            orderly close surfaces as the go-redis `unable to connect to redis: EOF`

Each connection any binary makes at a relay is recorded with its first bytes, so the receipt
compares the number of connections (go-redis' command-level retry count) per side as well as
the stdout/stderr surfaces.

Usage:
  python3 run-paired.py --official /abs/livekit-server --rust /abs/livecloud --output DIR
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import pathlib
import re
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time

ITEM = "CLI:list-nodes.RedisTlsConsumer"
NODE_ID = "owned-tls-node"

# Upstream `list-nodes` runs `service.InitializeRouter`, whose
# `rpc.NewKeepalivePubSub` registers a psrpc *server* that subscribes over the
# Redis message bus on a pinned connection of its own; this command is the
# read-only node-discovery port and does not build that bus. The official binary
# therefore dials Redis twice per successful run and this one dials once. That
# difference belongs to the message-bus surface (tracked by commands:list-nodes
# and SPEC-0076 workline B), not to the TLS dial under test, so it is recorded
# and named rather than absorbed: any other connection-count difference fails.
PEER_BUS_DIFFERENCE = (
    "official dials 2 Redis connections, rust dials 1: upstream's psrpc keepalive "
    "subscriber, absent from the read-only list-nodes port (commands:list-nodes, "
    "SPEC-0076 workline B)")

# Which ledger items each vector actually drives, declared inside the receipt.
# SPEC-0075 §2.10 only lets a tier 4/5 claim rest on an artifact that names the
# item, and the checker's matching is delimited (`YAML:redis.tls` does not match
# inside `YAML:redis.tls.enabled`), so each leaf has to be written out per vector
# instead of being inferred from the run as a whole.
LEAVES_ALL = ["CLI:list-nodes.RedisTlsConsumer", "YAML:redis.tls",
               "YAML:redis.tls.enabled"]
CA = "YAML:redis.tls.ca_cert_file"
INSECURE = "YAML:redis.tls.insecure"
SERVER_NAME = "YAML:redis.tls.server_name"
CLIENT_CERT = "YAML:redis.tls.client_cert_file"
CLIENT_KEY = "YAML:redis.tls.client_key_file"


def leaves(*extra: str) -> list[str]:
    return [*LEAVES_ALL, *extra]


# A leaf only counts as consumed by a vector that sets it *and* observes the
# result, so the failure branches count too: a wrong `ca_cert_file` still proves
# the leaf reached the dial (it is what made the handshake fail).
VECTOR_LEAVES = {
    "tls-ca-verified": leaves(CA),
    "tls-insecure": leaves(INSECURE),
    "tls-server-name-ok": leaves(CA, SERVER_NAME),
    "tls-client-cert": leaves(CA, SERVER_NAME, CLIENT_CERT, CLIENT_KEY),
    "tls-wrong-ca": leaves(CA),
    "tls-server-name-mismatch": leaves(CA, SERVER_NAME),
    "tls-against-plaintext": leaves(CA),
    "tls-missing-ca-file": leaves(CA),
    "tls-unparsable-ca-file": leaves(CA),
    "tls-disabled-to-relay": leaves(),
}


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def varint(value: int) -> bytes:
    out = bytearray()
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            out.append(bits | 0x80)
        else:
            out.append(bits)
            return bytes(out)


def field(number: int, wire: int, payload: bytes) -> bytes:
    return varint((number << 3) | wire) + payload


def vfield(number: int, value: int) -> bytes:
    return field(number, 0, varint(value))


def sfield(number: int, text: str) -> bytes:
    raw = text.encode()
    return field(number, 2, varint(len(raw)) + raw)


def ffield(number: int, value: float) -> bytes:
    import struct

    return field(number, 5, struct.pack("<f", value))


def node_proto(node_id: str) -> bytes:
    """Fixed-value livekit.Node so the rendered table is byte-stable (see c16)."""
    rate = (vfield(8, 1_048_576) + vfield(9, 2_097_152) + vfield(10, 3_000)
            + vfield(11, 4_000) + vfield(12, 5) + vfield(13, 6_000) + vfield(14, 7)
            + vfield(16, 8))
    stats = (vfield(1, 1_700_000_000) + vfield(2, 1_700_000_060) + vfield(3, 2)
             + vfield(4, 3) + vfield(5, 4) + vfield(6, 5) + vfield(7, 11_000)
             + vfield(8, 22_000) + vfield(9, 33) + vfield(10, 44) + vfield(11, 55)
             + vfield(17, 8) + ffield(18, 0.125) + ffield(19, 0.25) + ffield(20, 0.5)
             + ffield(21, 0.25) + vfield(22, 66) + vfield(23, 77) + vfield(28, 88)
             + vfield(29, 9) + vfield(34, 34_359_738_368) + vfield(35, 32_212_254_720)
             + field(50, 2, varint(len(rate)) + rate))
    return (sfield(1, node_id) + sfield(2, "127.0.0.1") + vfield(3, 8)
            + field(4, 2, varint(len(stats)) + stats) + vfield(5, 0) + vfield(6, 1)
            + sfield(7, "owned-tls"))


class Resp:
    def __init__(self, host: str, port: int):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.file = self.sock.makefile("rb")
        assert self.call(b"PING") == b"PONG"

    def read(self):
        line = self.file.readline(1 << 20)
        tag, data = line[:1], line[1:-2]
        if tag in (b"+", b"-"):
            return data
        if tag == b":":
            return int(data)
        if tag == b"$":
            n = int(data)
            body = self.file.read(n)
            assert len(body) == n and self.file.read(2) == b"\r\n"
            return body
        raise AssertionError("unexpected RESP type " + repr(tag))

    def call(self, *args):
        self.sock.sendall(b"*" + str(len(args)).encode() + b"\r\n"
                          + b"".join(b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
                                     for v in args))
        return self.read()

    def close(self):
        self.file.close()
        self.sock.close()


def free(port: int) -> bool:
    with socket.socket() as probe:
        return probe.connect_ex(("127.0.0.1", port)) != 0


def run_openssl(*args: str) -> None:
    done = subprocess.run(["openssl", *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    assert done.returncode == 0, ("openssl failed", args, done.stdout.decode())


class Certs:
    """Throwaway CA + server + client identity, regenerated per run, never committed."""

    def __init__(self, where: pathlib.Path):
        self.dir = where
        (where / "index.txt").touch()
        self.ca = where / "ca.pem"
        self.ca_key = where / "ca.key"
        self.server = where / "server.pem"
        self.server_key = where / "server.key"
        self.client = where / "client.pem"
        self.client_key = where / "client.key"
        other = where / "other-ca.pem"

        run_openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                    "-subj", "/CN=owned-test-ca", "-keyout", str(self.ca_key),
                    "-out", str(self.ca), "-addext", "basicConstraints=critical,CA:TRUE")
        run_openssl("req", "-newkey", "rsa:2048", "-nodes",
                    "-subj", "/CN=localhost", "-keyout", str(self.server_key),
                    "-out", str(where / "server.csr"))
        (where / "server.ext").write_text(
            "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n")
        run_openssl("x509", "-req", "-in", str(where / "server.csr"), "-CA", str(self.ca),
                    "-CAkey", str(self.ca_key), "-CAcreateserial", "-days", "2",
                    "-extfile", str(where / "server.ext"), "-out", str(self.server))
        run_openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=owned-test-client",
                    "-keyout", str(self.client_key), "-out", str(where / "client.csr"))
        run_openssl("x509", "-req", "-in", str(where / "client.csr"), "-CA", str(self.ca),
                    "-CAkey", str(self.ca_key), "-days", "2", "-out", str(self.client))
        # a second, unrelated CA: whatever it signs must not verify against the first
        run_openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                    "-subj", "/CN=other-ca", "-keyout", str(where / "other-ca.key"),
                    "-out", str(other))
        (where / "junk-ca.pem").write_text("not a certificate\n")
        self.other = other
        self.junk = where / "junk-ca.pem"

    def describe(self) -> dict:
        def fingerprint(path: pathlib.Path) -> str:
            done = subprocess.run(
                ["openssl", "x509", "-in", str(path), "-noout", "-fingerprint", "-sha256"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            return done.stdout.decode().strip()

        return {name: fingerprint(self.dir / name)
                for name in ("ca.pem", "server.pem", "client.pem", "other-ca.pem")}


class Relay:
    """TLS terminator in front of a plain Redis; also the mTLS enforcer."""

    def __init__(self, port: int, context: ssl.SSLContext, upstream: tuple[str, int]):
        self.port, self.context, self.upstream = port, context, upstream
        self.stop = threading.Event()
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", port))
        self.sock.listen(16)
        self.handshakes: list[str] = []
        # One record per inbound connection: what the first bytes were (a TLS
        # record or plaintext RESP) and how the relay ended the connection.
        self.connections: list[dict] = []
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        self.sock.settimeout(0.5)
        while not self.stop.is_set():
            try:
                client, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._pump, args=(client,), daemon=True).start()

    def _pump(self, client: socket.socket):
        # MSG_PEEK keeps the byte queued, so the TLS path still sees the full
        # ClientHello. It also decides the teardown shape: a peer that closes
        # with unread bytes makes the kernel send RST instead of FIN, and which
        # syscall consumes that error is a runtime detail, not a go-redis
        # contract. Draining first makes both binaries observe the same event.
        client.settimeout(5)
        try:
            first = client.recv(1, socket.MSG_PEEK)
        except OSError:
            first = b""
        entry = {"prefix_hex": first.hex(), "tls": first == b"\x16", "outcome": ""}
        self.connections.append(entry)
        if first != b"\x16":
            entry["outcome"] = "plaintext connection closed"
            self.handshakes.append(f"rejected: plaintext record 0x{first.hex()}")
            self._drain_then_close(client)
            return
        try:
            wrapped = self.context.wrap_socket(client, server_side=True)
        except (ssl.SSLError, OSError) as exc:
            entry["outcome"] = f"handshake rejected: {type(exc).__name__}: {exc}"
            self.handshakes.append(entry["outcome"])
            return
        entry["outcome"] = "accepted: " + (wrapped.version() or "")
        self.handshakes.append(entry["outcome"])
        try:
            remote = socket.create_connection(self.upstream, timeout=10)
        except OSError:
            wrapped.close()
            return
        for direction in ((wrapped, remote), (remote, wrapped)):
            threading.Thread(target=self._copy, args=direction, daemon=True).start()

    @staticmethod
    def _drain_then_close(client: socket.socket):
        client.settimeout(0.2)
        try:
            while client.recv(65536):
                pass
        except OSError:
            pass
        try:
            client.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        client.close()

    def observed(self, start: int) -> dict:
        """Per-connection summary for the records appended since `start`."""
        seen = self.connections[start:]
        return {"connections": len(seen),
                "tls_records": sum(1 for e in seen if e["tls"]),
                "plaintext_records": sum(1 for e in seen if not e["tls"]),
                "prefixes": [e["prefix_hex"] for e in seen]}

    @staticmethod
    def _copy(src: socket.socket, dst: socket.socket):
        try:
            while True:
                chunk = src.recv(65536)
                if not chunk:
                    break
                dst.sendall(chunk)
        except OSError:
            pass
        finally:
            for handle in (src, dst):
                try:
                    handle.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                handle.close()

    def stop_serving(self):
        self.stop.set()
        try:
            self.sock.close()
        except OSError:
            pass


def start_redis(port: int, staging: pathlib.Path) -> subprocess.Popen:
    home = staging / f"redis-{port}"
    home.mkdir(parents=True, exist_ok=True)
    log = (home / "redis.log").open("wb")
    proc = subprocess.Popen(
        ["redis-server", "--port", str(port), "--bind", "127.0.0.1",
         "--protected-mode", "no", "--save", "", "--appendonly", "no", "--dir", str(home)],
        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if not free(port):
            return proc
        time.sleep(0.05)
    raise AssertionError("owned redis never came up on " + str(port))


def invoke(binary: pathlib.Path, argv: list[str], out: pathlib.Path, tag: str) -> dict:
    import signal
    import sys

    proc = subprocess.Popen([str(binary), *argv], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=os.environ.copy(),
                            start_new_session=True)
    timed_out = False
    try:
        stdout, stderr = proc.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        stdout, stderr = proc.communicate()
        timed_out = True
    (out / f"{tag}.stdout").write_bytes(stdout)
    (out / f"{tag}.stderr").write_bytes(stderr)
    return {"exit": proc.returncode, "timed_out": timed_out,
            "stdout_b64": base64.b64encode(stdout).decode(),
            "stderr_b64": base64.b64encode(stderr).decode()}


STAMP = re.compile(r"^redis: \d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ")


def diagnostics(stderr_b64: str) -> list[str]:
    """The `redis:` library lines only, with the wall-clock stamp removed.

    Same rule as the accepted 21l harness: it is applied to both binaries, and the
    raw stderr stays in the output directory so nothing observed here is discarded.
    """
    text = base64.b64decode(stderr_b64).decode("utf-8", "replace")
    return [STAMP.sub("", line) for line in text.splitlines() if STAMP.match(line)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", required=True, type=pathlib.Path)
    ap.add_argument("--rust", required=True, type=pathlib.Path)
    ap.add_argument("--output", required=True, type=pathlib.Path)
    ap.add_argument("--base-port", type=int, default=21719)
    args = ap.parse_args()

    official, rust = args.official.resolve(), args.rust.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    staging = pathlib.Path(tempfile.mkdtemp(prefix="redis-tls-"))
    scratch = staging / "certs"
    scratch.mkdir()

    plain, relay_port, dead = args.base_port, args.base_port + 1, args.base_port + 2
    for port in (plain, relay_port, dead):
        assert free(port), f"fixture port {port} is already in use"

    report = {"status": "RUNNING", "item": ITEM,
              "scope": (f"real paired CLI list-nodes over a TLS Redis endpoint for {ITEM}: "
                        "ca-verified, insecure, server_name, mTLS, and the failure branches "
                        "(wrong CA, name mismatch, TLS-enabled against a plaintext port, missing "
                        "and unparsable ca_cert_file, TLS off against a TLS port). Redis client "
                        "TLS material comes from xtls.Config in the locked protocol module. "
                        "Not covered: sentinel/cluster TLS, redis.use_tls deprecation alias "
                        "beyond the enabled flag, HTTP/turn/webtransport TLS (separate items)."),
              "binaries": {"official_go": {"path": str(official), "sha256": sha(official.read_bytes())},
                           "rust": {"path": str(rust), "sha256": sha(rust.read_bytes())}},
              # the candidate binary is itself named `livekit-server`, so the paths in
              # `binaries` cannot tell the two sides apart; name each digest with its side
              "go_binary_sha256": sha(official.read_bytes()),
              "rust_binary_sha256": sha(rust.read_bytes()),
              "normalization": ("exit status and stdout compared exactly; stderr compared only on "
                                "lines matching the redis:/tls: diagnostic patterns after removing a "
                                "wall-clock stamp; per-side relay connection counts (total, TLS "
                                "records, plaintext records, first-byte prefixes) compared exactly; "
                                "raw bytes retained per case next to this receipt."),
              "certificates": {}, "cases": []}
    receipt = args.output / "BATCH.json"

    def save():
        receipt.write_text(json.dumps(report, indent=2) + "\n")

    save()
    redis_proc = None
    relays: list[Relay] = []
    code = 0
    try:
        version = invoke(official, ["--version"], args.output, "version-official")
        report["official_version"] = base64.b64decode(
            version["stdout_b64"]).decode().strip()
        assert report["official_version"] == "livekit-server version 1.13.10", report["official_version"]
        save()

        certs = Certs(scratch)
        report["certificates"] = certs.describe()
        report["certificates"]["generation"] = (
            "openssl req -x509 / x509 -req, 2-day validity, generated in a scratch dir under the "
            "run's temp path; keys are deliberately not committed")
        save()

        redis_proc = start_redis(plain, staging)
        conn = Resp("127.0.0.1", plain)
        conn.call(b"DEL", b"nodes")
        assert conn.call(b"HSET", b"nodes", NODE_ID.encode(), node_proto(NODE_ID)) == 1
        conn.close()

        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(str(certs.server), str(certs.server_key))
        server_context.load_verify_locations(str(certs.ca))

        mtls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        mtls_context.load_cert_chain(str(certs.server), str(certs.server_key))
        mtls_context.load_verify_locations(str(certs.ca))
        mtls_context.verify_mode = ssl.CERT_REQUIRED

        relay = Relay(relay_port, server_context, ("127.0.0.1", plain))
        relays.append(relay)
        mtls_port = args.base_port + 3
        assert free(mtls_port)
        mtls_relay = Relay(mtls_port, mtls_context, ("127.0.0.1", plain))
        relays.append(mtls_relay)
        time.sleep(0.3)

        def config_body(tls_block: str, address: str) -> str:
            return ("keys: {devkey: secret}\nlogging: {level: error}\n"
                    "rtc: {node_ip: 127.0.0.1, use_external_ip: false}\n"
                    f"redis:\n  address: {address}\n{tls_block}")

        vectors = [
            ("tls-ca-verified", f'  tls:\n    enabled: true\n    ca_cert_file: "{certs.ca}"\n',
             f'"127.0.0.1:{relay_port}"', "success"),
            ("tls-insecure", "  tls:\n    enabled: true\n    insecure: true\n",
             f'"127.0.0.1:{relay_port}"', "success"),
            ("tls-server-name-ok",
             f'  tls:\n    enabled: true\n    server_name: localhost\n    ca_cert_file: "{certs.ca}"\n',
             f'"127.0.0.1:{relay_port}"', "success"),
            ("tls-client-cert",
             (f'  tls:\n    enabled: true\n    server_name: localhost\n'
              f'    ca_cert_file: "{certs.ca}"\n    client_cert_file: "{certs.client}"\n'
              f'    client_key_file: "{certs.client_key}"\n'),
             f'"127.0.0.1:{mtls_port}"', "success"),
            ("tls-wrong-ca", f'  tls:\n    enabled: true\n    ca_cert_file: "{certs.other}"\n',
             f'"127.0.0.1:{relay_port}"', "failure"),
            ("tls-server-name-mismatch",
             f'  tls:\n    enabled: true\n    server_name: not.example\n    ca_cert_file: "{certs.ca}"\n',
             f'"127.0.0.1:{relay_port}"', "failure"),
            ("tls-against-plaintext", f'  tls:\n    enabled: true\n    ca_cert_file: "{certs.ca}"\n',
             f'"127.0.0.1:{plain}"', "failure"),
            ("tls-missing-ca-file",
             f'  tls:\n    enabled: true\n    ca_cert_file: "{scratch / "absent-ca.pem"}"\n',
             f'"127.0.0.1:{relay_port}"', "failure"),
            ("tls-unparsable-ca-file", f'  tls:\n    enabled: true\n    ca_cert_file: "{certs.junk}"\n',
             f'"127.0.0.1:{relay_port}"', "failure"),
            ("tls-disabled-to-relay", "  tls:\n    enabled: false\n",
             f'"127.0.0.1:{relay_port}"', "failure"),
        ]

        mismatches: list[tuple] = []
        peer_attributed: list[str] = []
        for name, tls_block, address, expectation in vectors:
            body = config_body(tls_block, address)
            path = staging / f"{name}.yaml"
            path.write_text(body)
            path.chmod(0o600)

            def peer_state() -> tuple[int, int]:
                return len(relay.connections), len(mtls_relay.connections)

            def total(seen: dict) -> int:
                return seen["relay"]["connections"] + seen["mtls_relay"]["connections"]

            def peer_seen(before: tuple[int, int]) -> dict:
                time.sleep(0.3)  # let the relay threads record the last attempts
                return {"relay": relay.observed(before[0]),
                        "mtls_relay": mtls_relay.observed(before[1])}

            go_mark = peer_state()
            official_result = invoke(official, ["--dev", "--config", str(path), "list-nodes"],
                                     args.output, f"{name}-official")
            go_peer = peer_seen(go_mark)
            rust_mark = peer_state()
            rust_result = invoke(rust, ["--dev", "--config", str(path), "list-nodes"],
                                 args.output, f"{name}-rust")
            rust_peer = peer_seen(rust_mark)
            rec = {"id": name, "expectation": expectation,
                   "ledger_items": VECTOR_LEAVES[name],
                   "config_sha256": sha(path.read_bytes()), "config": body,
                   "official": official_result, "rust": rust_result,
                   "official_peer": go_peer, "rust_peer": rust_peer}
            go_text = base64.b64decode(rec["official"]["stdout_b64"]).decode()
            rs_text = base64.b64decode(rec["rust"]["stdout_b64"]).decode()
            rec["stdout_exact"] = rec["official"]["stdout_b64"] == rec["rust"]["stdout_b64"]
            rec["exit_exact"] = rec["official"]["exit"] == rec["rust"]["exit"]
            go_diag = diagnostics(rec["official"]["stderr_b64"])
            rs_diag = diagnostics(rec["rust"]["stderr_b64"])
            rec["diagnostics_exact"] = go_diag == rs_diag
            rec["official_diagnostics"] = go_diag
            rec["rust_diagnostics"] = rs_diag
            # The peer also compares the number of connections each binary made:
            # go-redis' command-level retry count is part of the observable surface.
            rec["peer_exact"] = go_peer == rust_peer
            rec["peer_totals"] = {"official": total(go_peer), "rust": total(rust_peer)}
            # Every vector runs even after a divergence: one pass must show the whole set,
            # and a partial inventory would understate what the receipt proves.
            if not rec["stdout_exact"]:
                mismatches.append((name, "stdout differs", go_text[:400], rs_text[:400]))
            if not rec["exit_exact"]:
                mismatches.append((name, "exit differs", rec["official"]["exit"],
                                   rec["rust"]["exit"]))
            if not rec["diagnostics_exact"]:
                mismatches.append((name, "redis: diagnostics differ", go_diag[:2], rs_diag[:2]))
            if not rec["peer_exact"]:
                if expectation == "success" and rec["peer_totals"] == {"official": 2, "rust": 1}:
                    rec["peer_difference"] = PEER_BUS_DIFFERENCE
                    peer_attributed.append(name)
                else:
                    mismatches.append((name, "peer connection counts differ", go_peer, rust_peer))
            assert not rec["official"]["timed_out"] and not rec["rust"]["timed_out"], name
            if expectation == "success":
                assert rec["official"]["exit"] == 0, (name, go_text)
                assert NODE_ID in go_text, (name, "tls path never reached the seeded node", go_text)
            else:
                assert rec["official"]["exit"] != 0, (name, "failure vector exited 0", go_text)
                go_err = base64.b64decode(rec["official"]["stderr_b64"]).decode()
                assert go_text.strip() or go_err.strip(), (name, "no observable failure")
            if name == "tls-disabled-to-relay":
                # `enabled: false` must mean the client never emits a TLS record,
                # and both binaries must attempt the same number of connections.
                for side, seen in (("official", go_peer), ("rust", rust_peer)):
                    total = seen["relay"]["connections"] + seen["mtls_relay"]["connections"]
                    tls_records = seen["relay"]["tls_records"] + seen["mtls_relay"]["tls_records"]
                    plaintext = seen["relay"]["plaintext_records"] + seen["mtls_relay"]["plaintext_records"]
                    assert total > 0 and tls_records == 0 and plaintext == total, \
                        (name, side, seen)
                    assert all(e.startswith("2a") for e in
                               seen["relay"]["prefixes"]), (name, side, seen["relay"]["prefixes"])
                rec["tls_never_attempted"] = True
            report["cases"].append(rec)
            save()

        report["mismatches"] = mismatches
        assert not mismatches, f"{len(mismatches)} paired divergence(s): {mismatches}"

        successes = [c for c in report["cases"] if c["expectation"] == "success"]
        failures = [c for c in report["cases"] if c["expectation"] == "failure"]
        assert len(successes) == 4 and len(failures) == 6, "case coverage must not shrink"
        leaf_vectors: dict[str, list[str]] = {}
        for rec in report["cases"]:
            for leaf in rec["ledger_items"]:
                leaf_vectors.setdefault(leaf, []).append(rec["id"])
        declared_leaves = {*LEAVES_ALL, CA, INSECURE, SERVER_NAME, CLIENT_CERT, CLIENT_KEY}
        assert set(leaf_vectors) == declared_leaves, (
            "the leaf mapping must cover every redis.tls.* leaf and nothing else",
            sorted(declared_leaves - set(leaf_vectors)), sorted(set(leaf_vectors) - declared_leaves))
        assert all(v for v in leaf_vectors.values()), "a leaf claimed no vector"
        report["leaf_vectors"] = leaf_vectors
        assert relay.handshakes and mtls_relay.handshakes, \
            "no TLS handshake was ever observed, so the relay proved nothing"
        assert any(not e["tls"] for e in relay.connections), \
            "the TLS-off vector never reached the relay, so `enabled: false` was not observed"
        report["handshakes"] = {"relay": relay.handshakes, "mtls_relay": mtls_relay.handshakes}
        report["relay_connections"] = {
            "relay": relay.connections, "mtls_relay": mtls_relay.connections}
        report["known_differences"] = [{
            "id": "list-nodes.PSRPCMessageBusSecondRedisConnection",
            "outside_item_scope_of": ITEM,
            "tracked_by": ["commands:list-nodes", "CONFIG_CLUSTER_CONSUMER:psrpc",
                           "SPEC-0076 workline B"],
            "observed_on": peer_attributed,
            "difference": PEER_BUS_DIFFERENCE,
            "reproduction": ("tap-connections.py in this directory records the first RESP bytes "
                             "of every Redis connection each binary makes on the plaintext path "
                             "(official: 2 connections, rust: 1)"),
        }]
        report["aggregate_status"] = "PASS_CLI_LIST_NODES_REDIS_TLS_PAIRED"
        report["status"] = report["aggregate_status"]
    except BaseException as exc:
        report["status"] = "FAIL"
        report["failure"] = f"{type(exc).__name__}: {exc}"
        code = 1
    finally:
        for relay in relays:
            relay.stop_serving()
        if redis_proc is not None and redis_proc.poll() is None:
            redis_proc.terminate()
            try:
                redis_proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                redis_proc.kill()
        shutil.rmtree(staging, ignore_errors=True)
        save()
    print(json.dumps({"status": report["status"], "cases": len(report["cases"]),
                      "failure": report.get("failure")}))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
