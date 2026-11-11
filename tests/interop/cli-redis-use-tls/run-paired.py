#!/usr/bin/env python3
"""Paired Go-vs-Rust acceptance of the deprecated `redis.use_tls` alias, its hidden
generated flag and its ENV source (SPEC-0076 workline A, batch 21n).

Items this drives, declared per vector inside the receipt:
  YAML:redis.use_tls                                — the alias branch of the dial builder
  generated_flags:--redis.use_tls                   — the hidden generated flag + env source
  CLI:list-nodes.RedisTlsConsumer                   — the same consumer, alias input path
  CONFIG_BEHAVIOR:Redis.TlsServerStartupConsumer     — the server start path
  YAML:redis.tls                                    — precedence against the tls block
and two items it deliberately does **not** close, but bounds and records:
  TLS:x509.DarwinSystemTrustFailureText     — see known_differences in the receipt
  TLS:x509.CaUsedAsEndEntityVerdict         — ditto; a refusal *reason*, not only wording

What upstream says, from the locked sources:
  `references/protocol-stable/redis/redis.go:88-98` —
      if conf.TLS != nil && conf.TLS.Enabled  -> xtls.ClientTLSConfig()
      else if conf.UseTLS                     -> &tls.Config{MinVersion: tls.VersionTLS12}
  The alias gets no CA file, no ServerName override and no `insecure` escape hatch:
  `RootCAs` stays nil, which in Go means the **system** certificate pool. So a
  self-signed relay must FAIL the alias branch, and the failure is what proves the
  system-pool fallback rather than skipped verification — an outcome that cannot be
  observed any other way on a host where no installable system anchor exists.
  `references/livekit-stable/pkg/config/config.go:939-951` generates one hidden flag
  per dotted YAML path with a `LIVEKIT_<PATH>` env source, and `config.go:1079-1104`
  copies them into the config by walking `c.Flags` — the flags registered on the
  *running* command. `list-nodes` registers none, so there a generated flag is
  accepted and silently unused, while the start path (root command) does consume it.
  Both halves are asserted below: a flag being parsed is not a flag being applied.
  The simple-client branch (`redis.go:144-155`) passes no Dial/Read/Write timeouts, so
  the deadline vectors run on go-redis' own defaults — dial 5s, read 5s, write=read
  (`references/go-modules/go-redis-v9.22.0/options.go:485-532`) — which is what the Rust
  session uses; `redis.dial_timeout`/`read_timeout`/`write_timeout` are consumed only by
  the sentinel branch upstream (`redis.go:100-130`) and are separate items.
  Two peer-certificate shapes are dialed because the refusal *reason* differs between the
  verifiers, and the port is deliberately left as it is: `crates/livecloud/src/redis_tls.rs`
  renders Go's wording only for a rustls reason whose Go counterpart is derivable, so no
  certificate failure here is turned into a success or into an invented Go sentence. Each
  shape that can legitimately differ opts in per vector, and a third vector — a `tls` block
  pointed at a CA that did not sign the leaf — runs through Go's own verifier, where the
  texts already agree byte for byte, which is the control that keeps those two opt-ins narrow.

Vectors, both binaries, identical config bytes, identical argv and ENV:
  alias-true-to-plaintext           use_tls: true, plain Redis        -> TLS dial, deadline failure
  alias-true-to-relay               use_tls: true, CA-signed leaf alone -> system-pool x509 failure
  alias-selfsigned-ca-leaf-to-relay use_tls: true, self-signed CA-flagged leaf -> x509 failure
  alias-false-to-plain              use_tls: false                    -> success, node listed
  alias-absent-to-plain             no use_tls key                    -> success (default is false)
  alias-true-tls-block-off          use_tls: true + tls.enabled: false          -> still x509 failure
  alias-true-tls-insecure-ignored   use_tls: true + tls{enabled:false,insecure:true} -> still x509
  alias-off-tls-enabled-ca          use_tls: false + tls{enabled:true, ca}      -> success over TLS
  tls-block-wrong-ca-file           use_tls: false + tls{enabled:true, other ca} -> x509 failure,
                                    byte-identical on both sides (the control for both allowances)
  flag-not-consumed-by-list-nodes   --redis.use_tls=true, plain Redis -> success: subcommand ignores it
  env-not-consumed-by-list-nodes    LIVEKIT_REDIS_USE_TLS=true        -> success: same
  flag-consumed-on-start-path       start path + --redis.use_tls=true, relay -> x509 failure at startup
  tls-block-consumed-on-start-path  start path + tls{enabled:true,insecure:true} -> server reaches HTTP

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
import signal
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.request

YAML_LEAF = "YAML:redis.use_tls"
FLAG_LEAF = "generated_flags:--redis.use_tls"
CLI_LEAF = "CLI:list-nodes.RedisTlsConsumer"
STARTUP_LEAF = "CONFIG_BEHAVIOR:Redis.TlsServerStartupConsumer"
TLS_BLOCK = "YAML:redis.tls"
RACE_LEAF = "RUNTIME:Server.InterruptBeforeRunningRace"
# Clears upstream's post-listener `running` window before SIGINT; see run_server_start.
SETTLE_SECONDS = 0.5
NODE_ID = "owned-alias-node"

# Upstream's psrpc keepalive subscriber makes the official binary dial a second Redis
# connection per successful list-nodes run (see SPEC-0074 §12 / c17). That difference is
# recorded, not absorbed: it must not be able to hide a *different* count here, so the
# relay-side assertions below are written per vector and the count parity between the
# two binaries is reported rather than claimed.
PSRPC_DIFFERENCE = (
    "official dials 2 Redis connections per successful list-nodes, rust dials 1 "
    "(upstream's psrpc keepalive subscriber, absent from the read-only port); "
    "tracked by list-nodes.PSRPCMessageBusSecondRedisConnection")


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
    """Fixed-value livekit.Node so the rendered table is byte-stable (see c16/c17)."""
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
            + sfield(7, "owned-alias"))


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
        probe.settimeout(0.2)
        return probe.connect_ex(("127.0.0.1", port)) != 0


def run_openssl(*args: str) -> None:
    done = subprocess.run(["openssl", *args], stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT)
    assert done.returncode == 0, ("openssl failed", args, done.stdout.decode())


# What the relay itself saw, in families that are comparable across the two clients.
# The raw strings name the TLS alert the client sent, and the alert *number* is a
# declared difference of its own (ALERT_ITEM): Go sends 42 bad_certificate for every
# verification failure (references/go-stdlib-go1.26.3/src/crypto/tls/handshake_client.go
# :582, :696, :1110, :1137) while rustls 0.23.46 maps UnknownIssuer to 48 unknown_ca and
# its own certificate errors to 43 certificate_unknown (src/error.rs:625-660). Families
# are compared; a rendering that fits none of them stays unclassified and fails the run
# rather than being absorbed into a bucket.
def peer_family(outcome: str) -> str:
    if outcome.startswith("accepted"):
        return "accepted"
    if "alert" in outcome.lower():
        return "peer saw a certificate alert from the client"
    if "Connection reset" in outcome:
        return "connection reset during the handshake"
    if outcome.startswith("plaintext connection closed"):
        return "plaintext connection closed"
    return "unclassified: " + outcome


class Certs:
    """Throwaway CA + server identities, regenerated per run, never committed.

    Two *leaf* shapes matter here and only here, because they are the two ways the
    verifiers answer an untrusted chain (see SYSTEM_TRUST_ITEM and CERT_REASON_ITEM):
    a leaf whose issuer is not presented, and a leaf that is its own issuer and says
    CA:TRUE. `other_ca.pem` is a second, independent CA that signed nothing presented
    here: pointing `tls.ca_cert_file` at it is the negative control that runs through
    Go's own verifier instead of the platform one.
    """

    def __init__(self, where: pathlib.Path):
        self.dir = where
        (where / "index.txt").touch()
        self.ca = where / "ca.pem"
        self.ca_key = where / "ca.key"
        self.other_ca = where / "other-ca.pem"
        self.server = where / "server.pem"
        self.server_key = where / "server.key"
        self.selfsigned = where / "selfsigned.pem"
        self.selfsigned_key = where / "selfsigned.key"

        run_openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                    "-subj", "/CN=owned-test-ca", "-keyout", str(self.ca_key),
                    "-out", str(self.ca), "-addext", "basicConstraints=critical,CA:TRUE")
        run_openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                    "-subj", "/CN=owned-other-ca", "-keyout", str(where / "other-ca.key"),
                    "-out", str(self.other_ca),
                    "-addext", "basicConstraints=critical,CA:TRUE")
        run_openssl("req", "-newkey", "rsa:2048", "-nodes",
                    "-subj", "/CN=localhost", "-keyout", str(self.server_key),
                    "-out", str(where / "server.csr"))
        (where / "server.ext").write_text(
            "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n")
        run_openssl("x509", "-req", "-in", str(where / "server.csr"), "-CA", str(self.ca),
                    "-CAkey", str(self.ca_key), "-CAcreateserial", "-days", "2",
                    "-extfile", str(where / "server.ext"), "-out", str(self.server))
        # Self-signed leaf: presented alone, so its issuer *is* itself and there is no
        # chain for the verifier to complete. CA:TRUE is what makes webpki call it an
        # end-entity misuse rather than an unknown issuer, so the flag is the subject of
        # CERT_REASON_ITEM and has to stay.
        run_openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                    "-subj", "/CN=localhost", "-keyout", str(self.selfsigned_key),
                    "-out", str(self.selfsigned),
                    "-addext", "subjectAltName=IP:127.0.0.1,DNS:localhost",
                    "-addext", "basicConstraints=critical,CA:TRUE",
                    "-addext", "extendedKeyUsage=serverAuth")

    def describe(self) -> dict:
        def fingerprint(path: pathlib.Path) -> str:
            done = subprocess.run(
                ["openssl", "x509", "-in", str(path), "-noout", "-fingerprint", "-sha256"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            return done.stdout.decode().strip()

        return {name: fingerprint(self.dir / name)
                for name in ("ca.pem", "other-ca.pem", "server.pem", "selfsigned.pem")}


class Relay:
    """TLS terminator in front of a plain Redis, and the witness for both sides."""

    def __init__(self, port: int, context: ssl.SSLContext, upstream: tuple[str, int]):
        self.port, self.context, self.upstream = port, context, upstream
        self.stop = threading.Event()
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", port))
        self.sock.listen(16)
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
        # MSG_PEEK keeps the byte queued, so the TLS path still sees the whole
        # ClientHello. It also decides the teardown shape: a peer that closes with
        # unread bytes makes the kernel send RST instead of FIN, which is a runtime
        # detail and not a go-redis contract, so drain first (see c17).
        client.settimeout(5)
        try:
            first = client.recv(1, socket.MSG_PEEK)
        except OSError:
            first = b""
        entry = {"prefix_hex": first.hex(), "tls": first == b"\x16", "outcome": "",
                 "outcome_kind": ""}
        self.connections.append(entry)
        if first != b"\x16":
            entry["outcome"] = entry["outcome_kind"] = "plaintext connection closed"
            self._drain_then_close(client)
            return
        try:
            wrapped = self.context.wrap_socket(client, server_side=True)
        except (ssl.SSLError, OSError) as exc:
            entry["outcome"] = f"handshake rejected: {type(exc).__name__}: {exc}"
            # The alert text is a Python/OpenSSL rendering of what the *client* sent, so
            # only the category is comparable across the two binaries.
            entry["outcome_kind"] = "handshake rejected"
            return
        entry["outcome_kind"] = "accepted"
        entry["protocol"] = wrapped.version() or ""
        entry["outcome"] = "accepted: " + entry["protocol"]
        try:
            remote = socket.create_connection(self.upstream, timeout=10)
        except OSError:
            entry["outcome_kind"] = "accepted, upstream failed"
            entry["outcome"] = "accepted, upstream failed"
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

    def observed(self, start: int) -> dict:
        """Summary of the connections this side made since `start`.

        `connections` is deliberately kept out of `shape`: the official binary dials a
        second Redis connection per successful list-nodes (the psrpc message bus, see
        PSRPC_DIFFERENCE), so the raw count is a known difference, while the *category*
        of every connection — TLS record or not, accepted or rejected — is the alias
        behaviour under test and must agree.

        `peer_outcomes` is the comparable rendering of what the relay saw back: the
        category the peer put each connection in. The raw strings stay in
        `relay.connections` (and in `official_peer_details`/`rust_peer_details` per
        case), because they render the TLS **alert number** the client sent, and that
        is its own declared difference (ALERT_ITEM) rather than something this
        harness may fold into one bucket to make a comparison pass.
        """
        seen = self.connections[start:]
        kinds = {}
        peer = {}
        for e in seen:
            kinds[e["outcome_kind"]] = kinds.get(e["outcome_kind"], 0) + 1
            family = peer_family(e["outcome"])
            peer[family] = peer.get(family, 0) + 1
        return {"connections": len(seen),
                "tls_records": sum(1 for e in seen if e["tls"]),
                "plaintext_records": sum(1 for e in seen if not e["tls"]),
                "kinds": dict(sorted(kinds.items())),
                "peer_outcomes": dict(sorted(peer.items())),
                "protocols": sorted({e.get("protocol", "") for e in seen if e["tls"]})}

    def details(self, start: int) -> list[str]:
        """The raw per-connection outcome strings — recorded, never compared.

        They carry the TLS alert name the client sent, which is ALERT_ITEM.
        """
        return sorted({e["outcome"] for e in self.connections[start:]})

    def stop_serving(self):
        self.stop.set()
        try:
            self.sock.close()
        except OSError:
            pass


class Front:
    """The relays in front of the owned plaintext Redis, one per certificate shape.

    A relay keeps one SSLContext, so the two peer-certificate shapes that answer the
    alias branch differently (see SYSTEM_TRUST_ITEM) get separate endpoints and
    separate records; `marks` snapshots every relay before a run so each vector can
    report exactly what its own side did.
    """

    def __init__(self, upstream: tuple[str, int], shapes: dict[str, tuple[int, ssl.SSLContext]]):
        self.relays = {name: Relay(port, context, upstream)
                       for name, (port, context) in shapes.items()}
        time.sleep(0.3)

    def marks(self) -> dict:
        return {name: len(relay.connections) for name, relay in self.relays.items()}

    def observed(self, mark: dict, shape: str = "") -> dict:
        if shape:
            return self.relays[shape].observed(mark[shape])
        return {name: relay.observed(mark[name]) for name, relay in self.relays.items()}

    def details(self, mark: dict, shape: str) -> list[str]:
        """Raw outcome strings for the one relay a vector dialed — recorded, not compared."""
        return self.relays[shape].details(mark[shape])

    def totals(self, mark: dict) -> dict:
        """Connections every relay saw since `mark` — a plaintext vector must show none."""
        return {name: relay.observed(mark[name])["connections"]
                for name, relay in self.relays.items()}

    def any_tls(self) -> bool:
        return any(e["tls"] for r in self.relays.values() for e in r.connections)

    def stop_serving(self):
        for relay in self.relays.values():
            relay.stop_serving()


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


STAMP = re.compile(r"^redis: \d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ")


def stamped(text: str) -> list[str]:
    """The `redis:` library diagnostic lines, wall-clock stamp removed.

    Same rule as the accepted c16/c17 harnesses: applied to both binaries, and the raw
    bytes stay on disk next to this receipt so nothing observed is discarded.
    """
    return [STAMP.sub("", line) for line in text.splitlines() if STAMP.match(line)]


def failure_lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if "unable to connect to redis" in line]


def invoke(binary: pathlib.Path, argv: list[str], out: pathlib.Path, tag: str,
           env: dict[str, str]) -> dict:
    full = os.environ.copy()
    full.update(env)
    proc = subprocess.Popen([str(binary), *argv], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=full, start_new_session=True)
    timed_out = False
    try:
        stdout, stderr = proc.communicate(timeout=90)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        stdout, stderr = proc.communicate()
        timed_out = True
    (out / f"{tag}.stdout").write_bytes(stdout)
    (out / f"{tag}.stderr").write_bytes(stderr)
    return {"kind": "list-nodes", "exit": proc.returncode, "timed_out": timed_out,
            "stdout_b64": base64.b64encode(stdout).decode(),
            "stdout_text": stdout.decode("utf-8", "replace"),
            "stderr_text": stderr.decode("utf-8", "replace"),
            "failure_lines": failure_lines(stdout.decode("utf-8", "replace")),
            "diagnostics": stamped(stderr.decode("utf-8", "replace"))}


def start_server(binary: pathlib.Path, argv: list[str], log_path: pathlib.Path,
                 env: dict[str, str], wait: float = 45.0) -> dict:
    """Run the server start path and report only the Redis-dial outcome.

    A started server prints timestamps, node ids and bound ports, none of which is
    comparable byte for byte. What *is* comparable is whether the process survived the
    Redis dial, the `unable to connect to redis` line it printed if it did not, and the
    `redis:` diagnostics — so those are the observables and the whole log stays beside
    this receipt.
    """
    full = os.environ.copy()
    full.update(env)
    with log_path.open("wb") as log:
        proc = subprocess.Popen([str(binary), *argv], stdout=log,
                                stderr=subprocess.STDOUT, env=full,
                                start_new_session=True)
    reached_http = False
    deadline = time.monotonic() + wait
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{argv_http_port(argv)}/",
                                        timeout=1) as reply:
                reached_http = reply.status == 200
        except OSError:
            time.sleep(0.1)
        if reached_http:
            break
    exited_early = proc.poll() is not None
    exit_code = proc.poll()
    sigkilled = False
    drain_seconds = None
    if not exited_early:
        # Upstream only sets `running` 100ms after its listeners already answer
        # (`server.go:343-347`), and `Stop()` returns at `if !s.running.Swap(false)`
        # (`server.go:391`) without closing `doneChan`, so an interrupt inside that window
        # leaves the reference hung until it is killed. The c25 receipt shows exactly that:
        # official -9 against candidate 0 on `tls-block-consumed-on-start-path`. Settling
        # past the window makes the graceful-stop exit code comparable again; the race is
        # tracked as RUNTIME:Server.InterruptBeforeRunningRace (measured in the c26
        # dead-node-prune receipt).
        time.sleep(SETTLE_SECONDS)
        os.killpg(proc.pid, signal.SIGINT)
        drain_started = time.monotonic()
        try:
            exit_code = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            sigkilled = True
            os.killpg(proc.pid, signal.SIGKILL)
            exit_code = proc.wait()
        drain_seconds = round(time.monotonic() - drain_started, 3)
    text = log_path.read_bytes().decode("utf-8", "replace")
    return {"kind": "server-start", "exit": exit_code, "timed_out": False,
            "stdout_b64": None, "stdout_text": text,
            "reached_http": reached_http, "exited_early": exited_early,
            "sigkilled": sigkilled, "drain_seconds": drain_seconds,
            "failure_lines": failure_lines(text), "diagnostics": stamped(text)}


def argv_http_port(argv: list[str]) -> int:
    return int(argv[argv.index("--port") + 1])


SYSTEM_TRUST_ITEM = "TLS:x509.DarwinSystemTrustFailureText"
CERT_REASON_ITEM = "TLS:x509.CaUsedAsEndEntityVerdict"
ALERT_ITEM = "TLS.AlertNumberOnCertificateVerificationFailure"
# `x509.Certificate.systemVerify` (references/go-stdlib-go1.26.3/src/crypto/x509/
# root_darwin.go:13-71): with `RootCAs == nil` crypto/tls hands the leaf plus the
# presented intermediates (verify.go:556-567 dispatches to the platform verifier for
# a system pool) to `SecTrustEvaluateWithError`. When that call fails with a code
# other than the three it maps (expired / hostname mismatch / errSecNotTrusted), Go
# prints `fmt.Errorf("x509: %s", err)` -- the CFError *description*, which macOS
# renders as `<U+201C>leaf summary<U+201D> certificate is not trusted`. crypto/tls
# then wraps it (`common.go:1816-1818`) in the same
# `tls: failed to verify certificate: %s` sentence every other rejection uses.
#
# The Rust port verifies the same chain with the WebPKI verifier over the exported
# system anchors, so its peer is refused with the portable text Go uses when the Go
# verifier builds a chain to an unknown (or self-signed) root. The dial decision --
# refuse, after a real TLS ClientHello, with a certificate reason -- agrees; the
# platform sentence does not.
#
# The second recorded difference is a *reason*, not only wording. webpki refuses a leaf
# whose basicConstraints says CA:TRUE in the end-entity slot (rustls-webpki 0.103.16,
# per Cargo.lock, src/verify_cert.rs:449
# `(Role::EndEntity, true, _) => Err(Error::CaUsedAsEndEntity)`), while Go never tests the
# CA flag for a leaf — `isValid` rejects only a non-CA *intermediate*
# (verify.go:496) — so the reference reaches its usual unknown-authority answer for the
# same certificate (`ErrSecNotTrusted` maps to `UnknownAuthorityError{Cert: c}` at
# root_darwin.go:66-68, whose `Error()` is verify.go:157). Both clients refuse it on these
# vectors; what happens when such a leaf *is* the configured trust anchor is a separate
# functional gap, recorded under known_differences rather than fixed here by turning a
# certificate failure into a success.
DARWIN_CFERROR = re.compile(r"x509: \u201c[^\u201d]+\u201d certificate is not trusted")
PORTABLE_UNKNOWN_AUTHORITY = "x509: certificate signed by unknown authority"
CERT_REASON_OFFICIAL = PORTABLE_UNKNOWN_AUTHORITY
CERT_REASON_RUST = "Other(OtherError(CaUsedAsEndEntity))"
# The two surfaces a failed Redis dial puts its text on: `redis.go:158-159` wraps the
# Ping error in `unable to connect to redis: %w`, and go-redis' pool logs
# `pool.go:762: redis: connection pool: failed to dial after 5 attempts: %w`. Inside
# both, crypto/tls adds `tls: failed to verify certificate: ` (common.go:1816-1818)
# where rustls renders its own error.
OUTER_WRAPPERS = ("unable to connect to redis: ",
                  "pool.go:762: redis: connection pool: failed to dial after 5 attempts: ")
GO_WRAPPER = "tls: failed to verify certificate: "
RUST_WRAPPER = "invalid peer certificate: "
# Which allowance a nil-RootCAs alias dial may claim, keyed by the peer-certificate shape it
# dials. The two shapes were picked because they make the two verifiers answer differently in
# two different ways, and no other vector is granted either one.
SHAPE_TEXT_ALLOWANCE = {"ca-signed-leaf-alone": "system-trust",
                        "self-signed-leaf": "cert-reason"}
# For each allowance: (does this official reason match the recorded shape?,
#                     the only Rust reason accepted beside it).
ALLOWED_REASON_PAIRS = {
    "system-trust": (lambda reason: DARWIN_CFERROR.fullmatch(reason) is not None,
                     PORTABLE_UNKNOWN_AUTHORITY),
    "cert-reason": (lambda reason: reason == CERT_REASON_OFFICIAL, CERT_REASON_RUST),
}


def split_reason(line: str):
    """(outer wrapper, inner wrapper, reason); the inner wrapper is None when the
    line is not a certificate-verification refusal at all.

    Which inner wrapper the Rust side shows is a property of the *reason*, not of the
    vector: redis_tls.rs renders Go's own `tls: failed to verify certificate: ` whenever it
    has a Go counterpart for the rustls error, and keeps `invalid peer certificate: ` when
    it does not. So the pair is pinned on the reason and either wrapper is accepted, while
    the full raw line stays in the receipt.
    """
    for outer in OUTER_WRAPPERS:
        if not line.startswith(outer):
            continue
        rest = line[len(outer):]
        for middle in (GO_WRAPPER, RUST_WRAPPER):
            if rest.startswith(middle):
                return outer, middle, rest[len(middle):]
    return None, None, line


def text_divergence(go_lines: list[str], rs_lines: list[str], accept: str) -> dict:
    """Verdict for one text surface: identical, a recorded certificate-refusal
    difference, or a real divergence.

    `accept` is the caller's per-vector opt-in, chosen by the peer-certificate shape
    that vector dials: "system-trust", "cert-reason" or "none". Each allowance holds
    only when the *whole* surface is explained by it -- same line count, the same
    outer wrapper on every line, a certificate-verification refusal on both sides and
    exactly the declared pair of reasons -- so both raw texts stay in the receipt and
    any other wording change still fails the run. "none" vectors are the controls:
    that shape must agree byte for byte.
    """
    if go_lines == rs_lines:
        return {"kind": "identical"}
    if accept == "none" or accept not in ALLOWED_REASON_PAIRS \
            or len(go_lines) != len(rs_lines) or not go_lines:
        return {"kind": "differ", "official": go_lines[:3], "rust": rs_lines[:3]}
    official_shape, want_rust = ALLOWED_REASON_PAIRS[accept]
    for official, rust in zip(go_lines, rs_lines):
        go_outer, go_middle, go_reason = split_reason(official)
        rs_outer, rs_middle, rs_reason = split_reason(rust)
        if go_outer is None or go_outer != rs_outer:
            return {"kind": "differ", "official": go_lines[:3], "rust": rs_lines[:3]}
        if go_middle is None or rs_middle is None:
            return {"kind": "differ", "official": go_lines[:3], "rust": rs_lines[:3]}
        if not official_shape(go_reason) or rs_reason != want_rust:
            return {"kind": "differ", "official": go_lines[:3], "rust": rs_lines[:3]}
    item = SYSTEM_TRUST_ITEM if accept == "system-trust" else CERT_REASON_ITEM
    return {
        "kind": item,
        "official": go_lines,
        "rust": rs_lines,
        "normalized_equal": True,
        "recorded_as": item,
    }


def compare_shape(go: dict, rs: dict, texts: dict) -> list[tuple]:
    """Every surface the two kinds share, compared exactly.

    `texts` holds the verdict for the three text surfaces — the whole stdout on the
    list-nodes path, plus the `unable to connect to redis` lines and the stamped
    diagnostics — computed once by the caller with [`text_divergence`] so the receipt
    records the same verdict that is compared. One narrow, evidence-bound exception is
    possible and is never silently accepted: the caller has to opt in per vector, every
    line pair has to be a certificate refusal carrying exactly the declared reason, and
    both raw texts stay in the receipt.
    """
    out = []
    for key, verdict in texts.items():
        if verdict["kind"] == "differ":
            out.append((key + " differ", verdict["official"], verdict["rust"]))
    if go["kind"] != rs["kind"]:
        out.append(("kind differs", go["kind"], rs["kind"]))
    if go["kind"] == "list-nodes":
        # The line verdict above already fails the run on any real divergence; this one
        # closes the gap between "same lines" and "same bytes" so trailing whitespace or
        # an encoding difference cannot hide behind a line-level allowance.
        if texts["stdout_lines"]["kind"] == "identical" and go["stdout_b64"] != rs["stdout_b64"]:
            out.append(("stdout bytes differ on identical lines",
                        go["stdout_text"][:400], rs["stdout_text"][:400]))
        if go["exit"] != rs["exit"]:
            out.append(("exit differs", go["exit"], rs["exit"]))
    else:
        if go["exited_early"] != rs["exited_early"]:
            out.append(("start survival differs", go["exited_early"], rs["exited_early"]))
        if go["reached_http"] != rs["reached_http"]:
            out.append(("reached_http differs", go["reached_http"], rs["reached_http"]))
        if (go["exited_early"] != rs["exited_early"]) or (
                go["exited_early"] and go["exit"] != rs["exit"]):
            out.append(("start exit differs", go["exit"], rs["exit"]))
        if not go["exited_early"] and not rs["exited_early"]:
            # Both reached the running state past the settle, so this is the graceful-stop
            # exit code and whether SIGINT was honoured — surfaces c25 could not compare.
            if go["sigkilled"] != rs["sigkilled"]:
                out.append(("graceful stop differs", go["sigkilled"], rs["sigkilled"]))
            if go["exit"] != rs["exit"]:
                out.append(("graceful stop exit differs", go["exit"], rs["exit"]))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", required=True, type=pathlib.Path)
    ap.add_argument("--rust", required=True, type=pathlib.Path)
    ap.add_argument("--output", required=True, type=pathlib.Path)
    ap.add_argument("--base-port", type=int, default=21779)
    args = ap.parse_args()

    official, rust = args.official.resolve(), args.rust.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    staging = pathlib.Path(tempfile.mkdtemp(prefix="redis-alias-"))
    scratch = staging / "certs"
    scratch.mkdir()

    plain, relay_port = args.base_port, args.base_port + 1
    selfsigned_port = args.base_port + 5
    # one port triple per side on the start path, so the two servers never collide
    go_http, go_tcp, go_udp = args.base_port + 2, args.base_port + 3, args.base_port + 4
    rs_http, rs_tcp, rs_udp = args.base_port + 7, args.base_port + 8, args.base_port + 9
    for port in (plain, relay_port, selfsigned_port, go_http, go_tcp, go_udp,
                 rs_http, rs_tcp, rs_udp):
        assert free(port), f"fixture port {port} is already in use"

    report = {"status": "RUNNING",
              "items": sorted({YAML_LEAF, FLAG_LEAF, CLI_LEAF, STARTUP_LEAF, TLS_BLOCK,
                               SYSTEM_TRUST_ITEM, CERT_REASON_ITEM, ALERT_ITEM, RACE_LEAF}),
              "scope": (
                  "real paired list-nodes and server-start runs for the deprecated "
                  "redis.use_tls alias: its own TLS branch (system trust pool, TLS>=1.2, no CA "
                  "or insecure input), its precedence against a disabled redis.tls block, the "
                  "hidden generated flag --redis.use_tls with its LIVEKIT_REDIS_USE_TLS env "
                  "source, and the split between the list-nodes subcommand (generated flag "
                  "accepted but not applied) and the server start path (applied). Two "
                  "peer-certificate shapes probe the boundary of the platform-verifier failure "
                  "sentence, and a tls-block vector pointed at a CA that did not sign the leaf "
                  "controls it. Not covered: a use_tls handshake that verifies against a CA in the "
                  "machine's system trust store — no such anchor exists on this host and "
                  "installing one needs sudo — so the alias's success branch is evidenced by the "
                  "relay's TLS record and by who the peer accepted, not by a completed verified "
                  "handshake; the macOS CFError wording itself and the CaUsedAsEndEntity refusal "
                  "(each an open item, see known_differences, neither replicated); the TLS alert "
                  "number the peer sees (Go always sends 42 bad_certificate, rustls maps its own "
                  "reasons) and the accept/refuse gap that follows from the CA-flag verdict, which "
                  "this harness records but cannot certify because certifying it would mean "
                  "allowing a behavioural divergence; client-certificate inputs, which the alias "
                  "branch cannot express at all (`redis.go:94-97` builds `&tls.Config{MinVersion: "
                  "TLS12}` and nothing else) and which belong to CLI:list-nodes.RedisTlsConsumer; "
                  "sentinel/cluster TLS; turn/webtransport/HTTP TLS (separate items)."),
              "binaries": {"official_go": {"path": str(official),
                                           "sha256": sha(official.read_bytes())},
                           "rust": {"path": str(rust), "sha256": sha(rust.read_bytes())}},
              # the candidate binary is itself named `livekit-server`, so the paths in
              # `binaries` cannot tell the two sides apart; name each digest with its side
              "go_binary_sha256": sha(official.read_bytes()),
              "rust_binary_sha256": sha(rust.read_bytes()),
              "normalization": (
                  "exit status and stdout compared exactly on the list-nodes path; both paths "
                  "compared on the `unable to connect to redis` lines and on the redis:/tls: "
                  "diagnostic lines after removing the wall-clock stamp, with the two recorded "
                  "certificate-refusal differences (SYSTEM_TRUST_ITEM for the platform sentence, "
                  "CERT_REASON_ITEM for the CA-flag verdict) accepted only when the whole line "
                  "around them is byte-identical, the vector is a nil-RootCAs alias dial on that "
                  "certificate shape, and both sides still refused; every other vector, including "
                  "the wrong-CA tls-block control, must agree byte for byte. Server-start is "
                  "compared on survival (did it get past the Redis dial to HTTP) and on those "
                  "lines, never on the full log; the list-nodes relay observation compared exactly "
                  "except for the tracked psrpc second connection, and the server-start relay "
                  "comparison is on which handshakes the peer accepted or rejected because a "
                  "running server keeps re-dialling; the TLS alert number each client sent is "
                  "compared by family, not by raw text (ALERT_ITEM), and the raw strings are "
                  "kept; every raw stream is inside this receipt (list-nodes stdout as base64, "
                  "stderr as text, the full server-start log as text, per-connection relay "
                  "records), and re-running the harness regenerates the per-case raw files in "
                  "its output directory."),
              "certificates": {}, "cases": []}
    receipt = args.output / "BATCH.json"

    def save():
        receipt.write_text(json.dumps(report, indent=2) + "\n")

    save()
    redis_proc = None
    front = None
    code = 0
    try:
        version = invoke(official, ["--version"], args.output, "version-official", {})
        report["official_version"] = version["stdout_text"].strip()
        assert report["official_version"] == "livekit-server version 1.13.10", \
            report["official_version"]
        save()

        certs = Certs(scratch)
        report["certificates"] = certs.describe()
        report["certificates"]["generation"] = (
            "openssl req -x509 / x509 -req, 2-day validity, generated under the run's temp path. "
            "The CA is deliberately NOT installed into the system trust store: that absence is "
            "what makes the alias branch's failure the interesting observable. server.pem is a "
            "CA-signed leaf presented alone (its issuer is unknown and absent); selfsigned.pem is "
            "its own issuer and carries basicConstraints=critical,CA:TRUE. Those are the two "
            "answers the verifiers give. other-ca.pem signed nothing presented here, so using it "
            "as tls.ca_cert_file is the wrong-anchor control.")
        save()

        redis_proc = start_redis(plain, staging)
        conn = Resp("127.0.0.1", plain)
        conn.call(b"DEL", b"nodes")
        assert conn.call(b"HSET", b"nodes", NODE_ID.encode(), node_proto(NODE_ID)) == 1
        conn.close()

        ca_signed = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ca_signed.load_cert_chain(str(certs.server), str(certs.server_key))
        selfsigned = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        selfsigned.load_cert_chain(str(certs.selfsigned), str(certs.selfsigned_key))
        front = Front(("127.0.0.1", plain),
                      {"ca-signed-leaf-alone": (relay_port, ca_signed),
                       "self-signed-leaf": (selfsigned_port, selfsigned)})
        # What the two shapes must differ in, and only here: the CA-signed leaf presented
        # alone is the shape the platform verifier names in its own sentence, the self-signed
        # CA-flagged leaf is the shape webpki refuses for a reason Go does not have. Both
        # binaries must refuse both, and both must still refuse.
        report["relay_shapes"] = {"ca-signed-leaf-alone": relay_port,
                                  "self-signed-leaf": selfsigned_port}

        plain_addr = f'"127.0.0.1:{plain}"'
        relay_addr = f'"127.0.0.1:{relay_port}"'
        selfsigned_addr = f'"127.0.0.1:{selfsigned_port}"'

        def block(address: str, *lines: str) -> str:
            return f"  address: {address}\n" + "".join(line + "\n" for line in lines)

        # (id, path, redis yaml, extra argv, env, expectation, relay rule, relay shape,
        #  ledger items)
        vectors = [
            ("alias-true-to-plaintext", "list-nodes",
             block(plain_addr, "  use_tls: true"), [], {}, "deadline-failure", "none", "",
             [YAML_LEAF, CLI_LEAF]),
            ("alias-true-to-relay", "list-nodes",
             block(relay_addr, "  use_tls: true"), [], {}, "x509-failure", "tls-only",
             "ca-signed-leaf-alone", [YAML_LEAF, CLI_LEAF]),
            ("alias-selfsigned-ca-leaf-to-relay", "list-nodes",
             block(selfsigned_addr, "  use_tls: true"), [], {}, "x509-failure", "tls-only",
             "self-signed-leaf", [YAML_LEAF, CLI_LEAF]),
            ("alias-false-to-plain", "list-nodes",
             block(plain_addr, "  use_tls: false"), [], {}, "success", "none", "",
             [YAML_LEAF, CLI_LEAF]),
            ("alias-absent-to-plain", "list-nodes",
             block(plain_addr), [], {}, "success", "none", "", [YAML_LEAF, CLI_LEAF]),
            ("alias-true-tls-block-off", "list-nodes",
             (f'  address: {relay_addr}\n  use_tls: true\n'
              "  tls:\n    enabled: false\n"), [], {}, "x509-failure", "tls-only",
             "ca-signed-leaf-alone", [YAML_LEAF, TLS_BLOCK]),
            ("alias-true-tls-insecure-ignored", "list-nodes",
             (f'  address: {relay_addr}\n  use_tls: true\n'
              "  tls:\n    enabled: false\n    insecure: true\n"), [], {},
             "x509-failure", "tls-only", "ca-signed-leaf-alone", [YAML_LEAF, TLS_BLOCK]),
            ("alias-off-tls-enabled-ca", "list-nodes",
             (f'  address: {relay_addr}\n  use_tls: false\n'
              f'  tls:\n    enabled: true\n    ca_cert_file: "{certs.ca}"\n'),
             [], {}, "success", "tls-only", "ca-signed-leaf-alone",
             [YAML_LEAF, TLS_BLOCK, CLI_LEAF]),
            # The control: a `tls` block with a CA that did not sign the presented leaf still
            # refuses, and with RootCAs non-nil Go runs its own verifier instead of the platform
            # one, so this is the same leaf and the same refusal with byte-identical text. An
            # allowance that cannot tell these two vectors apart is too wide to be trusted.
            ("tls-block-wrong-ca-file", "list-nodes",
             (f'  address: {relay_addr}\n  use_tls: false\n'
              f'  tls:\n    enabled: true\n    ca_cert_file: "{certs.other_ca}"\n'),
             [], {}, "x509-failure", "tls-only", "ca-signed-leaf-alone",
             [YAML_LEAF, TLS_BLOCK, CLI_LEAF]),
            ("flag-not-consumed-by-list-nodes", "list-nodes",
             block(plain_addr), ["--redis.use_tls=true"], {}, "success", "none", "",
             [FLAG_LEAF, CLI_LEAF]),
            ("env-not-consumed-by-list-nodes", "list-nodes",
             block(plain_addr), [], {"LIVEKIT_REDIS_USE_TLS": "true"}, "success", "none", "",
             [FLAG_LEAF, CLI_LEAF]),
            ("flag-consumed-on-start-path", "server-start",
             block(relay_addr), ["--redis.use_tls=true"], {}, "x509-failure", "tls-only",
             "ca-signed-leaf-alone", [FLAG_LEAF, STARTUP_LEAF]),
            ("tls-block-consumed-on-start-path", "server-start",
             (f'  address: {relay_addr}\n'
              "  tls:\n    enabled: true\n    insecure: true\n"), [], {}, "success",
             "tls-only", "ca-signed-leaf-alone", [STARTUP_LEAF, RACE_LEAF]),
        ]

        mismatches: list[tuple] = []
        peer_attributed: list[str] = []
        system_trust_vectors: list[str] = []
        cert_reason_vectors: list[str] = []
        for name, kind, redis_yaml, extra, env, expectation, relay_rule, shape, leaves \
                in vectors:
            # Each side writes its own config copy, with its own rtc ports, then the two
            # copies are compared on everything except the port numbers they must differ in.
            def run_side(binary: pathlib.Path, tag_suffix: str, http_port: int,
                        tcp_port: int, udp_port: int) -> dict:
                path = staging / f"{name}-{tag_suffix}.yaml"
                body = ("keys: {devkey: secret}\nlogging: {level: error}\n"
                        + ("rtc: {node_ip: 127.0.0.1, use_external_ip: false}\n"
                           if kind == "list-nodes" else
                           "bind_addresses: [127.0.0.1]\n"
                           "rtc: {node_ip: 127.0.0.1, use_external_ip: false, "
                           f"tcp_port: {tcp_port}, udp_port: {udp_port}, "
                           "enable_loopback_candidate: true}\n")
                        + "turn: {enabled: false}\n"
                        + f"redis:\n{redis_yaml}")
                path.write_text(body)
                path.chmod(0o600)
                mark = front.marks()
                if kind == "list-nodes":
                    result = invoke(binary,
                                    ["--dev", "--config", str(path), "list-nodes", *extra],
                                    args.output, f"{name}-{tag_suffix}", env)
                else:
                    log_path = args.output / f"{name}-{tag_suffix}.server.log"
                    argv = ["--dev", "--config", str(path), "--port", str(http_port), *extra]
                    result = start_server(binary, argv, log_path, env)
                time.sleep(0.4)  # let the relay threads record the last attempts
                return {"config_body": body, "config_sha256": sha(path.read_bytes()),
                        "result": result, "relay": front.observed(mark, shape),
                        "relay_details": front.details(mark, shape) if shape else [],
                        "all_relays": front.observed(mark)}
            go = run_side(official, "official", go_http, go_tcp, go_udp)
            rs = run_side(rust, "rust", rs_http, rs_tcp, rs_udp)
            go_norm = go["config_body"].replace(f"tcp_port: {go_tcp}", "tcp_port: <T>") \
                                       .replace(f"udp_port: {go_udp}", "udp_port: <U>")
            rs_norm = rs["config_body"].replace(f"tcp_port: {rs_tcp}", "tcp_port: <T>") \
                                       .replace(f"udp_port: {rs_udp}", "udp_port: <U>")
            assert go_norm == rs_norm, (name, "both binaries must be fed the same config",
                                        go_norm, rs_norm)

            res_go, res_rs = go["result"], rs["result"]
            assert not res_go["timed_out"] and not res_rs["timed_out"], name
            # Only a nil-`RootCAs` alias dial can legitimately differ in its refusal *sentence*:
            # the YAML alias, the hidden generated flag and the env source all reach the same
            # branch, and that branch hands the chain to the platform verifier. Even there the
            # sentence depends on the peer-certificate shape — see SHAPE_TEXT_ALLOWANCE — and a
            # `tls.enabled: true` dial builds real `RootCAs`, goes through Go's own verifier and
            # already matches byte for byte (c17 attested that, and tls-block-wrong-ca-file
            # re-attests it on this very leaf).
            alias_dial = ("use_tls: true" in go_norm
                          or "--redis.use_tls=true" in extra
                          or env.get("LIVEKIT_REDIS_USE_TLS") == "true")
            accept = ("none" if expectation != "x509-failure" or not alias_dial
                      or "enabled: true" in go_norm
                      else SHAPE_TEXT_ALLOWANCE.get(shape, "none"))
            surfaces = {key: text_divergence(res_go[key], res_rs[key], accept)
                        for key in ("failure_lines", "diagnostics")}
            if res_go["kind"] == "list-nodes":
                # The whole stdout, line by line, so an allowance can never cover a second,
                # unrelated sentence that happens to look like a certificate refusal. The
                # server-start log is not this comparable and stays on the two surfaces above.
                surfaces["stdout_lines"] = text_divergence(
                    res_go["stdout_text"].splitlines(),
                    res_rs["stdout_text"].splitlines(), accept)
            texts = surfaces
            rec = {"id": name, "path": kind, "expectation": expectation, "argv": extra,
                   "env": env, "ledger_items": leaves, "text_allowance": accept,
                   "relay_shape": shape,
                   "config_sha256": {"official": go["config_sha256"],
                                     "rust": rs["config_sha256"]},
                   "config": go_norm,
                   "text_verdicts": texts,
                   "official": res_go, "rust": res_rs,
                   "official_relay": go["relay"], "rust_relay": rs["relay"],
                   "official_peer_details": go["relay_details"],
                   "rust_peer_details": rs["relay_details"]}
            for entry in compare_shape(res_go, res_rs, texts):
                mismatches.append((name, *entry))
            if any(v["kind"] == SYSTEM_TRUST_ITEM for v in texts.values()):
                system_trust_vectors.append(name)
            if any(v["kind"] == CERT_REASON_ITEM for v in texts.values()):
                cert_reason_vectors.append(name)

            # The relay is the witness that the alias really emitted a TLS ClientHello and
            # that the peer's answer was the same on both sides. Compared shapes differ per
            # path because the official binary keeps a second Redis connection alive for its
            # psrpc message bus (a tracked difference, see known_differences) and a started
            # server keeps re-dialling, so the stable observables are different:
            #   list-nodes, failure vector : the whole observation, exactly, since neither
            #       side starts a bus and go-redis' dial-attempt count is the contract;
            #   list-nodes, success vector : Rust's one usable connection, official's excess
            #       attributed to psrpc, both all-TLS;
            #   server start               : who was accepted and who was rejected, because
            #       the counts are not stable there.
            if kind == "list-nodes":
                rec["relay_exact"] = go["relay"] == rs["relay"]
                if not rec["relay_exact"]:
                    excess = go["relay"]["connections"] - rs["relay"]["connections"]
                    if (expectation == "success" and relay_rule == "tls-only"
                            and rs["relay"]["connections"] == 1
                            and rs["relay"]["kinds"] == {"accepted": 1}
                            and 0 < excess <= 1
                            and go["relay"]["kinds"].get("accepted", 0) >= 1
                            and set(go["relay"]["kinds"]) <= {"accepted", "handshake rejected"}):
                        rec["relay_difference"] = PSRPC_DIFFERENCE
                        peer_attributed.append(name)
                    else:
                        mismatches.append((name, "relay records differ",
                                           go["relay"], rs["relay"]))
            else:
                rec["relay_exact"] = None
                accepted = {side: seen["kinds"].get("accepted", 0)
                            for side, seen in (("official", go["relay"]),
                                               ("rust", rs["relay"]))}
                rejected = {side: seen["kinds"].get("handshake rejected", 0)
                            for side, seen in (("official", go["relay"]),
                                               ("rust", rs["relay"]))}
                rec["relay_accepted"] = accepted
                rec["relay_rejected"] = rejected
                if expectation == "success" and not all(v > 0 for v in accepted.values()):
                    mismatches.append((name, "no accepted TLS connection on one side",
                                       accepted, go["relay"], rs["relay"]))
                if expectation == "x509-failure" and (
                        any(v != 0 for v in accepted.values())
                        or not all(v > 0 for v in rejected.values())):
                    mismatches.append((name, "the system-pool rejection was not reproduced "
                                             "on both sides", accepted, rejected))

            # The reference behaviour of each vector is asserted on the official binary
            # first: agreement with Rust on a vector that never did what upstream does would
            # be agreement about the wrong thing. The per-side relay invariants are then
            # checked on both binaries, so a Rust that silently skipped TLS could not pass.
            if expectation == "success":
                if kind == "list-nodes":
                    assert res_go["exit"] == 0, (name, res_go["stdout_text"],
                                                 res_go["failure_lines"])
                    assert NODE_ID in res_go["stdout_text"], \
                        (name, "never listed the seeded node", res_go["stdout_text"][:300])
                else:
                    assert res_go["reached_http"] and not res_go["exited_early"], \
                        (name, "the server never got past the Redis dial",
                         res_go["failure_lines"], res_go["diagnostics"])
                    # c25 could not make this claim: the interrupt landed inside upstream's
                    # post-listener `running` window, so the reference was SIGKILLed (-9) and
                    # the teardown exit was excluded from the comparison. Past the settle the
                    # reference stops gracefully, so both sides must leave 0.
                    assert not res_go["sigkilled"], \
                        (name, "official ignored SIGINT for 30s after the settle",
                         res_go["drain_seconds"])
                    assert res_go["exit"] == 0, \
                        (name, "official left uncleanly after the settle",
                         res_go["exit"], res_go["drain_seconds"])
            elif expectation == "x509-failure":
                failed = (res_go["exited_early"] if kind == "server-start"
                          else res_go["exit"] != 0)
                assert failed, (name, "failure vector exited 0", res_go)
                joined = " ".join(res_go["failure_lines"] + res_go["diagnostics"])
                assert "x509" in joined, (name, "no x509 text on either surface",
                                          res_go["failure_lines"], res_go["diagnostics"])
                assert "certificate" in joined, \
                    (name, "x509 without a certificate reason is not the trust-pool failure",
                     joined[:400])
            else:  # deadline-failure
                assert res_go["exit"] != 0, (name, "deadline vector exited 0", res_go)
                assert any("context deadline exceeded" in line
                           for line in res_go["failure_lines"]), \
                    (name, "unexpected deadline text", res_go["failure_lines"])
            if relay_rule == "tls-only":
                for side, seen in (("official", go["relay"]), ("rust", rs["relay"])):
                    assert seen["connections"] > 0, (name, side, "the relay saw no connection")
                    assert seen["tls_records"] == seen["connections"] \
                        and seen["plaintext_records"] == 0, \
                        (name, side, "this client sent plaintext to the TLS relay", seen)
                    rec[side + "_all_tls"] = True
            elif relay_rule == "none":
                for side, seen in (("official", go["all_relays"]), ("rust", rs["all_relays"])):
                    touched = {key: value["connections"] for key, value in seen.items()
                               if value["connections"]}
                    assert not touched, \
                        (name, side, "a plaintext-Redis vector reached a TLS relay", touched)
            report["cases"].append(rec)
            save()

        report["mismatches"] = mismatches
        assert not mismatches, f"{len(mismatches)} paired divergence(s): {mismatches}"

        assert {c["id"] for c in report["cases"]} == {v[0] for v in vectors}, "vector set changed"
        successes = [c for c in report["cases"] if c["expectation"] == "success"]
        failures = [c for c in report["cases"] if c["expectation"] != "success"]
        # 6 success branches (plaintext alias off/absent, tls-block success, flag and env
        # accepted-but-inert on list-nodes, tls-block on the start path) and 7 failure
        # branches (deadline, five x509 refusals, x509 refusal at startup). A vector added or
        # dropped has to change this line, which is the point.
        assert len(successes) == 6 and len(failures) == 7, \
            ("case coverage must not shrink", len(successes), len(failures))
        # The two certificate shapes must have behaved as the recorded differences claim,
        # otherwise the opt-ins above would be unproven allowances: the leaf whose issuer is
        # absent gets the platform sentence from the reference and the portable text from Rust,
        # the CA-flagged self-signed leaf gets the same refusal for a different reason, and the
        # wrong-anchor `tls` block — same leaf, Go's own verifier — must match byte for byte.
        by_id = {c["id"]: c for c in report["cases"]}
        named = by_id["alias-true-to-relay"]["text_verdicts"]
        assert any(v["kind"] == SYSTEM_TRUST_ITEM for v in named.values()), (
            "the recorded system-trust sentence difference did not occur in this run: ", named)
        assert by_id["alias-true-to-relay"]["text_allowance"] == "system-trust", \
            by_id["alias-true-to-relay"]["text_allowance"]
        official_cf = "\n".join(by_id["alias-true-to-relay"]["official"]["failure_lines"])
        assert "\u201clocalhost\u201d" in official_cf, (
            "the platform sentence must name the leaf it refused: ", official_cf)
        cert = by_id["alias-selfsigned-ca-leaf-to-relay"]["text_verdicts"]
        assert any(v["kind"] == CERT_REASON_ITEM for v in cert.values()), (
            "the recorded CA-flag reason difference did not occur in this run: ", cert)
        assert by_id["alias-selfsigned-ca-leaf-to-relay"]["text_allowance"] == "cert-reason", \
            by_id["alias-selfsigned-ca-leaf-to-relay"]["text_allowance"]
        assert PORTABLE_UNKNOWN_AUTHORITY in "\n".join(
            by_id["alias-selfsigned-ca-leaf-to-relay"]["official"]["failure_lines"])
        assert CERT_REASON_RUST in "\n".join(
            by_id["alias-selfsigned-ca-leaf-to-relay"]["rust"]["failure_lines"])
        control = by_id["tls-block-wrong-ca-file"]
        assert all(v["kind"] == "identical" for v in control["text_verdicts"].values()), (
            "the wrong-anchor tls block is the byte-identical control: ",
            control["text_verdicts"])
        assert control["text_allowance"] == "none", control["text_allowance"]
        for side in ("official", "rust"):
            lines = "\n".join(control[side]["failure_lines"])
            assert PORTABLE_UNKNOWN_AUTHORITY in lines, (side, lines)
            assert DARWIN_CFERROR.search(lines) is None, (side, lines)
        leaf_vectors: dict[str, list[str]] = {}
        for rec in report["cases"]:
            for leaf in rec["ledger_items"]:
                leaf_vectors.setdefault(leaf, []).append(rec["id"])
        report["leaf_vectors"] = leaf_vectors
        assert set(leaf_vectors) == {YAML_LEAF, FLAG_LEAF, CLI_LEAF, STARTUP_LEAF, TLS_BLOCK,
                                    RACE_LEAF}, sorted(leaf_vectors)
        assert len(leaf_vectors[YAML_LEAF]) >= 8, \
            ("the alias leaf must carry several vectors, not one", leaf_vectors[YAML_LEAF])
        assert len(leaf_vectors[TLS_BLOCK]) >= 4, \
            ("the tls block needs precedence, success and wrong-anchor vectors",
             leaf_vectors[TLS_BLOCK])
        assert front.any_tls(), "no relay ever saw a TLS record, so nothing ever handshook"
        report["relay_connections"] = {name: relay.connections
                                       for name, relay in front.relays.items()}
        report["psrpc_difference_vectors"] = peer_attributed
        report["graceful_stops"] = {
            rec["id"]: {"official": {k: rec["official"][k] for k in
                                    ("exit", "sigkilled", "drain_seconds")},
                        "rust": {k: rec["rust"][k] for k in
                                 ("exit", "sigkilled", "drain_seconds")}}
            for rec in report["cases"]
            if rec["path"] == "server-start" and rec["expectation"] == "success"}
        report["known_differences"] = [{
            "id": "list-nodes.PSRPCMessageBusSecondRedisConnection",
            "outside_item_scope_of": sorted({YAML_LEAF, FLAG_LEAF, CLI_LEAF, STARTUP_LEAF}),
            "difference": PSRPC_DIFFERENCE,
            "observed_in_this_run": peer_attributed,
            "handling": ("compared, not absorbed: on a successful run through the relay Rust "
                         "must hold exactly one usable connection, the official side may make "
                         "at most one more, and that extra connection has to be one the relay "
                         "either accepted or saw torn down mid-handshake. Any other difference "
                         "— including on every failure vector, where neither side starts a bus "
                         "and go-redis' dial-attempt count is the contract — fails the run. A "
                         "started server keeps re-dialling, so on that path the observables are "
                         "which handshakes the peer accepted and which it saw rejected, and the "
                         "counts are recorded per side."),
            "reproduction": ("tests/interop/cli-list-nodes-redis-tls/tap-connections.py "
                             "records it on the plaintext path (official: 2 connections, "
                             "rust: 1)"),
        }, {
            "id": SYSTEM_TRUST_ITEM,
            "scope_of_this_item": (
                "the x509 verification-failure **sentence** produced when crypto/tls is "
                "handed a nil RootCAs on darwin, i.e. the deprecated redis.use_tls alias "
                "branch. It is a diagnostic-text item: the dial decision it is evidence for "
                "(refuse an untrusted chain after a real ClientHello) is certified by the "
                "vectors above, on both binaries, byte-identically around this sentence."),
            "difference": (
                "official prints the macOS Security.framework CFError description "
                "(x509: “<leaf summary>” certificate is not trusted) for a CA-signed leaf "
                "presented without its issuer, because x509.Certificate.systemVerify passes "
                "any CFError code it does not map through fmt.Errorf(\"x509: %s\", err) "
                "(references/go-stdlib-go1.26.3/src/crypto/x509/root_darwin.go:60-71, reached "
                "for a system pool from verify.go:556-567). The Rust port verifies the same "
                "chain with WebPKI over the exported system anchors, so it prints Go's "
                "portable wording (x509: certificate signed by unknown authority), which is "
                "what the reference itself prints on Linux and what it prints on this "
                "machine for the self-signed shape."),
            "observed_in_this_run": system_trust_vectors,
            "control_in_this_run": ("tls-block-wrong-ca-file — the same CA-signed leaf, refused "
                                    "by both binaries with byte-identical text because a non-nil "
                                    "RootCAs keeps Go on its own verifier — plus every success "
                                    "vector, whose texts are identical too"),
            "handling": (
                "recorded, not absorbed and not replicated: the allowance is granted only "
                "when the whole line around the sentence is byte-identical, the official "
                "sentence matches the CFError shape, the Rust sentence is exactly the portable "
                "text, and the vector is a nil-RootCAs alias dial. Both raw texts are in every "
                "case's text_verdicts. Replicating it would mean wrapping the rustls verifier "
                "in a Security.framework-shaped renderer, which is platform diagnostic text, "
                "not a consumer any SDK or CLI branches on; it stays an open item instead."),
            "not_covered_by_this_item": (
                "the refusal *reason* the platform verifier gives on other chains "
                "(CERT_REASON_ITEM, its own entry below) and the TLS alert number the peer "
                "sees (ALERT_ITEM, its own entry below) — recorded separately so neither item "
                "can absorb the other"),
        }, {
            "id": CERT_REASON_ITEM,
            "scope_of_this_item": (
                "the verification-failure **reason** reported for a peer certificate whose "
                "basicConstraints says CA:TRUE while it is presented as the end entity, on the "
                "same nil-RootCAs alias dial. Text item on this surface: both binaries refuse "
                "the connection, and the vectors above certify that."),
            "difference": (
                "official prints x509: certificate signed by unknown authority, because Go "
                "never checks the CA flag for a leaf (references/go-stdlib-go1.26.3/src/crypto/"
                "x509/verify.go:496 rejects a non-CA only for certType == intermediateCertificate; "
                "the leaf goes through isValid at :584) and the platform call answers "
                "errSecNotTrusted, which root_darwin.go:66-68 maps to UnknownAuthorityError. "
                "The Rust port prints invalid peer certificate: " + CERT_REASON_RUST + ", "
                "because rustls-webpki 0.103.16 rejects the role misuse "
                "outright (src/verify_cert.rs:449)."),
            "observed_in_this_run": cert_reason_vectors,
            "control_in_this_run": ("alias-true-to-relay dials a leaf with no CA flag and Rust "
                                    "answers with the portable sentence there, so this reason "
                                    "text is not Rust's generic answer; "
                                    "tls-block-wrong-ca-file is the byte-identical pair"),
            "handling": (
                "recorded, and deliberately not closed by a text mapping: redis_tls.rs's "
                "go_x509_text maps only reasons whose Go wording is derivable from what Rust "
                "reports, and inventing `unknown authority` for a webpki-only verdict would hide "
                "the functional half of this item — with such a leaf installed as the configured "
                "trust anchor, Go accepts the connection and Rust refuses it. That is an "
                "accept/refuse difference, not a wording difference; it is out of scope here "
                "because certifying it would mean allowing a behavioural divergence, and "
                "silencing Rust's refusal to agree with Go would turn a certificate failure into "
                "a success, which this project forbids. Needs a decision on whether the Rust "
                "verifier should relax role checking for an explicitly configured anchor."),
            "not_covered_by_this_item": (
                "the accept/refuse gap itself (recorded as TLS:x509.CaUsedAsEndEntityVerdict in "
                "the ledger, unexercised by any vector in this harness) and the alert number the "
                "peer sees on this shape (ALERT_ITEM: the relay recorded Go's 42 "
                "bad_certificate against Rust's 43 certificate_unknown for this very leaf)"),
        }, {
            "id": ALERT_ITEM,
            "scope_of_this_item": (
                "the TLS alert record a client sends when it refuses a peer certificate — a "
                "wire-observable, third-party-visible surface (any TLS server, not only Redis, "
                "sees it), which is why it is compared rather than absorbed."),
            "difference": (
                "Go sends 42 bad_certificate for every certificate-verification failure "
                "(references/go-stdlib-go1.26.3/src/crypto/tls/handshake_client.go:582, :696, "
                ":1110, :1137), while rustls 0.23.46 maps its reasons: UnknownIssuer to 48 "
                "unknown_ca and other certificate errors to 43 certificate_unknown "
                "(references/crates/rustls-0.23.46/src/error.rs:625-660). Both are 'the client "
                "rejected the certificate'; the numbers differ."),
            "observed_in_this_run": {
                rec["id"]: {side: rec[side + "_peer_details"]
                            for side in ("official", "rust")}
                for rec in report["cases"]
                if rec["expectation"] == "x509-failure" and rec["relay_shape"]},
            "handling": (
                "compared by family, raw strings retained: peer_family() buckets every "
                "connection the relay made into 'accepted' / 'peer saw a certificate alert from "
                "the client' / 'connection reset during the handshake' / 'plaintext connection "
                "closed', and the two sides must agree on the counts per family. The alert "
                "number lives inside the raw outcome text, which stays in relay.connections and "
                "in each case's relay observation, so the difference is visible in the receipt "
                "without failing an otherwise-matching vector. Closing it needs a rustls "
                "alert-mapping change, not a harness change."),
        }, {
            "id": RACE_LEAF,
            "scope_of_this_item": (
                "SIGINT landing while `Start()` is still inside the 100ms sleep it takes "
                "*after* the HTTP listeners answer (`server.go:343-347`): `Stop()` bails at "
                "`if !s.running.Swap(false) { return }` (`server.go:391`) and never reaches "
                "`close(s.doneChan)`, so `Start()` stays blocked on `<-s.doneChan`."),
            "difference": (
                "the c25 run of this harness recorded the symptom without the cause: on "
                "`tls-block-consumed-on-start-path` the official process needed a SIGKILL "
                "(exit -9) while the candidate exited 0, because the interrupt was delivered "
                "the instant HTTP turned green. Measured on the locked binary against the "
                "first HTTP 200: 0.00s/0.03s/0.08s all survived past a 20s cap and were "
                "SIGKILLed, 0.12s/0.20s/0.50s all exited 0 within 0.01s, and the candidate "
                "exited 0 at all six (the sweep is in the c26 dead-node-prune receipt). This "
                "harness now settles `SETTLE_SECONDS` before interrupting, so the "
                "graceful-stop exit code and `sigkilled` are compared instead of waived."),
            "observed_in_this_run": report["graceful_stops"],
            "handling": (
                "recorded, not closed. The candidate not reproducing an upstream hang is a "
                "deliberate divergence, and it is now written down with the measurements that "
                "define it rather than hidden behind an uncomparable exit code."),
        }]
        report["aggregate_status"] = "PASS_CLI_REDIS_USE_TLS_ALIAS_PAIRED"
        report["status"] = report["aggregate_status"]
    except BaseException as exc:
        report["status"] = "FAIL"
        report["failure"] = f"{type(exc).__name__}: {exc}"
        code = 1
    finally:
        if front is not None:
            front.stop_serving()
        if redis_proc is not None and redis_proc.poll() is None:
            redis_proc.terminate()
            try:
                redis_proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                redis_proc.kill()
        shutil.rmtree(staging, ignore_errors=True)
        save()
    print(json.dumps({"status": report["status"], "cases": len(report["cases"]),
                      "mismatches": [[m[0], m[1]] for m in report.get("mismatches", [])],
                      "failure": report.get("failure")}, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
