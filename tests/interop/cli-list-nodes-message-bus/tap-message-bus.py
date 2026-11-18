#!/usr/bin/env python3
"""Paired `list-nodes` message-bus second-connection differential (C41).

Closes `list-nodes.PSRPCMessageBusSecondRedisConnection` (SPEC-0076 workline B).
The official `list-nodes` builds the psrpc Redis message bus
(`service.InitializeRouter` -> `getMessageBus` -> `redisbus.New`, psrpc v0.8.2
`redisbus.go:63-83`). `redisbus.New` calls `rc.Subscribe(ctx)` with NO channels and
spawns `go r.readWorker()`, whose first `ReceiveMessage` eagerly dials a DEDICATED
pubsub connection — the official binary's SECOND Redis connection for one
successful run. `list-nodes` runs `InitializeRouter` + `ListNodes` (HVALS) but never
`router.Start()`, so `SubscribePing` never runs and NO channel is ever subscribed:
the read connection carries only the go-redis `initConn` handshake, never a command.

The read-only Rust port originally dialed once. C41 grows the same message-bus read
worker (`cli_redis_retry.rs::spawn_message_bus` -> `Session::open_message_bus` ->
`Wire::read_push`), reusing the exact `initConn` handshake the command pool uses.

What this harness proves, paired on the SAME owned Redis, both binaries hashed:

  * connection count 2 == 2 on every run (plaintext AND TLS), N runs per side;
  * the COMMAND connection's full RESP trace is byte-identical across sides
    (hello / maint_notifications / setinfo LIB-NAME / setinfo LIB-VER / ping / hvals);
  * the BUS connection is init-only: it opens with `hello`, every command it sends
    is a PREFIX of the command connection's init handshake, and it never sends
    ping / hvals / subscribe. The official bus connection is cut at a racy point
    during process exit (often after `hello` alone), so its trace is asserted as a
    prefix, not an exact equality — the CONTRACT (a second init-only pubsub read
    connection, no command/subscribe) is what both sides must meet.

Two profiles share one owned plain Redis:
  * plaintext: a RESP tap records every client write on every connection;
  * TLS: a TLS-terminating relay forwards the decrypted stream to a second RESP tap,
    so the TLS profile gets the SAME full-trace assertions PLUS proof that both
    connections completed a real TLS handshake (the relay rejects plaintext records).

Falsifiable: a Rust binary that drops the bus dial (count 1), or makes the bus
connection send hvals (two command connections), or subscribe (a subscribe trace),
fails the assertions below. Run with --rust <mutant> to record the positive control.

Usage:
  python3 tap-message-bus.py --official /abs/livekit-server-go --rust /abs/livecloud \
      --base-port 21749 --output DIR [--runs 3]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time

LOCK = threading.Lock()
ITEM = "list-nodes.PSRPCMessageBusSecondRedisConnection"
COMMAND_TOKENS = {"ping", "hvals"}
FORBIDDEN_BUS_TOKENS = {"ping", "hvals", "subscribe", "psubscribe", "ssubscribe"}


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def free(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(0.2)
        return probe.connect_ex(("127.0.0.1", port)) != 0


def start_redis(port: int, staging: pathlib.Path) -> subprocess.Popen:
    """An owned instance: the recording must not depend on an ambient Redis."""
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


class Tap:
    """RESP tap: keeps EVERY chunk the client wrote on every connection."""

    def __init__(self, tap_port: int, upstream: tuple[str, int]):
        self.upstream = upstream
        self.connections: list[dict] = []
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", tap_port))
        self.sock.listen(32)
        self.stop = threading.Event()
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
            threading.Thread(target=self._handle, args=(client,), daemon=True).start()

    def _handle(self, client: socket.socket):
        try:
            remote = socket.create_connection(self.upstream, timeout=10)
        except OSError:
            client.close()
            return
        entry = {"index": 0, "first_bytes": "", "responses": "", "client_writes": []}
        with LOCK:
            entry["index"] = len(self.connections) + 1
            self.connections.append(entry)
        for src, dst, key, record in ((client, remote, "first_bytes", True),
                                      (remote, client, "responses", False)):
            threading.Thread(target=self._pipe,
                             args=(src, dst, entry, key, record), daemon=True).start()

    @staticmethod
    def _pipe(src: socket.socket, dst: socket.socket, entry: dict, key: str,
              record: bool) -> None:
        try:
            src.settimeout(10)
            first = src.recv(4096)
        except OSError:
            first = b""
        chunks = [first]
        with LOCK:
            entry[key] = first.decode("latin-1").replace("\r\n", "\\r\\n")
        if first:
            try:
                dst.sendall(first)
            except OSError:
                pass
        while True:
            try:
                chunk = src.recv(65536)
            except OSError:
                break
            if not chunk:
                break
            chunks.append(chunk)
            try:
                dst.sendall(chunk)
            except OSError:
                break
        if record:
            with LOCK:
                entry["client_writes"] = [c.decode("latin-1").replace("\r\n", "\\r\\n")
                                          for c in chunks if c]
        for handle in (src, dst):
            try:
                handle.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            handle.close()

    def reset(self) -> None:
        with LOCK:
            self.connections.clear()

    def snapshot(self) -> list[dict]:
        with LOCK:
            return [dict(c) for c in self.connections]

    def close(self) -> None:
        self.stop.set()
        try:
            self.sock.close()
        except OSError:
            pass


def command_traces(wire: str) -> list[list[str]]:
    """Every RESP array in a captured chunk, as its full token list."""
    raw = wire.replace("\\r\\n", "\r\n").encode("latin-1")
    traces: list[list[str]] = []
    i = 0
    while i < len(raw):
        if raw[i:i + 1] != b"*":
            break
        end = raw.find(b"\r\n", i)
        if end < 0:
            break
        try:
            count = int(raw[i + 1:end])
        except ValueError:
            break
        i = end + 2
        tokens = []
        for _ in range(count):
            if raw[i:i + 1] != b"$":
                return traces
            end = raw.find(b"\r\n", i)
            if end < 0:
                return traces  # frame cut mid-header by process exit
            length = int(raw[i + 1:end])
            i = end + 2
            if i + length + 2 > len(raw):
                return traces  # frame cut mid-bulk by process exit
            tokens.append(raw[i:i + length].decode("latin-1"))
            i += length + 2
        if tokens:
            traces.append(tokens)
    return traces


def conn_trace(conn: dict) -> list[list[str]]:
    # Parse the concatenated client-write stream as ONE continuous RESP sequence.
    # recv() chunk boundaries are arbitrary: a command split across two chunks
    # would be dropped by per-chunk parsing (the tail chunk no longer starts with
    # '*'), which previously made the official bus trace lose a mid-stream command
    # and spuriously fail the init-prefix check. Joining reassembles split commands.
    return command_traces("".join(conn["client_writes"]))


def run_openssl(*args: str) -> None:
    done = subprocess.run(["openssl", *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    assert done.returncode == 0, ("openssl failed", args, done.stdout.decode())


class Certs:
    """Throwaway CA + server identity, regenerated per run, never committed."""

    def __init__(self, where: pathlib.Path):
        self.dir = where
        (where / "index.txt").touch()
        self.ca = where / "ca.pem"
        self.ca_key = where / "ca.key"
        self.server = where / "server.pem"
        self.server_key = where / "server.key"
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

    def describe(self) -> dict:
        def fingerprint(path: pathlib.Path) -> str:
            done = subprocess.run(
                ["openssl", "x509", "-in", str(path), "-noout", "-fingerprint", "-sha256"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            return done.stdout.decode().strip()
        return {name: fingerprint(self.dir / name) for name in ("ca.pem", "server.pem")}


class Relay:
    """TLS terminator in front of a downstream RESP tap (which fronts plain Redis).

    Records one entry per inbound connection: whether the first byte was a TLS
    record (0x16) and how the handshake ended. The decrypted stream is forwarded to
    the tap, so the tap sees the same RESP text the plaintext profile records.
    """

    def __init__(self, port: int, context: ssl.SSLContext, upstream: tuple[str, int]):
        self.port, self.context, self.upstream = port, context, upstream
        self.stop = threading.Event()
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", port))
        self.sock.listen(16)
        self.handshakes: list[str] = []
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
        client.settimeout(5)
        try:
            first = client.recv(1, socket.MSG_PEEK)
        except OSError:
            first = b""
        entry = {"prefix_hex": first.hex(), "tls": first == b"\x16", "outcome": ""}
        with LOCK:
            self.connections.append(entry)
        if first != b"\x16":
            entry["outcome"] = "plaintext connection closed"
            with LOCK:
                self.handshakes.append(f"rejected: plaintext record 0x{first.hex()}")
            self._drain_then_close(client)
            return
        try:
            wrapped = self.context.wrap_socket(client, server_side=True)
        except (ssl.SSLError, OSError) as exc:
            entry["outcome"] = f"handshake rejected: {type(exc).__name__}: {exc}"
            with LOCK:
                self.handshakes.append(entry["outcome"])
            return
        entry["outcome"] = "accepted: " + (wrapped.version() or "")
        with LOCK:
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
        with LOCK:
            seen = self.connections[start:]
            return {"connections": len(seen),
                    "tls_records": sum(1 for e in seen if e["tls"]),
                    "plaintext_records": sum(1 for e in seen if not e["tls"]),
                    "prefixes": [e["prefix_hex"] for e in seen],
                    "outcomes": [e["outcome"] for e in seen]}

    def mark(self) -> int:
        with LOCK:
            return len(self.connections)

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


def classify(traces: list[list[list[str]]]) -> tuple[list, list]:
    """Split per-connection traces into command connections and init-only bus ones.

    Content-based, not accept-order-based: the command connection is the one that
    runs ping/hvals; the bus connection carries only the init handshake. This is
    robust to the two dials racing on either side.
    """
    command, bus = [], []
    for trace in traces:
        names = {tokens[0].lower() for tokens in trace if tokens}
        if names & COMMAND_TOKENS:
            command.append(trace)
        else:
            bus.append(trace)
    return command, bus


def init_prefix(command_trace: list[list[str]]) -> list[list[str]]:
    """The command connection's handshake, up to (not including) its first command."""
    prefix = []
    for tokens in command_trace:
        if tokens and tokens[0].lower() in COMMAND_TOKENS:
            break
        prefix.append(tokens)
    return prefix


def is_prefix(short: list, full: list) -> bool:
    return len(short) <= len(full) and short == full[:len(short)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", required=True, type=pathlib.Path)
    ap.add_argument("--rust", required=True, type=pathlib.Path)
    ap.add_argument("--base-port", type=int, default=21749)
    ap.add_argument("--output", required=True, type=pathlib.Path)
    ap.add_argument("--runs", type=int, default=3)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    official, rust = args.official.resolve(), args.rust.resolve()

    plain = args.base_port
    tap_plain_port = args.base_port + 1
    relay_port = args.base_port + 2
    tap_tls_port = args.base_port + 3
    for port in (plain, tap_plain_port, relay_port, tap_tls_port):
        assert free(port), f"fixture port {port} is already in use"

    staging = pathlib.Path(tempfile.mkdtemp(prefix="msgbus-tap-"))
    scratch = staging / "certs"
    scratch.mkdir()
    report = {
        "status": "RUNNING", "item": ITEM,
        "scope": ("real paired CLI list-nodes message-bus second connection for "
                  f"{ITEM}: simple-client plaintext AND TLS, connection count 2==2 per run, "
                  "command-connection full RESP trace byte-identical across sides, bus "
                  "connection init-only (hello-first, prefix of the init handshake, never "
                  "ping/hvals/subscribe). Not covered: sentinel/cluster (those dial through "
                  "the redis-rs endpoint path, a separate consumer), and the racy exact cut "
                  "point of the official bus connection during process exit."),
        "binaries": {"official_go": {"path": str(official), "sha256": sha(official.read_bytes())},
                     "rust": {"path": str(rust), "sha256": sha(rust.read_bytes())}},
        "go_binary_sha256": sha(official.read_bytes()),
        "rust_binary_sha256": sha(rust.read_bytes()),
        "runs_per_side": args.runs,
        "normalization": ("none on the wire: every connection's client writes are recorded "
                          "verbatim with \\r\\n escaped and parsed into RESP token lists; the "
                          "command connection trace is compared exactly across sides; the bus "
                          "connection is compared as an init-handshake prefix because the "
                          "official cut point during process exit is racy. TLS profile adds "
                          "per-connection handshake outcomes from the terminating relay."),
        "certificates": {}, "profiles": {},
    }
    receipt = args.output / "BATCH.json"

    def save():
        receipt.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")

    save()
    redis_proc = None
    tap_plain = tap_tls = relay = None
    code = 0
    try:
        certs = Certs(scratch)
        report["certificates"] = certs.describe()
        report["certificates"]["generation"] = (
            "openssl req -x509 / x509 -req, 2-day validity, scratch dir under the run's temp "
            "path; keys deliberately not committed")
        save()

        redis_proc = start_redis(plain, staging)
        tap_plain = Tap(tap_plain_port, ("127.0.0.1", plain))
        # TLS: relay terminates TLS and forwards the decrypted stream to tap_tls,
        # which fronts the same plain Redis. tap_tls therefore records the same RESP
        # text the plaintext tap does, for both TLS connections.
        tap_tls = Tap(tap_tls_port, ("127.0.0.1", plain))
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(str(certs.server), str(certs.server_key))
        relay = Relay(relay_port, server_context, ("127.0.0.1", tap_tls_port))
        time.sleep(0.3)

        plain_config = staging / "plain.yaml"
        plain_config.write_text(
            "keys: {devkey: secret}\nlogging: {level: error}\n"
            "rtc: {node_ip: 127.0.0.1, use_external_ip: false}\n"
            f'redis:\n  address: "127.0.0.1:{tap_plain_port}"\n')
        plain_config.chmod(0o600)
        tls_config = staging / "tls.yaml"
        tls_config.write_text(
            "keys: {devkey: secret}\nlogging: {level: error}\n"
            "rtc: {node_ip: 127.0.0.1, use_external_ip: false}\n"
            f'redis:\n  address: "127.0.0.1:{relay_port}"\n'
            f'  tls:\n    enabled: true\n    ca_cert_file: "{certs.ca}"\n')
        tls_config.chmod(0o600)

        profiles = [
            ("plaintext", plain_config, tap_plain, None),
            ("tls", tls_config, tap_tls, relay),
        ]
        for profile, config, tap, tls_relay in profiles:
            prec = {"runs": {"official": [], "rust": []}, "reference_init": None}
            report["profiles"][profile] = prec
            # The official command connection defines the reference init handshake
            # for this profile; both sides' bus connections must be a prefix of it.
            for label, binary in (("official", official), ("rust", rust)):
                for run in range(args.runs):
                    tap.reset()
                    relay_mark = tls_relay.mark() if tls_relay is not None else 0
                    time.sleep(0.2)
                    proc = subprocess.run(
                        [str(binary), "--dev", "--config", str(config), "list-nodes"],
                        capture_output=True, timeout=60)
                    time.sleep(0.5)  # let the tap/relay threads record the last connection
                    seen = tap.snapshot()
                    traces = [conn_trace(c) for c in seen]
                    command, bus = classify(traces)
                    rec = {"run": run, "exit": proc.returncode, "connections": len(seen),
                           "command_count": len(command), "bus_count": len(bus),
                           "traces": traces,
                           "first_bytes": [c["first_bytes"] for c in seen],
                           "response_heads": [c["responses"][:120] for c in seen]}
                    if tls_relay is not None:
                        rec["relay"] = tls_relay.observed(relay_mark)
                    prec["runs"][label].append(rec)
                    save()
                    assert proc.returncode == 0, (label, profile, run, proc.stderr.decode()[:400])
                    # The command connection is deterministic on both sides/profiles:
                    # it completes its init + ping + hvals before list-nodes prints.
                    assert len(command) == 1, (ITEM, label, profile, run,
                                               "expected exactly one command connection", len(command))
                    cmd = command[0]
                    assert cmd and cmd[0][0].lower() == "hello", \
                        (label, profile, run, "command conn not hello-first", cmd)
                    assert all(c["first_bytes"].startswith("*") for c in seen), \
                        (label, profile, run, "client did not open with a RESP array", seen)
                    assert all(c["responses"].startswith(("+PONG", "-", "*", "$", ":", "%"))
                               for c in seen), \
                        (label, profile, run, "server never replied", [c["responses"] for c in seen])
                    if tls_relay is None:
                        # Plaintext: both connections establish deterministically.
                        assert len(seen) == 2, (ITEM, label, profile, run,
                                                "expected 2 Redis connections", len(seen))
                        assert len(bus) == 1, (ITEM, label, profile, run,
                                               "expected one message-bus connection", len(bus))
                    else:
                        # TLS: the authoritative connection count is the relay's TLS
                        # attempt count (the same layer run-paired.py compares). The
                        # official bus connection's handshake can be reset by process
                        # exit before it forwards to tap_tls, so the downstream count
                        # is not authoritative for the official side.
                        robs = rec["relay"]
                        assert robs["connections"] == 2 and robs["tls_records"] == 2 \
                            and robs["plaintext_records"] == 0, \
                            (ITEM, label, profile, run, "TLS relay must see 2 TLS attempts", robs)
                        accepted = [o for o in robs["outcomes"] if o.startswith("accepted:")]
                        assert len(accepted) >= 1, \
                            (label, profile, run, "command connection TLS handshake not accepted",
                             robs["outcomes"])
                        rec["tls_outcomes"] = robs["outcomes"]
                        rec["tls_bus_established_downstream"] = len(bus) == 1
                        if label == "rust":
                            # Rust's oneshot makes conn2 deterministic: it must reach tap_tls.
                            assert len(seen) == 2 and len(bus) == 1, \
                                (ITEM, profile, run, "rust TLS bus connection must establish",
                                 len(seen), len(bus))
                        else:
                            # Official conn2 may be cut mid-handshake; the relay still
                            # counts the attempt. Record it, do not hide it.
                            assert len(seen) in (1, 2) and len(bus) in (0, 1), \
                                (ITEM, label, profile, run, "unexpected official TLS connections",
                                 len(seen), len(bus))
                    if label == "official" and run == 0:
                        prec["reference_init"] = init_prefix(cmd)
                        prec["official_command_trace"] = cmd
                    reference = prec["reference_init"]
                    assert reference is not None, (profile, "reference init handshake missing")
                    # Bus contract: init-only + prefix, asserted whenever the bus
                    # connection is visible downstream (always for plaintext and rust).
                    if bus:
                        bus_trace = bus[0]
                        assert bus_trace and bus_trace[0][0].lower() == "hello", \
                            (label, profile, run, "bus connection must open with hello", bus_trace)
                        bus_names = {tokens[0].lower() for tokens in bus_trace if tokens}
                        assert not (bus_names & FORBIDDEN_BUS_TOKENS), \
                            (label, profile, run, "bus connection sent a command/subscribe",
                             sorted(bus_names))
                        assert is_prefix(bus_trace, reference), \
                            (label, profile, run, "bus trace is not a prefix of the init handshake",
                             bus_trace, reference)
                        rec["contract"] = {"command_trace": cmd, "bus_trace": bus_trace,
                                           "bus_is_init_prefix": True, "bus_command_free": True}
                    else:
                        rec["contract"] = {"command_trace": cmd, "bus_trace": None,
                                           "bus_cut_before_forwarding": True}
                    if label == "rust":
                        # The command connection's full RESP trace must be byte-identical
                        # to the official one (init handshake + ping + hvals nodes).
                        assert cmd == prec["official_command_trace"], \
                            (ITEM, profile, run, "rust command trace differs from official",
                             cmd, prec["official_command_trace"])
                    save()
            # Cross-side count parity attestation for the profile.
            if tls_relay is None:
                off_counts = [r["connections"] for r in prec["runs"]["official"]]
                rust_counts = [r["connections"] for r in prec["runs"]["rust"]]
                assert off_counts == rust_counts == [2] * args.runs, \
                    (ITEM, profile, "connection count parity", off_counts, rust_counts)
                prec["count_parity"] = {"official": off_counts, "rust": rust_counts,
                                        "layer": "downstream_tap"}
            else:
                off_attempts = [r["relay"]["connections"] for r in prec["runs"]["official"]]
                rust_attempts = [r["relay"]["connections"] for r in prec["runs"]["rust"]]
                assert off_attempts == rust_attempts == [2] * args.runs, \
                    (ITEM, profile, "TLS attempt count parity", off_attempts, rust_attempts)
                prec["count_parity"] = {
                    "official": off_attempts, "rust": rust_attempts,
                    "layer": "relay_tls_attempts",
                    "official_downstream": [r["connections"] for r in prec["runs"]["official"]],
                    "rust_downstream": [r["connections"] for r in prec["runs"]["rust"]],
                    "official_bus_cut_runs": [r["run"] for r in prec["runs"]["official"]
                                              if not r["tls_bus_established_downstream"]]}
            prec["command_trace_exact"] = True
            prec["bus_init_only"] = True
            save()

        report["parity_attested_for"] = [ITEM]
        report["aggregate_status"] = "PASS_CLI_LIST_NODES_MESSAGE_BUS_SECOND_CONNECTION_PAIRED"
        report["status"] = report["aggregate_status"]
        save()
    except BaseException as exc:
        report["status"] = "FAIL"
        report["failure"] = f"{type(exc).__name__}: {exc}"
        code = 1
        save()
    finally:
        for handle in (tap_plain, tap_tls):
            if handle is not None:
                handle.close()
        if relay is not None:
            relay.stop_serving()
        if redis_proc is not None and redis_proc.poll() is None:
            redis_proc.terminate()
            try:
                redis_proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                redis_proc.kill()
        shutil.rmtree(staging, ignore_errors=True)
        save()
        print(json.dumps({"status": report["status"], "failure": report.get("failure"),
                          "item": ITEM,
                          "plaintext_counts": report["profiles"].get("plaintext", {}).get("count_parity"),
                          "tls_counts": report["profiles"].get("tls", {}).get("count_parity")}))
    return code


if __name__ == "__main__":
    raise SystemExit(main())