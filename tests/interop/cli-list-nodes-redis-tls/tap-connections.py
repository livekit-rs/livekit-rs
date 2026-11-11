#!/usr/bin/env python3
"""Record every Redis connection the official and the Rust `list-nodes` make.

A RESP tap sits between each binary and a plain Redis and keeps **every** chunk the
client wrote on every connection, so the receipt shows (a) how many connections each
binary dials for one successful `list-nodes` run and (b) the exact command text each
one sends.

Why this exists: `run-paired.py` compares stdout/stderr/exit, which cannot see
either of those. Both turned out to differ when this was first run:

  * connection count — upstream `service.InitializeRouter` registers a psrpc
    keepalive server that subscribes over the Redis message bus on a pinned
    second connection; the read-only node-discovery port dials once.
    Still open, tracked as `list-nodes.PSRPCMessageBusSecondRedisConnection`
    (SPEC-0076 workline B).
  * command name case — go-redis writes its command builders verbatim
    (`hello`, `ping`, `hvals`), our CLI originally wrote them upper case. Redis
    matches command names case-insensitively, so no stdout differential could ever
    see it. Fixed in batch 21n (`cli_redis_wire.rs::command` lowercases the name,
    the session builders spell the sub-commands as go-redis does); this script now
    asserts the parity instead of the divergence.

The script starts and stops its own loopback `redis-server`, hashes both binaries,
and fails if either assertion breaks, so the recording is a check rather than a note.
Usage:
  python3 tap-connections.py --official /abs/livekit-server --rust /abs/livecloud \
      --base-port 21739 --output DIR
"""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import pathlib
import shutil
import socket
import subprocess
import tempfile
import threading
import time

LOCK = threading.Lock()
PSRPC_ITEM = "list-nodes.PSRPCMessageBusSecondRedisConnection"
CASING_ITEM = "CLI:redis.CommandNameCasing"


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
        """Copy one direction, keeping every chunk the client wrote.

        Recording only the first chunk was enough to *see* the divergence this
        harness was written for, but not to prove its absence: go-redis sends
        `hello` in one write and the `client setinfo` pair in a pipeline behind it,
        so the whole trace has to be on the receipt for a casing claim to cover
        more than the first command.
        """
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

    def close(self) -> None:
        self.stop.set()
        try:
            self.sock.close()
        except OSError:
            pass


def command_traces(wire: str) -> list[list[str]]:
    """Every RESP array in a captured chunk, as its full token list.

    `\r\n` in the recording is already escaped to the two-character form, so this
    reads the same text the receipt stores. A bulk-string length is consumed by
    position, which is enough because a client write here is only ever a command
    array of bulk strings.
    """
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
            length = int(raw[i + 1:end])
            i = end + 2
            tokens.append(raw[i:i + length].decode("latin-1"))
            i += length + 2
        if tokens:
            traces.append(tokens)
    return traces


def command_names(wire: str) -> list[str]:
    """The command token of every RESP array in a captured chunk."""
    return [tokens[0] for tokens in command_traces(wire)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", required=True, type=pathlib.Path)
    ap.add_argument("--rust", required=True, type=pathlib.Path)
    ap.add_argument("--base-port", type=int, default=21739)
    ap.add_argument("--output", required=True, type=pathlib.Path)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    plain, tap_port = args.base_port, args.base_port + 1
    for port in (plain, tap_port):
        assert free(port), f"fixture port {port} is already in use"
    staging = pathlib.Path(tempfile.mkdtemp(prefix="resp-tap-"))
    redis_proc = None
    tap = None
    report = {"status": "RUNNING", "items_observed": [PSRPC_ITEM, CASING_ITEM],
              "tap_port": tap_port, "upstream": plain,
              "binaries": {}, "runs": {},
              "normalization": ("none: every connection's inbound and outbound bytes are recorded "
                                "verbatim with \\r\\n escaped, per side; command traces are parsed "
                                "from those bytes, not restated")}
    try:
        redis_proc = start_redis(plain, staging)
        tap = Tap(tap_port, ("127.0.0.1", plain))
        time.sleep(0.3)
        config = staging / "tap.yaml"
        config.write_text(
            "keys: {devkey: secret}\nlogging: {level: error}\n"
            "rtc: {node_ip: 127.0.0.1, use_external_ip: false}\n"
            f'redis:\n  address: "127.0.0.1:{tap_port}"\n')
        config.chmod(0o600)

        for label, binary in (("official", args.official.resolve()),
                              ("rust", args.rust.resolve())):
            tap.reset()
            time.sleep(0.2)
            proc = subprocess.run([str(binary), "--dev", "--config", str(config), "list-nodes"],
                                  capture_output=True, timeout=60)
            time.sleep(0.5)  # let the tap threads record the last connection
            with LOCK:
                seen = [dict(c) for c in tap.connections]
            report["binaries"][label] = {"path": str(binary), "sha256": sha(binary.read_bytes())}
            report["runs"][label] = {
                "exit": proc.returncode,
                "connections": len(seen),
                "first_bytes": [c["first_bytes"] for c in seen],
                "commands": [command_names(c["first_bytes"]) for c in seen],
                "traces": [list(itertools.chain.from_iterable(command_traces(chunk)
                                                              for chunk in c["client_writes"]))
                           for c in seen],
                "response_heads": [c["responses"][:120] for c in seen],
            }
            assert proc.returncode == 0, (label, proc.stderr.decode()[:400])
            assert len(seen) > 0, (label, "the tap never saw a connection")
            assert all(c["first_bytes"].startswith("*") for c in seen), \
                (label, "the client did not open with a RESP array", seen)
            assert all(c["responses"].startswith(("+PONG", "-", "*", "$", ":", "%")) for c in seen), \
                (label, "the server never replied, so the session is not a usable connection",
                 [c["responses"] for c in seen])

        # The two observations this recording exists for. One of them is a parity
        # claim, checked by comparing the command text both sides actually wrote;
        # the other is still a divergence, asserted so the receipt records it
        # instead of papering over it.
        official = report["runs"]["official"]
        rust = report["runs"]["rust"]
        assert official["connections"] == 2 and rust["connections"] == 1, \
            (PSRPC_ITEM, official["connections"], rust["connections"])

        def traces(run: dict) -> set[str]:
            return {" ".join(tokens) for conn in run["traces"] for tokens in conn}

        def names(run: dict) -> set[str]:
            return {tokens[0] for conn in run["traces"] for tokens in conn}

        official_names, rust_names = names(official), names(rust)
        # go-redis writes every command builder's name as a lowercase literal
        # (`commands.go:339,364`), so anything uppercase on either side is the
        # divergence this item tracks. Both sides must now be clean.
        assert all(n == n.lower() for n in official_names | rust_names), \
            (CASING_ITEM, sorted(official_names), sorted(rust_names))
        # Names alone are not parity: go-redis also spells the sub-command
        # differently (`client setinfo`, lowercase, with `LIB-NAME` uppercase).
        # Every trace the Rust client sends must be one the official client sends.
        extra = traces(rust) - traces(official)
        assert not extra, (CASING_ITEM, "rust sends command text the official never sends",
                           sorted(extra))
        report["parity_attested_for"] = [CASING_ITEM]
        report["difference_recorded_for"] = [PSRPC_ITEM]
        report["command_traces"] = {"official": sorted(traces(official)),
                                    "rust": sorted(traces(rust))}
        report["difference"] = {
            PSRPC_ITEM: "official dials 2 Redis connections per successful list-nodes, rust dials 1"
                        " (upstream's psrpc keepalive subscriber is not in the read-only port)",
        }
        report["parity"] = {
            CASING_ITEM: (f"both sides send only lowercase command names "
                          f"{sorted(official_names | rust_names)}, and the rust traces "
                          f"{sorted(traces(rust))} are all present in the official traces"),
        }
        report["status"] = "PASS_REDIS_COMMAND_NAME_CASING_PAIRED"
    except BaseException as exc:
        report["status"] = "FAIL"
        report["failure"] = f"{type(exc).__name__}: {exc}"
        return 1
    finally:
        if tap is not None:
            tap.close()
        if redis_proc is not None and redis_proc.poll() is None:
            redis_proc.terminate()
            try:
                redis_proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                redis_proc.kill()
        shutil.rmtree(staging, ignore_errors=True)
        (args.output / "tap-connections.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"status": report["status"], "failure": report.get("failure"),
                          "official_connections": report["runs"].get("official", {}).get("connections"),
                          "rust_connections": report["runs"].get("rust", {}).get("connections"),
                          "official_commands": report["runs"].get("official", {}).get("commands"),
                          "rust_commands": report["runs"].get("rust", {}).get("commands")}))


if __name__ == "__main__":
    raise SystemExit(main())

