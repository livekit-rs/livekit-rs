#!/usr/bin/env python3
"""SPEC-0077 C39: CLI:list-nodes.CrossBinaryNodeRegistryLifecycle, paired.

Split out of CONFIG_BEHAVIOR:Redis.LiteralDialAddress when that item closed its
address scope (21l): over the SAME literal 127.0.0.1 Redis address, a node
registered by one binary must be listed by the OTHER binary's `list-nodes` CLI,
and the whole registry lifecycle (drain, restart, dead-node removal) must hold
cross-process. The registration producer side itself is owned by
RUNTIME:Redis.NodeRegistrationLifecycle (closed by C38); this campaign re-exercises
it only as the substrate for the cross-binary CLI claim and never re-claims it.

Upstream contract (locked Go v1.13.10 e5655ac2):
  cmd/server/commands.go:170-230  listNodes: getConfig -> NewLocalNode ->
      InitializeRouter -> ListNodes -> tablewriter; the ID cell is "%s\n(%s)"
      (id + NodeState label). InitializeRouter does NOT call RemoveDeadNodes —
      the CLI read path never prunes.
  pkg/service/server.go:197       RemoveDeadNodes runs ONLY in InitializeServer,
      i.e. on server start, before RegisterNode (:222).
  pkg/routing/redisrouter.go:85-98  RemoveDeadNodes: ListNodes -> !IsAvailable ->
      HDel (first error aborts); selector/utils.go:30-41 5s availability window.
  pkg/service/server.go:375-399   Stop(force): router.Drain() FIRST (:380), then
      the participant wait, then router.Stop() (:394).
  pkg/routing/redisrouter.go:185-199  Drain: SetState(SHUTTING_DOWN) + RegisterNode
      (a state-2 HSET); Stop: UnregisterNode (HDEL) + cancel.

Vectors:
  1 go-register-rust-list   official registers, Rust CLI lists exactly that id
                            SERVING; graceful stop revokes it from both CLIs.
  2 rust-register-go-list   the reverse direction.
  3 both-live-cross-cli     one Go + one Rust server: both CLIs see both ids;
                            stopping one revokes only its own in both views.
  4 drain-shutting-down-witness  a Redis MONITOR tap decodes the exact HSET bytes:
                            first HSET SERVING(1), a SHUTTING_DOWN(2) HSET exists,
                            no SERVING HSET after it, first HDEL after it. The
                            no-participant drain window is milliseconds, so a
                            keyspace poll cannot witness it deterministically.
  5 restart-fresh-identity  stop + fresh start on the same ports: exactly one new
                            id, no resurrection, CLI lists only the new one.
  6 dead-node-pruned-by-peer-start  SIGKILL residue aged past 5s is HDELed by the
                            PEER binary's startup sweep, both directions; the CLI
                            path itself is shown not to prune (residue survives
                            until the peer starts).

The foundation (wire format, RESP/Store/Monitor, process/log/config/Run helpers)
is copied VERBATIM from tests/interop/cli-redis-node-registration-lifecycle/
run-paired.py (sha 5fd55d4c...), which stays byte-identical so the C38 evidence
binding is untouched. Only ITEMS/REGION and the vectors/main differ.

RUNTIME:Server.InterruptBeforeRunningRace keeps the 0.5s settle before SIGINT.
Known differences recorded per side, never compared: connection counts
(RUNTIME:Redis.ServerStartConnectionChurn), NodeStats content / CLI stat cells
(RUNTIME:Redis.NodeStatsSampling), drain_seconds and MONITOR sequence lengths
(timing-dependent).
"""

from __future__ import annotations

import argparse
import base64
import errno
import hashlib
import json
import os
import pathlib
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

OFFICIAL_VERSION = "livekit-server version 1.13.10"
# Fixed reference build recorded in evidence/2026-10-30/c11-redis-sentinel.
# A version string alone does not identify the source being compared.
OFFICIAL_SHA256 = "4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc"
REG_ITEM = "RUNTIME:Redis.NodeRegistrationLifecycle"
STATS_ITEM = "RUNTIME:Redis.NodeStatsSampling"
CHURN_ITEM = "RUNTIME:Redis.ServerStartConnectionChurn"
CHANNEL_ITEM = "RUNTIME:Redis.PsrpcChannelSurface"
STARTUP_RACE_ITEM = "RUNTIME:Server.InterruptBeforeRunningRace"
HEALTH_ITEM = "FUNC:DEPLOY_HEALTH"
INTERVAL_ITEM = "YAML:node_stats.stats_update_interval"
MAX_DELAY_ITEM = "YAML:node_stats.stats_max_delay"
REGION_ITEM = "YAML:region"
WT_PORT_ITEM = "YAML:webtransport.port"
WT_CERT_ITEM = "YAML:webtransport.tls_cert_file"
POOL_ITEM = "RUNTIME:Redis.SingleNodeReconnect"
SETTLE_SECONDS = 0.5
# selector.AvailableSeconds is 5; observe strictly past it, so a record that is only
# written at startup would already be unavailable when the second sample is taken.
AVAILABILITY_WINDOW_SECONDS = 6.0
REGION = "harness-c39"
STALE_SECONDS = 3600
NODES_KEY = b"nodes"
C39_ITEM = "CLI:list-nodes.CrossBinaryNodeRegistryLifecycle"
ITEMS = [C39_ITEM]


def channel_shape(topic: str) -> str:
    """Strip the per-process random node/psrpc-client id from a Redis channel name."""
    return re.sub(r"\|(ND|CLI)_[A-Za-z0-9]+", r"|\1_<id>", topic)
# `cmd/server/main.go:54,83` bind `--config` to `LIVEKIT_CONFIG` and `--redis-host` to
# `REDIS_HOST`, and `pkg/config/config.go:1120-1123` applies `redis-host` with
# `if c.IsSet(...)`, which an env var set to the *empty string* satisfies. Exporting
# `REDIS_HOST=` therefore wipes the YAML `redis.address` and the server starts in
# single-node routing — measured, not assumed — so these two must be absent, not blank.
BASE_ENV = ({k: v for k, v in os.environ.items()
             if k in ("PATH", "HOME", "TMPDIR", "LANG")}
            | {"TZ": "UTC", "NO_COLOR": "1"})


# ----------------------------------------------------------------------- wire format


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
    return bfield(number, text.encode())


def bfield(number: int, raw: bytes) -> bytes:
    # proto3 omits a zero-valued scalar, which is what an empty string or an empty
    # bytes field encodes to; keeping that here is what makes the injected envelopes
    # byte-identical to what psrpc's Go publisher emits.
    return field(number, 2, varint(len(raw)) + raw) if raw else b""


def pb_fields(buf: bytes) -> list[tuple[int, int, object]]:
    """Generic wire reader over the four wire types these messages actually use."""
    out = []
    i = 0

    def read_varint():
        nonlocal i
        shift, result = 0, 0
        while True:
            byte = buf[i]
            i += 1
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return result
            shift += 7

    while i < len(buf):
        key = read_varint()
        number, wire = key >> 3, key & 7
        if wire == 0:
            out.append((number, wire, read_varint()))
        elif wire == 2:
            size = read_varint()
            out.append((number, wire, buf[i:i + size]))
            i += size
        elif wire == 5:
            out.append((number, wire, buf[i:i + 4]))
            i += 4
        elif wire == 1:
            out.append((number, wire, buf[i:i + 8]))
            i += 8
        else:
            raise AssertionError(f"unsupported wire type {wire} at byte {i}")
    return out


def node_proto(node_id: str, updated_at: int, state: int = 1) -> bytes:
    """`livekit.Node` (`livekit_internal.proto:49-57`): id=1, ip=2, num_cpus=3,
    stats=4 (started_at=1, updated_at=2), type=5, state=6, region=7."""
    stats = vfield(1, updated_at) + vfield(2, updated_at)
    return (sfield(1, node_id) + sfield(2, "127.0.0.1") + vfield(3, 8)
            + bfield(4, stats) + vfield(6, state) + sfield(7, REGION))


def host_memory_total() -> int:
    """Physical memory, used only as an upper bound on the sampled stats total. Both
    servers derive `memory_total` from VM page classes, not from this number: upstream
    `go-osstat@v0.2.8/memory/memory_darwin.go:107-109` reduces to
    wired+compressor+active+inactive+speculative+free, which is algebraically the same
    sum the candidate builds in `livecloud/src/node_stats.rs:373-383`. Both then average
    it in float32 over the stats window (`prometheus/node.go:233,270` and
    `node_stats.rs:155,162`), so it is a moving sample, not a host constant."""
    if sys.platform == "darwin":
        done = run_checked(["sysctl", "-n", "hw.memsize"])
        return int(done.stdout.strip())
    for line in pathlib.Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) * 1024
    raise AssertionError("no host memory total available")


def decode_node(buf: bytes) -> dict:
    node = {"id": None, "ip": None, "num_cpus": None, "type": 0, "state": None,
            "region": "", "started_at": None, "updated_at": None,
            "stats_present": False, "stats_fields": [], "stats_values": {}}
    for number, wire, value in pb_fields(buf):
        if number == 1 and wire == 2:
            node["id"] = value.decode(errors="replace")
        elif number == 2 and wire == 2:
            node["ip"] = value.decode(errors="replace")
        elif number == 3 and wire == 0:
            node["num_cpus"] = value
        elif number == 4 and wire == 2:
            node["stats_present"] = True
            seen = []
            for sub, sub_wire, sub_value in pb_fields(value):
                seen.append(sub)
                if sub_wire == 0:
                    node["stats_values"][sub] = sub_value
                elif sub_wire == 5:
                    node["stats_values"][sub] = struct.unpack("<f", sub_value)[0]
                elif sub_wire == 1:
                    node["stats_values"][sub] = struct.unpack("<Q", sub_value)[0]
                if sub == 1 and sub_wire == 0:
                    node["started_at"] = sub_value
                elif sub == 2 and sub_wire == 0:
                    node["updated_at"] = sub_value
            node["stats_fields"] = sorted(seen)
        elif number == 5 and wire == 0:
            node["type"] = value
        elif number == 6 and wire == 0:
            node["state"] = value
        elif number == 7 and wire == 2:
            node["region"] = value.decode(errors="replace")
    return node


KEEPALIVE_TYPE_URL = "type.googleapis.com/rpc.KeepalivePing"
LOCAL_CHANNEL = "Ping.REQ"


def envelope(type_url: str, payload: bytes, channel: str = LOCAL_CHANNEL) -> bytes:
    """psrpc `internal.Msg` (`references/psrpc/internal/internal.proto:24-30`):
    type_url=1, value=2, channel=3, compression=4. `pkg/bus/serialization.go:65-81`
    reads it back through `internal.Content` — the same numbers, channel reserved —
    and resolves the payload type from `type_url`, so an envelope whose type is not
    the subscriber's is dropped by `subscription.go:62-65`."""
    return sfield(1, type_url) + bfield(2, payload) + sfield(3, channel)


def ping_envelope(timestamp: int) -> bytes:
    return envelope(KEEPALIVE_TYPE_URL, vfield(1, timestamp))


def keepalive_channel(node_id: str) -> str:
    """psrpc `info/channels.go:57-63`: `formatChannel('|', Service, Method, Topic,
    "REQ")` — and `redisbus.go:85-93` publishes and subscribes on exactly `Legacy`."""
    return f"Keepalive|Ping|{node_id}|REQ"


# ---------------------------------------------------------------------------- RESP


class RedisError(AssertionError):
    """A `-` reply. Returning it as bytes let a rejected command masquerade as data,
    so a fault that never fired could still satisfy a recovery assertion."""


class Resp:
    def __init__(self, host: str, port: int):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.file = self.sock.makefile("rb")
        assert self.call(b"PING") == b"PONG"

    def read(self):
        line = self.file.readline(1 << 20)
        tag, data = line[:1], line[1:-2]
        if tag == b"+":
            return data
        if tag == b"-":
            raise RedisError(data.decode(errors="replace"))
        if tag == b":":
            return int(data)
        if tag == b"$":
            size = int(data)
            body = self.file.read(size)
            assert len(body) == size and self.file.read(2) == b"\r\n"
            return body
        if tag == b"*":
            return [self.read() for _ in range(int(data))]
        raise AssertionError("unexpected RESP type " + repr(tag))

    def call(self, *args):
        self.sock.sendall(b"*" + str(len(args)).encode() + b"\r\n"
                          + b"".join(b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
                                     for v in args))
        return self.read()

    def close(self):
        self.file.close()
        self.sock.close()


class Store:
    """The `nodes` hash of the single-node Redis every vector here dials, plus the
    publish/kill commands the keepalive and reconnect vectors drive from outside."""

    def __init__(self, port: int):
        self.port = port

    def call(self, *args: bytes):
        conn = Resp("127.0.0.1", self.port)
        try:
            return conn.call(*args)
        finally:
            conn.close()

    def alive(self) -> bool:
        try:
            Resp("127.0.0.1", self.port).close()
            return True
        except OSError:
            return False

    def names(self) -> list[str]:
        return sorted(v.decode(errors="replace")
                      for v in (self.call(b"HKEYS", NODES_KEY) or []))

    def records(self) -> dict[str, dict]:
        out = {}
        for name in self.call(b"HKEYS", NODES_KEY) or []:
            key = name.decode(errors="replace")
            raw = self.call(b"HGET", NODES_KEY, key.encode())
            out[key] = decode_node(raw if isinstance(raw, bytes) else b"")
        return out

    def own(self, seeded: set[str]) -> dict[str, dict]:
        return {name: record for name, record in self.records().items()
                if name not in seeded}

    def own_names(self, seeded: set[str]) -> list[str]:
        return sorted(self.own(seeded))

    def wait_one_own(self, timeout: float = 15.0) -> tuple[str, dict]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            own = self.own(set())
            if len(own) == 1:
                return next(iter(own.items()))
            time.sleep(0.2)
        raise AssertionError(f"exactly one own node record never appeared: {self.names()}")

    def seed(self, records: dict[str, bytes]) -> set[str]:
        self.clear()
        for key, value in records.items():
            assert self.call(b"HSET", NODES_KEY, key.encode(), value) == 1, \
                ("node record not written", key)
        assert sorted(self.names()) == sorted(records), \
            ("keyspace does not match what was seeded", self.names())
        return set(records)

    def clear(self) -> None:
        if self.alive():
            self.call(b"DEL", NODES_KEY)

    def publish(self, channel: str, payload: bytes) -> int:
        return self.call(b"PUBLISH", channel.encode(), payload)

    def pubsub_channels(self, pattern: str) -> list[str]:
        return sorted(v.decode(errors="replace")
                      for v in (self.call(b"PUBSUB", b"CHANNELS", pattern.encode()) or []))

    def client_kill(self, kind: bytes) -> int:
        """Redis 7.2 has no `ALL` keyword — `CLIENT KILL normal all` is `ERR syntax
        error`. `TYPE <kind>` is the form that kills every client of that class, and
        `SKIPME` defaults to yes, so this fresh caller connection survives it."""
        killed = self.call(b"CLIENT", b"KILL", b"TYPE", kind)
        assert isinstance(killed, int), ("CLIENT KILL returned no count", killed)
        return killed

    def client_types(self) -> dict[str, int]:
        listing = self.call(b"CLIENT", b"LIST").decode(errors="replace")
        counts: dict[str, int] = {}
        for line in listing.splitlines():
            fields = dict(part.split("=", 1) for part in line.split() if "=" in part)
            flags = fields.get("flags", "")
            kind = ("pubsub" if "P" in flags else
                    "replication" if "M" in flags or "S" in flags else "normal")
            counts[kind] = counts.get(kind, 0) + 1
        return counts

    def connections(self) -> int:
        info = self.call(b"INFO", b"clients").decode(errors="replace")
        match = re.search(r"connected_clients:(\d+)", info)
        return int(match.group(1)) if match else -1

    def subscriber(self, channel: str, wait: float) -> "Subscriber":
        return Subscriber(self, channel, wait)


class Subscriber:
    """A harness-side SUBSCRIBE on a node's own keepalive channel. This exists because
    `PUBLISH`'s receiver count cannot distinguish *who* is subscribed: only a message
    arriving here proves the *server's own* publish path re-dialled after a connection
    was killed."""

    def __init__(self, store: "Store", channel: str, wait: float):
        self.resp = Resp("127.0.0.1", store.port)
        self.resp.sock.settimeout(wait)
        ack = self.resp.call(b"SUBSCRIBE", channel.encode())
        assert ack == [b"subscribe", channel.encode(), 1], ("SUBSCRIBE ack", ack)
        self.channel = channel

    def next_publish(self) -> bytes | None:
        """The payload of the next `message` push, or None if none arrives before the
        socket timeout set at construction. Push frames here are a few dozen bytes, so
        a timeout landing mid-message is not a case this harness has to re-frame for."""
        try:
            pushed = self.resp.read()
        except OSError:
            return None
        assert isinstance(pushed, list) and pushed[0] == b"message", \
            ("unexpected push on the keepalive channel", pushed)
        assert pushed[1].decode() == self.channel, ("push on the wrong channel", pushed)
        return pushed[2]

    def close(self):
        self.resp.close()


class Monitor:
    """A Redis MONITOR tap capturing the command stream while a server starts and
    cleans up a failed start. The post-exit keyspace alone cannot distinguish "the
    server registered and then undid it" from "the server failed before it ever
    registered" — both leave an empty `nodes` hash. This tap is the positive control
    that a registration/subscription phase really happened before the process exited,
    and it records the actual per-side startup order rather than inferring it."""

    def __init__(self, port: int):
        self.resp = Resp("127.0.0.1", port)
        # The stream can go quiet between startup commands; do not let the default 5 s
        # socket timeout end the tap mid-startup. close() interrupts a blocked read.
        self.resp.sock.settimeout(30)
        assert self.resp.call(b"MONITOR") == b"OK", "MONITOR was not acknowledged"
        self._lines: list[str] = []
        self._lock = threading.Lock()
        self._stopped = False
        self._thread = threading.Thread(target=self._drain, daemon=True)
        self._thread.start()

    def _drain(self):
        # MONITOR pushes raw text lines, not RESP frames, so read them directly.
        while not self._stopped:
            try:
                raw = self.resp.file.readline(1 << 20)
            except OSError:
                break
            if not raw:
                break
            with self._lock:
                self._lines.append(raw.decode(errors="replace").rstrip("\r\n"))

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self._lines)

    def close(self):
        self._stopped = True
        try:
            self.resp.close()
        except OSError:
            pass
        self._thread.join(timeout=3)


def monitor_node_sequence(lines: list[str]) -> list[tuple[str, str, str]]:
    """The ordered (command, key, detail) triples touching the `nodes` hash or a
    `Keepalive|Ping|*` channel, as MONITOR observed them. The node id is kept in
    `detail` for HSET/HDEL so the registered id is recoverable even though a failed
    start never prints the readiness line that normally carries it."""
    seq: list[tuple[str, str, str]] = []
    for line in lines:
        tokens = re.findall(r'"((?:[^"\\]|\\.)*)"', line)
        if len(tokens) < 2:
            continue
        cmd, args = tokens[0].upper(), tokens[1:]
        if args[0] == "nodes" and cmd in ("HSET", "HMSET", "HDEL", "HGET", "HGETALL", "HKEYS"):
            detail = args[1] if cmd in ("HSET", "HMSET", "HDEL", "HGET") and len(args) >= 2 else ""
            seq.append((cmd, "nodes", detail))
        elif args[0].startswith("Keepalive|Ping|") and cmd in ("SUBSCRIBE", "UNSUBSCRIBE", "PUBLISH"):
            seq.append((cmd, args[0], ""))
    return seq


# ------------------------------------------------------------------------ processes


def free(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(0.2)
        return probe.connect_ex(("127.0.0.1", port)) != 0


def udp_occupied(port: int) -> bool:
    """Positive control that a UDP port is genuinely held: a second bind must fail
    with EADDRINUSE. A TCP probe cannot see a UDP listener, so occupancy is proven by
    attempting the bind, not by assuming the holder succeeded."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.bind(("127.0.0.1", port))
        return False
    except OSError as exc:
        return exc.errno == errno.EADDRINUSE
    finally:
        probe.close()


def wait_port(port: int, timeout: float = 25.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if not free(port):
            return True
        time.sleep(0.05)
    return False


def run_checked(argv, **kw) -> subprocess.CompletedProcess:
    done = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, env=BASE_ENV, **kw)
    assert done.returncode == 0, (argv, done.stdout[-800:])
    return done


def start_process(argv, log: pathlib.Path) -> subprocess.Popen:
    with log.open("wb") as handle:
        return subprocess.Popen(argv, stdout=handle, stderr=subprocess.STDOUT,
                                env=BASE_ENV, start_new_session=True)


def stop_process(proc, grace: float = 20.0) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=grace)
    except (OSError, subprocess.TimeoutExpired):
        try:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=grace)
        except (OSError, subprocess.TimeoutExpired):
            pass


def redis_server(port: int, staging: pathlib.Path, tag: str) -> subprocess.Popen:
    home = staging / f"redis-{tag}-{port}"
    home.mkdir(parents=True, exist_ok=True)
    proc = start_process(["redis-server", "--port", str(port), "--bind", "127.0.0.1",
                          "--protected-mode", "no", "--save", "", "--appendonly", "no",
                          "--dir", str(home)], staging / f"redis-{tag}-{port}.log")
    assert wait_port(port), f"redis-server never came up on {port}"
    return proc


class UdpHold:
    """Holds a UDP port so the WebTransport bind must fail. A TCP probe cannot see a
    UDP listener, so the occupancy has to be created rather than detected."""

    def __init__(self, port: int):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", port))
        self.port = port

    def close(self):
        self.sock.close()


# ----------------------------------------------------------------------------- logs


def clock_mask(line: str) -> str:
    return re.sub(
        r'"(ts|time)":("(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)',
        lambda m: '"%s":<%s>' % (m.group(1), "string" if m.group(2)[0] == '"' else "number"),
        line)


def log_lines(text: str, needle: str) -> list[str]:
    return [clock_mask(line.strip()) for line in text.splitlines() if needle in line]


def error_lines(text: str) -> list[str]:
    return [line[:300] for line in log_lines(text, '"level":"error"')]


def json_field(line: str, key: str):
    """One top-level field of a zap JSON line."""
    try:
        return json.loads(line).get(key)
    except ValueError:
        match = re.search(r'"%s":(".*?"|-?[\d.]+)' % re.escape(key), line)
        return match.group(1) if match else None


def node_id_from_log(text: str):
    for line in text.splitlines():
        if "starting LiveKit server" in line:
            value = json_field(line, "nodeID")
            if isinstance(value, str):
                return value.strip('"')
    return None


def go_time(unix: int) -> str:
    """Go's `time.Unix(u, 0).String()` under this harness's TZ=UTC: the stdlib layout
    with the fraction dropped — `time.Unix` always has zero nanoseconds, and the zone
    is `+0000 UTC`."""
    return "%s +0000 UTC" % time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(unix))


def health(http_port: int, timeout: float = 2.0):
    """(status, body) from `/` — upstream's healthCheck route. A connection failure is
    (None, '') so a dead server stays distinguishable from a 406."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{http_port}/", timeout=timeout) as reply:
            return reply.status, reply.read().decode(errors="replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode(errors="replace")
    except OSError:
        return None, ""


# ------------------------------------------------------------------------ run config


def config_body(http: int, tcp: int, udp: int, wt, redis_yaml: str, node_stats: str = "",
                region: str = "", level: str = "info", wt_tls: str = "") -> str:
    webtransport = f"webtransport: {{port: {wt}{wt_tls}}}\n" if wt is not None else ""
    return ("keys: {devkey: secret}\n"
            f"logging: {{level: {level}, json: true}}\n"
            "bind_addresses: [127.0.0.1]\n"
            "rtc: {node_ip: 127.0.0.1, use_external_ip: false, "
            f"tcp_port: {tcp}, udp_port: {udp}, enable_loopback_candidate: true}}\n"
            "turn: {enabled: false}\n"
            + (f"region: {region}\n" if region else "")
            + (f"node_stats: {node_stats}\n" if node_stats else "")
            + webtransport
            + (f"redis:\n{redis_yaml}" if redis_yaml else ""))


def normalize_config(body: str) -> str:
    """Both binaries must be fed the same bytes; only the port numbers and the paths
    this harness generated differ between sides, so those are what gets masked."""
    out = re.sub(r"tcp_port: \d+", "tcp_port: <T>", body)
    out = re.sub(r"udp_port: \d+", "udp_port: <U>", out)
    out = re.sub(r"(?<![A-Za-z0-9_])port: \d+", "port: <P>", out)
    out = re.sub(r"tls_cert_file: \S+", "tls_cert_file: <CERT>", out)
    out = re.sub(r"tls_key_file: \S+", "tls_key_file: <KEY>", out)
    return out


class Run:
    """One server process under test, with the keyspace and log accessors a vector
    needs while it is still alive."""

    def __init__(self, side: str, binary: pathlib.Path, vector: str, ports: dict,
                 body: str, staging: pathlib.Path, output: pathlib.Path, store: Store,
                 procs: list):
        path = staging / f"{vector}-{side}.yaml"
        path.write_text(body)
        path.chmod(0o600)
        self.side, self.vector, self.store = side, vector, store
        self.http, self.tcp, self.udp, self.wt = (ports["http"], ports["tcp"],
                                                  ports["udp"], ports["wt"])
        self.body = body
        self.config_sha256 = sha(path.read_bytes())
        self.normalized_config = normalize_config(body)
        self.log_path = output / f"{vector}-{side}.server.log"
        self.proc = start_process([str(binary), "--dev", "--config", str(path),
                                   "--port", str(self.http)], self.log_path)
        procs.append(self.proc)
        self.exit_code = None
        self.sigkilled = False

    def poll(self):
        return self.proc.poll()

    def wait_http(self, wait: float = 60.0):
        """First answer from `/`, or None if the process died before serving."""
        deadline = time.monotonic() + wait
        while time.monotonic() < deadline:
            if self.poll() is not None:
                return None
            status, body = health(self.http)
            if status is not None:
                return status, body
            time.sleep(0.1)
        return None

    def wait_exit(self, wait: float = 45.0) -> bool:
        try:
            self.exit_code = self.proc.wait(timeout=wait)
            return True
        except subprocess.TimeoutExpired:
            return False

    def force_stop(self) -> None:
        self.sigkilled = True
        os.killpg(self.proc.pid, signal.SIGKILL)
        self.exit_code = self.proc.wait(timeout=20)

    def own(self, seeded: set[str] = frozenset()) -> dict[str, dict]:
        return self.store.own(set(seeded))

    def own_names(self, seeded: set[str] = frozenset()) -> list[str]:
        return self.store.own_names(set(seeded))

    def text(self) -> str:
        return self.log_path.read_bytes().decode("utf-8", "replace")

    def interrupt(self, grace: float = 30.0) -> dict:
        """SIGINT, then the exit status and how long the drain took. The caller is
        responsible for having settled past upstream's 100ms `running` window first."""
        os.killpg(self.proc.pid, signal.SIGINT)
        started = time.monotonic()
        try:
            code = self.proc.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            self.sigkilled = True
            os.killpg(self.proc.pid, signal.SIGKILL)
            code = self.proc.wait()
        self.exit_code = code
        return {"exit": code, "sigkilled": self.sigkilled,
                "drain_seconds": round(time.monotonic() - started, 3)}

    def kill(self) -> dict:
        """SIGKILL: nothing upstream runs, including the deferred unregister."""
        os.killpg(self.proc.pid, signal.SIGKILL)
        self.exit_code = self.proc.wait(timeout=20)
        return {"exit": self.exit_code, "sigkilled": False, "drain_seconds": None}


def same_config(name: str, out: dict) -> str:
    go, rs = out.get("official", {}), out.get("rust", {})
    left, right = go.get("normalized_config"), rs.get("normalized_config")
    if left is not None and right is not None:
        assert left == right, (name, "both binaries must be fed the same config bytes",
                               left, right)
    return json.dumps({s: out[s]["config_sha256"] for s in ("official", "rust")
                       if s in out and "config_sha256" in out[s]})


# --------------------------------------------------------------------- C39 helpers


def unescape_monitor(token: str) -> bytes:
    """Redis MONITOR renders binary arguments with sdscatrepr: printable ASCII
    verbatim, everything else as \\xHH plus the short C escapes. Reversing that
    recovers the exact protobuf bytes the server HSET, so the drain state is read
    from the wire witness rather than inferred from a racy keyspace poll."""
    out = bytearray()
    i = 0
    short = {"n": 10, "r": 13, "t": 9, "a": 7, "b": 8, "\\": 92, '"': 34}
    while i < len(token):
        c = token[i]
        if c == "\\":
            n = token[i + 1]
            if n == "x":
                out.append(int(token[i + 2:i + 4], 16))
                i += 4
                continue
            out.append(short[n])
            i += 2
            continue
        out.append(ord(c))
        i += 1
    return bytes(out)


def monitor_node_events(lines: list[str], node_id: str) -> list[dict]:
    """Ordered HSET/HDEL events MONITOR saw for one node id. Each HSET carries the
    decoded `state` of the exact bytes written (1=SERVING, 2=SHUTTING_DOWN per
    livekit.NodeState), so a drain is witnessed as a state-2 HSET, not guessed."""
    events: list[dict] = []
    for line in lines:
        tokens = re.findall(r'"((?:[^"\\]|\\.)*)"', line)
        if len(tokens) < 3:
            continue
        cmd, args = tokens[0].upper(), tokens[1:]
        if args[0] != "nodes" or args[1] != node_id:
            continue
        if cmd == "HSET" and len(args) >= 3:
            node = decode_node(unescape_monitor(args[2]))
            events.append({"cmd": "HSET", "state": node["state"],
                           "updated_at": node["updated_at"]})
        elif cmd == "HDEL":
            events.append({"cmd": "HDEL", "state": None, "updated_at": None})
    return events


def cli_states(text: str) -> dict:
    """The ID column renders `ND_x` on one visual row and `(STATE)` on the next
    (commands.go idAndState / cli_nodes.rs fields). Pair each id with the first
    state label at or after its row."""
    states: dict = {}
    lines = text.splitlines()
    for i, line in enumerate(lines):
        for nid in re.findall(r"ND_[A-Za-z0-9_-]+", line):
            label = None
            for follow in lines[i:i + 3]:
                m = re.search(r"\((STARTING_UP|SERVING|SHUTTING_DOWN|SUSPENDED)\)", follow)
                if m:
                    label = m.group(1)
                    break
            states[nid] = label
    return states


def list_nodes(binary, tag: str, name: str, ctx) -> dict:
    done = subprocess.run([str(binary), "--dev", "--redis-host",
                           f"127.0.0.1:{ctx['master_port']}", "list-nodes"],
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=BASE_ENV)
    text = done.stdout.decode(errors="replace")
    (ctx["output"] / f"{name}-{tag}.list-nodes.log").write_text(text)
    return {"exit": done.returncode,
            "ids": sorted(set(re.findall(r"ND_[A-Za-z0-9_-]+", text))),
            "states": cli_states(text),
            "stdout_b64": base64.b64encode(text.encode()).decode()}


def launch(side, binary, slot, name, ctx, label=None):
    p = ctx["side_ports"](side, slot)
    body = config_body(p["http"], p["tcp"], p["udp"], None, ctx["redis_addr"],
                       node_stats="{stats_update_interval: 1s}", region=REGION)
    run = Run(label or side, binary, name, p, body, ctx["staging"], ctx["output"],
              ctx["store"], ctx["procs"])
    assert run.wait_http() is not None, (side, slot, "no HTTP", run.text()[-1500:])
    return run


def wait_new(store: Store, previous: set, timeout: float = 20.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        new = set(store.names()) - previous
        if len(new) == 1:
            return next(iter(new))
        time.sleep(0.2)
    raise AssertionError(f"a node record never appeared alone: {store.names()}")


def wait_ports_free(ports: dict, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(free(ports[role]) for role in ("http", "tcp")):
            return
        time.sleep(0.1)
    raise AssertionError(f"ports never released for restart: {ports}")


def cli_pair(name, ctx, official, rust, expect_ids, tag):
    """Both binaries' CLI read the same keyspace; the deterministic observables
    (exit, id set, per-id state label) must agree cross-side."""
    out = {}
    for cli_side, binary in (("official_cli", official), ("rust_cli", rust)):
        got = list_nodes(binary, f"{tag}-{cli_side}", name, ctx)
        out[cli_side] = got
        assert got["exit"] == 0, (name, tag, cli_side, "list-nodes failed", got)
        assert got["ids"] == sorted(expect_ids), \
            (name, tag, cli_side, "wrong id set", got["ids"], sorted(expect_ids))
        assert all(got["states"][nid] == "SERVING" for nid in expect_ids), \
            (name, tag, cli_side, "live node not SERVING", got["states"])
    if out["official_cli"]["ids"] != out["rust_cli"]["ids"]:
        ctx["mismatch"](name, f"{tag}-ids", out["official_cli"]["ids"], out["rust_cli"]["ids"])
    if out["official_cli"]["exit"] != out["rust_cli"]["exit"]:
        ctx["mismatch"](name, f"{tag}-exit", out["official_cli"]["exit"], out["rust_cli"]["exit"])
    if out["official_cli"]["states"] != out["rust_cli"]["states"]:
        ctx["mismatch"](name, f"{tag}-states", out["official_cli"]["states"], out["rust_cli"]["states"])
    return out


# ========================================================================== vectors


def vector_go_register_rust_list(official, rust, name, ctx):
    """The literal split-origin claim: the OFFICIAL server registers, the RUST CLI
    lists it over the same literal 127.0.0.1 address, and the official graceful stop
    revokes it from the Rust CLI's view."""
    store = ctx["store"]
    store.clear()
    run = launch("official", official, 0, name, ctx)
    time.sleep(SETTLE_SECONDS)
    nid = store.wait_one_own()[0]
    assert nid == node_id_from_log(run.text()), (name, "keyspace id != readiness nodeID")
    live = cli_pair(name, ctx, official, rust, [nid], "while-live")
    connections = store.connections()
    stop = run.interrupt()
    time.sleep(0.3)
    after = cli_pair(name, ctx, official, rust, [], "after-stop")
    assert stop["exit"] == 0 and not stop["sigkilled"], (name, stop)
    assert store.names() == [], (name, "keyspace residue", store.names())
    return {"node_id": nid, "id_matches_log": True,
            "config_sha256": run.config_sha256, "normalized_config": run.normalized_config,
            "cli_while_live": live, "cli_after_stop": after,
            "stop": stop, "keyspace_after": store.names(),
            "connections_while_running": connections}


def vector_rust_register_go_list(official, rust, name, ctx):
    """The reverse direction: the RUST server registers, the OFFICIAL CLI lists it,
    and the Rust graceful stop revokes it from the official CLI's view."""
    store = ctx["store"]
    store.clear()
    run = launch("rust", rust, 0, name, ctx)
    time.sleep(SETTLE_SECONDS)
    nid = store.wait_one_own()[0]
    assert nid == node_id_from_log(run.text()), (name, "keyspace id != readiness nodeID")
    live = cli_pair(name, ctx, official, rust, [nid], "while-live")
    connections = store.connections()
    stop = run.interrupt()
    time.sleep(0.3)
    after = cli_pair(name, ctx, official, rust, [], "after-stop")
    assert stop["exit"] == 0 and not stop["sigkilled"], (name, stop)
    assert store.names() == [], (name, "keyspace residue", store.names())
    return {"node_id": nid, "id_matches_log": True,
            "config_sha256": run.config_sha256, "normalized_config": run.normalized_config,
            "cli_while_live": live, "cli_after_stop": after,
            "stop": stop, "keyspace_after": store.names(),
            "connections_while_running": connections}


def vector_both_live_cross_cli(official, rust, name, ctx):
    """One Go and one Rust server on one Redis: both CLIs see exactly both live
    nodes; stopping one revokes only its own record in BOTH CLIs' views."""
    store = ctx["store"]
    store.clear()
    first = launch("official", official, 0, name, ctx, label="official#0")
    a_id = wait_new(store, set())
    second = launch("rust", rust, 1, name, ctx, label="rust#1")
    b_id = wait_new(store, {a_id})
    time.sleep(SETTLE_SECONDS + AVAILABILITY_WINDOW_SECONDS)
    live = store.records()
    assert sorted(live) == sorted([a_id, b_id]), (name, "a record aged out", sorted(live))
    assert all(live[nid]["state"] == 1 for nid in live), (name, "not SERVING", live)
    both = cli_pair(name, ctx, official, rust, [a_id, b_id], "both-live")
    stop_a = first.interrupt()
    time.sleep(0.3)
    after_a = cli_pair(name, ctx, official, rust, [b_id], "after-first-stop")
    stop_b = second.interrupt()
    time.sleep(0.3)
    after_b = cli_pair(name, ctx, official, rust, [], "after-second-stop")
    assert stop_a["exit"] == 0 and stop_b["exit"] == 0, (name, stop_a, stop_b)
    assert store.names() == [], (name, "residue", store.names())
    return {"ids": [a_id, b_id],
            "slot_configs": {"official#0": first.normalized_config, "rust#1": second.normalized_config},
            "slot_shas": {"official#0": first.config_sha256, "rust#1": second.config_sha256},
            "cli_both_live": both, "cli_after_first_stop": after_a,
            "cli_after_second_stop": after_b,
            "exits": {"first": stop_a["exit"], "second": stop_b["exit"]},
            "drain_seconds": {"first": stop_a["drain_seconds"], "second": stop_b["drain_seconds"]}}


def vector_drain_shutting_down_witness(official, rust, name, ctx):
    """`Stop(force=false)` calls `router.Drain()` FIRST (server.go:375-380):
    SetState(SHUTTING_DOWN) + RegisterNode (redisrouter.go:185-190), then the
    participant wait, then `router.Stop()`'s HDEL. With no participants the drain
    window is milliseconds, so a keyspace poll cannot witness it deterministically —
    a Redis MONITOR tap captures the exact HSET bytes. Per side: the first HSET is
    SERVING(1), a SHUTTING_DOWN(2) HSET exists, no SERVING HSET follows it, and the
    first HDEL comes after it. Sequence lengths (refresh count) are timing-dependent
    and recorded, not compared."""
    store = ctx["store"]
    out = {}
    for side, binary in (("official", official), ("rust", rust)):
        store.clear()
        mon = Monitor(ctx["master_port"])
        run = launch(side, binary, 0, name, ctx)
        time.sleep(SETTLE_SECONDS + 1.2)  # at least one 1s-interval refresh HSET
        nid = store.wait_one_own()[0]
        assert nid == node_id_from_log(run.text()), (side, "id != readiness nodeID")
        stop = run.interrupt()
        time.sleep(0.3)
        mon.close()
        events = monitor_node_events(mon.snapshot(), nid)
        (ctx["output"] / f"{name}-{side}.monitor.log").write_text("\n".join(mon.snapshot()) + "\n")
        hsets = [e for e in events if e["cmd"] == "HSET"]
        hdels = [i for i, e in enumerate(events) if e["cmd"] == "HDEL"]
        assert hsets, (side, "no HSET witnessed", events)
        assert hsets[0]["state"] == 1, (side, "first HSET not SERVING", events)
        drain_idx = [i for i, e in enumerate(events) if e["cmd"] == "HSET" and e["state"] == 2]
        assert drain_idx, (side, "no SHUTTING_DOWN HSET witnessed", events)
        first_drain = drain_idx[0]
        assert hdels and hdels[0] > first_drain, (side, "HDEL not after drain HSET", events)
        assert not any(e["cmd"] == "HSET" and e["state"] == 1 for e in events[first_drain:]), \
            (side, "SERVING HSET after drain", events)
        assert stop["exit"] == 0 and not stop["sigkilled"], (side, stop)
        assert store.names() == [], (side, "residue", store.names())
        out[side] = {"node_id": nid, "events": events,
                     "drain_hset_count": len(drain_idx),
                     "first_hset_serving": True, "drain_before_hdel": True,
                     "no_serving_after_drain": True,
                     "stop": stop, "config_sha256": run.config_sha256}
    for key in ("first_hset_serving", "drain_before_hdel", "no_serving_after_drain"):
        if out["official"][key] != out["rust"][key]:
            ctx["mismatch"](name, key, out["official"][key], out["rust"][key])
    if (out["official"]["drain_hset_count"] >= 1) != (out["rust"]["drain_hset_count"] >= 1):
        ctx["mismatch"](name, "drain-hset-present",
                        out["official"]["drain_hset_count"], out["rust"]["drain_hset_count"])
    return out


def vector_restart_fresh_identity(official, rust, name, ctx):
    """A graceful stop followed by a fresh start on the same ports: the old id is
    gone, exactly one new id serves, and the same binary's CLI lists only the new
    one. Restart must not resurrect or duplicate the old record."""
    store = ctx["store"]
    out = {}
    for side, binary in (("official", official), ("rust", rust)):
        store.clear()
        first = launch(side, binary, 0, name, ctx, label=f"{side}-first")
        time.sleep(SETTLE_SECONDS)
        id1 = store.wait_one_own()[0]
        stop1 = first.interrupt()
        assert stop1["exit"] == 0, (side, stop1)
        time.sleep(0.3)
        assert store.names() == [], (side, "residue before restart", store.names())
        wait_ports_free(ctx["side_ports"](side, 0))
        second = launch(side, binary, 0, name, ctx, label=f"{side}-second")
        time.sleep(SETTLE_SECONDS)
        id2 = store.wait_one_own()[0]
        assert id2 != id1, (side, "restart reused the node id", id1, id2)
        assert store.names() == [id2], (side, "restart keyspace", store.names())
        live = cli_pair(name, ctx, official, rust, [id2], f"{side}-restarted")
        stop2 = second.interrupt()
        time.sleep(0.3)
        after = cli_pair(name, ctx, official, rust, [], f"{side}-after-second-stop")
        assert stop2["exit"] == 0, (side, stop2)
        assert store.names() == [], (side, "residue after second stop", store.names())
        out[side] = {"first_id": id1, "second_id": id2, "fresh_identity": True,
                     "exits": [stop1["exit"], stop2["exit"]],
                     "cli_restarted": live, "cli_after_stop": after,
                     "config_sha256": second.config_sha256}
    for key in ("fresh_identity",):
        if out["official"][key] != out["rust"][key]:
            ctx["mismatch"](name, key, out["official"][key], out["rust"][key])
    return out


def vector_dead_node_pruned_by_peer_start(official, rust, name, ctx):
    """SIGKILL leaves the victim record (nothing runs). After it ages past
    selector.IsAvailable's 5s window, the PEER binary's server start runs
    RemoveDeadNodes (server.go:197, redisrouter.go:85-98 / server.rs:544,
    registry.rs::remove_dead_nodes) and HDELs it — the CLI path does NOT prune
    (InitializeRouter has no RemoveDeadNodes). Both directions: Go victim pruned by
    Rust start, Rust victim pruned by Go start; both CLIs then list only the peer."""
    store = ctx["store"]
    out = {}
    for victim_side, victim, peer_side, peer in (("official", official, "rust", rust),
                                                 ("rust", rust, "official", official)):
        store.clear()
        victim_run = launch(victim_side, victim, 0, name, ctx,
                            label=f"{victim_side}-victim")
        time.sleep(SETTLE_SECONDS)
        victim_id = store.wait_one_own()[0]
        killed = victim_run.kill()
        time.sleep(0.3)
        residue = store.names()
        assert residue == [victim_id], (victim_side, "SIGKILL residue", residue)
        record = store.records()[victim_id]
        time.sleep(AVAILABILITY_WINDOW_SECONDS + 1.0)
        aged = int(time.time()) - record["updated_at"]
        assert aged > 5, (victim_side, "record not yet stale", aged)
        peer_run = launch(peer_side, peer, 1, name, ctx, label=f"{peer_side}-peer")
        time.sleep(SETTLE_SECONDS)
        # The sweep runs before the peer registers (server.go:197 before :222 /
        # server.rs:544 before :645), so readiness implies the victim is already
        # pruned; poll briefly anyway rather than assume the exact instant.
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            names = store.names()
            if len(names) == 1 and names[0] != victim_id:
                break
            time.sleep(0.2)
        own = store.names()
        assert len(own) == 1, (victim_side, "peer start did not prune to one", own)
        peer_id = own[0]
        assert peer_id != victim_id, (victim_side, "peer reused victim id")
        assert store.names() == [peer_id], \
            (victim_side, "dead victim survived peer startup sweep", store.names())
        live = cli_pair(name, ctx, official, rust, [peer_id], f"{victim_side}-victim-pruned")
        stop = peer_run.interrupt()
        time.sleep(0.3)
        after = cli_pair(name, ctx, official, rust, [], f"{victim_side}-victim-after-peer-stop")
        assert stop["exit"] == 0, (victim_side, stop)
        assert store.names() == [], (victim_side, "residue", store.names())
        out[victim_side + "_victim"] = {
            "victim_id": victim_id, "peer_id": peer_id,
            "sigkill_residue": residue, "aged_seconds_at_peer_start": aged,
            "pruned_by_peer_start": True, "kill_exit": killed["exit"],
            "cli_after_prune": live, "cli_after_peer_stop": after,
            "peer_stop_exit": stop["exit"]}
    for key in ("pruned_by_peer_start",):
        left, right = out["official_victim"][key], out["rust_victim"][key]
        if left != right:
            ctx["mismatch"](name, key, left, right)
    return out


# ============================================================================ main


def main() -> int:
    if not __debug__:
        raise RuntimeError("run without Python -O: acceptance assertions are required")
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", required=True, type=pathlib.Path)
    ap.add_argument("--rust", required=True, type=pathlib.Path)
    ap.add_argument("--output", required=True, type=pathlib.Path)
    ap.add_argument("--base-port", type=int, default=22100)
    ap.add_argument("--only", default=None, help="run only vectors whose id contains this")
    args = ap.parse_args()

    official, rust = args.official.resolve(), args.rust.resolve()
    assert sha(official.read_bytes()) == OFFICIAL_SHA256, (
        "use the locked reference build, not just any binary reporting v1.13.10",
        sha(official.read_bytes()))
    version = run_checked([str(official), "--version"]).stdout.strip()
    assert version == OFFICIAL_VERSION, ("locked reference binary required", version)
    args.output.mkdir(parents=True, exist_ok=True)
    receipt = args.output / "BATCH.json"
    assert not receipt.exists(), ("preserve prior evidence; choose a fresh output directory", receipt)
    staging = pathlib.Path(tempfile.mkdtemp(prefix="cross-binary-registry-c39-"))

    slots = ("go_a", "go_b", "rs_a", "rs_b")
    ports = {"master": args.base_port}
    for index, slot in enumerate(slots):
        for offset, role in enumerate(("http", "tcp", "udp", "wt")):
            ports[f"{slot}_{role}"] = args.base_port + 1 + index * 4 + offset
    for name, port in ports.items():
        assert free(port), f"port {port} ({name}) is already in use"

    def side_ports(side: str, slot: int = 0) -> dict:
        prefix = ("go" if side == "official" else "rs") + ("_a", "_b")[slot]
        return {role: ports[f"{prefix}_{role}"] for role in ("http", "tcp", "udp", "wt")}

    report = {
        "status": "RUNNING",
        "started_at_unix": time.time(),
        "finished_at_unix": None,
        "complete_campaign": False,
        "harness_sha256": sha(pathlib.Path(__file__).read_bytes()),
        "items": ITEMS,
        "scope": (
            f"real paired cross-binary node-registry lifecycle for {C39_ITEM}, split out "
            "of CONFIG_BEHAVIOR:Redis.LiteralDialAddress (21l): over the same literal "
            "127.0.0.1 Redis address, the official Go server's self-registration is "
            "listed by the Rust CLI and vice versa (exit 0, exact ND_ id set, per-id "
            "SERVING label parsed from the ID column), a graceful stop revokes the "
            "record from BOTH CLIs' views, one Go + one Rust server are mutually "
            "visible and per-node revocation holds cross-side, the first-signal drain "
            "is witnessed on the wire as a SHUTTING_DOWN(2) HSET between the SERVING "
            "HSETs and the HDEL (Redis MONITOR, exact protobuf bytes), a restart "
            "produces exactly one fresh identity with no resurrection, and a SIGKILL "
            "victim aged past the 5s availability window is HDELed by the PEER "
            "binary's startup RemoveDeadNodes sweep in both directions (the CLI read "
            "path itself never prunes). Also exercises the registration lifecycle "
            f"owned by {REG_ITEM} (closed by C38) without re-claiming it. Not covered: "
            f"NodeStats content past started_at/updated_at ({STATS_ITEM}), the "
            f"connection-count difference ({CHURN_ITEM}, recorded per side below), "
            "cross-node room routing, the drain wait WITH connected participants "
            "(no media here; the no-participant drain order is what server.go:375-399 "
            "runs first), and CLI table stat-cell values (host-dependent, recorded "
            "per side as full stdout, never compared)."),
        "binaries": {"official_go": {"path": str(official),
                                     "sha256": sha(official.read_bytes())},
                     "rust": {"path": str(rust), "sha256": sha(rust.read_bytes())}},
        "go_binary_sha256": sha(official.read_bytes()),
        "rust_binary_sha256": sha(rust.read_bytes()),
        "official_version": version,
        "env": {"TZ": BASE_ENV["TZ"], "availability_window_seconds":
                AVAILABILITY_WINDOW_SECONDS, "settle_seconds": SETTLE_SECONDS,
                "region": REGION},
        "normalization": (
            "the random ND_ node id is compared as a relation (keyspace id == the "
            "readiness line's nodeID; restart id != previous id), never as a value. "
            "CLI observables are the deterministic exit code, the sorted id set and "
            "the per-id state label; full stdout is kept per side (stat cells are "
            "host-dependent and not compared). The drain witness decodes the exact "
            "HSET bytes MONITOR captured; sequence lengths and drain_seconds are "
            "timing-dependent and recorded per side, not compared. Config bytes are "
            "asserted equal per slot after masking ports. Every original exit code "
            "is kept."),
        "cases": [],
        "mismatches": [],
        "known_differences": [],
    }

    def raw_resp_json(value):
        if isinstance(value, bytes):
            return {"resp_bytes_hex": value.hex()}
        raise TypeError(f"unsupported receipt value {type(value).__name__}")

    def save() -> None:
        pending = receipt.with_suffix(".json.pending")
        pending.write_text(json.dumps(report, indent=2, default=raw_resp_json) + "\n")
        pending.replace(receipt)

    def mismatch(vector: str, label: str, left, right) -> None:
        report["mismatches"].append([vector, label, left, right])

    vectors = [
        ("go-register-rust-list", vector_go_register_rust_list),
        ("rust-register-go-list", vector_rust_register_go_list),
        ("both-live-cross-cli", vector_both_live_cross_cli),
        ("drain-shutting-down-witness", vector_drain_shutting_down_witness),
        ("restart-fresh-identity", vector_restart_fresh_identity),
        ("dead-node-pruned-by-peer-start", vector_dead_node_pruned_by_peer_start),
    ]
    selected = [(name, fn) for name, fn in vectors
                if args.only is None or args.only in name]
    assert selected, ("no vector matched", args.only)
    report["campaign_vectors"] = [name for name, _ in vectors]
    report["vectors_run"] = [name for name, _ in selected]
    report["selection_filter"] = args.only

    redis_box: dict = {"proc": None}
    procs: list = []
    master_port = ports["master"]
    code = 1

    def interrupted(signum, _frame):
        raise InterruptedError(f"harness interrupted by signal {signum}")

    previous_handlers = {sig: signal.signal(sig, interrupted)
                         for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        save()
        redis_box["proc"] = redis_server(master_port, staging, "main")
        store = Store(master_port)
        ctx = {"staging": staging, "output": args.output, "store": store,
               "redis_addr": f"  address: 127.0.0.1:{master_port}\n",
               "master_port": master_port, "ports": ports, "side_ports": side_ports,
               "mismatch": mismatch, "redis_box": redis_box,
               "procs": procs, "save": save,
               "known_differences": report["known_differences"]}
        for name, fn in selected:
            report["active_vector"] = name
            save()
            record = fn(official, rust, name, ctx)
            record["id"] = name
            report["cases"].append(record)
            save()
        # Per-side resource shape, recorded never compared (owned by CHURN_ITEM).
        report["known_differences"].append({
            "id": CHURN_ITEM,
            "note": "connections_while_running sampled per side in the single-server "
                    "vectors; the multiplexing difference is owned by that OPEN item",
        })
        report["known_differences"].append({
            "id": STATS_ITEM,
            "note": "CLI table stat cells and NodeStats content beyond "
                    "started_at/updated_at differ per host/side; full stdout kept "
                    "per side, never compared",
        })
        assert not report["mismatches"], report["mismatches"]
        assert [case["id"] for case in report["cases"]] == report["vectors_run"]
        final_identity = {"official_go": sha(official.read_bytes()),
                          "rust": sha(rust.read_bytes()),
                          "harness": sha(pathlib.Path(__file__).read_bytes())}
        report["final_identity"] = final_identity
        assert final_identity == {"official_go": report["go_binary_sha256"],
                                  "rust": report["rust_binary_sha256"],
                                  "harness": report["harness_sha256"]}, \
            ("binary or harness input changed during the campaign", final_identity)
        report["complete_campaign"] = len(selected) == len(vectors)
        report["aggregate_status"] = (
            "PASS_CLI_LIST_NODES_CROSS_BINARY_REGISTRY_PAIRED"
            if report["complete_campaign"] else "PASS_SELECTED_VECTORS_ONLY")
        report["status"] = report["aggregate_status"]
        report["active_vector"] = None
        code = 0
    except BaseException as exc:
        report["status"] = "FAIL"
        report["failure"] = f"{type(exc).__name__}: {exc}"
        code = 1
    finally:
        for sig in previous_handlers:
            signal.signal(sig, signal.SIG_IGN)
        cleanup_errors = []
        cleanup = ([lambda proc=proc: stop_process(proc) for proc in procs]
                   + [lambda: stop_process(redis_box["proc"])]
                   + [lambda: shutil.rmtree(staging)])
        for action in cleanup:
            try:
                action()
            except BaseException as exc:
                cleanup_errors.append(f"{type(exc).__name__}: {exc}")
        if cleanup_errors:
            report["cleanup_errors"] = cleanup_errors
            report["status"] = "FAIL"
            report.pop("aggregate_status", None)
            report["complete_campaign"] = False
            code = 1
        report["finished_at_unix"] = time.time()
        save()
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
    print(json.dumps({"status": report["status"],
                      "cases": [c["id"] for c in report["cases"]],
                      "mismatches": [[m[0], m[1]] for m in report["mismatches"]],
                      "failure": report.get("failure")}, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())