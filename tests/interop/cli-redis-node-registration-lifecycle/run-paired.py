#!/usr/bin/env python3
"""SPEC-0074 C12/C14/C15 / SPEC-0076 workline B: the node registration lifecycle, paired.

This is the producer side of the `nodes` hash — the half `tests/interop/
cli-redis-startup-dead-node-prune` deliberately left open as
`RUNTIME:Redis.NodeRegistrationLifecycle`. Upstream `LivekitServer.Start()`
(`references/livekit-stable/pkg/service/server.go:216-277`) is, in order:

  RegisterNode()            HSET nodes <own id> = proto(livekit.Node)   :222
  defer UnregisterNode()    HDEL, with `could not unregister node`      :225-229
  router.Start()            statsWorker + keepaliveWorker; the SubscribePing
                            error is returned here            redisrouter.go:168-183
  ioService.Start()                                               :235
  net.Listen  http/prom/debug                                     :244-270
  webtransportServer.Listen(...)   ← the one post-HSET bind that can fail :272-276
  `starting LiveKit server` readiness line                        :309

so every failure after the HSET is covered by the deferred unregister, and the
WebTransport TLS material is built *earlier still* — inside `InitializeServer`
(`server.go:184-195`, `WebTransportTLS(...)` at :186) before `router.RemoveDeadNodes()`
at :197. That ordering is what the last two vectors measure: a bad cert pair
registers nothing and prunes nothing, while an occupied UDP port registers and must
undo.

`RedisRouter.statsWorker` (`redisrouter.go:202-227`) publishes one
`rpc.KeepalivePing{Timestamp}` per `node_stats.stats_update_interval` through psrpc
and discards the error, then compares `SecondsSinceNodeStatsUpdate()` against
`stats_max_delay` and prints the `status update delayed, possible deadlock` line once
(it latches on `goroutineDumped`). `RedisRouter.keepaliveWorker`
(`redisrouter.go:229-252`) refreshes and re-HSETs the own record for every ping that
is not older than the interval — `keep alive too old, skipping` for the stale ones,
while `bus/subscription.go:58-65` drops anything that does not deserialize or is not
this message type without a word (psrpc's logger is `logr.Discard()`,
`references/psrpc/internal/logger/logger.go:21`). That worker is the *only* thing
keeping a live node inside the `selector.IsAvailable` 5 s window
(`pkg/routing/selector/utils.go:30-41`) and inside the literal 4 s the `/` health
check compares (`server.go:450-464`), which is why the same heartbeat is asserted
here from the keyspace, from the HTTP status and from the log.

Coverage, per vector:
  1 register/refresh/revoke on one live record — the HSET, its identity fields, an
    advancing `updated_at` across more than one availability window, a frozen
    `started_at`, the HDEL on graceful stop, and that a ping arriving after the stop
    revives nothing (and finds no subscriber).
  2 SIGKILL leaves the record behind — nothing runs.
  3 two servers on one Redis (go+go, rs+rs, go+rs): both records live, neither ages
    out, `list-nodes` from both binaries lists exactly both ids, and stopping one
    revokes only its own record.
  4 the psrpc keepalive channel from the outside: with a 30 s interval only an
    injected fresh ping can move the record; a stale ping logs `keep alive too old,
    skipping` and does not; garbage and a wrong type_url are dropped silently.
  5 `/` health: 200 → 406 carrying Go's `time.Time.String()` of the record's own
    stamp once it passes 4 s, back to 200 after a fresh ping.
  6 the `stats_max_delay` diagnostic fires and latches: exactly one line.
  7 a real `redis-server` restart, and 7b a real `CLIENT KILL` — command, publish and
    subscribe paths all recover the registration.
  8 an occupied WebTransport UDP port: non-zero exit, no record left, no continuing
    refresh after the failure.
  9 an unparsable WebTransport cert pair: fails during construction, so it registers
    nothing *and* prunes nothing (the seeded stale record survives).
  10 single-node routing writes no keyspace at all while `/` stays ready.

`INFO clients` is sampled too, which is what
`RUNTIME:Redis.ServerStartConnectionChurn` records as an open difference.
`RUNTIME:Server.InterruptBeforeRunningRace` keeps the 0.5 s settle before SIGINT:
upstream sets `running` 100 ms *after* the listeners answer (`server.go:343-347`) and
`Stop()` bails out when the flag was never set (`server.go:391`), so an interrupt
inside that window hangs the reference instead of measuring the unregister path.

Not covered here: the *content* of `NodeStats` beyond `started_at`/`updated_at`
(`RUNTIME:Redis.NodeStatsSampling`; the field sets are recorded per side on vector 1
so the gap stays visible), cross-node room signal routing, and the WebTransport
session plane itself (C15).
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
REGION = "harness-21q"
STALE_SECONDS = 3600
NODES_KEY = b"nodes"
ITEMS = sorted({REG_ITEM, STATS_ITEM, CHURN_ITEM, CHANNEL_ITEM, STARTUP_RACE_ITEM,
                HEALTH_ITEM, INTERVAL_ITEM, MAX_DELAY_ITEM, REGION_ITEM, WT_PORT_ITEM,
                WT_CERT_ITEM, POOL_ITEM})


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


# ========================================================================== vectors


def vector_register(official, rust, name, ctx):
    """The whole lifecycle on one live record: HSET, identity, heartbeat across more
    than one availability window, HDEL on graceful stop, then no revival."""
    out = {}
    for side, binary in (("official", official), ("rust", rust)):
        p = ctx["side_ports"](side)
        body = config_body(p["http"], p["tcp"], p["udp"], None, ctx["redis_addr"],
                           node_stats="{stats_update_interval: 1s}", region=REGION)
        ctx["store"].clear()
        run = Run(side, binary, name, p, body, ctx["staging"], ctx["output"],
                  ctx["store"], ctx["procs"])
        reached = run.wait_http()
        assert reached is not None, (side, "server never served HTTP", run.text()[-2000:])
        node_id, record = ctx["store"].wait_one_own()
        connections = ctx["store"].connections()
        connection_kinds = ctx["store"].client_types()
        subscribed_channels = ctx["store"].pubsub_channels("*")
        time.sleep(SETTLE_SECONDS + AVAILABILITY_WINDOW_SECONDS)
        after = ctx["store"].records().get(node_id)
        assert after is not None, (side, "the live record vanished", ctx["store"].names())
        health_live = health(p["http"])[0]
        text = run.text()
        stop = run.interrupt()
        time.sleep(0.3)
        after_stop = run.own_names()
        # P1-4: a ping that lands after the stop must not resurrect the record. The
        # process is gone, so a record reappearing here could only come from a worker
        # or a subscription that outlived `Stop()`. `receivers` is Redis' own answer
        # to "is anything still listening on that node's keepalive topic".
        receivers = ctx["store"].publish(keepalive_channel(node_id),
                                         ping_envelope(int(time.time())))
        time.sleep(2.0)
        out[side] = {
            "normalized_config": run.normalized_config,
            "config_sha256": run.config_sha256,
            "health_first": reached[0], "health_first_body": reached[1],
            "health_live": health_live,
            "node_id": node_id, "log_node_id": node_id_from_log(text),
            "id_matches_log": node_id_from_log(text) == node_id,
            "record": {k: v for k, v in record.items() if k != "id"},
            "after": {k: v for k, v in after.items() if k != "id"},
            "updated_at_advanced": after["updated_at"] > record["updated_at"],
            "started_at_fixed": after["started_at"] == record["started_at"],
            "connections_while_running": connections,
            "connection_kinds_while_running": connection_kinds,
            "subscribed_channels_while_running": subscribed_channels,
            "stop": stop,
            "own_after_stop": after_stop,
            "late_ping_receivers": receivers,
            "own_after_late_ping": run.own_names(),
            "unregister_error_lines": log_lines(text, "could not unregister node"),
            "update_error_lines": log_lines(text, "could not update node"),
            "error_lines": error_lines(text),
        }
    go, rs = out["official"], out["rust"]

    # --- the reference behaviour is asserted on the official binary first ---
    assert go["health_first"] == 200 and go["health_first_body"] == "OK", \
        ("official / while registered", go["health_first"], go["health_first_body"])
    assert go["node_id"].startswith("ND_"), ("official node id is not an ND_ id",
                                             go["node_id"])
    assert go["id_matches_log"], ("official registered a node that is not its own",
                                  go["log_node_id"], go["node_id"])
    rec = go["record"]
    assert rec["ip"] == "127.0.0.1", ("official registered the wrong ip", rec["ip"])
    assert rec["state"] == 1, ("official node state is not SERVING", rec["state"])
    assert rec["type"] == 0, ("official node type is not SERVER", rec["type"])
    assert rec["region"] == REGION, ("official dropped the configured region", rec["region"])
    assert rec["num_cpus"] and rec["num_cpus"] > 0, ("official num_cpus", rec["num_cpus"])
    assert rec["stats_present"], "the official record must carry stats"
    assert go["updated_at_advanced"], ("the official record did not refresh across one "
                                       "availability window", rec, go["after"])
    assert go["started_at_fixed"], ("the official started_at moved", go["record"],
                                    go["after"])
    assert go["health_live"] == 200, ("official / not ready while refreshing",
                                      go["health_live"])
    assert go["stop"]["exit"] == 0 and not go["stop"]["sigkilled"], \
        ("official left uncleanly", go["stop"])
    assert go["own_after_stop"] == [], ("official record survived a graceful stop",
                                        go["own_after_stop"])
    assert go["late_ping_receivers"] == 0, ("a keepalive subscriber outlived the official "
                                            "process", go["late_ping_receivers"])
    assert go["own_after_late_ping"] == [], ("a ping after the stop revived the record",
                                             go["own_after_late_ping"])
    assert not go["unregister_error_lines"], ("official unregister failed",
                                              go["unregister_error_lines"])
    assert not go["update_error_lines"], ("official re-HSET failed",
                                          go["update_error_lines"])
    # The system-stats comparisons below are only meaningful if the official actually
    # sampled them: 0 == 0 would otherwise pass for two servers that report nothing.
    assert (go["after"]["stats_values"].get(17) or 0) > 0, \
        ("official reported no cpu count, so the stats comparison would be vacuous",
         go["after"]["stats_values"])
    assert (go["after"]["stats_values"].get(34) or 0) > 0, \
        ("official reported no memory total, so the stats comparison would be vacuous",
         go["after"]["stats_values"])

    # --- the two binaries ---
    comparisons = (
        ("id-prefix", go["node_id"][:3], rs["node_id"][:3]),
        ("registered-node-is-own-node", go["id_matches_log"], rs["id_matches_log"]),
        ("ip", rec["ip"], rs["record"]["ip"]),
        ("state", rec["state"], rs["record"]["state"]),
        ("type", rec["type"], rs["record"]["type"]),
        ("region", rec["region"], rs["record"]["region"]),
        ("num_cpus", rec["num_cpus"], rs["record"]["num_cpus"]),
        ("stats_present", rec["stats_present"], rs["record"]["stats_present"]),
        ("stats-field-set", go["after"]["stats_fields"], rs["after"]["stats_fields"]),
        ("stats-num-cpus", go["after"]["stats_values"].get(17),
         rs["after"]["stats_values"].get(17)),
        ("stats-memory-total-plausible",
         0 < (go["after"]["stats_values"].get(34) or 0) <= host_memory_total(),
         0 < (rs["after"]["stats_values"].get(34) or 0) <= host_memory_total()),
        ("health-200-while-live", go["health_live"], rs["health_live"]),
        ("updated_at-advanced", go["updated_at_advanced"], rs["updated_at_advanced"]),
        ("started_at-fixed", go["started_at_fixed"], rs["started_at_fixed"]),
        ("exit", go["stop"]["exit"], rs["stop"]["exit"]),
        ("sigkilled", go["stop"]["sigkilled"], rs["stop"]["sigkilled"]),
        ("own-after-stop", go["own_after_stop"], rs["own_after_stop"]),
        ("late-ping-receivers", go["late_ping_receivers"], rs["late_ping_receivers"]),
        ("own-after-late-ping", go["own_after_late_ping"], rs["own_after_late_ping"]),
        ("unregister-error-line", bool(go["unregister_error_lines"]),
         bool(rs["unregister_error_lines"])),
        ("could-not-update-node-line", bool(go["update_error_lines"]),
         bool(rs["update_error_lines"])),
    )
    for label, left, right in comparisons:
        if left != right:
            ctx["mismatch"](name, label, left, right)
    assert same_config(name, out)
    assert rs["own_after_late_ping"] == [], ("a worker outlived the candidate's stop",
                                             rs["own_after_late_ping"])

    ctx["known_differences"].append({
        "id": STATS_ITEM,
        "scope_of_this_item": (
            "the media-derived half of `livekit.NodeStats`. Upstream replaces the whole "
            "Stats from `prometheus.GetNodeStats` (`pkg/routing/node.go:142-153`): rooms, "
            "clients, tracks in/out, bytes, packets, nacks and the rate ring, alongside "
            "the system load/memory fields. This vector runs an idle server with no room, "
            "so proto3 omits every zero counter on BOTH sides and the media half is never "
            "exercised here."),
        "difference": (
            "none on what this vector measures. Both sides carry the same field set below "
            "— started_at(1), updated_at(2), num_cpus(17), the three load averages(18-20), "
            "cpu_load(21), memory_total(34), memory_used(35) — and num_cpus is compared as "
            "an exact host-invariant value, with the official asserted non-zero so the "
            "comparison cannot pass as 0 == 0. memory_total is NOT compared for equality: "
            "both sides build it from VM page classes and then average it in float32 over "
            "the stats window, so it moves between two runs. The two definitions were "
            "checked against each other and reduce to the same sum "
            "(wired+compressor+active+inactive+speculative+free): upstream "
            "`go-osstat@v0.2.8/memory/memory_darwin.go:107-109` via "
            "`prometheus/node.go:233,270`, candidate `node_stats.rs:373-383,155,162`. "
            "Each side is instead bounded by the host's physical memory, and both raw "
            "values are recorded below. The load averages, cpu_load and memory_used are "
            "sampled per side and recorded, not compared. HISTORY — earlier batches of "
            "this campaign recorded the candidate carrying only started_at/updated_at; "
            "that is no longer true and the old receipt is kept unedited at "
            "evidence/2026-11-04/c31-redis-node-registration-lifecycle-paired."),
        "observed_in_this_run": {"official_stats_fields": go["after"]["stats_fields"],
                                 "rust_stats_fields": rs["after"]["stats_fields"],
                                 "official_stats_values": go["after"]["stats_values"],
                                 "rust_stats_values": rs["after"]["stats_values"],
                                 "host_memory_total": host_memory_total(),
                                 "memory_total_relative_delta": abs(
                                     (go["after"]["stats_values"].get(34) or 0)
                                     - (rs["after"]["stats_values"].get(34) or 0))
                                 / max(1, host_memory_total())},
        "handling": (
            "stays OPEN, narrowed to the media-derived counters and the rate ring. "
            "Liveness (availability window, readiness, revocation) and the system-stats "
            "field set are accepted by this vector. num_rooms/num_clients/num_tracks/"
            "bytes/packets under real load, and the node selector's load/region sorting "
            "that reads them, are NOT accepted here and need a room-bearing differential."),
    })
    ctx["known_differences"].append({
        "id": CHURN_ITEM,
        "scope_of_this_item": (
            "how many Redis connections one healthy idle server start leaves open, read "
            "from `INFO clients` and broken down by `CLIENT LIST` flags while the server "
            "serves. One harness caller connection is included in both counts."),
        "difference": (
            f"the official holds {go['connections_while_running']} and the candidate "
            f"{rs['connections_while_running']}. The per-kind breakdown and the enumerated "
            "channel list below locate the difference: the candidate gives every "
            "subscription its own connection (`lk-compat/src/service_bus/redis_transport.rs"
            ":1-3,226-237`, one bounded RESP3 push queue per subscription), and a claimed "
            "RPC handler subscribes to both its request and its claim channel "
            "(`service_bus/rpc.rs:1122-1132`). Upstream instead batches every channel onto "
            "ONE `redis.PubSub` (`references/psrpc/pkg/bus/redisbus/redisbus.go:68,262`) "
            "shared by the whole server (`pkg/service/wire_gen.go:45`). So the candidate's "
            "pubsub socket count tracks its channel count, while the official's stays at "
            "one regardless of how many services are wired."),
        "observed_in_this_run": {"official_connections": go["connections_while_running"],
                                 "rust_connections": rs["connections_while_running"],
                                 "official_kinds": go["connection_kinds_while_running"],
                                 "rust_kinds": rs["connection_kinds_while_running"]},
        "handling": (
            "recorded, not compared, and still OPEN. The architecture is identified, not "
            "excused: matching upstream's footprint means multiplexing channels onto "
            "shared connections, which is a transport change with its own recovery and "
            "back-pressure acceptance. A connection count is not automatically a leak or "
            "a performance result, and this vector does not claim it is either. The "
            f"distinct channel-set difference is its own item ({CHANNEL_ITEM})."),
    })
    go_channels = set(go["subscribed_channels_while_running"])
    rs_channels = set(rs["subscribed_channels_while_running"])
    # Node and psrpc client ids are random per process, so a raw set difference reports
    # every node-scoped topic as missing on both sides. Diff the topic shape instead and
    # keep the raw sets as the observed record.
    go_shape = {channel_shape(c) for c in go_channels}
    rs_shape = {channel_shape(c) for c in rs_channels}
    only_go = go_shape - rs_shape
    # `…|CLI_<id>|…` is a psrpc client topic; everything else is a server-side topic the
    # candidate never subscribes at all, which an idle measurement *can* call a gap.
    client_only = sorted(c for c in only_go if "|CLI_" in c)
    server_only = sorted(only_go - set(client_only))
    ctx["known_differences"].append({
        "id": CHANNEL_ITEM,
        "scope_of_this_item": (
            "which psrpc topics an idle single-node server has subscribed to in Redis, "
            "enumerated with `PUBSUB CHANNELS *` while it serves. This is the topic "
            "surface, not the socket count. Node/client ids are normalized to `<id>` "
            "before diffing because they are random per process."),
        "difference": (
            f"the official subscribes to {len(go_channels)} channels and the candidate to "
            f"{len(rs_channels)}; normalized to topic shape that is {len(go_shape)} vs "
            f"{len(rs_shape)}. {len(client_only)} of the official-only shapes are the "
            "psrpc *client* side (`…|CLI_<id>|CLAIM` and `…|CLI_<id>|RES`) for the service "
            "clients it constructs at startup; whether the candidate creates those lazily "
            "on first call or never creates them is NOT determined by an idle measurement. "
            f"The remaining {len(server_only)} are server-side topics, listed in "
            "`official_only_server_topics`: the official subscribes the node-scoped "
            "`Signal|<node>|CLAIM`, `|RES` and `|STR`, where the candidate subscribes only "
            "`Signal|RelaySignal|<node>|STR`. Node-scoped signal addressing is a concrete "
            "candidate gap, not a lazy-creation ambiguity."),
        "observed_in_this_run": {
            "official_channel_count": len(go_channels),
            "rust_channel_count": len(rs_channels),
            "official_shape_count": len(go_shape),
            "rust_shape_count": len(rs_shape),
            "official_only": sorted(only_go),
            "official_only_client_topics": client_only,
            "official_only_server_topics": server_only,
            "rust_only": sorted(rs_shape - go_shape),
            "shared": sorted(go_shape & rs_shape),
            "raw_official_channels": sorted(go_channels),
            "raw_rust_channels": sorted(rs_channels)},
        "handling": (
            "OPEN, split. The client-topic half needs a real cross-node RPC where a remote "
            "worker answers on the candidate's client channel (the multi-node PSRPC "
            "workline). The `Signal|<node>` half is a missing server subscription and needs "
            "its own cross-node signal relay acceptance; neither is closed by this "
            "registration vector."),
    })
    return out


def vector_sigkill(official, rust, name, ctx):
    """Nothing runs on SIGKILL, so the record stays and must age out of the window the
    same way for both binaries."""
    out = {}
    for side, binary in (("official", official), ("rust", rust)):
        p = ctx["side_ports"](side)
        body = config_body(p["http"], p["tcp"], p["udp"], None, ctx["redis_addr"],
                           node_stats="{stats_update_interval: 1s}")
        ctx["store"].clear()
        run = Run(side, binary, name, p, body, ctx["staging"], ctx["output"],
                  ctx["store"], ctx["procs"])
        assert run.wait_http() is not None, (side, "no HTTP", run.text()[-1500:])
        node_id, _ = ctx["store"].wait_one_own()
        killed = run.kill()
        time.sleep(0.5)
        remaining = ctx["store"].records()
        out[side] = {"normalized_config": run.normalized_config,
                     "config_sha256": run.config_sha256,
                     "kill": killed, "left": sorted(remaining),
                     "states": [v["state"] for v in remaining.values()],
                     "updated_at": [v["updated_at"] for v in remaining.values()]}
        assert node_id in remaining
    go, rs = out["official"], out["rust"]
    assert len(go["left"]) == 1, ("official SIGKILL must leave exactly its own record",
                                  go["left"])
    assert go["states"] == [1], ("official record must still say SERVING", go["states"])
    assert len(rs["left"]) == 1, ("candidate SIGKILL must leave exactly its own record",
                                  rs["left"])
    assert same_config(name, out)
    for label, left, right in (("states-after-kill", go["states"], rs["states"]),
                               ("exit", go["kill"]["exit"], rs["kill"]["exit"]),
                               ("left-count", len(go["left"]), len(rs["left"]))):
        if left != right:
            ctx["mismatch"](name, label, left, right)
    ctx["store"].clear()
    return out


def vector_pair(official, rust, name, ctx):
    """Two servers, one Redis. Three combinations, so a candidate record is proven
    visible to the official CLI and vice versa, and revocation is proven to be
    per-node rather than 'clear the hash'."""
    store = ctx["store"]
    out = {}

    def launch(side, binary, slot):
        p = ctx["side_ports"](side, slot)
        body = config_body(p["http"], p["tcp"], p["udp"], None, ctx["redis_addr"],
                           node_stats="{stats_update_interval: 1s}", region=REGION)
        run = Run(f"{side}#{slot}", binary, name, p, body, ctx["staging"], ctx["output"],
                  store, ctx["procs"])
        assert run.wait_http() is not None, (side, slot, "no HTTP", run.text()[-1500:])
        return run

    def wait_new(previous: set[str], timeout: float = 20.0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            new = set(store.names()) - previous
            if len(new) == 1:
                return next(iter(new))
            time.sleep(0.2)
        raise AssertionError(f"a second node record never appeared alone: {store.names()}")

    def list_nodes(binary, tag: str) -> dict:
        done = subprocess.run([str(binary), "--dev", "--redis-host",
                               f"127.0.0.1:{ctx['master_port']}", "list-nodes"],
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=BASE_ENV)
        text = done.stdout.decode(errors="replace")
        (ctx["output"] / f"{name}-{tag}.list-nodes.log").write_text(text)
        return {"exit": done.returncode,
                "ids": sorted(set(re.findall(r"ND_[A-Za-z0-9_-]+", text))),
                "stdout_b64": base64.b64encode(text.encode()).decode()}

    binaries = {"go": official, "rs": rust}
    sides = {"go": "official", "rs": "rust"}
    for combo in (("go", "go"), ("rs", "rs"), ("go", "rs")):
        key = "+".join(combo)
        store.clear()
        first = launch(sides[combo[0]], binaries[combo[0]], 0)
        a_id = wait_new(set())
        second = launch(sides[combo[1]], binaries[combo[1]], 1)
        b_id = wait_new({a_id})
        time.sleep(SETTLE_SECONDS + AVAILABILITY_WINDOW_SECONDS)
        live = store.records()
        rec = {"slot_configs": {sides[combo[0]] + "#0": first.normalized_config,
                               sides[combo[1]] + "#1": second.normalized_config},
               "slot_shas": {sides[combo[0]] + "#0": first.config_sha256,
                             sides[combo[1]] + "#1": second.config_sha256},
               "ids": [a_id, b_id],
               "own_while_both_live": sorted(live),
               "both_stayed_available": sorted(live) == sorted([a_id, b_id]),
               "states": {k: live[k]["state"] for k in live},
               "regions": {k: live[k]["region"] for k in live}}
        cli = {}
        for cli_side, cli_binary in (("official_cli", official), ("rust_cli", rust)):
            got = list_nodes(cli_binary, f"{key}-{cli_side}")
            cli[cli_side] = got
            assert got["exit"] == 0, (key, cli_side, "list-nodes failed", got)
            assert sorted(got["ids"]) == sorted([a_id, b_id]), \
                (key, cli_side, "list-nodes did not show exactly the two live nodes",
                 got["ids"], [a_id, b_id])
        rec["list_nodes"] = cli
        stop_a = first.interrupt()
        time.sleep(0.5)
        after_a = set(store.names())
        stop_b = second.interrupt()
        time.sleep(0.3)
        rec["after_stopping_first"] = sorted(after_a)
        rec["first_revoked_only_own"] = after_a == {b_id}
        rec["after_stopping_second"] = store.names()
        rec["exits"] = {"first": stop_a["exit"], "second": stop_b["exit"],
                        "sigkilled": [stop_a["sigkilled"], stop_b["sigkilled"]]}
        assert rec["both_stayed_available"], (key, "a record aged out of the window", rec)
        assert rec["first_revoked_only_own"], \
            (key, "stopping one server revoked the other", rec["after_stopping_first"])
        assert rec["after_stopping_second"] == [], (key, "residue after both stopped", rec)
        assert set(rec["regions"].values()) == {REGION}, (key, "region lost", rec["regions"])
        if cli["official_cli"]["ids"] != cli["rust_cli"]["ids"]:
            ctx["mismatch"](name, f"{key}-list-nodes-ids", cli["official_cli"]["ids"],
                            cli["rust_cli"]["ids"])
        if cli["official_cli"]["exit"] != cli["rust_cli"]["exit"]:
            ctx["mismatch"](name, f"{key}-list-nodes-exit", cli["official_cli"]["exit"],
                            cli["rust_cli"]["exit"])
        out[key] = rec

    # Slot roles must be fed identical bytes wherever the same side appears twice.
    for slot_key in ("official#0", "rust#0", "official#1", "rust#1"):
        seen = {combo: rec["slot_configs"].get(slot_key)
                for combo, rec in out.items() if slot_key in rec["slot_configs"]}
        assert len(set(seen.values())) <= 1, ("slot config bytes differ", slot_key, seen)
    return out


def vector_ping_refresh(official, rust, name, ctx):
    """The keepalive channel driven from outside. With a 30 s interval the server's own
    tick cannot fire inside the observation window, so only an injected ping can move
    the record — which is what makes the stale, garbage and wrong-type controls mean
    something rather than merely being fast."""
    store = ctx["store"]
    out = {}
    for side, binary in (("official", official), ("rust", rust)):
        p = ctx["side_ports"](side)
        body = config_body(p["http"], p["tcp"], p["udp"], None, ctx["redis_addr"],
                           node_stats="{stats_update_interval: 30s}")
        store.clear()
        run = Run(side, binary, name, p, body, ctx["staging"], ctx["output"],
                  store, ctx["procs"])
        assert run.wait_http() is not None, (side, "no HTTP", run.text()[-1500:])
        node_id, record = store.wait_one_own()
        channel = keepalive_channel(node_id)

        def stamp():
            return store.records().get(node_id, {}).get("updated_at")

        u0 = record["updated_at"]
        time.sleep(2.0)
        self_tick = stamp()
        receivers_at_fresh = store.publish(channel, ping_envelope(int(time.time())))
        jumped, deadline = None, time.monotonic() + 6
        while time.monotonic() < deadline:
            if stamp() not in (None, u0):
                jumped = stamp()
                break
            time.sleep(0.2)
        before_stale = stamp()
        stale_ts = int(time.time()) - STALE_SECONDS
        receivers_stale = store.publish(channel, ping_envelope(stale_ts))
        time.sleep(1.5)
        after_stale = stamp()
        before_garbage = stamp()
        receivers_garbage = store.publish(channel, b"this is not a psrpc envelope")
        time.sleep(0.8)
        after_garbage = stamp()
        before_wrong = stamp()
        wrong = store.publish(channel, envelope("type.googleapis.com/livekit.Room",
                                                sfield(1, "room-sid")))
        time.sleep(0.8)
        after_wrong_type = stamp()
        text = run.text()
        stop = run.interrupt()
        side_out = {
            "normalized_config": run.normalized_config, "config_sha256": run.config_sha256,
            "channel": channel, "u0": u0, "self_tick": self_tick,
            "self_tick_froze": self_tick == u0, "fresh_ping_jumped": jumped,
            "receivers": {"fresh": receivers_at_fresh, "stale": receivers_stale,
                          "garbage": receivers_garbage, "wrong_type": wrong},
            "after_stale": after_stale,
            "stale_ping_changed": after_stale != before_stale,
            "garbage_changed": after_garbage != before_garbage,
            "wrong_type_changed": after_wrong_type != before_garbage,
            "stale_log_lines": log_lines(text, "keep alive too old, skipping"),
            "error_lines": error_lines(text), "stop": stop,
            "own_after_stop": run.own_names(),
        }
        for line in side_out["stale_log_lines"]:
            assert str(json_field(line, "timestamp")).strip('"') == str(stale_ts), \
                (side, "the stale line lost its timestamp field", line, stale_ts)
        out[side] = side_out
    go, rs = out["official"], out["rust"]

    assert go["self_tick_froze"], ("the official refreshed inside 2 s on a 30 s interval "
                                   "— the tick is not the configured interval", go["u0"],
                                   go["self_tick"])
    assert go["fresh_ping_jumped"], ("an injected fresh psrpc ping did not refresh the "
                                     "official record", go["u0"], go["receivers"])
    assert go["receivers"]["fresh"] == 1, ("nothing was subscribed to the official's "
                                           "keepalive topic", go["receivers"])
    assert not go["stale_ping_changed"], ("the official applied a ping older than the "
                                          "interval", go["u0"], go["after_stale"])
    assert len(go["stale_log_lines"]) == 1, ("official stale-skip line count",
                                             go["stale_log_lines"])
    assert not go["garbage_changed"], ("the official applied a garbage payload", go)
    assert not go["wrong_type_changed"], ("the official applied a foreign message type", go)
    assert go["own_after_stop"] == [], go["own_after_stop"]

    comparisons = (
        ("self-tick-froze", go["self_tick_froze"], rs["self_tick_froze"]),
        ("fresh-ping-jumped", bool(go["fresh_ping_jumped"]), bool(rs["fresh_ping_jumped"])),
        ("subscriber-count-fresh", go["receivers"]["fresh"], rs["receivers"]["fresh"]),
        ("subscriber-count-stale", go["receivers"]["stale"], rs["receivers"]["stale"]),
        ("subscriber-count-garbage", go["receivers"]["garbage"], rs["receivers"]["garbage"]),
        ("subscriber-count-wrong-type", go["receivers"]["wrong_type"],
         rs["receivers"]["wrong_type"]),
        ("stale-ping-changed", go["stale_ping_changed"], rs["stale_ping_changed"]),
        ("stale-log-line-count", len(go["stale_log_lines"]), len(rs["stale_log_lines"])),
        ("garbage-changed", go["garbage_changed"], rs["garbage_changed"]),
        ("wrong-type-changed", go["wrong_type_changed"], rs["wrong_type_changed"]),
        ("error-line-count", len(go["error_lines"]), len(rs["error_lines"])),
        ("exit", go["stop"]["exit"], rs["stop"]["exit"]),
        ("own-after-stop", go["own_after_stop"], rs["own_after_stop"]),
    )
    for label, left, right in comparisons:
        if left != right:
            ctx["mismatch"](name, label, left, right)
    assert same_config(name, out)
    assert rs["fresh_ping_jumped"], ("the candidate never applied an injected ping",
                                     rs["u0"], rs["receivers"])
    assert rs["own_after_stop"] == [], rs["own_after_stop"]
    return out


def vector_health(official, rust, name, ctx):
    """`/` is the readiness consumer of the same stamp: `server.go:450-464` compares
    `time.Since(time.Unix(updated_at, 0))` against the literal `4*time.Second` and, on
    failure, writes `Not Ready\\nNode Updated At %s` with Go's own time rendering — so
    the body is checked against a rendering computed here from the record Redis holds."""
    store = ctx["store"]
    out = {}
    for side, binary in (("official", official), ("rust", rust)):
        p = ctx["side_ports"](side)
        body = config_body(p["http"], p["tcp"], p["udp"], None, ctx["redis_addr"],
                           node_stats="{stats_update_interval: 30s}")
        store.clear()
        run = Run(side, binary, name, p, body, ctx["staging"], ctx["output"],
                  store, ctx["procs"])
        reached = run.wait_http()
        assert reached is not None, (side, "no HTTP", run.text()[-1500:])
        node_id, record = store.wait_one_own()
        time.sleep(4.6)
        stale_status, stale_body = health(p["http"])
        stamp_at_406 = store.records().get(node_id, {}).get("updated_at")
        store.publish(keepalive_channel(node_id), ping_envelope(int(time.time())))
        back, deadline = None, time.monotonic() + 10
        while time.monotonic() < deadline:
            status, _ = health(p["http"])
            if status == 200:
                back = status
                break
            time.sleep(0.2)
        stop = run.interrupt()
        out[side] = {"normalized_config": run.normalized_config,
                     "config_sha256": run.config_sha256,
                     "first_status": reached[0], "first_body": reached[1],
                     "registered_updated_at": record["updated_at"],
                     "stamp_at_406": stamp_at_406,
                     "stale_status": stale_status, "stale_body": stale_body,
                     "expected_stale_body": ("Not Ready\nNode Updated At "
                                             + go_time(record["updated_at"])),
                     "status_after_ping": back, "stop": stop}
    go, rs = out["official"], out["rust"]

    assert go["first_status"] == 200 and go["first_body"] == "OK", \
        ("official health while fresh", go["first_status"], go["first_body"])
    assert go["stale_status"] == 406, ("official / stayed ready past 4 s with a frozen "
                                       "stamp", go["stale_status"], go["stale_body"])
    assert go["stamp_at_406"] == go["registered_updated_at"], \
        ("the official record moved before the 406 sample", go)
    assert go["stale_body"] == go["expected_stale_body"], \
        ("official 406 body is not Go's rendering of the stamp Redis holds",
         go["stale_body"], go["expected_stale_body"])
    assert go["status_after_ping"] == 200, ("official / never recovered after a fresh "
                                            "ping", go["status_after_ping"])

    comparisons = (
        ("first-status", go["first_status"], rs["first_status"]),
        ("first-body", go["first_body"], rs["first_body"]),
        ("stale-status", go["stale_status"], rs["stale_status"]),
        ("stale-body-matches-own-rendering", go["stale_body"] == go["expected_stale_body"],
         rs["stale_body"] == rs["expected_stale_body"]),
        ("stale-body-shape", re.sub(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", "<TIME>",
                                    go["stale_body"]),
         re.sub(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", "<TIME>", rs["stale_body"])),
        ("stamp-frozen-at-406", go["stamp_at_406"] == go["registered_updated_at"],
         rs["stamp_at_406"] == rs["registered_updated_at"]),
        ("status-after-fresh-ping", go["status_after_ping"], rs["status_after_ping"]),
        ("exit", go["stop"]["exit"], rs["stop"]["exit"]),
    )
    for label, left, right in comparisons:
        if left != right:
            ctx["mismatch"](name, label, left, right)
    assert same_config(name, out)
    assert rs["stale_body"] == rs["expected_stale_body"], \
        ("candidate 406 body is not Go's rendering of the stamp Redis holds",
         rs["stale_body"], rs["expected_stale_body"])
    assert rs["first_body"] == "OK", ("candidate / body while fresh", rs["first_body"])
    assert rs["status_after_ping"] == 200, ("candidate / never recovered", rs)
    return out


def vector_delayed_stats(official, rust, name, ctx):
    """`statsWorker`'s own deadlock diagnostic: `delaySeconds >
    node_stats.stats_max_delay` prints once and latches until a tick measures a record
    fresh enough — and `1ns` guarantees it never does, so the latch is observable
    rather than a coincidence."""
    store = ctx["store"]
    out = {}
    for side, binary in (("official", official), ("rust", rust)):
        p = ctx["side_ports"](side)
        body = config_body(p["http"], p["tcp"], p["udp"], None, ctx["redis_addr"],
                           node_stats="{stats_update_interval: 1s, stats_max_delay: 1ns}")
        store.clear()
        run = Run(side, binary, name, p, body, ctx["staging"], ctx["output"],
                  store, ctx["procs"])
        assert run.wait_http() is not None, (side, "no HTTP", run.text()[-1500:])
        time.sleep(4.0)
        text = run.text()
        lines = log_lines(text, "status update delayed, possible deadlock")
        stop = run.interrupt()
        out[side] = {"normalized_config": run.normalized_config,
                     "config_sha256": run.config_sha256,
                     "count": len(lines),
                     "delays": [json_field(line, "delay") for line in lines],
                     "goroutines_present": [json_field(line, "goroutines") is not None
                                            for line in lines],
                     "goroutines_lengths": [len(str(json_field(line, "goroutines")))
                                            for line in lines],
                     "first_line_truncated": [line[:400] for line in lines[:1]],
                     "callers": sorted({str(json_field(line, "caller")) for line in lines}),
                     "levels": sorted({str(json_field(line, "level")) for line in lines}),
                     "stop": stop, "own_after_stop": run.own_names()}
    go, rs = out["official"], out["rust"]

    assert go["count"] == 1, ("the official delayed-stats line is not latched to one",
                              go["count"], go["first_line_truncated"])
    assert go["delays"] and go["delays"][0] is not None, "official lost the delay field"
    delay = float(go["delays"][0])
    assert 0 < delay < 10, ("official delay field is not a bounded float", go["delays"])
    assert go["goroutines_present"] == [True], ("official lost the goroutines field",
                                               go["goroutines_present"])
    assert sorted({lv.strip('"') for lv in go["levels"]}) == ["error"], \
        ("official delayed line is not an error", go["levels"])

    comparisons = (
        ("line-count", go["count"], rs["count"]),
        ("goroutines-field-present", go["goroutines_present"], rs["goroutines_present"]),
        ("delay-is-positive-float", delay > 0,
         bool(rs["delays"]) and float(rs["delays"][0]) > 0),
        ("exit", go["stop"]["exit"], rs["stop"]["exit"]),
        ("own-after-stop", go["own_after_stop"], rs["own_after_stop"]),
    )
    for label, left, right in comparisons:
        if left != right:
            ctx["mismatch"](name, label, left, right)
    assert same_config(name, out)
    assert rs["delays"], ("the candidate never printed the delayed-stats diagnostic",
                          rs["first_line_truncated"])
    ctx["known_differences"].append({
        "id": "RUNTIME:Redis.GoroutineProfileField",
        "scope_of_this_item": (
            "the payload of the `goroutines` field on the delayed-stats line only. "
            "Upstream writes `pprof.Lookup(\"goroutine\").WriteTo(buf, 2)` "
            "(`redisrouter.go:213-218`), a full Go goroutine dump. The line's level, "
            "caller, presence, latch and `delay` value are compared above."),
        "difference": (
            f"the official field carries a {go['goroutines_lengths'][0]} byte goroutine "
            f"profile; the candidate carries an explicit 'unavailable: no Rust goroutine "
            "profile' string, because the Rust runtime has no such profile and "
            "`/debug/goroutine` already answers 501 for the same reason (SPEC-0074 C14). "
            "Manufacturing a Go-shaped dump would be exactly the fixed-success failure "
            "mode this program forbids."),
        "observed_in_this_run": {"official_goroutines_bytes": go["goroutines_lengths"],
                                 "rust_goroutines_bytes": rs["goroutines_lengths"]},
        "handling": "recorded, not closed: the diagnostic itself fires and latches "
                    "identically.",
    })
    return out


def vector_restart(official, rust, name, ctx):
    """A real `redis-server` restart. The static endpoint's command connection is a
    `MultiplexedConnection`, not a go-redis `ConnectionManager`, so this is where a
    dial-once-and-hope design stays broken forever: the registry HSET, the psrpc
    publish and the psrpc subscription must each come back on their own."""
    store, box = ctx["store"], ctx["redis_box"]
    out = {}
    for side, binary in (("official", official), ("rust", rust)):
        p = ctx["side_ports"](side)
        body = config_body(p["http"], p["tcp"], p["udp"], None, ctx["redis_addr"],
                           node_stats="{stats_update_interval: 1s}")
        store.clear()
        run = Run(side, binary, name, p, body, ctx["staging"], ctx["output"],
                  store, ctx["procs"])
        assert run.wait_http() is not None, (side, "no HTTP", run.text()[-1500:])
        node_id, _ = store.wait_one_own()
        stop_process(box["proc"])
        box["proc"] = None
        down_at = time.monotonic()
        while store.alive() and time.monotonic() - down_at < 15:
            time.sleep(0.05)
        assert not store.alive(), (side, "redis-server did not stop")
        time.sleep(AVAILABILITY_WINDOW_SECONDS + 0.5)
        health_while_down = health(p["http"])[0]
        box["proc"] = redis_server(ctx["master_port"], ctx["staging"], f"restart-{side}")
        dial_ok = time.monotonic()
        recovered = None
        while time.monotonic() < dial_ok + 25:
            if node_id in store.names():
                recovered = time.monotonic()
                break
            time.sleep(0.2)
        recovery_seconds = round(recovered - dial_ok, 3) if recovered else None
        health_after, refresh = None, None
        if recovered:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                health_after = health(p["http"])[0]
                if health_after == 200:
                    break
                time.sleep(0.2)
            first = store.records().get(node_id, {}).get("updated_at")
            time.sleep(2.5)
            later = store.records().get(node_id, {}).get("updated_at")
            refresh = {"first": first, "later": later, "keeps_refreshing": later != first}
        text = run.text()
        stop = run.interrupt()
        out[side] = {"normalized_config": run.normalized_config,
                     "config_sha256": run.config_sha256,
                     "health_while_down": health_while_down,
                     "recovery_seconds": recovery_seconds,
                     "health_after_recovery": health_after, "refresh": refresh,
                     "connections_after_recovery": (store.connections() if store.alive()
                                                    else None),
                     "could_not_update_node": log_lines(text, "could not update node")[:4],
                     "error_lines": error_lines(text)[:8],
                     "library_dial_lines": [line[:300] for line in text.splitlines()
                                            if "failed to dial" in line
                                            or "connection refused" in line][:6],
                     "stop": stop,
                     "own_after_stop": run.own_names() if store.alive() else None}
    go, rs = out["official"], out["rust"]

    assert go["health_while_down"] == 406, ("official / stayed 200 while Redis was down "
                                            "and the stamp frozen", go["health_while_down"])
    assert go["recovery_seconds"] is not None, \
        ("official never re-registered after a real redis-server restart", go)
    assert go["recovery_seconds"] <= 5.0, ("official re-registration took longer than the "
                                           "availability window", go["recovery_seconds"])
    assert go["health_after_recovery"] == 200, ("official / never recovered", go)
    assert go["refresh"] and go["refresh"]["keeps_refreshing"], \
        ("the official record stopped refreshing after the restart", go["refresh"])
    assert not go["could_not_update_node"], ("official re-HSET kept failing",
                                             go["could_not_update_node"])

    comparisons = (
        ("recovered-within-the-window", go["recovery_seconds"] is not None,
         rs["recovery_seconds"] is not None),
        ("health-while-down", go["health_while_down"], rs["health_while_down"]),
        ("health-after-recovery", go["health_after_recovery"], rs["health_after_recovery"]),
        ("keeps-refreshing", bool(go["refresh"] and go["refresh"]["keeps_refreshing"]),
         bool(rs["refresh"] and rs["refresh"]["keeps_refreshing"])),
        ("exit", go["stop"]["exit"], rs["stop"]["exit"]),
        ("own-after-stop", go["own_after_stop"], rs["own_after_stop"]),
    )
    for label, left, right in comparisons:
        if left != right:
            ctx["mismatch"](name, label, left, right)
    assert same_config(name, out)
    assert rs["recovery_seconds"] is not None, \
        ("the candidate never re-registered after a real redis-server restart — the "
         "static endpoint is not a ConnectionManager", rs["error_lines"])
    assert rs["recovery_seconds"] <= 5.0, ("candidate re-registration took longer than the "
                                           "availability window", rs["recovery_seconds"])
    assert rs["own_after_stop"] == [], ("the candidate record survived the stop after "
                                        "recovery", rs["own_after_stop"])
    ctx["known_differences"].append({
        "id": POOL_ITEM,
        "scope_of_this_item": (
            "the sentences printed *while* Redis is unreachable. go-redis reports its own "
            "pool failures (`internal/pool/pool.go` — `redis: connection pool: failed to "
            "dial after 5 attempts`); redis-rs surfaces the error of the command that was "
            "in flight, and the candidate's registry wraps it as `could not register "
            "node`."),
        "difference": (
            "the recovery outcome is compared above and matches — the registration comes "
            f"back in {go['recovery_seconds']}s on the official and "
            f"{rs['recovery_seconds']}s on the candidate, and the readiness endpoint "
            "follows. The wording printed on the way there differs and is not ported."),
        "observed_in_this_run": {"official_dial_lines": go["library_dial_lines"],
                                 "rust_dial_lines": rs["library_dial_lines"],
                                 "official_could_not_update_node": go["could_not_update_node"],
                                 "rust_could_not_update_node": rs["could_not_update_node"]},
        "handling": "recorded, not closed; the library-diagnostic family is its own item.",
    })
    return out


def vector_client_kill(official, rust, name, ctx):
    """`CLIENT KILL` without a restart: the command connection and the pubsub
    connection are both closed by the server while the process keeps serving, so the
    registry HSET, the psrpc publish and the psrpc subscription must each re-dial.

    The three paths are witnessed separately rather than inferred from one another:
    a harness-side SUBSCRIBE that receives a *server-published* ping proves the
    publish connection came back; `PUBLISH`'s receiver count after the kill proves
    the server re-joined its own topic; and the record's `updated_at` advancing again
    proves the receiving side's HSET went through a live command connection."""
    store = ctx["store"]
    out = {}
    for side, binary in (("official", official), ("rust", rust)):
        p = ctx["side_ports"](side)
        body = config_body(p["http"], p["tcp"], p["udp"], None, ctx["redis_addr"],
                           node_stats="{stats_update_interval: 1s}")
        store.clear()
        run = Run(side, binary, name, p, body, ctx["staging"], ctx["output"],
                  store, ctx["procs"])
        assert run.wait_http() is not None, (side, "no HTTP", run.text()[-1500:])
        node_id, _ = store.wait_one_own()
        channel = keepalive_channel(node_id)

        sub = store.subscriber(channel, 12.0)
        baseline_publish = sub.next_publish() is not None
        sub.close()

        clients_before = store.client_types()
        killed = {"normal": store.client_kill(b"normal"),
                  "pubsub": store.client_kill(b"pubsub")}
        clients_after = store.client_types()

        before = store.records()[node_id]["updated_at"]
        deadline = time.monotonic() + 12
        publish_after_kill = False
        receivers_after_kill = None
        while time.monotonic() < deadline:
            witness = store.subscriber(channel, 6.0)
            got = witness.next_publish()
            witness.close()
            if got is not None:
                publish_after_kill = True
                receivers_after_kill = store.publish(channel, ping_envelope(int(time.time())))
                break
        after = store.records().get(node_id, {}).get("updated_at")
        stamps = [before]
        for _ in range(3):
            time.sleep(1.2)
            stamps.append(store.records().get(node_id, {}).get("updated_at"))
        health_after = health(p["http"])[0]
        text = run.text()
        stop = run.interrupt()
        out[side] = {"normalized_config": run.normalized_config,
                     "config_sha256": run.config_sha256,
                     "killed": killed, "clients_before": clients_before,
                     "clients_after": clients_after,
                     "before": before, "stamps": stamps,
                     "baseline_publish": baseline_publish,
                     "publish_after_kill": publish_after_kill,
                     "receivers_after_kill": receivers_after_kill,
                     "refreshed_after_kill": all(s is not None for s in stamps)
                                          and stamps[-1] > stamps[0],
                     "health_after_kill": health_after,
                     "error_lines": error_lines(text)[:8],
                     "could_not_update_node": log_lines(text, "could not update node")[:4],
                     "stop": stop, "own_after_stop": run.own_names()}
    go, rs = out["official"], out["rust"]

    assert go["clients_before"].get("pubsub", 0) >= 1, \
        ("the official held no pubsub connection, so there was nothing to kill",
         go["clients_before"])
    assert go["killed"]["normal"] >= 1, ("CLIENT KILL TYPE normal closed no connection — "
                                         "the fault never fired", go["killed"],
                                         go["clients_before"])
    assert go["killed"]["pubsub"] >= 1, ("CLIENT KILL TYPE pubsub closed no connection — "
                                         "the fault never fired", go["killed"],
                                         go["clients_before"])
    assert go["baseline_publish"], ("the official never published a keepalive ping on "
                                    "its own channel before the kill", go)
    assert go["publish_after_kill"], ("the official publish connection never came back "
                                      "after CLIENT KILL", go)
    assert (go["receivers_after_kill"] or 0) >= 1, ("the official never re-joined its own "
                                                    "keepalive topic after CLIENT KILL", go)
    assert go["refreshed_after_kill"], ("the official record stopped refreshing after "
                                        "CLIENT KILL", go["before"], go["stamps"])
    assert go["health_after_kill"] == 200, ("official / not ready after CLIENT KILL",
                                             go["health_after_kill"])
    assert not go["could_not_update_node"], ("official re-HSET failed after CLIENT KILL",
                                             go["could_not_update_node"])

    comparisons = (
        ("normal-clients-killed", go["killed"]["normal"] >= 1, rs["killed"]["normal"] >= 1),
        ("pubsub-clients-killed", go["killed"]["pubsub"] >= 1, rs["killed"]["pubsub"] >= 1),
        ("publish-after-kill", go["publish_after_kill"], rs["publish_after_kill"]),
        ("subscriber-rejoined", (go["receivers_after_kill"] or 0) >= 1,
         (rs["receivers_after_kill"] or 0) >= 1),
        ("refreshed-after-kill", go["refreshed_after_kill"], rs["refreshed_after_kill"]),
        ("health-after-kill", go["health_after_kill"], rs["health_after_kill"]),
        ("could-not-update-node-line", bool(go["could_not_update_node"]),
         bool(rs["could_not_update_node"])),
        ("exit", go["stop"]["exit"], rs["stop"]["exit"]),
        ("own-after-stop", go["own_after_stop"], rs["own_after_stop"]),
    )
    for label, left, right in comparisons:
        if left != right:
            ctx["mismatch"](name, label, left, right)
    assert same_config(name, out)
    assert rs["killed"]["normal"] >= 1 and rs["killed"]["pubsub"] >= 1, \
        ("the candidate side never had its connections killed, so its recovery below "
         "would be vacuous", rs["killed"], rs["clients_before"])
    assert rs["publish_after_kill"], ("the candidate publish connection never came back "
                                      "after CLIENT KILL", rs)
    assert (rs["receivers_after_kill"] or 0) >= 1, ("the candidate never re-joined its own "
                                                    "keepalive topic after CLIENT KILL", rs)
    assert rs["refreshed_after_kill"], ("the candidate stopped refreshing after "
                                        "CLIENT KILL", rs["before"], rs["stamps"])
    return out


def vector_wt_bind_failure(official, rust, name, ctx):
    """P1-1: the WebTransport UDP bind is the startup step after the HSET that can
    fail, so it must leave no node record and no continuing refresh behind. Upstream
    covers it with the `defer UnregisterNode` installed at `server.go:225-229`; the
    candidate has to undo it explicitly, and does it in the order `stop()` then the
    delete — a live keepalive worker would re-HSET the record after a bare delete."""
    store = ctx["store"]
    out = {}
    for side, binary in (("official", official), ("rust", rust)):
        p = ctx["side_ports"](side)
        hold = UdpHold(p["wt"])
        ctx["holds"].append(hold)
        # Positive control 1: the UDP port is genuinely occupied (a second bind fails
        # with EADDRINUSE), so a non-zero exit cannot be attributed to a free port.
        occupied_while_held = udp_occupied(p["wt"])
        body = config_body(p["http"], p["tcp"], p["udp"], p["wt"], ctx["redis_addr"],
                           node_stats="{stats_update_interval: 1s}", level="debug")
        store.clear()
        # Positive control 2: tap the command stream so the registration/subscription
        # phase before the failed-start cleanup is observed, not inferred from an empty
        # post-exit keyspace (which a pre-registration failure would also leave).
        monitor = Monitor(store.port)
        run = Run(side, binary, name, p, body, ctx["staging"], ctx["output"],
                  store, ctx["procs"])
        if not run.wait_exit():
            run.force_stop()
        # Drain until the undo (HDEL) is captured, or 3 s, then stop the tap before the
        # post-exit keyspace checks so the harness's own commands do not pollute it.
        deadline = time.monotonic() + 3.0
        seq = []
        while time.monotonic() < deadline:
            seq = monitor_node_sequence(monitor.snapshot())
            if any(c == "HDEL" and k == "nodes" for c, k, _ in seq):
                break
            time.sleep(0.05)
        monitor.close()
        order = [f"{c} {k}" + (f" {d}" if d else "") for c, k, d in seq]
        shape = [[c, "keepalive" if k.startswith("Keepalive|Ping|") else k] for c, k, _ in seq]
        hset = [i for i, (c, k, _) in enumerate(seq) if c == "HSET" and k == "nodes"]
        hdel = [i for i, (c, k, _) in enumerate(seq) if c == "HDEL" and k == "nodes"]
        subs = [i for i, (c, k, _) in enumerate(seq)
                if c == "SUBSCRIBE" and k.startswith("Keepalive|Ping|")]
        immediate = run.own_names()
        channels = store.pubsub_channels("Keepalive|Ping|*")
        subscribers = sum(store.publish(ch, ping_envelope(int(time.time())))
                          for ch in channels)
        time.sleep(3.0)
        text = run.text()
        hold.close()
        out[side] = {"normalized_config": run.normalized_config,
                     "config_sha256": run.config_sha256,
                     "exit": run.exit_code, "sigkilled": run.sigkilled,
                     "http_port_free_after_exit": free(p["http"]),
                     "udp_occupied_while_held": occupied_while_held,
                     "udp_occupied_after_release": udp_occupied(p["wt"]),
                     "registration_hset_observed": bool(hset),
                     "keepalive_subscribe_observed": bool(subs),
                     "unregister_hdel_observed": bool(hset) and bool(hdel) and max(hdel) > min(hset),
                     "registered_node_id": seq[hset[0]][2] if hset else None,
                     "startup_command_order": order,
                     "startup_command_shape": shape,
                     "own_immediately_after_exit": immediate,
                     "own_after_3s": run.own_names(),
                     "keepalive_channels_after_exit": channels,
                     "ping_receivers_after_exit": subscribers,
                     "dev_cert_warning": bool(log_lines(
                         text, "generated self-signed cert (dev mode)")),
                     "stopping_router_debug_line": bool(log_lines(
                         text, "stopping RedisRouter")),
                     "unregister_error_lines": log_lines(text, "could not unregister node"),
                     "address_in_use_lines": [line[:300] for line in text.splitlines()
                                              if "address already in use" in line.lower()][:4],
                     "webtransport_lines": [line[:300] for line in text.splitlines()
                                             if "webtransport" in line.lower()][:4],
                     "error_lines": error_lines(text)[:4]}
    go, rs = out["official"], out["rust"]

    assert go["exit"] not in (None, 0), ("official started despite an occupied "
                                         "WebTransport port", go["exit"],
                                         go["webtransport_lines"])
    assert not go["sigkilled"], ("official hung after the bind failure", go)
    assert go["own_immediately_after_exit"] == [], ("official left its node record behind",
                                                   go["own_immediately_after_exit"])
    assert go["own_after_3s"] == [], ("official kept refreshing a record after the failure",
                                      go["own_after_3s"])
    assert go["keepalive_channels_after_exit"] == [], \
        ("a keepalive subscription outlived the official's failed start",
         go["keepalive_channels_after_exit"])
    assert go["ping_receivers_after_exit"] == 0, ("a keepalive subscriber outlived the "
                                                  "failed start", go["ping_receivers_after_exit"])
    assert go["dev_cert_warning"], ("official lost the dev-mode cert warning", go)

    # Positive controls (both sides): the occupied port is real, and the failed start
    # verifiably registered (HSET) and subscribed (Keepalive SUBSCRIBE) before it undid
    # the registration (HDEL). Without these, "no residue after exit" would also hold
    # for a server that failed before registering, making the cleanup contract vacuous.
    for side in ("official", "rust"):
        o = out[side]
        assert o["udp_occupied_while_held"], \
            (side, "the WebTransport UDP port was not actually occupied", o)
        assert not o["udp_occupied_after_release"], \
            (side, "the UDP port stayed occupied after the hold was released", o)
        assert o["registration_hset_observed"], \
            (side, "no HSET nodes observed before exit: the failed start never registered, "
             "so the no-residue contract would be vacuous", o["startup_command_order"])
        assert o["keepalive_subscribe_observed"], \
            (side, "no Keepalive SUBSCRIBE observed before exit", o["startup_command_order"])
        assert o["unregister_hdel_observed"], \
            (side, "no HDEL nodes after the HSET: the failed start did not undo its "
             "registration", o["startup_command_order"])

    comparisons = (
        ("nonzero-exit", go["exit"] not in (None, 0), rs["exit"] not in (None, 0)),
        ("udp-occupied-while-held", go["udp_occupied_while_held"], rs["udp_occupied_while_held"]),
        ("registration-hset-observed", go["registration_hset_observed"],
         rs["registration_hset_observed"]),
        ("keepalive-subscribe-observed", go["keepalive_subscribe_observed"],
         rs["keepalive_subscribe_observed"]),
        ("unregister-hdel-observed", go["unregister_hdel_observed"],
         rs["unregister_hdel_observed"]),
        ("sigkilled", go["sigkilled"], rs["sigkilled"]),
        ("own-immediately-after-exit", go["own_immediately_after_exit"],
         rs["own_immediately_after_exit"]),
        ("own-after-3s", go["own_after_3s"], rs["own_after_3s"]),
        ("keepalive-channels-after-exit", go["keepalive_channels_after_exit"],
         rs["keepalive_channels_after_exit"]),
        ("ping-receivers-after-exit", go["ping_receivers_after_exit"],
         rs["ping_receivers_after_exit"]),
        ("dev-cert-warning", go["dev_cert_warning"], rs["dev_cert_warning"]),
        ("unregister-error-line", bool(go["unregister_error_lines"]),
         bool(rs["unregister_error_lines"])),
        ("http-port-released", go["http_port_free_after_exit"],
         rs["http_port_free_after_exit"]),
    )
    for label, left, right in comparisons:
        if left != right:
            ctx["mismatch"](name, label, left, right)
    assert same_config(name, out)
    assert rs["own_immediately_after_exit"] == [], \
        ("the candidate left its node record behind after the WebTransport bind failed",
         rs["own_immediately_after_exit"], rs["webtransport_lines"])
    assert rs["own_after_3s"] == [], ("a worker outlived the candidate's failed start and "
                                      "kept refreshing", rs["own_after_3s"])
    assert rs["keepalive_channels_after_exit"] == [], \
        ("the candidate left a keepalive subscription behind after the failed start",
         rs["keepalive_channels_after_exit"])
    assert rs["ping_receivers_after_exit"] == 0, \
        ("the candidate's keepalive subscription survived the failed start",
         rs["ping_receivers_after_exit"])
    if go["stopping_router_debug_line"] != rs["stopping_router_debug_line"]:
        ctx["known_differences"].append({
            "id": REG_ITEM,
            "scope_of_this_item": (
                "which debug line marks the undo of a failed `Start()`. Upstream returns "
                "the bind error at `server.go:274` and only the deferred `UnregisterNode` "
                "runs; `RedisRouter.Stop()`, which prints `stopping RedisRouter` "
                "(`redisrouter.go:196`), is reached on the signal path instead. The "
                "candidate stops the router explicitly, because the contract certified "
                "here is that no worker or subscription outlives a failed start rather "
                "than that the process is about to die."),
            "difference": (
                f"official printed `stopping RedisRouter`: {go['stopping_router_debug_line']}; "
                f"candidate: {rs['stopping_router_debug_line']}."),
            "observed_in_this_run": {"official": go["stopping_router_debug_line"],
                                     "rust": rs["stopping_router_debug_line"]},
            "handling": (
                "recorded, not closed. The observable keyspace contract — non-zero exit, "
                "no residue, no continuing refresh, no surviving subscriber — is compared "
                "above and matches."),
        })
    return out


def vector_wt_bad_cert(official, rust, name, ctx):
    """P1-1's other half: `WebTransportTLS` is a *construction* step
    (`server.go:184-195`), ahead of `RemoveDeadNodes()` (:197) and of `RegisterNode()`
    (:222), so an unparsable cert pair must register nothing *and* prune nothing. The
    upstream rule being honoured: a non-empty cert+key pair fails with
    `tls.LoadX509KeyPair`'s own error even in dev mode — only the empty pair falls back
    to the generated self-signed cert."""
    store = ctx["store"]
    out = {}
    stale_id = "ND_21q_stale_cert"
    for side, binary in (("official", official), ("rust", rust)):
        p = ctx["side_ports"](side)
        cert = ctx["staging"] / f"bad-{side}.crt"
        key = ctx["staging"] / f"bad-{side}.key"
        cert.write_text("not a certificate\n")
        key.write_text("not a private key\n")
        body = config_body(p["http"], p["tcp"], p["udp"], p["wt"], ctx["redis_addr"],
                           node_stats="{stats_update_interval: 1s}", level="debug",
                           wt_tls=f", tls_cert_file: {cert}, tls_key_file: {key}")
        seeded = store.seed({stale_id: node_proto(stale_id, int(time.time()) - STALE_SECONDS)})
        run = Run(side, binary, name, p, body, ctx["staging"], ctx["output"],
                  store, ctx["procs"])
        if not run.wait_exit():
            run.force_stop()
        time.sleep(1.5)
        text = run.text()
        names = store.names()
        out[side] = {"normalized_config": run.normalized_config,
                     "config_sha256": run.config_sha256,
                     "exit": run.exit_code, "sigkilled": run.sigkilled,
                     "own_entries": [n for n in names if n not in seeded],
                     "stale_survived": stale_id in names,
                     "keyspace": names,
                     "stopping_router_debug_line": bool(log_lines(
                         text, "stopping RedisRouter")),
                     "unregister_error_lines": log_lines(text, "could not unregister node"),
                     "dev_cert_warning": bool(log_lines(
                         text, "generated self-signed cert (dev mode)")),
                     "tls_lines": [line[:300] for line in text.splitlines()
                                   if "certificate" in line.lower()
                                   or "tls" in line.lower()][:4],
                     "error_lines": error_lines(text)[:4]}
    go, rs = out["official"], out["rust"]

    assert go["exit"] not in (None, 0), ("official accepted an unparsable cert pair", go)
    assert not go["sigkilled"], ("official hung on the cert failure", go)
    assert go["own_entries"] == [], ("official registered a node despite the TLS failure",
                                     go["own_entries"])
    assert go["stale_survived"], ("official pruned before building the WebTransport TLS "
                                  "config — the construction order changed", go["keyspace"])
    assert not go["dev_cert_warning"], ("official generated a dev cert despite the "
                                        "explicit pair", go)
    assert not go["unregister_error_lines"], ("official tried to unregister a node it "
                                              "never registered", go["unregister_error_lines"])
    assert not go["stopping_router_debug_line"], ("official stopped a router that never "
                                                  "started", go)

    comparisons = (
        ("nonzero-exit", go["exit"] not in (None, 0), rs["exit"] not in (None, 0)),
        ("sigkilled", go["sigkilled"], rs["sigkilled"]),
        ("own-entries", go["own_entries"], rs["own_entries"]),
        ("stale-survived", go["stale_survived"], rs["stale_survived"]),
        ("dev-cert-warning", go["dev_cert_warning"], rs["dev_cert_warning"]),
        ("stopping-router-line", go["stopping_router_debug_line"],
         rs["stopping_router_debug_line"]),
        ("unregister-error-line", bool(go["unregister_error_lines"]),
         bool(rs["unregister_error_lines"])),
    )
    for label, left, right in comparisons:
        if left != right:
            ctx["mismatch"](name, label, left, right)
    assert same_config(name, out)
    assert rs["own_entries"] == [], ("the candidate registered a node despite the TLS "
                                     "failure", rs["own_entries"], rs["tls_lines"])
    assert rs["stale_survived"], ("the candidate pruned before building the WebTransport "
                                  "TLS config — construction order differs from upstream",
                                  rs["keyspace"])
    assert not rs["dev_cert_warning"], ("the candidate fell back to a dev cert despite the "
                                        "explicit pair", rs)
    assert not rs["stopping_router_debug_line"], ("the candidate stopped a router it never "
                                                  "started", rs)
    return out


def vector_single_node(official, rust, name, ctx):
    """`redis` unset means `LocalRouter`: `RegisterNode` is a no-op, so the keyspace
    must stay empty while `/` keeps answering 200 off the local stats tick."""
    store = ctx["store"]
    out = {}
    for side, binary in (("official", official), ("rust", rust)):
        p = ctx["side_ports"](side)
        body = config_body(p["http"], p["tcp"], p["udp"], None, "",
                           node_stats="{stats_update_interval: 1s}")
        store.clear()
        run = Run(side, binary, name, p, body, ctx["staging"], ctx["output"],
                  store, ctx["procs"])
        reached = run.wait_http()
        assert reached is not None, (side, "no HTTP", run.text()[-1500:])
        time.sleep(SETTLE_SECONDS + AVAILABILITY_WINDOW_SECONDS)
        after_window = health(p["http"])
        names = store.names()
        stop = run.interrupt()
        text = run.text()
        out[side] = {"normalized_config": run.normalized_config,
                     "config_sha256": run.config_sha256,
                     "status_first": reached[0], "body_first": reached[1],
                     "status_after_window": after_window[0],
                     "body_after_window": after_window[1],
                     "keyspace": names, "exit": stop["exit"],
                     "sigkilled": stop["sigkilled"],
                     "redis_dial_lines": log_lines(text, "connecting to redis"),
                     "error_lines": error_lines(text)[:4]}
    go, rs = out["official"], out["rust"]

    assert go["keyspace"] == [], ("official wrote the nodes hash with no Redis config",
                                  go["keyspace"])
    assert go["status_first"] == go["status_after_window"] == 200, \
        ("official / not ready in single-node routing", go)
    assert go["redis_dial_lines"] == [], ("official dialled Redis with no Redis config",
                                          go["redis_dial_lines"])

    comparisons = (
        ("keyspace", go["keyspace"], rs["keyspace"]),
        ("status-first", go["status_first"], rs["status_first"]),
        ("body-first", go["body_first"], rs["body_first"]),
        ("status-after-window", go["status_after_window"], rs["status_after_window"]),
        ("body-after-window", go["body_after_window"], rs["body_after_window"]),
        ("redis-dial-line-count", len(go["redis_dial_lines"]), len(rs["redis_dial_lines"])),
        ("exit", go["exit"], rs["exit"]),
        ("sigkilled", go["sigkilled"], rs["sigkilled"]),
        ("error-line-count", len(go["error_lines"]), len(rs["error_lines"])),
    )
    for label, left, right in comparisons:
        if left != right:
            ctx["mismatch"](name, label, left, right)
    assert same_config(name, out)
    assert rs["keyspace"] == [], ("the candidate wrote the nodes hash with no Redis "
                                  "config", rs["keyspace"])
    assert rs["status_after_window"] == 200, ("the candidate lost readiness in single-node "
                                              "routing", rs)
    return out


# ============================================================================== main


def main() -> int:
    if not __debug__:
        raise RuntimeError("run without Python -O: acceptance assertions are required")
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", required=True, type=pathlib.Path)
    ap.add_argument("--rust", required=True, type=pathlib.Path)
    ap.add_argument("--output", required=True, type=pathlib.Path)
    ap.add_argument("--base-port", type=int, default=21900)
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
    staging = pathlib.Path(tempfile.mkdtemp(prefix="node-lifecycle-21q-"))

    slots = ("go_a", "go_b", "rs_a", "rs_b")
    ports = {"master": args.base_port}
    for index, slot in enumerate(slots):
        for offset, role in enumerate(("http", "tcp", "udp", "wt")):
            ports[f"{slot}_{role}"] = args.base_port + 1 + index * 4 + offset
    for name, port in ports.items():
        assert free(port), f"port {port} ({name}) is already in use"
    # UDP occupancy is asserted by UdpHold when the failure vector binds it.

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
            f"real paired node-registration lifecycle for {REG_ITEM}: the HSET of "
            "`Start()` (server.go:222) and its identity fields, the psrpc keepalive "
            "heartbeat that keeps the record inside `selector.IsAvailable`'s 5 s window "
            "and the `/` health check's literal 4 s, the HDEL on graceful stop, no "
            "revival and no surviving subscriber after the stop, two servers on one Redis "
            "(go+go, rs+rs, go+rs — each revoking only its own record, both visible to "
            "`list-nodes` from both binaries), the stale/garbage/wrong-type ping rules, "
            "the `stats_max_delay` delayed diagnostic, recovery across a real redis-server "
            "restart and a real CLIENT KILL, and both WebTransport startup failure orders "
            "(bad cert registers nothing and prunes nothing; an occupied UDP port "
            f"registers then undoes). Also consumes {INTERVAL_ITEM}, {MAX_DELAY_ITEM}, "
            f"{REGION_ITEM}, {WT_PORT_ITEM}, {WT_CERT_ITEM} and {HEALTH_ITEM} on the real "
            f"start path. Not covered: the content of NodeStats past started_at/updated_at "
            f"({STATS_ITEM}), cross-node room routing, the WebTransport session plane, "
            f"the connection-count difference ({CHURN_ITEM}, recorded below), the psrpc "
            f"topic surface ({CHANNEL_ITEM}, recorded below), a *slow* rather than dead "
            "Redis during shutdown (the bounded worker join is not exercised here), and "
            "a panicking worker's release reporting."),
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
            "identity fields are compared after masking the random node id, which both "
            "sides are asserted to derive as an `ND_` id equal to the `nodeID` of their "
            "own readiness line; timestamps are compared as relations (`started_at` fixed, "
            "`updated_at` advancing, the 406 body against a Go rendering computed here "
            "from the record Redis holds), not as values. Config bytes are asserted equal "
            "per slot after masking ports and generated paths. Raw logs, full keyspaces, "
            "both config hashes and every original exit code are kept in the receipt; "
            "`drain_seconds` and the recovery latencies are recorded per side, not "
            "compared."),
        "cases": [],
        "mismatches": [],
        "known_differences": [],
    }

    def raw_resp_json(value):
        # RESP simple strings are bytes in this independent parser. Preserve
        # their wire identity instead of losing the entire final receipt.
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
        ("register-refresh-revoke", vector_register),
        ("sigkill-leaves-record", vector_sigkill),
        ("two-servers-shared-redis", vector_pair),
        ("keepalive-ping-drives-refresh", vector_ping_refresh),
        ("health-406-when-stale", vector_health),
        ("delayed-stats-diagnostic", vector_delayed_stats),
        ("redis-restart-recovery", vector_restart),
        ("client-kill-recovery", vector_client_kill),
        ("wt-bind-failure-leaves-no-residue", vector_wt_bind_failure),
        ("wt-bad-cert-no-residue", vector_wt_bad_cert),
        ("single-node-mode-writes-nothing", vector_single_node),
    ]
    selected = [(name, fn) for name, fn in vectors
                if args.only is None or args.only in name]
    assert selected, ("no vector matched", args.only)
    report["campaign_vectors"] = [name for name, _ in vectors]
    report["vectors_run"] = [name for name, _ in selected]
    report["selection_filter"] = args.only

    redis_box: dict = {"proc": None}
    procs: list = []
    holds: list[UdpHold] = []
    master_port = ports["master"]
    code = 1

    def interrupted(signum, _frame):
        raise InterruptedError(f"harness interrupted by signal {signum}")

    previous_handlers = {sig: signal.signal(sig, interrupted)
                         for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        save()  # A killed run is visibly incomplete, even before its first vector.
        redis_box["proc"] = redis_server(master_port, staging, "main")
        ctx = {"staging": staging, "output": args.output, "store": Store(master_port),
               "redis_addr": f"  address: 127.0.0.1:{master_port}\n",
               "master_port": master_port, "ports": ports, "side_ports": side_ports,
               "mismatch": mismatch, "redis_box": redis_box, "holds": holds,
               "procs": procs, "save": save,
               "known_differences": report["known_differences"]}
        for name, fn in selected:
            report["active_vector"] = name
            save()
            record = fn(official, rust, name, ctx)
            record["id"] = name
            report["cases"].append(record)
            save()
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
            "PASS_CLI_REDIS_NODE_REGISTRATION_LIFECYCLE_PAIRED"
            if report["complete_campaign"] else "PASS_SELECTED_VECTORS_ONLY")
        report["status"] = report["aggregate_status"]
        report["active_vector"] = None
        code = 0
    except BaseException as exc:
        report["status"] = "FAIL"
        report["failure"] = f"{type(exc).__name__}: {exc}"
        code = 1
    finally:
        # A second termination signal must not interrupt owned-process cleanup.
        for sig in previous_handlers:
            signal.signal(sig, signal.SIG_IGN)
        cleanup_errors = []
        cleanup = ([hold.close for hold in holds]
                   + [lambda proc=proc: stop_process(proc) for proc in procs]
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
