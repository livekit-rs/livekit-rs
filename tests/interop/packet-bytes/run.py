#!/usr/bin/env python3
"""Measure livekit_packet_bytes on one server binary under real official-SDK traffic.

Two per-project instances are started from the same YAML shape the Region Agent
generates (livekit-region-agent/instances.go instanceYAML). The unmodified
official server-sdk-go client drives a real call. Every scrape is kept verbatim
and also reduced with the Agent's own parser (metrics.go scrapeOutgoingBytes).
"""
import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOM = "meeting-001"
FAMILY = "livekit_packet_bytes"


def identity(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def save(path, data):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n")
    tmp.replace(path)


def free_port_base():
    """Reserve 20 consecutive ports so two slots never collide with a neighbour."""
    for _ in range(200):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            base = probe.getsockname()[1]
        if base % 10:
            base -= base % 10
        if base < 20000 or base > 60000:
            continue
        held = []
        try:
            for offset in range(20):
                s = socket.socket()
                s.bind(("127.0.0.1", base + offset))
                held.append(s)
        except OSError:
            continue
        finally:
            for s in held:
                s.close()
        if len(held) == 20:
            return base
    raise RuntimeError("no free 20-port window")


def b64(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def admin_token(key, secret, grant):
    """Official LiveKit server-API JWT: HS256, iss=apiKey, video grants."""
    now = int(time.time())
    head = b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    body = b64(
        json.dumps(
            {"iss": key, "nbf": now - 5, "exp": now + 120, "sub": "packet-bytes-probe", "video": grant},
            separators=(",", ":"),
        ).encode()
    )
    sig = b64(hmac.new(secret.encode(), f"{head}.{body}".encode(), hashlib.sha256).digest())
    return f"{head}.{body}.{sig}"


def twirp(url, method, key, secret, grant, payload):
    req = urllib.request.Request(
        f"{url}/twirp/livekit.RoomService/{method}",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + admin_token(key, secret, grant),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode()


def scrape(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10) as resp:
        return resp.status, resp.headers.get("Content-Type", ""), resp.read().decode()


def agent_outgoing_total(text):
    """Byte-for-byte port of livekit-region-agent/metrics.go scrapeOutgoingBytes."""
    total = 0.0
    matched = []
    for line in text.splitlines():
        if not line.startswith(FAMILY + "{") or 'direction="outgoing"' not in line:
            continue
        idx = line.rfind(" ")
        try:
            total += float(line[idx + 1 :])
        except ValueError:
            continue
        matched.append(line)
    return int(total), matched


def series(text):
    """Every sample of the family, with its parsed labels, in exposition order.

    Matches the exact family only: the node also exposes the unrelated
    livekit_packet_bytes_in / _out active-room gauges, which share the prefix.
    """
    out = []
    for line in text.splitlines():
        if line.startswith("#") or not line.startswith(FAMILY + "{"):
            continue
        head, _, value = line.rpartition(" ")
        name, _, rest = head.partition("{")
        labels = dict(re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', rest))
        out.append({"sample": name, "labels": labels, "value": float(value)})
    return out


def declarations(text):
    return [
        l
        for l in text.splitlines()
        if l.startswith(("# HELP ", "# TYPE ")) and l.split()[2] == FAMILY
    ]


class Instance:
    def __init__(self, binary, directory, slot, base, key, secret):
        self.binary, self.slot, self.base = binary, slot, base
        self.key, self.secret = key, secret
        self.http = base + slot * 10
        self.tcp = self.http + 1
        self.udp = self.http + 2
        self.prom = self.http + 3
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.proc = None
        self.log = None

    def yaml(self):
        # Same fields and order as the Agent's instanceYAML; no extra tuning.
        return (
            f'port: {self.http}\nbind_addresses: ["127.0.0.1"]\n'
            f"rtc:\n  tcp_port: {self.tcp}\n  udp_port: {self.udp}\n"
            f"prometheus:\n  port: {self.prom}\n"
            f"keys:\n  {self.key}: {self.secret}\n"
            f"webhook:\n  api_key: {self.key}\n  urls:\n"
            f"    - http://127.0.0.1:1/webhook/unused\n"
            f"logging:\n  level: info\n"
        )

    def start(self):
        cfg = self.dir / "livekit.yaml"
        cfg.write_text(self.yaml())
        self.log = open(self.dir / "livekit.log", "ab")
        self.proc = subprocess.Popen(
            [str(self.binary), "--config", str(cfg)],
            cwd=str(self.dir),
            stdout=self.log,
            stderr=subprocess.STDOUT,
            env={"HOME": str(self.dir), "PATH": os.environ["PATH"]},
            start_new_session=True,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"instance exited {self.proc.returncode}: {self.dir}/livekit.log")
            try:
                with socket.create_connection(("127.0.0.1", self.http), 0.5):
                    return cfg
            except OSError:
                time.sleep(0.2)
        raise RuntimeError(f"instance never became ready: {self.dir}/livekit.log")

    def stop(self):
        if self.proc is None or self.proc.poll() is not None:
            return
        os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            self.proc.wait(timeout=10)
        if self.log:
            self.log.close()
            self.log = None


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--server-bin", type=Path, required=True)
    p.add_argument("--side", required=True, help="label recorded with every measurement")
    p.add_argument("--client-bin", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--port-base", type=int)
    p.add_argument("--call-timeout", type=int, default=120)
    p.add_argument("--settle", type=int, default=7, help="seconds to wait past one Go stats tick")
    a = p.parse_args()
    if not __debug__:
        p.error("assertions must stay enabled")
    a.server_bin = a.server_bin.resolve()
    a.client_bin = a.client_bin.resolve()
    a.output = a.output.resolve()
    a.output.mkdir(parents=True, exist_ok=False)
    base = a.port_base or free_port_base()

    receipt = {
        "status": "RUNNING",
        "side": a.side,
        "runner": identity(__file__),
        "server_binary": identity(a.server_bin),
        "client_binary": identity(a.client_bin),
        "port_base": base,
        "commands": [],
        "scrapes": [],
        "phases": {},
    }
    path = a.output / "execution.json"
    save(path, receipt)

    keys = [
        ("APIkeyPacketBytesA", "fixture-secret-a-0123456789abcdef0123456789abcdef"),
        ("APIkeyPacketBytesB", "fixture-secret-b-0123456789abcdef0123456789abcdef"),
    ]
    work = a.output / "instances"
    instances = [
        Instance(a.server_bin, work / f"p{i}", i, base, *keys[i]) for i in range(2)
    ]

    def take(phase, inst):
        status, ctype, text = scrape(inst.prom)
        name = f"{phase}-p{inst.slot}.txt"
        (a.output / name).write_text(text)
        total, matched = agent_outgoing_total(text)
        row = {
            "phase": phase,
            "slot": inst.slot,
            "http_status": status,
            "content_type": ctype,
            "file": name,
            "sha256": hashlib.sha256(text.encode()).hexdigest(),
            "declarations": declarations(text),
            "series": series(text),
            "agent_outgoing_total": total,
            "agent_matched_lines": matched,
            "at_ms": int(time.time() * 1000),
        }
        receipt["scrapes"].append(row)
        save(path, receipt)
        return row

    try:
        for inst in instances:
            cfg = inst.start()
            receipt["commands"].append(
                {"argv": [str(a.server_bin), "--config", str(cfg)], "slot": inst.slot, "pid": inst.proc.pid}
            )
        save(path, receipt)
        for inst in instances:
            take("idle", inst)

        url1 = f"ws://127.0.0.1:{instances[0].http}"
        url2 = f"ws://127.0.0.1:{instances[1].http}"
        argv = [
            str(a.client_bin), "call",
            url1, keys[0][0], keys[0][1],
            url2, keys[1][0], keys[1][1],
        ]
        started = time.monotonic()
        call = subprocess.run(argv, capture_output=True, timeout=a.call_timeout)
        (a.output / "call.stdout").write_bytes(call.stdout)
        (a.output / "call.stderr").write_bytes(call.stderr)
        receipt["commands"].append(
            {
                "argv": argv,
                "exit_code": call.returncode,
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
        )
        try:
            receipt["phases"]["call"] = json.loads(call.stdout.decode())
        except (ValueError, UnicodeDecodeError):
            receipt["phases"]["call"] = {"unparsed_stdout": True}
        save(path, receipt)
        for inst in instances:
            take("after-call", inst)
        # Go samples connection/signal stats on a 5s ticker plus a final flush on
        # close (connectionquality/connectionstats.go:34, signalanddatastats.go:166),
        # so a scrape taken right after the SDK disconnects can still be behind.
        # Rust counts at the write itself. Settling past one tick separates that
        # sampling lag from a real accounting difference.
        time.sleep(a.settle)
        for inst in instances:
            take("after-call-settled", inst)

        # Room close through the official admin API. DeleteRoom
        # (references/livekit-stable/pkg/service/roomservice.go:140) calls
        # EnsureCreatePermission (auth.go:200), so a roomAdmin-only token is
        # rejected and would leave the room open, making this phase vacuous.
        closes = []
        for inst in instances:
            status, body = twirp(
                f"http://127.0.0.1:{inst.http}", "DeleteRoom", inst.key, inst.secret,
                {"roomCreate": True}, {"room": ROOM},
            )
            listed, listing = twirp(
                f"http://127.0.0.1:{inst.http}", "ListRooms", inst.key, inst.secret,
                {"roomList": True}, {},
            )
            closes.append(
                {
                    "slot": inst.slot,
                    "http_status": status,
                    "body": body,
                    "list_status": listed,
                    "room_still_listed": ROOM in listing,
                }
            )
        receipt["phases"]["delete_room"] = closes
        receipt["phases"]["delete_room_ok"] = all(
            c["http_status"] == 200 and c["list_status"] == 200 and not c["room_still_listed"]
            for c in closes
        )
        save(path, receipt)
        time.sleep(7)
        for inst in instances:
            take("after-room-close", inst)

        # A second call in a fresh generation of the same room name must add to
        # the same process counter, never restart it.
        call2 = subprocess.run(argv, capture_output=True, timeout=a.call_timeout)
        (a.output / "call2.stdout").write_bytes(call2.stdout)
        (a.output / "call2.stderr").write_bytes(call2.stderr)
        receipt["commands"].append({"argv": argv, "exit_code": call2.returncode, "run": 2})
        save(path, receipt)
        for inst in instances:
            take("after-second-call", inst)

        # Process restart: the Agent treats a lower total as a fresh baseline.
        before = {i.slot: receipt["scrapes"][-2 + i.slot]["agent_outgoing_total"] for i in instances}
        instances[0].stop()
        instances[0].start()
        time.sleep(2)
        restarted = take("after-restart", instances[0])
        receipt["phases"]["restart"] = {
            "slot": 0,
            "total_before": before[0],
            "total_after": restarted["agent_outgoing_total"],
        }
        save(path, receipt)

        receipt["status"] = "MEASURED"
        save(path, receipt)
    finally:
        for inst in instances:
            inst.stop()
        save(path, receipt)

    calls = [c for c in receipt["commands"] if "exit_code" in c]
    failed = [c for c in calls if c["exit_code"] != 0]
    close_ok = receipt["phases"].get("delete_room_ok", False)
    print(f"SIDE={a.side} PORT_BASE={base} SCRAPES={len(receipt['scrapes'])}")
    for row in receipt["scrapes"]:
        print(
            f"  {row['phase']:<20} p{row['slot']} "
            f"outgoing={row['agent_outgoing_total']:<12} series={len(row['series'])}"
        )
    print(f"DELETE_ROOM_VERIFIED={close_ok}")
    print(f"COMMAND_FAILURES={len(failed)}")
    return 0 if not failed and close_ok else 1


if __name__ == "__main__":
    sys.exit(main())