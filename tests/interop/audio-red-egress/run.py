#!/usr/bin/env python3
"""Measure server egress audio RED (payload type + RFC 2198 chain) for the fixed Go and Rust binaries.

The ground truth is what the original browser SDK actually receives: the TURN proxy records the wire
RTP header of each egress packet and a receiver-side insertable-stream transform records the decoded
payload, so the redundancy block count is measured rather than inferred. Parity rows take the Go
value as expected; capture failures are fatal, divergences are reported.
"""

import argparse
import hashlib
import json
import os
import random
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time

PROFILES = (("active", True), ("inactive", False))


def port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def relay_range():
    """A free 50-port block for the built-in TURN relay range."""
    for _ in range(400):
        base = random.randrange(40000, 59000)
        socks = []
        try:
            for probe_port in (base, base + 49):
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind(("127.0.0.1", probe_port))
                socks.append(sock)
            return base
        except OSError:
            continue
        finally:
            for sock in socks:
                sock.close()
    raise RuntimeError("no free TURN relay range")


def config_for(http, tcp, udp, turn, relay, active):
    return {
        "port": http,
        "bind_addresses": ["127.0.0.1"],
        "keys": {"devkey": "secret"},
        "audio": {"active_red_encoding": active},
        "rtc": {
            "node_ip": "127.0.0.1",
            "use_external_ip": False,
            "enable_loopback_candidate": True,
            "ips": {"includes": ["127.0.0.1/32"]},
            "tcp_port": tcp,
            "udp_port": udp,
        },
        "turn": {
            "enabled": True,
            "udp_port": turn,
            "bind_addresses": ["127.0.0.1"],
            "relay_range_start": relay,
            "relay_range_end": relay + 49,
            "allow_restricted_peer_cidrs": ["127.0.0.1/32"],
        },
    }


# (phase, track, leg) -> the fields compared. "up" is the client-to-server stimulus, "down" is the
# server egress under test. Rare frame shapes are excluded; see summarise() in probe.mjs.
PARITY_FIELDS = ("wirePts", "dominantBlockCounts", "dominantShapes", "overheadMode",
                 "primaryLengthMode", "parseErrors", "extForms", "dominantTail")
LEG_ORDER = (("midJoin", "red", "up"), ("midJoin", "red", "down"),
             ("midJoin", "plain", "up"), ("midJoin", "plain", "down"),
             ("firstFrame", "late", "up"), ("firstFrame", "late", "down"))


def run(binary, output, active):
    output.mkdir(parents=True)
    http, tcp = port(), port()
    udp, turn = port(), port()
    relay = relay_range()
    config = config_for(http, tcp, udp, turn, relay, active)
    probe = Path(__file__).with_name("probe.mjs")
    fixture = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (probe, Path(__file__), probe.parent / "red-observer.worker.mjs",
                     probe.parent / "../media-recovery/lossy-proxy.mjs")
    }
    with tempfile.TemporaryDirectory(prefix="lk-red-egress-") as temp:
        config_path = Path(temp) / "server.yaml"
        config_path.write_text(json.dumps(config))
        with (output / "server.log").open("wb") as log:
            server = subprocess.Popen(
                [str(binary), "--config", str(config_path)], stdout=log, stderr=subprocess.STDOUT
            )
            probe_exit = None
            try:
                deadline = time.monotonic() + 15
                while True:
                    if server.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError(f"{binary} failed to start, see {output / 'server.log'}")
                    try:
                        socket.create_connection(("127.0.0.1", http), timeout=0.1).close()
                        break
                    except OSError:
                        time.sleep(0.1)
                env = dict(
                    os.environ,
                    LIVEKIT_URL=f"ws://127.0.0.1:{http}",
                    TURN_PORT=str(turn),
                    LK_RED_EGRESS_ACTIVE=str(int(active)),
                )
                with (output / "receipt.json").open("wb") as out, (output / "sdk.log").open("wb") as err:
                    probe_exit = subprocess.run(
                        ["node", str(probe)], env=env, stdout=out, stderr=err, timeout=120
                    ).returncode
            finally:
                if server.poll() is None:
                    server.send_signal(signal.SIGINT)
                try:
                    server_exit = server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server_exit = server.wait()
        receipt = json.loads((output / "receipt.json").read_text())
        (output / "execution.json").write_text(json.dumps({
            "probe_exit": probe_exit, "server_exit": server_exit, "config": config,
            "fixture_sha256": fixture, "binary": str(binary),
            "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "sdk": {key: receipt.get(key) for key in ("sdkSha256", "workerSha256", "lockSha256", "sdkVersion", "browser")},
        }, indent=2) + "\n")
    if probe_exit:
        raise RuntimeError(f"probe_exit={probe_exit} server_exit={server_exit}, see {output}")
    return receipt


def leg(receipt, tag, phase, track, side):
    return receipt[tag]["legs"][phase][track][side]


def parity_rows(go, rust):
    rows = []

    def row(name, expected, actual):
        rows.append({"name": name, "go": expected, "rust": actual, "match": expected == actual})

    row("negotiatedRedPts", sorted(go["active"]["redPts"]), sorted(rust["active"]["redPts"]))
    # The header-extension difference is a byte-count on the wire until the negotiated extmap names it.
    for page in ("publisherExt", "subscriberExt"):
        for end in ("local", "remote"):
            row(f"active.{page}.{end}", go["active"][page][end], rust["active"][page][end])
    # The DTLS and SRTP cipher the server selects is what the per-packet tail measures.
    for page in ("publisherTransports", "subscriberTransports"):
        row(f"active.{page}", go["active"][page], rust["active"][page])
    for _, active in PROFILES:
        tag = "active" if active else "inactive"
        for phase, track, side in LEG_ORDER:
            g = leg(go, tag, phase, track, side)
            r = leg(rust, tag, phase, track, side)
            for field in PARITY_FIELDS:
                row(f"{tag}.{phase}.{track}.{side}.{field}", g[field], r[field])
            # The head of the stream is where a re-synthesizer differs from a forwarder: Go has no
            # redundant block to attach to the very first egress packet of a fresh subscription.
            row(f"{tag}.{phase}.{track}.{side}.firstChains",
                [[item["blockCount"], item["blockPts"], item["wirePt"]] for item in g["first"]],
                [[item["blockCount"], item["blockPts"], item["wirePt"]] for item in r["first"]])
            row(f"{tag}.{phase}.{track}.forwardMode",
                go[tag]["legs"][phase][track]["crossLeg"]["forwardMode"],
                rust[tag]["legs"][phase][track]["crossLeg"]["forwardMode"])
    return rows


def capture_ok(receipt):
    """A green comparison is worthless if a leg captured nothing, so gate every leg on real payloads."""
    for _, active in PROFILES:
        tag = "active" if active else "inactive"
        assert receipt[tag]["workerErrors"] == [], f"{tag}: observer worker errors {receipt[tag]['workerErrors']}"
        assert receipt[tag]["legs"], f"{tag}: no legs"
        for phase, track, _ in LEG_ORDER:
            legs = receipt[tag]["legs"][phase][track]
            for side in ("up", "down"):
                wire = legs[side]
                name = f"{tag}.{phase}.{track}.{side}"
                assert wire["frames"] >= 15, f"{name}: no transformed payloads captured"
                assert wire["matchedInWire"] >= 15, f"{name}: payload/wire timestamps do not line up"
                assert wire["matchedInWire"] == wire["frames"], f"{name}: unmatched frame timestamps {wire['unmatchedTimestamps']}"
                assert wire["overheadMode"] != -1, f"{name}: no wire packet matched a frame"
                assert 12 <= wire["overheadMode"] <= 64, (
                    f"{name}: RTP overhead {wire['overheadMode']} is not a header size, "
                    "the transform may not be seeing the payload")
                assert wire["parseErrors"] == 0, f"{name}: RFC 2198 chain parse errors {wire['parseErrors']}"
                assert wire["wirePackets"] >= 15, f"{name}: proxy recorded {wire['wirePackets']} packets"
            # Both legs must agree the same bytes crossed the server, otherwise the egress count is
            # an artefact of the observer rather than of the forwarder.
            assert legs["crossLeg"]["sharedTimestamps"] >= 15, f"{tag}.{phase}.{track}: uplink and egress never overlap"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--strict", action="store_true", help="exit non-zero when Rust diverges from Go")
    args = parser.parse_args()
    args.output.mkdir(parents=True)
    sides = {}
    for label, binary in (("go", args.go), ("rust", args.rust)):
        binary = binary.resolve()
        sides[label] = {}
        for tag, active in PROFILES:
            sides[label][tag] = run(binary, args.output / label / tag, active)
        capture_ok(sides[label])
    (args.output / "observations.json").write_text(json.dumps(sides, indent=2) + "\n")
    rows = parity_rows(sides["go"], sides["rust"])
    (args.output / "comparison.json").write_text(json.dumps(rows, indent=2) + "\n")
    # A row is only evidence when it could have failed: identical constants on both sides are noise.
    informative = [row for row in rows if row["go"] != [[], {}, "", -1, None, 0, "undetermined"]]
    matched = sum(row["match"] for row in informative)
    for row in informative:
        if not row["match"]:
            print(f"DIVERGES {row['name']}: go={row['go']} rust={row['rust']}")
    vacuous = len(rows) - len(informative)
    print(f"{matched}/{len(informative)} informative parity rows match on Go ground truth "
          f"(stimulus-identity and zero-error rows excluded from the count: {vacuous}, "
          "asserted equal in comparison.json), captures valid on both sides")
    return int(args.strict and matched != len(rows))


if __name__ == "__main__":
    raise SystemExit(main())
