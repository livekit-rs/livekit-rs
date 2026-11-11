#!/usr/bin/env python3
"""Compare SDK-published Opus/RED over real TURN, recording wire PTs and decoded samples."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time


def port(kind):
    with socket.socket(socket.AF_INET, kind) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run(binary, output, active, loss, video=False, encrypt_plain=False):
    output.mkdir()
    http, tcp = port(socket.SOCK_STREAM), port(socket.SOCK_STREAM)
    udp, turn = port(socket.SOCK_DGRAM), port(socket.SOCK_DGRAM)
    relay = 37000 + (os.getpid() % 190) * 50
    config = {
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
    receipt = output / "sdk.json"
    probe = Path(__file__).with_name("probe.mjs")
    fixture = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (Path(__file__), probe, probe.parent / "../media-recovery/lossy-proxy.mjs")
    }
    with tempfile.TemporaryDirectory(prefix="lk-audio-red-") as temp:
        config_path = Path(temp) / "server.yaml"
        config_path.write_text(json.dumps(config))
        with (output / "server.log").open("wb") as log:
            server = subprocess.Popen(
                [str(binary), "--config", str(config_path)], stdout=log, stderr=subprocess.STDOUT
            )
            probe_exit = None
            try:
                deadline = time.monotonic() + 10
                while True:
                    if server.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("server failed to start")
                    try:
                        socket.create_connection(("127.0.0.1", http), timeout=0.1).close()
                        break
                    except OSError:
                        time.sleep(0.1)
                env = dict(
                    os.environ, LIVEKIT_URL=f"ws://127.0.0.1:{http}", TURN_PORT=str(turn),
                    LK_AUDIO_RED_LOSS=str(int(loss)),
                    LK_AUDIO_RED_VIDEO=str(int(video)),
                    LK_AUDIO_RED_ENCRYPT_PLAIN=str(int(encrypt_plain)),
                )
                with receipt.open("wb") as out, (output / "sdk.log").open("wb") as err:
                    probe_exit = subprocess.run(
                        ["node", str(probe)],
                        env=env, stdout=out, stderr=err, timeout=90,
                    ).returncode
            finally:
                if server.poll() is None:
                    server.send_signal(signal.SIGINT)
                try:
                    server_exit = server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server_exit = server.wait()
                (output / "execution.json").write_text(json.dumps({
                    "probe_exit": probe_exit, "server_exit": server_exit, "config": config,
                    "fixture_sha256": fixture,
                    "profile": {"loss": loss, "video": video, "encrypt_plain": encrypt_plain},
                    "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                }, indent=2) + "\n")
    if probe_exit or server_exit:
        raise RuntimeError(f"probe={probe_exit} server={server_exit}; see {output}")
    return json.loads(receipt.read_text())


def compare(go, rust, loss, video=False, encrypt_plain=False):
    fields = ["sdkSha256", "workerSha256", "lockSha256", "sdkVersion", "browser"]
    assert all(go[key] == rust[key] for key in fields), "different SDK or browser identities"
    assert [row["single"] for row in go["runs"]] == [True, False]
    assert [row["single"] for row in rust["runs"]] == [True, False]
    rows = []
    for g, r in zip(go["runs"], rust["runs"]):
        def gate(name, reference, candidate, expected):
            rows.append({
                "single": g["single"], "name": name, "go": reference,
                "rust": candidate, "expected": expected,
                "match": reference == candidate == expected,
            })
        for stage, track, pt in (
            ("uplink", "plain", "111"), ("uplink", "red", "63"),
            ("downlink", "plain", "111"), ("downlink", "red", "63"),
        ):
            gate(f"{stage}.{track}", sorted(g[stage][track]["packetsByPt"]),
                 sorted(r[stage][track]["packetsByPt"]), [pt])
        for stage in ("plainOnly", "after"):
            gate(f"{stage}.plain", sorted(g[stage]["packetsByPt"]),
                 sorted(r[stage]["packetsByPt"]), ["111"])
        if encrypt_plain:
            gate("encryption.plain", g["tones"]["plain"]["encrypted"], r["tones"]["plain"]["encrypted"], True)
            gate("encryption.red", g["tones"]["red"]["encrypted"], r["tones"]["red"]["encrypted"], False)
        if video:
            def video_continues(case, stage, previous):
                current, before = case["video"][stage], case["video"][previous]
                return current["ssrc"] == before["ssrc"] and current["frames"] > before["frames"]

            for stage in ("initial", "uplink", "mixed", "after"):
                gate(f"video.{stage}.codec", g["video"][stage]["codec"].lower(),
                     r["video"][stage]["codec"].lower(), "video/vp8")
            gate("video.mixed.continues", video_continues(g, "mixed", "initial"),
                 video_continues(r, "mixed", "initial"), True)
            gate("video.after.continues", video_continues(g, "after", "mixed"),
                 video_continues(r, "after", "mixed"), True)
            for stage in ("initial", "mixed", "after"):
                gate(f"video.{stage}.encryption", g["video"][stage]["encrypted"],
                     r["video"][stage]["encrypted"], encrypt_plain)
        if not g["single"]:
            gate("downlink.plain.nack", g["downlink"]["plain"]["opusNack"],
                 r["downlink"]["plain"]["opusNack"], True)
            gate("downlink.red.nack", g["downlink"]["red"]["opusNack"],
                 r["downlink"]["red"]["opusNack"], False)
            if loss:
                gate("loss.dropped", len(g["loss"]["dropped"]), len(r["loss"]["dropped"]), 3)
                gate("loss.nack", g["loss"]["nackCountDelta"] > 0, r["loss"]["nackCountDelta"] > 0, True)
                gate("loss.primary_retransmits",
                     sum(packet["recovered"] for packet in g["loss"]["dropped"]),
                     sum(packet["recovered"] for packet in r["loss"]["dropped"]), 3)
                if video:
                    gate("video.loss.continues", video_continues(g, "loss", "mixed"),
                         video_continues(r, "loss", "mixed"), True)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--inactive", action="store_true")
    parser.add_argument("--loss", action="store_true")
    parser.add_argument("--video", action="store_true")
    parser.add_argument("--encrypt-plain", action="store_true")
    args = parser.parse_args()
    args.output.mkdir()
    go = run(args.go.resolve(), args.output / "go", not args.inactive, args.loss, args.video, args.encrypt_plain)
    rust = run(args.rust.resolve(), args.output / "rust", not args.inactive, args.loss, args.video, args.encrypt_plain)
    rows = compare(go, rust, args.loss, args.video, args.encrypt_plain)
    (args.output / "comparison.json").write_text(json.dumps(rows, indent=2) + "\n")
    passed = sum(row["match"] for row in rows)
    print(f"{passed}/{len(rows)} matched")
    return int(passed != len(rows))


if __name__ == "__main__":
    raise SystemExit(main())
