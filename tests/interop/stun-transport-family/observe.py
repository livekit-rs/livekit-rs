#!/usr/bin/env python3
"""Observe the pinned ports command's real external-address STUN sockets."""

import argparse
import hashlib
import ipaddress
import json
import os
import selectors
import socket
import struct
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

COOKIE = b"\x21\x12\xa4\x42"
FORCE_ENV = "LIVEKIT_RTC_REQUIRE_IPV4"
PROFILES = (
    ("dual-default", "yaml", False, True),
    ("dual-force-yaml", "yaml", True, True),
    ("ports-ignore-root-cli", "cli", True, True),
    ("ports-ignore-root-env", "env", True, True),
    ("v6-default", "yaml", False, False),
    ("v6-force", "yaml", True, False),
    ("daemon-force-cli", "cli", True, True),
    ("daemon-force-env", "env", True, True),
)


def digest(binary):
    with binary.open("rb") as source:
        sha = hashlib.sha256()
        for block in iter(lambda: source.read(1 << 20), b""):
            sha.update(block)
        return sha.hexdigest()


def open_v6(port):
    sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        sock.bind(("::1", port))
        return sock
    except Exception:
        sock.close()
        raise


def open_v4(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind(("127.0.0.1", port))
        return sock
    except Exception:
        sock.close()
        raise


def serve(sockets, seen, errors, stop):
    try:
        with selectors.DefaultSelector() as ready:
            for sock in sockets:
                ready.register(sock, selectors.EVENT_READ)
            while not stop.is_set():
                for key, _ in ready.select(0.05):
                    sock = key.fileobj
                    data, source = sock.recvfrom(1024)
                    assert len(data) >= 20 and data[:2] == b"\x00\x01" and data[4:8] == COOKIE
                    ip = ipaddress.ip_address(source[0])
                    family = ip.version
                    bits = bytes(a ^ b for a, b in zip(ip.packed, data[4:20]))
                    attr = struct.pack(
                        ">HHBBH", 0x20, 4 + len(bits), 0, 1 if family == 4 else 2,
                        source[1] ^ 0x2112,
                    ) + bits
                    response = struct.pack(">HH", 0x101, len(attr)) + data[4:20] + attr
                    sock.sendto(response, source)
                    seen.append({"family": family, "source": source[0]})
    except Exception as error:
        errors.append(repr(error))


def daemon_config(config):
    with socket.socket() as http, socket.socket() as media, open_v6(0) as udp:
        http.bind(("127.0.0.1", 0))
        media.bind(("127.0.0.1", 0))
        port = http.getsockname()[1]
        media_port = media.getsockname()[1]
        udp_port = udp.getsockname()[1]
        config.update({
            "port": port,
            "bind_addresses": ["127.0.0.1"],
            "keys": {"fixture": "owned-fixture-credential-with-32-bytes"},
        })
        config["rtc"].update({
            "tcp_port": media_port, "udp_port": udp_port,
            "enable_loopback_candidate": True, "skip_external_ip_validation": True,
            "ips": {"includes": ["::1/128"]},
        })
    return port, media_port, udp_port


def run_daemon(command, env, output, profile, ports):
    port, media_port, udp_port = ports
    healthy = False
    with (output / (profile + ".log")).open("wb") as log:
        proc = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline and proc.poll() is None:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=0.2) as response:
                        if response.status == 200:
                            healthy = True
                            break
                except (urllib.error.URLError, TimeoutError):
                    pass
                time.sleep(0.025)
            if healthy:
                # Go publishes HTTP before the final running flag.
                time.sleep(0.25)
        finally:
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(timeout=4)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                healthy = False
    ports_released = False
    try:
        with socket.socket() as http, socket.socket() as media, open_v6(udp_port):
            # Ignore TIME_WAIT from our health request while still requiring
            # exclusive TCP listen without SO_REUSEPORT.
            http.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            media.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            http.bind(("127.0.0.1", port))
            media.bind(("127.0.0.1", media_port))
            http.listen(1)
            media.listen(1)
            ports_released = True
    except OSError:
        pass
    return proc.returncode, healthy, ports_released


def driven_items(config, command, env):
    """Name the ledger leaves this profile actually wrote (SPEC-0075 §2.7): a receipt
    reporting only families and exit codes cannot show which leaf it exercised."""
    leaves = []

    def walk(prefix, value):
        if isinstance(value, dict):
            for key, child in value.items():
                walk(f"{prefix}{key}.", child)
        else:
            leaves.append("YAML:" + prefix.rstrip("."))

    walk("", config)
    for arg in command:
        if arg.startswith("--"):
            leaves.append("generated_flags:" + arg.split("=", 1)[0])
    if env.get(FORCE_ENV):
        leaves.append("env:" + FORCE_ENV)
    return sorted(set(leaves))


def run(binary, output, profile, mode, forced, dual):
    v6 = open_v6(0)
    port = v6.getsockname()[1]
    sockets = [v6]
    seen, errors = [], []
    stop = threading.Event()
    try:
        if dual:
            sockets.append(open_v4(port))
        host = "localhost" if dual else "[::1]"
        config = {"rtc": {"use_external_ip": True,
                          "require_ipv4": forced and mode == "yaml",
                          "stun_servers": [f"{host}:{port}"]}}
        is_daemon = profile.startswith("daemon-")
        daemon_ports = daemon_config(config) if is_daemon else None
        command = [str(binary), "--config-body", json.dumps(config)]
        env = {key: value for key, value in os.environ.items() if key != FORCE_ENV}
        if mode == "cli":
            command.append("--rtc.require_ipv4=true")
        if mode == "env":
            env[FORCE_ENV] = "true"
        if not is_daemon:
            command.append("ports")
        thread = threading.Thread(target=serve, args=(sockets, seen, errors, stop))
        thread.start()
        try:
            if is_daemon:
                exit_code, healthy, daemon_ports_released = run_daemon(
                    command, env, output, profile, daemon_ports
                )
                stdout, stderr = "", ""
            else:
                completed = subprocess.run(command, env=env, capture_output=True, timeout=8)
                exit_code = completed.returncode
                stdout, stderr = completed.stdout.decode(), completed.stderr.decode()
                healthy = daemon_ports_released = None
        finally:
            stop.set()
            thread.join(timeout=1)
            assert not thread.is_alive()
    finally:
        for sock in sockets:
            sock.close()

    expected_family = 4 if forced else 6
    if dual and (not forced or profile.startswith("ports-ignore-")):
        first = socket.getaddrinfo(host, port, type=socket.SOCK_DGRAM)[0][0]
        expected_family = 6 if first == socket.AF_INET6 else 4

    families = [event["family"] for event in seen]
    if is_daemon:
        # First request is root config detection. The next source-bound
        # request is the distinct IPv6 NAT mapping consumer.
        accepted = exit_code == 0 and healthy and daemon_ports_released and families == [4, 6]
    elif profile == "v6-force":
        accepted = (exit_code == 1 and not families and not stderr
                    and stdout == "could not validate RTC config: could not resolve external IP: "
                    "dial udp4: address ::1: no suitable address found\n")
    else:
        accepted = (exit_code == 0 and families == [expected_family]
                    and stdout.startswith("TCP Ports\n") and not stderr)

    # The owned fixture listeners must relinquish both actual UDP sockets.
    with open_v6(port):
        if dual:
            with open_v4(port):
                pass
    return {
        "profile": profile, "command": command, "generated_env": {FORCE_ENV: env[FORCE_ENV]}
        if mode == "env" else {}, "exit_code": exit_code, "expected_family": expected_family,
        "driven_items": driven_items(config, command, env),
        "daemon_health": healthy, "daemon_ports_released": daemon_ports_released,
        "stderr": stderr, "stdout": stdout, "wire": seen, "fixture_errors": errors,
        "udp_ports_released": True, "accepted": accepted and not errors,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    args.output.mkdir()
    before = digest(binary)
    results = []
    for profile, mode, forced, dual in PROFILES:
        row = run(binary, args.output, profile, mode, forced, dual)
        results.append(row)
        print(profile, row["exit_code"], row["wire"], row["accepted"], flush=True)
    unchanged = before == digest(binary)
    (args.output / "result.json").write_text(json.dumps({
        "binary": str(binary), "sha256": before, "binary_unchanged": unchanged,
        "observer_sha256": digest(Path(__file__)), "results": results,
    }, indent=2) + "\n")
    return 0 if unchanged and all(row["accepted"] for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
