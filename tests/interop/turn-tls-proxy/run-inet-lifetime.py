#!/usr/bin/env python3
"""Run one pinned server through real UDP, plain TCP and verified TLS TURN."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import itertools
import hashlib
import json
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--probe", type=Path, required=True)
parser.add_argument("--server", type=Path, required=True)
parser.add_argument("--fixtures", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--reference", type=Path)
parser.add_argument("--ipv6", action="store_true", help="bind the listener and relay to IPv6 loopback")
mode = parser.add_mutually_exclusive_group()
mode.add_argument("--late-refresh", action="store_true")
mode.add_argument("--refresh-owner", action="store_true")
mode.add_argument("--refresh-family", action="store_true")
args = parser.parse_args()
output = args.output.resolve()
output.mkdir()
identity = {}
for kind in ("probe", "server"):
    binary = getattr(args, kind).resolve()
    with binary.open("rb") as file:
        identity[kind] = hashlib.file_digest(file, "sha256").hexdigest()
(output / "identities.json").write_text(json.dumps(identity, indent=2) + "\n")

def run(case):
    wire, operation, lifetime = case
    name = f"{wire}-{operation}-{lifetime}"
    dest = output / name
    env = {
        **os.environ,
        "LK_TURN_TLS_FIXTURE": str(args.fixtures.resolve()),
        "LK_TURN_PROXY_MODE": "v1" if wire == "tls" else "",
        "LK_TURN_INET_WIRE": wire,
        "LK_TURN_INET_OPERATION": operation,
        "LK_TURN_INET_LIFETIME": lifetime,
        "LK_TURN_INET_LATE_REFRESH": "1" if args.late_refresh else "",
        "LK_TURN_INET_REFRESH_OWNER": "1" if args.refresh_owner else "",
        "LK_TURN_INET_REFRESH_FAMILY": "1" if args.refresh_family else "",
        "LK_TURN_UNIX_LIFECYCLE": "",
        "LK_TURN_UNIX_OWNERSHIP": "",
        "LK_TURN_UNIX_NAMES": "",
    }
    command = [
        str(args.probe.resolve()), "-profile", "explicit-v6" if args.ipv6 else "proxy-v4",
        "-transport", "udp" if wire == "udp" else "tcp",
        "-binary", str(args.server.resolve()), "-out", str(dest),
    ]
    with (output / f"{name}.driver.log").open("wb") as log:
        completed = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=60)
    result_path = dest / "result.json"
    result = json.loads(result_path.read_text()) if result_path.is_file() else {}
    print(name, completed.returncode, result.get("advertised_seconds"),
          result.get("released_with_connection_open"), result.get("error"), flush=True)
    return {"name": name, "exit": completed.returncode, "result": result}

wires = ("udp", "tcp", "tls")
cases = list(itertools.product(wires, ("allocate", "refresh"), ("default", "2", "3599", "3600", "3601")))
cases.extend((wire, "allocate", "0") for wire in wires)
cases.append(("tls", "extend", "2"))
if args.late_refresh:
    cases = list(itertools.product(wires, ("allocate", "refresh"), ("2",)))
if args.refresh_owner or args.refresh_family:
    cases = [(wire, "allocate", "default") for wire in wires]
with ThreadPoolExecutor(max_workers=4) as pool:
    rows = list(pool.map(run, cases))
(output / "campaign.json").write_text(json.dumps(rows, indent=2) + "\n")
failed = any(row["exit"] != 0 for row in rows)
if args.late_refresh:
    failed |= any(row["result"].get("late_refresh") != "no_response"
                  or row["result"].get("same_socket_reallocate") is not True
                  for row in rows)
if args.reference:
    reference = json.loads(args.reference.read_text())
    fields = (
        "advertised_seconds", "allocate_error", "released_with_connection_open",
        "same_socket_reallocate", "initial_advertised_seconds",
        "retained_same_port_after_original_deadline",
    )
    if args.late_refresh:
        fields += ("late_refresh",)
    if args.refresh_owner or args.refresh_family:
        fields += ("wrong_owner_refresh", "original_owner_refresh",
                   "rotated_owner_refresh", "rotated_owner_released_relay")
    if args.refresh_family:
        fields += ("refresh_family",)
    differences = {}
    for row, expected in itertools.zip_longest(rows, reference):
        if row is None or expected is None or row["name"] != expected["name"] or expected["exit"] != 0:
            raise ValueError("incompatible reference campaign")
        delta = {
            field: {"reference": expected["result"].get(field), "observed": row["result"].get(field)}
            for field in fields
            if row["result"].get(field) != expected["result"].get(field)
        }
        if delta:
            differences[row["name"]] = delta
    (output / "differences.json").write_text(json.dumps(differences, indent=2) + "\n")
    print(f"{len(differences)} divergent cases", flush=True)
    failed |= bool(differences)
raise SystemExit(failed)
