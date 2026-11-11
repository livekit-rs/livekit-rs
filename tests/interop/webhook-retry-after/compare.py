#!/usr/bin/env python3
"""Assert the observed contract for the unchanged Go and scoped Rust daemons."""
import argparse
import json
from pathlib import Path


def verify(path: Path, variant: str) -> dict:
    data = json.loads(path.read_text())
    assert data["status"] == "OBSERVED" and not data["errors"], data
    assert data["protocol"] in ("http", "https"), data["protocol"]
    baseline = variant == "old-rust"
    capped = variant == "capped-rust"
    pending_shutdown = "secondSignalSent" in data
    process = data["process"]
    force_killed = baseline or (capped and pending_shutdown)
    assert (
        (process["exit"], process["signal"])
        == ((None, "SIGKILL") if force_killed else (0, None))
    ), process
    rows = {row["name"]: row for row in data["rows"]}
    if pending_shutdown:
        assert set(rows) == {"shutdown-during-retry"}, rows
        [attempt] = rows["shutdown-during-retry"]["attempts"]
        assert attempt["verifiedByOfficialSDK"] and attempt["status"] == 429, attempt
        assert attempt["sentRetryAfter"] == "302", attempt
        assert (attempt["tls"] == "TLSv1.3") == (data["protocol"] == "https"), attempt
        assert data["secondSignalSent"] == capped, data
        return {"binary_sha256": data["binarySha256"], "protocol": data["protocol"],
                "delays_ms": {}, "process": process}
    waits = {
        "accepted-then-tcp-closed": "default",
        "503-past-date": "default" if baseline else "immediate",
        "429-future-date": "default" if baseline else "future",
        "429-negative-delta": "default",
        "429-plus-zero-delta": "immediate",
        "503-integer-outside-i64": "pending" if baseline else "default",
        "503-multiplied-negative-duration": "pending" if baseline else "immediate",
        "500-ignores-past-date": "default",
    }
    if "429-over-300-seconds" in rows:
        waits["429-over-300-seconds"] = "old-cap" if capped else "long-explicit"
    assert set(rows) == set(waits), set(rows)
    result = {}
    for name, wait in waits.items():
        row = rows[name]
        attempts = row["attempts"]
        assert all(a["verifiedByOfficialSDK"] for a in attempts), row
        assert all((a["tls"] == "TLSv1.3") == (data["protocol"] == "https") for a in attempts), row
        if name == "accepted-then-tcp-closed":
            assert attempts[0]["action"] == "disconnect", row
        if wait == "pending":
            assert len(attempts) == 1, row
            result[name] = None
            continue
        first, second = attempts
        for key in ("id", "bodySha256", "jwtSha256"):
            assert first[key] == second[key], row
        assert second["status"] == 200, row
        elapsed = row["delayMs"]
        if wait == "default":
            assert 750 <= elapsed <= 1750, row
        elif wait == "immediate":
            assert 0 <= elapsed <= 350, row
        elif wait == "future":
            expected = first["expectedWaitMsFromDate"]
            assert expected >= 4000 and abs(expected - elapsed) <= 750, row
        elif wait == "old-cap":
            assert 299750 <= elapsed <= 301000, row
        elif wait == "long-explicit":
            assert 301750 <= elapsed <= 305000, row
        result[name] = round(elapsed, 3)
    return {"binary_sha256": data["binarySha256"], "protocol": data["protocol"],
            "delays_ms": result, "process": process}


parser = argparse.ArgumentParser()
parser.add_argument("go", type=Path)
parser.add_argument("rust", type=Path)
parser.add_argument("--expect", choices=("old-rust", "capped-rust", "fixed"), required=True)
args = parser.parse_args()
go = verify(args.go, "go")
rust = verify(args.rust, args.expect)
assert set(go["delays_ms"]) == set(rust["delays_ms"])
assert go["protocol"] == rust["protocol"]
status = "CONFIRMED_BASELINE_DIFFERENCE" if args.expect == "capped-rust" else "MATCHED_OBSERVATION"
print(json.dumps({"status": status, "go": go, "rust": rust}, indent=2))
