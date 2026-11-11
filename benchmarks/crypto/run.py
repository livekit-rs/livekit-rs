#!/usr/bin/env python3
"""Build and execute the isolated provider comparison, retaining source identity."""
import hashlib
import json
import os
import platform
import shlex
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
PLATFORM = "macos" if platform.system() == "Darwin" else platform.system().lower()
ARCH = "aarch64" if platform.machine() in ("arm64", "aarch64") else platform.machine()
BUILD_ENV = os.environ.copy()
DEFAULT_SOFTWARE_OVERRIDE = not any(key in BUILD_ENV for key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"))
if DEFAULT_SOFTWARE_OVERRIDE:
    # An explicitly present empty RUSTFLAGS overrides ancestor Cargo target cfgs.
    # On aarch64 this restores the aes/polyval crate-default software path.
    BUILD_ENV["RUSTFLAGS"] = ""
if "CARGO_ENCODED_RUSTFLAGS" in BUILD_ENV:
    EFFECTIVE_RUSTFLAGS = [flag for flag in BUILD_ENV["CARGO_ENCODED_RUSTFLAGS"].split("\x1f") if flag]
else:
    EFFECTIVE_RUSTFLAGS = shlex.split(BUILD_ENV.get("RUSTFLAGS", ""))
DEFAULT_LABEL = "software" if ARCH == "aarch64" and not EFFECTIVE_RUSTFLAGS else "crate-default"
LABEL = os.environ.get("CRYPTO_BENCH_LABEL", DEFAULT_LABEL)
OUTPUT = ROOT / "results" / f"{PLATFORM}-{ARCH}-{LABEL}.json"
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target")))
if not TARGET.is_absolute():
    TARGET = Path.cwd() / TARGET

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

SOURCE_PATHS = [ROOT / "Cargo.toml", ROOT / "Cargo.lock", ROOT / "src/main.rs", ROOT / "run.py", ROOT.parent.parent / "Cargo.toml", ROOT.parent.parent / ".cargo/config.toml", ROOT.parent.parent / "crates/rtc-crypto/Cargo.toml", *sorted((ROOT.parent.parent / "crates/rtc-crypto/src").glob("*.rs"))]

def snapshot():
    return {str(path.relative_to(ROOT.parent.parent)): digest(path) for path in SOURCE_PATHS if path.exists()}

def changes(before, after):
    return {key: {"before": before.get(key), "after": after.get(key)} for key in before.keys() | after.keys() if before.get(key) != after.get(key)}

sources_before_build = snapshot()
build_environment = {k: BUILD_ENV.get(k, "") for k in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "CARGO_TARGET_DIR", "CARGO_PROFILE_RELEASE_LTO", "CARGO_PROFILE_RELEASE_CODEGEN_UNITS")}
subprocess.run(["cargo", "build", "--manifest-path", str(ROOT / "Cargo.toml"), "--release", "--locked"], check=True, env=BUILD_ENV)
sources_after_build = snapshot()
compiled_executable_sha256 = digest(TARGET / "release/livekit-rs-crypto-benchmark")
OUTPUT.parent.mkdir(exist_ok=True)
subprocess.run([str(TARGET / "release/livekit-rs-crypto-benchmark"), "--output", str(OUTPUT), *sys.argv[1:]], check=True, env=BUILD_ENV)
report = json.loads(OUTPUT.read_text())
report["compiled_executable_sha256"] = compiled_executable_sha256
report["compiled_executable_changed_during_run"] = compiled_executable_sha256 != digest(TARGET / "release/livekit-rs-crypto-benchmark")
report["source_sha256"] = sources_before_build
report["source_hash_capture"] = "BEFORE_BUILD"
report["source_changes_during_build"] = changes(sources_before_build, sources_after_build)
report["source_changes_during_run"] = changes(sources_after_build, snapshot())
report["build_environment"] = build_environment
report["effective_rustflags"] = EFFECTIVE_RUSTFLAGS
report["ancestor_cargo_flags_overridden_by_default"] = DEFAULT_SOFTWARE_OVERRIDE
report["timestamp_utc"] = subprocess.check_output(["date", "-u", "+%Y-%m-%dT%H:%M:%SZ"], text=True).strip()
summary = []
for key in sorted({(s["provider"], s["suite"], s["operation"], s["payload_bytes"]) for s in report["samples"]}):
    samples = [s for s in report["samples"] if (s["provider"], s["suite"], s["operation"], s["payload_bytes"]) == key]
    values = [s["cpu_seconds_per_million"] for s in samples]
    row = dict(zip(("provider", "suite", "operation", "payload_bytes"), key))
    row.update(cpu_seconds_per_million_median=statistics.median(values), cpu_seconds_per_million_stdev=statistics.stdev(values) if len(values)>1 else 0, samples=len(samples), allocation_calls_per_packet_median=statistics.median(s["allocation_calls_per_packet"] for s in samples))
    summary.append(row)
report["summary"] = summary
OUTPUT.write_text(json.dumps(report, indent=2) + "\n")
for row in summary:
    print(f'{row["provider"]:10} {row["suite"]:20} {row["operation"]:8} {row["payload_bytes"]:4} B: {row["cpu_seconds_per_million_median"]:.5f} CPU sec/1M ±{row["cpu_seconds_per_million_stdev"]:.5f}, alloc/packet {row["allocation_calls_per_packet_median"]:.1f}')
print(OUTPUT)
