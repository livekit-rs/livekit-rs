#!/usr/bin/env python3
"""Removal and restoration arms for the audio RED pass-through retirement.

The retired vendor patch (`go-red-passthrough-on-negotiated-protected-pt`) moved the RED wire decision
into `rtc-media`. This driver makes that claim falsifiable at both tiers instead of asserting it:

* `retired`        - the current tree: the RED component suite, the reconstruction gate, and a wire
                     campaign scored against the pinned Go binary.
* `chain-stub`     - mutant: the stored chain is never consulted. A wire divergence proves the app-side
                     gate is what puts the publisher's redundancy on the down leg.
* `gate-blind`     - mutant: the subscriber's advertised chain type is no longer checked. A component
                     failure proves that conjunct is load-bearing rather than decorative.
* `patch-reapplied`- restoration: the retired patch text is replayed onto `vendor/str0m`. The gate must
                     refuse the now-orphan patch and the suite must not change, i.e. the vendor code is
                     no longer the path that decides the egress.

Every arm restores the tree it touched and verifies the restore by digest, so an interrupted run cannot
leave a mutated source behind.
"""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
FORWARD = REPO / "crates/rtc-media/src/forward.rs"
VENDOR = "vendor/str0m"
PATCH_NAME = "go-red-passthrough-on-negotiated-protected-pt.patch"
PATCH_BLOB = f"182bfe0:vendor/str0m/{PATCH_NAME}"
PATCH_FILES = [
    "src/change/sdp.rs",
    "src/format/codec_config.rs",
    "src/lib.rs",
    "src/media/event.rs",
    "src/media/mod.rs",
    "src/media/writer.rs",
    "src/session.rs",
]
# The pass-through APIs the vendor patch introduced. Nothing in the app may call them after the move.
RETIRED_APIS = ("set_red_wire_payload_enabled", "negotiated_red_pt")

CHAIN_ORIGINAL = """                let chain = (!is_video && !d.encrypted)
                    .then(|| up.red_chain(data.time.numer() as u32, seq.0 as u16, seq.1 as u16))
                    .flatten();
"""
CHAIN_MUTANT = """                let chain: Option<std::sync::Arc<[u8]>> = None;
                let _ = &up;
"""
GATE_ORIGINAL = """                    Some((chain, red_pt))
                        if t.remote_red_pts
                            .get(&mid)
                            .is_some_and(|pts| pts.contains(&*red_pt)) =>
                    {
"""
GATE_MUTANT = """                    Some((chain, red_pt)) =>
                    {
"""

SUMMARY_RE = re.compile(r"(\d+) passed; (\d+) failed; (\d+) ignored")
FAILED_RE = re.compile(r"^---- (\S+) stdout", re.MULTILINE)


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def run(cmd, cwd=REPO, log=None, env=None, timeout=3600):
    proc = subprocess.run(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, timeout=timeout)
    if log is not None:
        Path(log).write_text(proc.stdout)
    return proc


def suite(arm_dir):
    """`--lib red_` on rtc-media: the component-tier RED behaviour, with the failing test names."""
    proc = run(["cargo", "test", "-p", "rtc-media", "--lib", "red_"], log=arm_dir / "suite.log")
    text = (arm_dir / "suite.log").read_text()
    counts = SUMMARY_RE.search(text)
    return {
        "exit": proc.returncode,
        "passed": int(counts.group(1)) if counts else None,
        "failed": int(counts.group(2)) if counts else None,
        "ignored": int(counts.group(3)) if counts else None,
        "failures": sorted(set(FAILED_RE.findall(text))),
    }


def gate(arm_dir, name="gate.log"):
    proc = run(["python3", "scripts/check-str0m-reconstruction.py"], log=arm_dir / name)
    tail = (arm_dir / name).read_text().strip().splitlines()
    return {"exit": proc.returncode, "tail": tail[-1] if tail else ""}


def no_call_sites():
    """The retired vendor API must not be reachable from the app, or the move is a copy, not a move."""
    hits = []
    for path in (REPO / "crates").rglob("*.rs"):
        text = path.read_text()
        hits += [f"{path.relative_to(REPO)}: {api}" for api in RETIRED_APIS if api in text]
    return hits


def release_binary(arm_dir):
    started = time.monotonic()
    proc = run(["cargo", "build", "--release", "-p", "livecloud"], log=arm_dir / "build.log", timeout=5400)
    binary = REPO / "target" / "release" / "livekit-server"
    if proc.returncode or not binary.exists():
        raise RuntimeError(f"release build failed (exit {proc.returncode}), see {arm_dir / 'build.log'}")
    return {"binary": str(binary), "binary_sha256": sha256(binary), "seconds": round(time.monotonic() - started)}


def campaign(arm_dir, go, rust, name):
    out = arm_dir / name
    shutil.rmtree(out, ignore_errors=True)
    proc = run([sys.executable, "tests/interop/audio-red-egress/run.py", "--go", str(go),
                "--rust", str(rust), "--output", str(out)], log=arm_dir / f"{name}.log", timeout=1800)
    rows = json.loads((out / "comparison.json").read_text()) if (out / "comparison.json").exists() else []
    diverging = [r["name"] for r in rows if not r["match"]]
    summary = (arm_dir / f"{name}.log").read_text().strip().splitlines()
    return {
        "exit": proc.returncode,
        "rows": len(rows),
        "binary_sha256": sha256(rust),
        "divergences": diverging,
        "summary": summary[-1] if summary else "",
        "egress": egress_rows(out),
    }


def egress_rows(out):
    """Absolute PT/block-count evidence from the Rust side, not only the Go-vs-Rust match flags."""
    obs = out / "observations.json"
    if not obs.exists():
        return {}
    data = json.loads(obs.read_text())["rust"]
    rows = {}
    for tag in ("active", "inactive"):
        for phase, track in (("midJoin", "red"), ("midJoin", "plain"), ("firstFrame", "late")):
            leg = data[tag]["legs"][phase][track]["down"]
            rows[f"{tag}.{phase}.{track}.down"] = {
                "frames": leg["frames"],
                "wirePts": leg["wirePts"],
                "dominantBlockCounts": leg["dominantBlockCounts"],
                "dominantShapes": leg["dominantShapes"],
                "firstChains": [[c["blockCount"], c["blockPts"], c["wirePt"]] for c in leg["first"]],
            }
    return rows


def write_patch(arm_dir):
    proc = run(["git", "show", PATCH_BLOB])
    if proc.returncode:
        raise RuntimeError(f"cannot read the retired patch blob: {proc.stdout[:400]}")
    patch = arm_dir / PATCH_NAME
    patch.write_text(proc.stdout)
    return patch


def mutate(replacement, arm_dir):
    original = FORWARD.read_text()
    old, new = replacement
    if original.count(old) != 1:
        raise RuntimeError(f"mutation anchor matched {original.count(old)} times, refusing to guess")
    (arm_dir / "forward.rs.original").write_text(original)
    FORWARD.write_text(original.replace(old, new))
    return original


def restore(original):
    FORWARD.write_text(original)


ARMS = {
    "retired": [],
    "chain-stub": [(CHAIN_ORIGINAL, CHAIN_MUTANT)],
    "gate-blind": [(GATE_ORIGINAL, GATE_MUTANT)],
    "patch-reapplied": [],
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--go", type=Path, default=Path("/tmp/c99-build/livekit-server-go"))
    parser.add_argument("--arm", choices=[*ARMS, "all"], default="all")
    parser.add_argument("--skip-build", action="store_true", help="score against the binary already on disk")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    vendor = REPO / "vendor" / "str0m"
    baseline = {vendor / p: sha256(vendor / p) for p in PATCH_FILES}
    baseline[FORWARD] = sha256(FORWARD)
    arms = [args.arm] if args.arm != "all" else list(ARMS)
    report_path = args.output / "arms.json"
    report = json.loads(report_path.read_text()) if report_path.exists() else {}
    report.setdefault("arms", {})
    report["baseline_digests"] = {str(k): v for k, v in baseline.items()}

    for name in arms:
        arm_dir = args.output / name
        arm_dir.mkdir(parents=True, exist_ok=True)
        result = {"restored": True}
        patch_path = None
        original = None
        try:
            if name == "patch-reapplied":
                patch_path = write_patch(arm_dir)
                applied = run(["git", "apply", "--whitespace=nowarn", f"--directory={VENDOR}", str(patch_path)])
                result["patch_apply_exit"] = applied.returncode
                result["patch_apply_output"] = applied.stdout[:400]
                # The chain on disk must carry the file the declared list no longer names, which is the
                # state a partial revert would leave behind.
                shutil.copy2(patch_path, vendor / PATCH_NAME)
            for old, new in ARMS[name]:
                original = mutate((old, new), arm_dir)
            result["no_call_sites"] = no_call_sites()
            result["gate"] = gate(arm_dir)
            result["suite"] = suite(arm_dir)
            if name in ("retired", "chain-stub"):
                # `--skip-build` is only honest for the unmutated tree, where the candidate already on
                # disk *is* the arm. A mutant has to be compiled or its campaign scores the baseline.
                if args.skip_build and name == "retired":
                    result["build"] = {"skipped": True}
                else:
                    result["build"] = release_binary(arm_dir)
                result["campaign"] = campaign(arm_dir, args.go, REPO / "target" / "release" / "livekit-server",
                                              f"campaign-{name}")
        finally:
            if original is not None:
                restore(original)
            if patch_path is not None:
                run(["git", "apply", "-R", "--whitespace=nowarn", f"--directory={VENDOR}", str(patch_path)])
                if (vendor / PATCH_NAME).exists():
                    shutil.copy2(vendor / PATCH_NAME, arm_dir / f"{PATCH_NAME}.recreated")
                    (vendor / PATCH_NAME).unlink()
            for path, digest in baseline.items():
                current = sha256(path)
                if current != digest:
                    result["restored"] = False
                    result.setdefault("restore_failures", []).append(
                        {"file": str(path), "expected": digest, "actual": current})
            if name == "patch-reapplied":
                result["gate_after_restore"] = gate(arm_dir, "gate.after_restore.log")
        report["arms"][name] = result
        (args.output / "arms.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"{name}: restored={result['restored']} gate={result.get('gate', {}).get('exit')} "
              f"suite={result.get('suite', {}).get('passed')}p/{result.get('suite', {}).get('failed')}f "
              f"campaign_divergences={len(result.get('campaign', {}).get('divergences', []))}", flush=True)

    if not all(r["restored"] for r in report["arms"].values()):
        raise SystemExit("a arm failed to restore the tree, see arms.json")
    untracked = run(["git", "status", "--porcelain", "vendor/str0m", "crates/rtc-media/src/forward.rs"])
    print(f"final tree status for touched paths: {untracked.stdout.strip() or 'only the intended retirement diff'}")


if __name__ == "__main__":
    main()
