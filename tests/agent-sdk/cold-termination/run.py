#!/usr/bin/env python3
"""Run one real SDK cold-termination case using the archived owned runner."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


GO_SHA256 = "96fc261756d541f727dc19ba77b932f5e79942863f5b8e216856f71f3e65f503"
RUNNER_SHA256 = "6e9d8805df8d9e9f658ff43a05697eaeb151971004c0561f6852f7775df7957d"
PROCESS_HELPER_SHA256 = "ab76ce12102f4e962ec85dceb077e4c031692b1f0ecbe3011066334dce98dd4a"
SDK_PROJECT_SHA256 = {
    "package.json": "725a12091f689b54b2d8d60febed6f6b2150caad08ceb008697338e284165912",
    "package-lock.json": "cd3e2d184f9a0550a350654816065a9c395847dbc20fb59dda4b154278e0962d",
}
INTEROP_PROJECT_SHA256 = {
    "package.json": "f2ef6ff2d07cfbc00ed423c96c92ac610c2f7bb1ea19027bb21ff5b26e475a67",
    "package-lock.json": "e05d7e3d2d8cab16ac81217cc81192abe15c8dd8c0747b5d3e8246c0ac7aa23b",
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("fixture-root", "go", "agent-sdk-root", "interop-project", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--rust", type=Path)
    parser.add_argument("--rust-sha256")
    parser.add_argument("--owner", choices=("go", "rust"), required=True)
    parser.add_argument("--expect", choices=("cold-race", "retired"), required=True)
    parser.add_argument("--node", default="node")
    parser.add_argument("--redis-server", default="redis-server")
    args = parser.parse_args()
    if args.owner == "rust" and not (args.rust and args.rust_sha256):
        parser.error("Rust owner requires an exact executable and SHA-256")
    if args.owner == "go" and args.expect != "cold-race":
        parser.error("The fixed Go control reproduces the cold race")
    source = args.fixture_root.resolve()
    runner = source / "tests/agent-sdk/delayed-availability/run-owned.py"
    helper = source / "tests/interop/egress-linux/run-owned-stack.py"
    observer = Path(__file__).resolve().with_name("run-observer.mjs")
    assert digest(runner) == RUNNER_SHA256, "archived owned runner changed"
    assert digest(helper) == PROCESS_HELPER_SHA256, "archived process helper changed"
    assert digest(args.go) == GO_SHA256, "original Go binary changed"
    for project, expected in ((args.agent_sdk_root, SDK_PROJECT_SHA256),
                              (args.interop_project, INTEROP_PROJECT_SHA256)):
        assert all(digest(project / name) == sha for name, sha in expected.items()), project
    rust, rust_sha = (args.rust, args.rust_sha256) if args.owner == "rust" else (args.go, GO_SHA256)
    case = f"0-{args.owner}-go"
    command = [
        sys.executable, str(runner), "--source-root", str(source),
        "--go", str(args.go), "--go-sha256", GO_SHA256,
        "--rust", str(rust), "--rust-sha256", rust_sha,
        "--agent-sdk-root", str(args.agent_sdk_root), "--interop-project", str(args.interop_project),
        "--node", args.node, "--redis-server", args.redis_server,
        "--probe", str(observer), "--output", str(args.output),
        "--profile", "delayed-availability-observe", "--only", case,
    ]
    assert not args.output.exists(), "fresh output path required"
    run = subprocess.run(command, check=False)
    comparison = {
        "status": "FAIL",
        "owner": args.owner,
        "expected": args.expect,
        "owned_runner_exit": run.returncode,
        "launcher_sha256": digest(Path(__file__)),
        "observer_sha256": digest(observer),
        "owned_runner_sha256": digest(runner),
        "process_helper_sha256": digest(helper),
    }
    try:
        assert run.returncode == 0, "owned runner failed"
        execution = args.output / "execution.json"
        probe = args.output / case / "probe.json"
        comparison.update(execution_sha256=digest(execution), probe_sha256=digest(probe))
        receipt = json.loads(execution.read_text())
        evidence = json.loads(probe.read_text())
        assert receipt["status"] == "OBSERVED_ALL_REQUESTED_CASES"
        assert len(receipt["cases"]) == 1 and receipt["cases"][0]["name"] == case
        row = receipt["cases"][0]
        assert row["status"] == "PASS_SCOPED" and row["probe_sha256"] == digest(probe)
        assert row["ports_released"] and row["owned_tree"]["released_without_reclamation"]
        assert evidence["status"] == "OBSERVED_COMPLETE" and not evidence["cleanup_errors"]
        observations = evidence["observations"]
        assert not observations["relay_errors"]
        outcome = evidence["outcome"]
        assert outcome["actual_request_count"] == 1 and outcome["request_resolution"] == "resolved"
        assert outcome["delete_http_status"] == 200 and not outcome["dispatch_present_before_cleanup"]
        gate = observations["prewarm_gate"]
        assert gate["active_jobs_at_release"] == []
        job_id = gate["assignment"]["job_id"]
        terminations = [event for event in observations["wire"]
                        if event["type"] == "termination" and event["job_id"] == job_id]
        assert len(terminations) == 1, "expected one exact worker termination"
        termination = terminations[0]
        assert gate["assignment"]["elapsed_ms"] < termination["elapsed_ms"]
        child = observations["final_children_before_cleanup"][0]
        assert child["pid"] == gate["prewarm"]["pid"] and child["job_id"] == job_id
        assert child["entry_at_ms"] >= evidence["started_at_ms"] + gate["release_elapsed_ms"]
        if args.expect == "cold-race":
            assert gate["early_termination"] == termination
            assert (gate["prewarm"]["at_ms"] - evidence["started_at_ms"]
                    <= termination["elapsed_ms"] < gate["release_elapsed_ms"])
            assert outcome["ever_child_connected"] and outcome["agent_present_before_cleanup"]
            assert outcome["child_alive_before_cleanup"] and not outcome["child_shutdown_before_cleanup"]
        else:
            assert gate["early_termination"] is None
            assert termination["elapsed_ms"] >= gate["release_elapsed_ms"]
            assert not outcome["agent_present_before_cleanup"] and not outcome["child_alive_before_cleanup"]
            assert outcome["child_shutdown_before_cleanup"]
        comparison.update(
            status="OBSERVED_COLD_RACE" if args.expect == "cold-race" else "VERIFIED_RETIRED",
            job_id=job_id,
            gate=gate,
            termination=termination,
            child_before_cleanup=child,
            outcome=outcome,
        )
    except (AssertionError, IndexError, KeyError, OSError, ValueError) as error:
        comparison["error"] = str(error)
    if args.output.is_dir():
        (args.output / "comparison.json").write_text(json.dumps(comparison, indent=2) + "\n")
    print(json.dumps(comparison))
    return comparison["status"] == "FAIL"


if __name__ == "__main__":
    raise SystemExit(main())
