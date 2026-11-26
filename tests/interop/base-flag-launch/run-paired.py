#!/usr/bin/env python3
"""C100 lane A: launched-MAIN paired differential for the config-source base flags.

SPEC-0077 rows driven here (through the *base* flag, never a generated `--<path>`
flag and never a YAML scalar of the same name):

    base_flags:--config                 references/livekit-stable cmd/server/main.go:47-49
    base_flags:--config-body            cmd/server/main.go:51-55  (env LIVEKIT_CONFIG)
    base_flags:--key-file               cmd/server/main.go:56-59
    base_flags:--keys                   cmd/server/main.go:60-64  (env LIVEKIT_KEYS)
    base_flags:--disable-strict-config  cmd/server/main.go:112-116 (Hidden)

Why a launch is required: `--config` / `--config-body` only produce a byte string
(`getConfigString`, cmd/server/main.go:324-335) that `config.NewConfig`
(pkg/config/config.go:669-695) strict-decodes, and `--keys` / `--key-file` only
land in `conf.Keys` / `conf.KeyFile` (pkg/config/config.go:1106-1115, :707-711).
The consumer that proves the value was used is the HTTP authorizer fed by
`ValidateKeys` (pkg/config/config.go:820-838), so the effect rows sign a token
with the flag's key and require the API to accept it, then require the same key
with a wrong secret to be refused.

Modes:
  --record  run both binaries, write raw evidence, assert only what a record run
            can assert (no crash, raw exit preserved, ports loopback).
  --report  re-derive every verdict from a recorded execution.json offline, so the
            gate can be re-checked without another launch.

Every original exit code is preserved: no pipe, no shell, `capture_output` only.
Shutdown follows the calibration recorded in tests/interop/config-wiring-startup:
official livekit-server needs two signals because the first is consumed by the
`for i := range 2` loop (cmd/server/main.go:313-320), so the harness sends SIGINT,
waits, and records which signal actually ended the process.
"""
from __future__ import annotations

import argparse
import base64
import collections
import hashlib
import hmac
import http.client
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

GOOD_SECRET = "devsecretdevsecretdevsecretdevsecret"
BAD_SECRET = "devsecretdevsecretdevsecretXXXXX"


def default_port_state() -> dict:
    """Who owns 7881/7882 right now. Recorded before and after so the receipt can show the gate
    never took the developer host's ports."""
    state = {}
    for port in OFF_LIMITS:
        out = subprocess.run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"],
                             capture_output=True, text=True)
        lines = [l for l in out.stdout.splitlines()[1:] if l.strip()]
        state[str(port)] = {"listeners": len(lines), "pids": sorted({l.split()[1] for l in lines})}
    return state


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(path: Path) -> dict:
    path = Path(path).resolve()
    return {"path": str(path), "sha256": sha256(path)}


OFF_LIMITS = (7881, 7882)  # the developer host runs its own server there; this harness never binds it

# The oracle this batch is pinned to, same as the three other launched lanes: the Go binary hash is
# gated per run, and the checkout the sentences above were read from has to be the commit they name.
FIXED_GO_COMMIT = "e5655ac20267fa5005dccaf3f0e3bfc3ea688c07"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ----------------------------------------------------------------------------- tokens
def make_token(key: str, secret: str, room: str) -> str:
    now = int(time.time())
    part = lambda obj: base64.urlsafe_b64encode(
        json.dumps(obj, separators=(",", ":")).encode()).rstrip(b"=").decode()
    header = part({"alg": "HS256", "typ": "JWT"})
    body = part({
        "iss": key, "sub": "c100-owned-observer", "nbf": now - 5, "exp": now + 600,
        "video": {"room": room, "roomJoin": True, "roomCreate": True,
                  "roomAdmin": True, "roomList": True, "canPublish": True},
    })
    signing = f"{header}.{body}".encode()
    tag = base64.urlsafe_b64encode(
        hmac.new(secret.encode(), signing, hashlib.sha256).digest()).rstrip(b"=").decode()
    return f"{header}.{body}.{tag}"


def list_rooms(port: int, key: str, secret: str) -> dict:
    """One real API request. A refused request is a result, so nothing raises."""
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        conn.putrequest("POST", "/twirp/Livekit.RoomService/ListRooms")
        conn.putheader("Content-Type", "application/json")
        conn.putheader("Authorization", "Bearer " + make_token(key, secret, "c100-lane-a"))
        conn.putheader("Content-Length", "2")
        conn.putheader("Connection", "close")
        conn.endheaders()
        conn.send(b"{}")
        resp = conn.getresponse()
        payload = resp.read()
        conn.close()
        return {"status": resp.status, "body_head": payload[:120].decode("utf-8", "replace")}
    except Exception as exc:  # noqa: BLE001
        return {"status": None, "error": f"{type(exc).__name__}: {exc}"}


# -------------------------------------------------------------------------- processes
def wait_accept(port: int, deadline: float) -> bool:
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def wait_exit(proc, seconds: float) -> bool:
    try:
        proc.wait(timeout=seconds)
        return True
    except subprocess.TimeoutExpired:
        return False


def capture(binary: Path, argv: list[str], env: dict[str, str], work: Path,
            label: str, port: int | None, probes: list[tuple[str, str]],
            settle: float, refuse_wait: float | None = None) -> dict:
    """Run one profile against one binary. `port=None` marks a row that must refuse before it
    ever serves, so it carries a guard port instead (see main) and can never reach 7881/7882."""
    out = work / f"{label}.stdout"
    err = work / f"{label}.stderr"
    started = time.time()
    with open(out, "wb") as out_f, open(err, "wb") as err_f:
        proc = subprocess.Popen([str(binary), *argv], stdout=out_f, stderr=err_f,
                                env=env, start_new_session=True)
        accepted = None
        self_exited = None
        exit_wait_ms = None
        if refuse_wait is not None:
            # A refusal has to end the process on its own before the harness touches it. Signalled
            # straight away, the child dies inside its first millisecond and prints nothing, and the
            # row's parity checks then compare two empty streams and agree.
            self_exited = wait_exit(proc, refuse_wait)
            exit_wait_ms = int((time.time() - started) * 1000)
        probe_results: dict[str, dict] = {}
        if port is not None:
            accepted = wait_accept(port, started + settle)
            if accepted:
                for name, secret in probes:
                    probe_results[name] = list_rooms(port, "c100a", secret)
        signals = []
        sig_at = time.time()
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            signals.append("SIGINT")
            if not wait_exit(proc, 8.0):
                proc.send_signal(signal.SIGINT)
                signals.append("SIGINT#2")
                if not wait_exit(proc, 8.0):
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    signals.append("SIGKILL")
                    proc.wait()
        else:
            signals.append("already-exited")
    stop_latency_ms = int((time.time() - sig_at) * 1000)
    stdout = out.read_text(encoding="utf-8", errors="replace")
    stderr = err.read_text(encoding="utf-8", errors="replace")
    return {"argv": argv, "env_added": {k: v for k, v in env.items() if k not in BASE_ENV},
            "exit": proc.returncode, "accepted": accepted, "signals": signals,
            "self_exited": self_exited, "exit_wait_ms": exit_wait_ms,
            "stop_latency_ms": stop_latency_ms,
            "probes": probe_results, "stdout": stdout[-6000:], "stderr": stderr[-3000:],
            "elapsed_ms": int((time.time() - started) * 1000)}


BASE_ENV_KEYS = ("PATH", "HOME", "LANG", "TMPDIR")
BASE_ENV = {k: os.environ[k] for k in BASE_ENV_KEYS if k in os.environ}


def build_env(work: Path, extra: dict[str, str]) -> dict[str, str]:
    env = dict(BASE_ENV)
    env.update(extra)
    return env


# --------------------------------------------------------------------------- profiles
def profiles(work: Path) -> list[dict]:
    """Each profile is one argv/env shape. Files are created here, per run."""
    files = work / "fixtures"
    files.mkdir(parents=True, exist_ok=True)

    def write(name: str, text: str, mode: int | None = None) -> Path:
        path = files / name
        path.write_text(text, encoding="utf-8")
        if mode is not None:
            path.chmod(mode)
        return path

    p1, p2, p3 = free_port(), free_port(), free_port()
    cfg_keys = write("cfg-keys.yaml", cfg_with_keys(p1))
    cfg_plain = write("cfg-plain.yaml", cfg_plain_body(p2))
    cfg_empty = write("cfg-empty.yaml", "")
    cfg_bad = write("cfg-bad.yaml", "keys:\n\t c100a: x\n  : : : broken\n")
    cfg_unknown = write("cfg-unknown.yaml", cfg_plain_body(p3) + "c100_unknown_setting: 1\n")
    keys_good = write("keys-good.yaml", f"c100a: {GOOD_SECRET}\n", 0o600)
    keys_open = write("keys-open.yaml", f"c100a: {GOOD_SECRET}\n", 0o644)
    keys_missing = files / "keys-absent.yaml"
    body_good = cfg_plain_body(p1) + f"keys:\n  c100a: {GOOD_SECRET}\n"

    return [
        dict(id="A-01-config-file-start", kind="start", port=p1,
             argv=["--config", str(cfg_keys)],
             probes=[("flag_key_right_secret", GOOD_SECRET),
                     ("flag_key_wrong_secret", BAD_SECRET)],
             rows=["base_flags:--config"], note="config file keys must serve and authorize"),
        dict(id="A-02-config-file-missing", kind="refusal",
             expect=["no such file or directory"],  # upstream text, see README citations
             argv=["--config", str(files / "no-such-file.yaml")],
             rows=["base_flags:--config"], note="missing file is a startup failure, not a default"),
        dict(id="A-03-config-file-is-directory", kind="refusal",
             expect=["is a directory"],  # upstream text, see README citations
             argv=["--config", str(files)],
             rows=["base_flags:--config"], note="a directory is a read error"),
        dict(id="A-04-config-file-malformed", kind="refusal",
             expect=["could not parse config:"],  # upstream text, see README citations
             argv=["--config", str(cfg_bad)],
             rows=["base_flags:--config"], note="yaml that cannot decode"),
        dict(id="A-05-config-file-empty", kind="refusal",
             expect=["one of key-file or keys must be provided"],  # upstream text, see README citations
             argv=["--config", str(cfg_empty)],
             rows=["base_flags:--config"], note="empty file means no keys, so the run must refuse"),
        dict(id="A-06-body-wins-over-file", kind="start", port=p1,
             argv=["--config", str(files / "no-such-file.yaml"), "--config-body", body_good],
             probes=[("flag_key_right_secret", GOOD_SECRET),
                     ("flag_key_wrong_secret", BAD_SECRET)],
             rows=["base_flags:--config-body", "base_flags:--config"],
             note="main.go:325 returns the body without reading the file"),
        dict(id="A-07-body-from-env", kind="start", port=p1,
             argv=[], env={"LIVEKIT_CONFIG": body_good},
             probes=[("env_key_right_secret", GOOD_SECRET),
                     ("env_key_wrong_secret", BAD_SECRET)],
             rows=["base_flags:--config-body"], note="env spelling of --config-body"),
        dict(id="A-08-body-empty-string-uses-file", kind="start", port=p1,
             argv=["--config", str(cfg_keys), "--config-body", ""],
             probes=[("flag_key_right_secret", GOOD_SECRET)],
             rows=["base_flags:--config-body"],
             note="inConfigBody != '' is false, so the file must still be read"),
        dict(id="A-09-unknown-key-strict-refuses", kind="refusal",
             expect=["could not parse config:"],  # upstream text, see README citations
             argv=["--config", str(cfg_unknown)],
             rows=["base_flags:--config", "base_flags:--disable-strict-config"],
             note="strict mode is the default"),
        # The pair A-09/A-10 is the discriminator: the same file refuses on the parse error under
        # strict parsing and reaches the keys check once strict parsing is disabled. If the flag
        # were ignored the two rows would print the same sentence.
        dict(id="A-10-unknown-key-relaxed-reaches-keys-check", kind="refusal",
             expect=["one of key-file or keys must be provided"],  # upstream text, see README citations
             argv=["--config", str(cfg_unknown), "--disable-strict-config"],
             rows=["base_flags:--disable-strict-config", "base_flags:--config"],
             note="the unknown key stops being fatal; the missing keys are still fatal"),
        dict(id="A-11-keys-valid", kind="start", port=p2,
             argv=["--config", str(cfg_plain), "--keys", f"c100a: {GOOD_SECRET}\n"],
             probes=[("flag_key_right_secret", GOOD_SECRET),
                     ("flag_key_wrong_secret", BAD_SECRET)],
             rows=["base_flags:--keys"], note="--keys must reach the authorizer"),
        dict(id="A-12-keys-env", kind="start", port=p2,
             argv=["--config", str(cfg_plain)],
             env={"LIVEKIT_KEYS": f"c100a: {GOOD_SECRET}\n"},
             probes=[("env_key_right_secret", GOOD_SECRET)],
             rows=["base_flags:--keys"], note="LIVEKIT_KEYS is the env spelling"),
        dict(id="A-13-keys-malformed", kind="refusal",
             expect=["Could not parse keys, it needs to be exactly"],  # upstream text, see README citations
             argv=["--config", str(cfg_plain), "--keys", MALFORMED_KEYS_TEXT],
             rows=["base_flags:--keys"], note="fixed upstream message, not the yaml error"),
        dict(id="A-14-keys-cli-beats-env", kind="start", port=p2,
             argv=["--config", str(cfg_plain), "--keys", f"c100a: {GOOD_SECRET}\n"],
             env={"LIVEKIT_KEYS": f"c100a: {BAD_SECRET}\n"},
             probes=[("cli_key_right_secret", GOOD_SECRET),
                     ("env_secret_refused", BAD_SECRET)],
             rows=["base_flags:--keys"], note="CLI must beat the environment value"),
        dict(id="A-15-key-file-valid", kind="start", port=p2,
             argv=["--config", str(cfg_plain), "--key-file", str(keys_good)],
             probes=[("file_key_right_secret", GOOD_SECRET),
                     ("file_key_wrong_secret", BAD_SECRET)],
             rows=["base_flags:--key-file"], note="0600 key file must load"),
        dict(id="A-16-key-file-world-readable", kind="refusal",
             expect=["key file others permissions must be set to 0"],  # upstream text, see README citations
             argv=["--config", str(cfg_plain), "--key-file", str(keys_open)],
             rows=["base_flags:--key-file"], note="others-permission refusal"),
        dict(id="A-17-key-file-missing", kind="refusal",
             expect=["no such file or directory"],  # upstream text, see README citations
             argv=["--config", str(cfg_plain), "--key-file", str(keys_missing)],
             rows=["base_flags:--key-file"], note="stat failure is fatal"),
        dict(id="A-18-key-file-var-expansion", kind="start", port=p2,
             argv=["--config", str(cfg_plain), "--key-file", "$C100_KEYS_DIR/keys-good.yaml"],
             env={"C100_KEYS_DIR": str(files)},
             probes=[("expanded_key_right_secret", GOOD_SECRET)],
             rows=["base_flags:--key-file"], note="config.go:707-711 expands $VAR"),
        dict(id="A-19-no-keys-nondev-refuses", kind="refusal",
             expect=["one of key-file or keys must be provided"],  # upstream text, see README citations
             argv=["--config", str(cfg_plain)],
             rows=["base_flags:--keys", "base_flags:--key-file"],
             note="neither source supplied a key outside development mode"),
    ]


MALFORMED_KEYS_TEXT = "- not\n- a\n- mapping\n"


def cfg_plain_body(port: int) -> str:
    return f"port: {port}\nbind_addresses: [127.0.0.1]\nrtc:\n  tcp_port: 0\n"


def cfg_with_keys(port: int) -> str:
    return cfg_plain_body(port) + f"keys:\n  c100a: {GOOD_SECRET}\n"


# --------------------------------------------------------------------------- verdicts
NORMALIZERS = [
    (re.compile(r"/tmp/[^ \n\"']*"), "<TMP>"),
    (re.compile(r"\b127\.0\.0\.1:(\d+)\b"), r"127.0.0.1:<PORT>"),
    (re.compile(r"port: ?\d+"), "port: <PORT>"),
    (re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[^\s]*\b"), "<TS>"),
    (re.compile(r"0x[0-9a-f]+"), "<HEX>"),
    # The node id is minted per launch (`ND_` + random, pkg/rtc/node_id.go), so it is not a value
    # either side was told to use. Left raw, every start row fails stream parity on its own noise.
    (re.compile(r"\bND_[0-9A-Za-z_-]{6,}\b"), "<NODEID>"),
]

# Two classes of line are printed by the *service* after it is already serving: the per-request
# error log (pkg/service/utils.go:79-81) and the shutdown notice (cmd/server/main.go:313-320).
# A base flag governs what happens before and during startup, so these are compared and reported
# apart from the flag surface rather than silently folded into it or dropped from it.
REQUEST_LOG = re.compile(r"error handling request|\"path\": \"/")
SHUTDOWN_LOG = re.compile(r"exit requested, shutting down")


def normalize(text: str) -> str:
    out = text
    for pattern, repl in NORMALIZERS:
        out = pattern.sub(repl, out)
    return out.strip()


def split_lines(text: str) -> dict[str, list[str]]:
    """Bucket one stream by who decides to print it, keeping the order inside each bucket."""
    buckets: dict[str, list[str]] = {"flag": [], "request_log": [], "shutdown": []}
    for line in normalize(text).splitlines():
        if not line.strip():
            continue
        buckets["request_log" if REQUEST_LOG.search(line)
                else "shutdown" if SHUTDOWN_LOG.search(line) else "flag"].append(line)
    return buckets


def compare_streams(go: dict, rust: dict) -> dict:
    """Compare one pair of recorded streams by bucket and as a whole.

    `flag` holds the lines a base flag actually governs and must match exactly. The service's own
    logging (per-request errors, the shutdown notice) is reported as a set difference so a Go-only
    or Rust-only line there stays visible without deciding the flag row. If the whole streams
    differ while every bucket matches, the difference is one of *order*, which no bucket accounts
    for and the gate treats as a finding rather than noise.
    """
    go_total, rust_total = go["stdout"] + go["stderr"], rust["stdout"] + rust["stderr"]
    g, r = split_lines(go_total), split_lines(rust_total)
    divergences = {}
    for bucket in ("flag", "request_log", "shutdown"):
        if g[bucket] == r[bucket]:
            continue
        divergences[bucket] = {"go_only": [l for l in g[bucket] if l not in r[bucket]][:4],
                               "rust_only": [l for l in r[bucket] if l not in g[bucket]][:4],
                               "counts": {"go": len(g[bucket]), "rust": len(r[bucket])}}
    whole_equal = normalize(go_total) == normalize(rust_total)
    buckets_equal = all(g[b] == r[b] for b in g)
    return {"divergences": divergences, "whole_equal": whole_equal,
            "buckets_equal": buckets_equal,
            "counts": {"go": {b: len(v) for b, v in g.items()},
                       "rust": {b: len(v) for b, v in r.items()}}}


def first_diff(go: str, rust: str, width: int = 3) -> list[str]:
    """The cheapest thing that makes a parity failure attributable: the first lines that differ."""
    g, r = go.splitlines(), rust.splitlines()
    out = []
    for i in range(max(len(g), len(r))):
        gl = g[i] if i < len(g) else "<absent>"
        rl = r[i] if i < len(r) else "<absent>"
        if gl != rl:
            out.append(f"#{i} go={gl[:160]!r} rust={rl[:160]!r}")
            if len(out) >= width:
                break
    return out


def verdict(profile: dict, go: dict, rust: dict) -> dict:
    checks, problems = [], []
    checks.append(("exit_preserved", go["exit"] is not None and rust["exit"] is not None))
    checks.append(("parity_exit", go["exit"] == rust["exit"]))
    if profile["kind"] == "refusal":
        # `expect_*` alone would pass if both sides served and were then SIGINTed to 0; a refusal
        # row is only a refusal if the process itself returned 1.
        checks.append(("refusal_exit_1", go["exit"] == 1 and rust["exit"] == 1))
        # ...and it only means the process reached the refusal if it ended before the harness
        # signalled it. A row killed by SIGINT exits negative whatever its argv said.
        checks.append(("refusal_self_exits",
                       go.get("self_exited") is True and rust.get("self_exited") is True))
    # Stream choice is part of the contract (main.go:190-192 prints refusals on stdout),
    # so stdout and stderr are compared apart before they are compared together.
    checks.append(("parity_stdout", normalize(go["stdout"]) == normalize(rust["stdout"])))
    cmp = compare_streams(go, rust)
    divergences = cmp["divergences"]
    # The bucket a base flag actually governs must match line for line; the service's own
    # request/shutdown logging is reported below so a difference there names itself instead of
    # either failing this row or disappearing from it.
    checks.append(("parity_flag_lines", "flag" not in divergences))
    if profile["kind"] == "refusal":
        # Two empty streams are "equal" on every parity check above, so a refusal row also has to
        # show that *something* flag-decided was printed on each side.
        checks.append(("refusal_printed", cmp["counts"]["go"]["flag"] > 0
                       and cmp["counts"]["rust"]["flag"] > 0))
    # A whole-stream difference that no bucket accounts for is an ordering difference between the
    # classes of line, which is behaviour and not log noise.
    checks.append(("parity_line_order", not (cmp["whole_equal"] is False and cmp["buckets_equal"])))
    for side, run in (("go", go), ("rust", rust)):
        for needle in profile.get("expect", []):
            checks.append((f"expect_{side}", needle in run["stdout"] + run["stderr"]))
    if profile["kind"] == "start":
        checks.append(("go_accepted", go["accepted"] is True))
        checks.append(("rust_accepted", rust["accepted"] is True))
        for name in profile.get("probes", []):
            probe = name[0] if isinstance(name, (tuple, list)) else name
            go_status = (go["probes"].get(probe) or {}).get("status")
            rust_status = (rust["probes"].get(probe) or {}).get("status")
            checks.append((f"probe_{probe}_agrees", go_status == rust_status))
            if go_status != rust_status:
                problems.append(f"{probe}: go={go_status} rust={rust_status}")
    # A row may only claim a *graceful* stop if the harness never had to kill the process group.
    for side, run in (("go", go), ("rust", rust)):
        checks.append((f"clean_stop_{side}", "SIGKILL" not in run["signals"]))
    diffs = {}
    # Every failed check is a problem. An earlier draft only recorded checks whose label started
    # with "parity"/"expect", so a start row where neither side ever served (accepted=False on both,
    # exits equal, empty streams) scored `pass=True`: the parity checks compared two failures and
    # found them equal. Attribution stays on the stream checks, which are the ones with bytes to show.
    for label, ok in checks:
        if ok:
            continue
        problems.append(label)
        if label == "parity_stdout":
            diffs["stdout"] = first_diff(normalize(go["stdout"]), normalize(rust["stdout"]))
        elif label == "parity_flag_lines":
            diffs["stderr_flag_lines"] = divergences.get("flag", {})
        elif label == "parity_line_order":
            diffs["line_order"] = {"go": cmp["counts"]["go"], "rust": cmp["counts"]["rust"],
                                   "first": first_diff(normalize(go["stdout"] + go["stderr"]),
                                                       normalize(rust["stdout"] + rust["stderr"]))}
        elif label.startswith("clean_stop_"):
            diffs[label] = {"signals": {"go": go["signals"], "rust": rust["signals"]},
                            "stop_latency_ms": {"go": go.get("stop_latency_ms"),
                                                "rust": rust.get("stop_latency_ms")}}
        elif label.startswith("expect_"):
            side = label.split("_", 1)[1]
            run = {"go": go, "rust": rust}[side]
            needles = [n for n in profile.get("expect", [])
                       if n not in run["stdout"] + run["stderr"]]
            diffs[label] = {"missing": needles,
                            "head": first_diff(run["stdout"] + run["stderr"], "")[:2]}
        elif label.endswith("_accepted"):
            diffs[label] = {"accepted": {"go": go["accepted"], "rust": rust["accepted"]},
                            "exit": {"go": go["exit"], "rust": rust["exit"]},
                            "signals": {"go": go["signals"], "rust": rust["signals"]}}
        elif label.startswith("refusal_exit"):
            diffs[label] = {"exit": {"go": go["exit"], "rust": rust["exit"]}}
        elif label == "refusal_self_exits":
            diffs[label] = {"self_exited": {"go": go.get("self_exited"),
                                            "rust": rust.get("self_exited")},
                            "exit": {"go": go["exit"], "rust": rust["exit"]},
                            "exit_wait_ms": {"go": go.get("exit_wait_ms"),
                                             "rust": rust.get("exit_wait_ms")},
                            "signals": {"go": go["signals"], "rust": rust["signals"]}}
        elif label == "refusal_printed":
            diffs[label] = {"flag_lines": {"go": cmp["counts"]["go"]["flag"],
                                           "rust": cmp["counts"]["rust"]["flag"]}}
    problems = list(dict.fromkeys(problems))
    return {"id": profile["id"], "kind": profile["kind"], "rows": profile["rows"],
            "pass": not problems, "problems": problems, "diffs": diffs,
            "exits": {"go": go["exit"], "rust": rust["exit"]},
            "self_exited": {"go": go.get("self_exited"), "rust": rust.get("self_exited")},
            "exit_wait_ms": {"go": go.get("exit_wait_ms"), "rust": rust.get("exit_wait_ms")},
            "accepted": {"go": go["accepted"], "rust": rust["accepted"]},
            "streams_equal": cmp["whole_equal"],
            "line_counts": cmp["counts"],
            "service_log_divergences": {k: v for k, v in divergences.items() if k != "flag"},
            "stop": {"signals": {"go": go["signals"], "rust": rust["signals"]},
                     "latency_ms": {"go": go.get("stop_latency_ms"),
                                    "rust": rust.get("stop_latency_ms")}},
            "checks": [c[0] for c in checks if c[1]]}


def assert_no_off_limits(argv: list[str]) -> None:
    """The developer host runs its own livekit-server on 7881/7882. No argv this harness builds,
    and no argv an injection imagines, may name either port."""
    for token in argv:
        for port in OFF_LIMITS:
            assert not re.search(rf"(^|[^0-9]){port}($|[^0-9])", token), f"off-limits port in {argv}"


def refuse_filed_slot(path: Path) -> None:
    """A --report re-derives a receipt and writes next to it, so pointing it at committed evidence
    rewrites that evidence in place. Filed slots are immutable; report a copy in a scratch dir."""
    resolved = Path(path).resolve()
    if "/server/evidence/" in str(resolved):
        raise SystemExit(json.dumps({
            "error": "refusing_to_touch_filed_evidence", "path": str(resolved),
            "why": "this harness writes execution.json next to the receipt it reads",
            "use": "cp the receipt to a scratch directory and point --report/--selftest-gate there"}))


def identity_gate(doc: dict) -> dict:
    """Do the recorded identities still describe the files on disk? A receipt whose binary or
    harness moved is provenance about an older run, and the gate has to say so out loud."""
    bins = doc.get("binaries", {})
    now = {}
    for side in ("official", "rust", "harness"):
        rec = bins.get(side, {})
        path = Path(rec.get("path", ""))
        now[side] = sha256(path) if path.exists() else None
    doc["identity_now"] = now
    gate = {f"{side}_unchanged": (now[side] is not None
                                  and now[side] == rec.get("sha256"))
            for side, rec in ((s, bins.get(s, {})) for s in ("official", "rust", "harness"))}
    # Part of the same provenance question as a moved binary: the upstream sentences this lane
    # asserts were read from a specific checkout, and a receipt whose reference commit is not that
    # commit describes a run against something else.
    gate["go_reference_matches_locked"] = (bins.get("go_reference_commit") == FIXED_GO_COMMIT)
    return gate


def aggregate(results: list[dict]) -> dict:
    """Lane-level view of what the launch asserted and what it only observed. The flag bucket is
    what these rows claim; anything the service prints on its own (request logs, shutdown notes)
    is listed here so a scoped-out difference stays visible instead of being folded into a pass."""
    observed = {r["id"]: {b: v["counts"] for b, v in r["service_log_divergences"].items()}
                for r in results if r["service_log_divergences"]}
    stops = {r["id"]: r["stop"] for r in results
             if r["stop"]["signals"]["go"] != r["stop"]["signals"]["rust"]}
    return {
        "flag_surface_rows": len(results),
        "full_streams_equal": sum(1 for r in results if r["streams_equal"]),
        "service_log_divergences": observed,
        "shutdown_surface_divergences": stops,
    }


GATE_INJECTIONS = (
    ("parity_exit_drifted", "parity_exit",
     "two sides exiting differently is a behaviour difference, not noise"),
    ("service_log_only_difference", "RECORDED:request_log",
     "a Go-only request-error line must not decide the flag row, and must not be dropped either"),
    ("line_order_swapped", "parity_line_order",
     "streams with the same lines in a different class order differ in behaviour"),
    ("forced_kill", "clean_stop_rust",
     "a process that ignored SIGINT until SIGKILL never demonstrated a graceful stop"),
    ("refusal_starts_serving", "refusal_exit_1",
     "a refusal row that serves and is then signalled is not a refusal"),
    ("refusal_survives_the_wait", "refusal_self_exits",
     "a refusal the harness had to signal never demonstrated that the flag ends the process"),
    ("refusal_prints_nothing", "refusal_printed",
     "two empty streams agree on every parity check while proving nothing was printed"),
    ("never_accepts", "accepted",
     "if neither side ever binds, equal failures must not read as parity"),
    ("needle_missing", "expect_rust",
     "the refusal sentence the row attests has to be in the recorded stream"),
    ("probe_secret_split", "probe_",
     "a key one authorizer accepts and the other refuses is a finding"),
    ("binary_identity_drifted", "IDENTITY_DRIFT",
     "a receipt re-read after the candidate was rebuilt must name the moved binary"),
    ("harness_identity_drifted", "IDENTITY_DRIFT",
     "a receipt re-read after the harness was edited describes an older lane"),
    ("go_reference_moved", "IDENTITY_DRIFT",
     "the upstream sentences are quoted from one checkout, so a receipt from another cannot attest them"),
    ("off_limit_port_in_argv", "AssertionError",
     "7881/7882 belong to the developer host and may never reach argv"),
)


def selftest_gate(doc: dict, out_dir: Path) -> int:
    """Recompute every verdict from a recorded run under named in-memory mutations and assert each
    mutation reddens the check it targets. Nothing is launched and nothing on disk changes."""
    # Every injection needs a row of a particular shape. On a partial receipt it would silently
    # target nothing (or crash in a dict lookup), and a green falsification sweep that never
    # exercised a check is worse than no sweep.
    needs = {"parity_exit_drifted": ("start",), "refusal_starts_serving": ("refusal",),
             "refusal_survives_the_wait": ("refusal",), "refusal_prints_nothing": ("refusal",),
             "never_accepts": ("start",), "needle_missing": ("expect",),
             "probe_secret_split": ("probes",), "service_log_only_difference": ("start",),
             "line_order_swapped": ("start",), "forced_kill": ("start",),
             "binary_identity_drifted": (), "harness_identity_drifted": (),
             "go_reference_moved": (),
             "off_limit_port_in_argv": ()}
    kinds = {m.get("kind") for m in doc["profiles"].values()}
    have_expect = any(m.get("expect") for m in doc["profiles"].values())
    have_probes = any(m.get("probes") for m in doc["profiles"].values())
    missing = {name: [n for n in need
                      if (n in ("start", "refusal") and n not in kinds)
                      or (n == "expect" and not have_expect)
                      or (n == "probes" and not have_probes)]
               for name, need in needs.items()}
    missing = {k: v for k, v in missing.items() if v}
    if missing:
        raise SystemExit(json.dumps({"error": "receipt_cannot_falsify_every_gate",
                                     "missing_shapes": missing,
                                     "why": "run the full profile set before the gate sweep"}))

    def recompute_results(d):
        out = []
        for pid, meta in d["profiles"].items():
            out.append(verdict({"id": pid, "kind": meta["kind"], "rows": meta.get("rows", []),
                                "probes": meta.get("probes", []), "expect": meta.get("expect", [])},
                               d["runs"][pid]["go"], d["runs"][pid]["rust"]))
        return out

    def recompute(d):
        return [f"{r['id']}:{p}" for r in recompute_results(d) for p in r["problems"]]

    def one(kind):
        return next((pid for pid, m in doc["profiles"].items() if m["kind"] == kind), None)

    def with_doc(mutate):
        d = json.loads(json.dumps(doc))
        mutate(d)
        return d

    def inj_parity(d):
        d["runs"][one("start")]["rust"]["exit"] += 1

    def inj_refusal(d):
        pid = one("refusal")
        for side in ("go", "rust"):
            d["runs"][pid][side]["exit"] = 0

    def inj_refusal_survives(d):
        """The process was still alive when the harness signalled it: the row cannot claim the
        flag ended the run, whatever the streams look like afterwards."""
        pid = one("refusal")
        for side in ("go", "rust"):
            d["runs"][pid][side]["self_exited"] = False
            d["runs"][pid][side]["exit"] = 0
            d["runs"][pid][side]["signals"] = ["SIGINT"]

    def inj_refusal_silent(d):
        """Both streams empty, exit 1 on both sides: every parity check agrees because there is
        nothing to compare, which is the vacuous pass this injection exists to redden."""
        pid = one("refusal")
        for side in ("go", "rust"):
            run = d["runs"][pid][side]
            run["stdout"] = ""
            run["stderr"] = ""
            run["self_exited"] = True
            run["exit"] = 1
            run["signals"] = ["already-exited"]

    def inj_accept(d):
        pid = one("start")
        for side in ("go", "rust"):
            d["runs"][pid][side]["accepted"] = False

    def inj_needle(d):
        pid = next(p for p, m in doc["profiles"].items() if m.get("expect"))
        d["runs"][pid]["rust"]["stdout"] = ""
        d["runs"][pid]["rust"]["stderr"] = ""

    def inj_probe(d):
        pid = next(p for p, m in doc["profiles"].items() if m.get("probes"))
        probe = d["profiles"][pid]["probes"][0]
        d["runs"][pid]["rust"]["probes"][probe]["status"] = 401

    def inj_binary(d):
        d["binaries"]["rust"]["sha256"] = "stale-hash-from-an-older-build"

    def inj_harness(d):
        d["binaries"]["harness"]["sha256"] = "stale-hash-from-an-older-harness"

    def inj_reference(d):
        d["binaries"]["go_reference_commit"] = "0000000000000000000000000000000000000000"

    REQ_LINE = ('<TS>\tWARN\tlivekit\tservice/utils.go:85\terror handling request\t'
                '{"status": 401, "method": "POST", "path": "/twirp/Livekit.RoomService/ListRooms",'
                ' "error": "invalid authorization token"}')
    START_LINE = ('<TS>\tINFO\tlivekit\tservice/server.go:309\tstarting LiveKit server\t'
                  '{"portHttp": 55086, "nodeID": "ND_xxxxxx", "version": "1.13.10"}')

    def inj_service_log_only(d):
        """A Go-only request-error line: the flag row must stay green on it, and the receipt must
        still name it. Either half alone is a pass that proves nothing."""
        pid = one("start")
        d["runs"][pid]["go"]["stderr"] = d["runs"][pid]["go"]["stderr"] + "\n" + REQ_LINE

    def inj_order_swap(d):
        pid = one("start")
        d["runs"][pid]["go"]["stderr"] = START_LINE + "\n" + REQ_LINE
        d["runs"][pid]["rust"]["stderr"] = REQ_LINE + "\n" + START_LINE

    def inj_forced_kill(d):
        pid = one("start")
        d["runs"][pid]["rust"]["signals"] = ["SIGINT", "SIGINT#2", "SIGKILL"]

    mutators = {"parity_exit_drifted": inj_parity, "refusal_starts_serving": inj_refusal,
                "refusal_survives_the_wait": inj_refusal_survives,
                "refusal_prints_nothing": inj_refusal_silent,
                "never_accepts": inj_accept, "needle_missing": inj_needle,
                "probe_secret_split": inj_probe, "binary_identity_drifted": inj_binary,
                "harness_identity_drifted": inj_harness, "go_reference_moved": inj_reference,
                "service_log_only_difference": inj_service_log_only,
                "line_order_swapped": inj_order_swap, "forced_kill": inj_forced_kill}
    rows = []
    for name, expect, why in GATE_INJECTIONS:
        if name == "off_limit_port_in_argv":
            try:
                assert_no_off_limits(["--config", "x.yaml", "--port", str(OFF_LIMITS[0])])
                discriminated, detail = False, "no assertion raised"
            except AssertionError as exc:
                discriminated, detail = True, str(exc)[:80]
        else:
            d = with_doc(mutators[name])
            gate = identity_gate(d)
            problems = recompute(d)
            if expect == "IDENTITY_DRIFT":
                discriminated = not all(gate.values())
                detail = json.dumps({"gate": gate, "problems": problems[:3]})[:200]
            elif expect.startswith("RECORDED:"):
                bucket = expect.split(":", 1)[1]
                pid = one("start")
                row = next(r for r in recompute_results(d) if r["id"] == pid)
                # green on the flag surface *and* the difference still on the record
                discriminated = (bucket in row["service_log_divergences"]
                                 and not row["problems"])
                detail = json.dumps({"problems": row["problems"],
                                     "divergences": sorted(row["service_log_divergences"])})[:200]
            else:
                discriminated = any(expect in p for p in problems)
                detail = json.dumps(problems[:3])[:200]
        rows.append({"case": name, "injected": why, "expect": expect,
                     "discriminated": discriminated, "detail": detail})
    base = identity_gate(json.loads(json.dumps(doc)))
    base_problems = recompute(doc)
    base_obs = aggregate(recompute_results(doc))
    rows.insert(0, {"case": "untouched_measurement", "injected": "the recorded run as filed",
                    "expect": "clean", "discriminated": not base_problems and all(base.values()),
                    "detail": json.dumps({"problems": base_problems[:3], "gate": base})[:200]})
    all_ok = all(r["discriminated"] for r in rows)
    (out_dir / "gate-falsification.json").write_text(
        json.dumps({"cases": rows, "all_discriminated": all_ok,
                    "uninjected_observations": base_obs}, indent=1) + "\n", encoding="utf-8")
    print(json.dumps({"all_discriminated": all_ok,
                      "cases": {r["case"]: r["discriminated"] for r in rows}}, indent=1))
    return 0 if all_ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", type=Path, default=None)
    ap.add_argument("--rust", type=Path, default=None)
    ap.add_argument("--output", type=Path, default=None)
    ap.add_argument("--only", default=None, help="run one profile id")
    ap.add_argument("--settle", type=float, default=12.0)
    ap.add_argument("--refuse-wait", type=float, default=20.0,
                    help="how long a refusal row may take to exit on its own before the harness "
                         "signals it; a refusal the harness had to kill proves nothing")
    ap.add_argument("--reference", type=Path,
                    default=Path(__file__).resolve().parents[3],
                    help="repository root holding references/livekit-stable")
    ap.add_argument("--report", type=Path, default=None,
                    help="re-derive verdicts from a recorded execution.json")
    ap.add_argument("--selftest-gate", type=Path, default=None,
                    help="re-derive verdicts from a recorded execution.json under named "
                         "injections and assert each one reddens the gate it is meant to break")
    args = ap.parse_args()

    if args.report or args.selftest_gate:
        source = args.report or args.selftest_gate
        refuse_filed_slot(source.parent)
        doc = json.loads(source.read_text(encoding="utf-8"))
        if args.selftest_gate:
            return selftest_gate(doc, source.parent)
        results = [verdict({"id": pid, "kind": meta["kind"], "rows": meta.get("rows", []),
                            "probes": meta.get("probes", []),
                            "expect": meta.get("expect", [])},
                           doc["runs"][pid]["go"], doc["runs"][pid]["rust"])
                   for pid, meta in doc["profiles"].items() if profile_selected({"id": pid}, args)]
        if not results:
            raise SystemExit(json.dumps({"error": "no_legs_selected", "filter": args.only,
                                         "known_ids": sorted(doc["profiles"])}))
        gate = identity_gate(doc)
        agg = aggregate(results)
        ok = all(r["pass"] for r in results) and all(gate.values())
        print(json.dumps({"status": "DONE" if ok else ("FAILED" if not all(r["pass"] for r in results)
                                                       else "IDENTITY_DRIFT"),
                          "legs": sum(1 for r in results if r["pass"]),
                          "total": len(results),
                          "failed": [r["id"] for r in results if not r["pass"]],
                          "identity_gates": gate,
                          "scoped_observations": agg,
                          "identity_now": doc.get("identity_now")}, indent=1))
        return 0 if ok else 1

    if not (args.official and args.rust and args.output):
        raise SystemExit(json.dumps({"error": "launch_mode_needs_binaries",
                                     "use": "--official, --rust and --output are required to run"}))
    refuse_filed_slot(args.output)
    work = args.output
    # Fresh directory, like the other launched lanes: appending a second run's records to an
    # existing slot would mix two candidates in one receipt.
    try:
        work.mkdir(parents=True)
    except FileExistsError:
        raise SystemExit(json.dumps({"error": "output_dir_exists", "path": str(work.resolve()),
                                     "use": "pass a directory that does not exist yet"}))
    (work / "logs").mkdir()
    doc = {"batch": "C100-lane-A", "kind": "config-source-base-flags",
           "work_dir": str(work), "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
           "binaries": {"official": identity(args.official), "rust": identity(args.rust),
                        "go_reference_commit": go_reference_commit(args.reference),
                        "go_reference_dirty": go_reference_dirty(args.reference),
                        "harness": {"path": str(Path(__file__).resolve()),
                                    "sha256": sha256(Path(__file__).resolve())},
                        "python": sys.version.split()[0]},
           "timing": {"settle_s": args.settle, "refuse_wait_s": args.refuse_wait},
           "safety": {"off_limits": list(OFF_LIMITS), "ports_before": default_port_state()},
           "profiles": {}, "runs": {}, "verdicts": []}
    all_profiles = profiles(work)
    selected = [p for p in all_profiles if profile_selected(p, args)]
    # The same vacuity the --report path refuses: a filter that matched nothing would write a
    # receipt with zero legs that every aggregate below reads as unanimous success.
    if not selected:
        raise SystemExit(json.dumps({"error": "no_legs_selected", "filter": args.only,
                                     "known_ids": [p["id"] for p in all_profiles]}))
    doc["profiles"] = {p["id"]: {"kind": p["kind"], "rows": p["rows"], "argv": p["argv"],
                                 "expect": p.get("expect", []),
                                 "probes": [name for name, _ in p.get("probes", [])]}
                       for p in selected}
    for profile in selected:
        argv = list(profile["argv"])
        if profile["kind"] == "refusal":
            # A row that is supposed to refuse must still refuse on a port of its own: if the
            # refusal regressed, the daemon would otherwise try the upstream default 7881, which
            # belongs to the developer host and is out of bounds for this harness.
            guard = free_port()
            assert guard not in OFF_LIMITS, f"ephemeral guard port collided: {guard}"
            argv += ["--port", str(guard)]
        assert_no_off_limits(argv)
        doc["profiles"][profile["id"]]["argv"] = argv
        runs = {}
        for side, binary in (("go", args.official), ("rust", args.rust)):
            env = build_env(work, profile.get("env", {}))
            runs[side] = capture(binary, argv, env, work / "logs",
                                 f"{profile['id']}.{side}", profile.get("port"),
                                 profile.get("probes", []), args.settle,
                                 None if profile["kind"] != "refusal" else args.refuse_wait)
        doc["runs"][profile["id"]] = runs
        result = verdict(profile, runs["go"], runs["rust"])
        doc["verdicts"].append(result)
        print(f"{profile['id']:38s} go_exit={runs['go']['exit']} "
              f"rust_exit={runs['rust']['exit']} pass={result['pass']} "
              f"{result['problems'] if result['problems'] else ''}", flush=True)
        for name, detail in result["diffs"].items():
            print(f"    {name}: {json.dumps(detail, ensure_ascii=False)[:400]}", flush=True)
    doc["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    doc["safety"]["ports_after"] = default_port_state()
    gate = identity_gate(doc)              # also records doc["identity_now"]
    doc["identity_gate"] = gate
    complete = not args.only or len(selected) == len(all_profiles)
    passed = sum(1 for v in doc["verdicts"] if v["pass"])
    clean = bool(doc["verdicts"]) and passed == len(doc["verdicts"]) and all(gate.values())
    doc["summary"] = {"legs": len(doc["verdicts"]), "passed": passed,
                      "failed": [v["id"] for v in doc["verdicts"] if not v["pass"]],
                      "identity_gate": gate,
                      "scoped_observations": aggregate(doc["verdicts"]),
                      "ports_untouched": doc["safety"]["ports_before"] == doc["safety"]["ports_after"],
                      "claims": "flag-surface lines, exit codes, that a refusal ended the process on "
                                "its own, listener acceptance and the two auth probes per start row; "
                                "service request/shutdown logging is "
                                "reported under scoped_observations, not asserted here",
                      "status": "DONE" if clean else ("FAILED" if passed != len(doc["verdicts"])
                                                      else "IDENTITY_DRIFT")}
    doc["status"] = ("PASS_LAUNCHED_MAIN_BASE_FLAGS" if clean and complete
                     else "PARTIAL_SELECTION_BASE_FLAGS" if clean
                     else "IDENTITY_DRIFT" if all(v["pass"] for v in doc["verdicts"])
                     else "FAILED")
    # A ledger auditor reads the *top level* of a receipt to decide whether the run accepted
    # anything (scripts/check-parity-ledger.py only looks at `status`/`result` there), and the
    # other launched lanes all publish a lane-level BATCH.json beside the raw records. This lane
    # has to say the same thing in the same place, or a passing pair of servers cannot support a
    # row. Nothing new is claimed here: every field is lifted from the verdicts above. A `--only`
    # run is spelled PARTIAL on purpose: it attests the rows it selected, not the lane.
    selection = {"filter": args.only, "selected": len(selected),
                 "total": len(all_profiles), "complete": complete}
    doc["summary"]["selection"] = selection
    rows = collections.Counter(r for v in doc["verdicts"] for r in v["rows"])
    batch = {"status": doc["status"], "case_count": len(doc["verdicts"]),
             "passed": passed, "failed_cases": doc["summary"]["failed"],
             "selection": selection,
             "ledger_rows": sorted(rows), "rows_attested": dict(sorted(rows.items())),
             "identity_gate": gate, "identity_now": doc["identity_now"],
             "binaries": doc["binaries"], "timing": doc["timing"],
             "ports_untouched": doc["summary"]["ports_untouched"],
             "scoped_observations": doc["summary"]["scoped_observations"],
             "claims": doc["summary"]["claims"],
             "started_at": doc["started_at"], "finished_at": doc["finished_at"]}
    (work / "BATCH.json").write_text(json.dumps(batch, indent=1, ensure_ascii=True) + "\n",
                                     encoding="utf-8")
    (work / "execution.json").write_text(json.dumps(doc, indent=1, ensure_ascii=True) + "\n",
                                        encoding="utf-8")
    print(json.dumps(doc["summary"]))
    return 0 if doc["summary"]["status"] == "DONE" else 1


def go_reference_commit(ref: Path) -> str:
    ref = ref / "references" / "livekit-stable"
    out = subprocess.run(["git", "-C", str(ref), "rev-parse", "HEAD"],
                         capture_output=True, text=True)
    return out.stdout.strip() or "unknown"


def go_reference_dirty(ref: Path) -> int:
    out = subprocess.run(["git", "-C", str(ref / "references" / "livekit-stable"),
                          "status", "--porcelain"], capture_output=True, text=True)
    return len([l for l in out.stdout.splitlines() if l.strip()])


def selected_id(pid: str, only) -> bool:
    """`--only` accepts the README's short case number (A-06) or the full id. The ids carry a
    trailing slug, so the old exact-equality filter selected nothing for `A-01` and the run then
    reported DONE over zero legs -- a pass with no assertion in it."""
    return only is None or pid == only or pid.startswith(only + "-")


def profile_selected(profile: dict, args) -> bool:
    return selected_id(profile["id"], args.only)


if __name__ == "__main__":
    sys.exit(main())
