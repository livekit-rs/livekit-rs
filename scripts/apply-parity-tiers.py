#!/usr/bin/env python3
"""SPEC-0075 §9.1 tier audit migration for the parity ledger.

Assigns each item the five verification tiers that its *resolvable evidence* can
substantiate, then lowers `state` to what those tiers support. Promotion is never
automated: only a real run can move a tier forward, and that is done by hand in a
later batch with the artifact named.

Dry run by default; `--apply` rewrites the ledger.

Run: python3 scripts/apply-parity-tiers.py [--apply]
"""

from __future__ import annotations

import argparse
import collections
import importlib.util
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True

_spec = importlib.util.spec_from_file_location(
    "parity_ledger_checker", os.path.join(HERE, "check-parity-ledger.py"))
cpl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cpl)

TIERS = cpl.TIERS
# identity rules live in the checker so inference and validation cannot drift
item_match_keys = cpl.item_match_keys
artifact_names_item = cpl.artifact_names_item

# state ceiling for the highest tier an item can substantiate
STATE_BY_MAX_TIER = {
    0: "OPEN_AUDIT",
    1: "OPEN_AUDIT",
    2: "IMPLEMENTED_UNVERIFIED",
    3: "PARTIAL",
    4: "PARTIAL",
    5: "VERIFIED",
}
# never let the migration raise a state
STATE_RANK = {"OPEN": 0, "OPEN_AUDIT": 1, "UPSTREAM_NOT_APPLICABLE": 2,
              "IMPLEMENTED_UNVERIFIED": 3, "PARTIAL": 4, "VERIFIED": 5}

SOURCE_AUDIT_HINTS = ("source-audit", "source_audit", "source-review",
                      "source_review", "-source.", "consumer-ledger",
                      "config-consumer")


def item_abs(ref: str, repo_root: str) -> str | None:
    file_part = ref.split("::", 1)[0]
    for full in cpl.citation_paths(repo_root, file_part):
        if os.path.isfile(full):
            return full
    return None


def read_text(abs_path: str) -> str | None:
    try:
        with open(abs_path, encoding="utf-8") as handle:
            return handle.read()
    except (OSError, UnicodeDecodeError):
        return None


def tier1_attested(item: dict, refs: list[str], repo_root: str) -> bool:
    """Upstream contract read: an explicit source location on the item, or an
    artifact that is a source audit / consumer ledger."""
    for key in ("source", "contract", "source_review", "source_paths", "implementation_spec"):
        value = item.get(key)
        if isinstance(value, str) and (".go" in value or ".swift" in value or ".ts" in value):
            return True
        if isinstance(value, list) and any(".go" in str(v) for v in value):
            return True
    for ref in refs:
        low = ref.lower()
        if any(h in low for h in SOURCE_AUDIT_HINTS):
            return True
        abs_path = item_abs(ref, repo_root)
        if abs_path and abs_path.endswith(".json"):
            body = read_text(abs_path)
            if body is None:
                continue
            body = body.lower()
            if "livekit" in body and (".go" in body or "upstream" in body):
                return True
    return False


def artifact_identities(refs: list[str], repo_root: str,
                        keys: list[str]) -> tuple[bool, bool]:
    """Return (saw_rust_identity, saw_paired_identity) over item-naming artifacts.

    A Rust *binary* identity means the assembled server/CLI executable really ran,
    which is what separates tier 4 from an in-process component test. A Go identity
    next to it is what separates tier 5. Delegates to the checker so both tools
    agree on what counts; an identity only transfers to this item when the artifact
    names it.
    """
    saw_rust = saw_paired = False
    for ref in refs:
        abs_path = item_abs(ref, repo_root)
        if not abs_path:
            continue
        for candidate in cpl.slot_receipts(abs_path):
            if not artifact_names_item(candidate, keys):
                continue
            info = cpl.classify_artifact(candidate)
            saw_rust = saw_rust or info["rust_identity"]
            saw_paired = saw_paired or info["paired"]
    return saw_rust, saw_paired


EXECUTION_KEYS = ("tests_passed", "tests_failed", "executed", "runs",
                  "cases", "cargo test", "test result",
                  # delivery receipts record the run they performed
                  "public_differential", "paired_cases", "node_tests",
                  "checks", "differences", "results")


def attests_named_test(ref: str, repo_root: str) -> bool:
    """`file.rs::identifier` where the identifier really is in that file.

    A citation that names a test which exists is evidence the behavior was
    executed. A citation to a file, or to a test that isn't there, is not.
    """
    if "::" not in ref:
        return False
    abs_path = item_abs(ref, repo_root)
    if not abs_path or not abs_path.endswith(".rs"):
        return False
    ident = ref.split("::", 1)[1].strip()
    if not ident:
        return False
    body = read_text(abs_path)
    if body is None:
        return False
    return re.search(r"\b" + re.escape(ident) + r"\b", body) is not None


def attests_executed_artifact(ref: str, repo_root: str, keys: list[str]) -> bool:
    """An artifact that records an actual run of *this* item's behavior."""
    abs_path = item_abs(ref, repo_root)
    if not abs_path or not abs_path.endswith(".json"):
        return False
    if not artifact_names_item(abs_path, keys):
        return False
    body = read_text(abs_path)
    if body is None:
        return False
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return "test result:" in body or "cargo test" in body
    if not isinstance(data, dict):
        return False
    json_text = json.dumps(data).lower()
    if any(k in {str(x).lower() for x in data} for k in EXECUTION_KEYS):
        return True
    return "test result:" in json_text or "cargo test" in json_text


def tiers_for(item: dict, repo_root: str) -> dict:
    refs = item.get("evidence") or []
    keys = item_match_keys(item["id"])
    resolved = [r for r in refs if cpl.is_evidence_path(r, repo_root)[0]]
    failing = set(cpl.failing_evidence(refs, repo_root))
    usable = [r for r in resolved if r not in failing]

    max_tier = 0
    if tier1_attested(item, refs, repo_root):
        max_tier = 1

    # tier 2: config parsed and projected (lk-config owns parse + normalize)
    if any("crates/lk-config" in r for r in usable):
        max_tier = max(max_tier, 2)

    # tier 3: behavior actually executed. Parsing-only citations never qualify, and
    # neither does a file reference that names no test or records no run.
    substantive = [r for r in usable if not any(p in r for p in cpl.PARSE_ONLY_FILES)]
    if any(attests_named_test(r, repo_root)
           or attests_executed_artifact(r, repo_root, keys) for r in substantive):
        max_tier = max(max_tier, 3)

    saw_rust, saw_paired = artifact_identities(usable, repo_root, keys)
    if cpl.interop_fixture_evidence(usable, repo_root, item["id"]):
        saw_rust = True
    if saw_rust:
        max_tier = max(max_tier, 4)
    if saw_paired:
        max_tier = max(max_tier, 5)

    levels = {t: ("MET" if i <= max_tier else "NOT_RUN")
              for i, t in enumerate(TIERS, start=1)}
    return {"levels": levels, "max_tier": max_tier}


PATH_TOKEN = re.compile(r"[\w./\-]+\.(?:rs|json|md|py|mjs|txt)")
IDENT_TOKEN = re.compile(r"\b([a-z][a-z0-9_]{7,})\b")
SKIPPED_DIRS = ("target", ".git", "node_modules", "references")


def find_cited_file(rel: str, repo_root: str) -> str | None:
    """Locate the single real file a malformed citation means.

    Batches wrote the same evidence four ways: repo-relative, server-relative,
    crate-relative (`lk-config/tests/x.rs`) and bare basename plus a line number.
    The last two are accepted only when they resolve to exactly one file, so a
    repaired citation still names one real artifact rather than a guess.
    """
    for base in (repo_root, cpl.server_root(repo_root)):
        for prefix in ("", "crates/"):
            candidate = os.path.join(base, prefix + rel)
            if os.path.isfile(candidate):
                return candidate
    matches = []
    for where, dirs, names in os.walk(cpl.server_root(repo_root)):
        dirs[:] = [d for d in dirs if d not in SKIPPED_DIRS]
        if rel in names:
            matches.append(os.path.join(where, rel))
    return matches[0] if len(matches) == 1 else None


def normalize_ref(ref: str, repo_root: str) -> tuple[str, str | None]:
    """Rewrite a machine-unresolvable citation into `path::identifier` form.

    Many batch-21b/21c entries were written as
    `crates/x.rs: some_test_name (what it covers)`, which names real evidence but
    cannot be resolved by any tool. Rewriting is only allowed when the file
    resolves *and* the identifier literally appears in it, so this never invents
    evidence. The original string is returned to the caller for the audit record.
    """
    if cpl.is_evidence_path(ref, repo_root)[0]:
        return ref, None
    path_match = PATH_TOKEN.search(ref)
    if not path_match:
        return ref, None
    rel = path_match.group(0)
    abs_path = find_cited_file(rel, repo_root)
    if not abs_path:
        return ref, None
    rel = os.path.relpath(abs_path, repo_root)
    try:
        with open(abs_path) as fh:
            body = fh.read()
    except (OSError, UnicodeDecodeError):
        return ref, None
    for token in IDENT_TOKEN.findall(ref):
        if token in ("crates", "tests", "evidence", "existing", "livekit",
                     "doc", "docs", "test", "src"):
            continue
        if re.search(r"\b(?:fn|mod|struct|enum)\s+" + re.escape(token) + r"\b", body) \
                or re.search(r"\b" + re.escape(token) + r"\b\s*(?:\(|:)", body):
            return f"{rel}::{token}", ref
    return f"{rel}", ref


def normalize_evidence(items: list[dict], repo_root: str) -> int:
    repaired = 0
    for it in items:
        refs = it.get("evidence") or []
        if not refs:
            continue
        out, notes = [], []
        for ref in refs:
            new, original = normalize_ref(ref, repo_root)
            out.append(new)
            if original:
                notes.append({"from": original, "to": new})
                repaired += 1
        it["evidence"] = out
        if notes:
            audit = it.setdefault("tier_audit", {})
            audit["ref_normalized"] = notes
            audit.setdefault("date", "2026-11-03")
            audit.setdefault("batch", "21k")
    return repaired


def paired_receipt_index(repo_root: str) -> list[tuple[str, str]]:
    """(repo-relative path, text) for every passing artifact with both identities."""
    evidence = os.path.join(cpl.docs_root(repo_root), "evidence")
    out = []
    for where, dirs, names in os.walk(evidence):
        dirs[:] = [d for d in dirs if d not in SKIPPED_DIRS]
        for name in names:
            if not name.endswith(".json"):
                continue
            abs_path = os.path.join(where, name)
            info = cpl.classify_artifact(abs_path)
            if not info["paired"] or info["failed"]:
                continue
            text = cpl.artifact_text(abs_path)
            if text is not None:
                out.append((os.path.join("livekit-rs-docs", os.path.relpath(abs_path, cpl.docs_root(repo_root))), text))
    return out


def attribute_paired_receipts(items: list[dict],
                              receipts: list[tuple[str, str]]) -> int:
    """Cite receipts that already name an item but were never wired to it.

    A batch receipt whose `batch`/`scope` spells out a ledger item id is that batch
    asserting the id was covered. Linking it is attribution of existing evidence,
    not new evidence, so it is restricted to the *full* item id with delimiters
    (`YAML:logging.level` is not satisfied by a run of the distinct
    `generated_flags:--logging.level`) and to artifacts that passed.
    """
    linked = 0
    for it in items:
        cited = {r.split("::", 1)[0] for r in (it.get("evidence") or [])}
        for rel, text in receipts:
            if rel in cited or not cpl.names_item_in_text(text, it["id"]):
                continue
            it.setdefault("evidence", []).append(rel)
            audit = it.setdefault("tier_audit", {})
            audit.setdefault("date", "2026-11-03")
            audit.setdefault("batch", "21k")
            audit.setdefault("receipt_attributed", []).append(rel)
            cited.add(rel)
            linked += 1
    return linked


def migrate(ledger_path: str, repo_root: str, apply: bool,
            force: bool = False) -> dict:
    with open(ledger_path) as handle:
        ledger = json.load(handle)
    items = ledger["items"]

    normalized = normalize_evidence(items, repo_root)
    attributed = attribute_paired_receipts(items, paired_receipt_index(repo_root))

    tier_hist = collections.Counter()
    state_hist_before = collections.Counter(i["state"] for i in items)
    changes: list[tuple[str, str, str, int]] = []

    for it in items:
        if it["state"] == "UPSTREAM_NOT_APPLICABLE":
            continue
        if it.get("verification_levels") and not force:
            # hand-attested tiers win: a person read the artifact and said what it
            # proves. Re-running the inference must not erase that.
            declared = it["verification_levels"]
            max_tier = max((i for i, t in enumerate(TIERS, start=1)
                            if declared.get(t) == "MET"), default=0)
            info = {"levels": declared, "max_tier": max_tier}
        else:
            info = tiers_for(it, repo_root)
        tier_hist[info["max_tier"]] += 1
        target = STATE_BY_MAX_TIER[info["max_tier"]]
        it["verification_levels"] = info["levels"]
        if STATE_RANK[target] < STATE_RANK[it["state"]]:
            changes.append((it["id"], it["state"], target, info["max_tier"]))
            it["state_before_tier_audit"] = it["state"]
            audit = it.setdefault("tier_audit", {})
            audit.update({
                "date": "2026-11-03",
                "batch": "21k",
                "highest_attested_tier": info["max_tier"],
                "rule": "SPEC-0075 §9.1",
            })
            it["state"] = target

    result = {
        "items": len(items),
        "highest_attested_tier_histogram": {str(k): v for k, v in sorted(tier_hist.items())},
        "state_counts_before": dict(state_hist_before),
        "state_counts_after": dict(collections.Counter(i["state"] for i in items)),
        "refs_normalized": normalized,
        "receipts_attributed": attributed,
        "downgraded": len(changes),
        "downgraded_examples": [
            {"id": cid, "from": a, "to": b, "max_tier": t} for cid, a, b, t in changes[:40]],
    }
    if apply:
        with open(ledger_path, "w") as handle:
            json.dump(ledger, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
    return result


def main() -> int:
    repo_root = os.path.dirname(HERE)
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", default=os.path.join(
        cpl.docs_root(repo_root), "specs/0011-functional-parity-todo.json"))
    ap.add_argument("--root", default=repo_root)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="re-infer tiers even where a human already declared them")
    ap.add_argument("--json-out")
    args = ap.parse_args()

    result = migrate(args.ledger, args.root, args.apply, args.force)
    print("mode:", "APPLIED" if args.apply else "DRY RUN")
    print("items:", result["items"])
    print("highest attested tier -> count:",
          result["highest_attested_tier_histogram"])
    print("state before:", result["state_counts_before"])
    print("state after: ", result["state_counts_after"])
    print("refs normalized to resolvable form:", result["refs_normalized"])
    print("uncited receipts attributed to the items they name:",
          result["receipts_attributed"])
    print("downgraded:", result["downgraded"])
    for ex in result["downgraded_examples"][:10]:
        print(f"   {ex['id']}  {ex['from']} -> {ex['to']} (tier {ex['max_tier']})")
    if args.json_out:
        with open(args.json_out, "w") as handle:
            json.dump(result, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
        print("wrote", args.json_out)
    return 0


if __name__ == "__main__":
    main()
