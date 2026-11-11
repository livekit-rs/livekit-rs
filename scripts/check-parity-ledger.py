#!/usr/bin/env python3
"""SPEC-0075 parity-ledger state checker.

Validates livekit-rs-docs/specs/0011-functional-parity-todo.json against the
evidence-tier rules in specs/0075-verification-evidence-tiers.md:

  * every evidence reference resolves to a real repo artifact (prose claims do not count)
  * VERIFIED requires the tiers SPEC-0075 §2 demands
  * VERIFIED must not contradict its own free text
  * parent items need a coverage map, not a child count
  * parse-test-only evidence caps an item at config_projection

Exit code 0 when no finding of a *blocking* kind is present, 1 otherwise.
`--baseline` records the current finding counts into a JSON file instead, so a
workline can ratchet the numbers down without a red build mid-slice.
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import sys

TIERS = [
    "upstream_source_contract",
    "config_projection",
    "component_behavior",
    "main_server_consumer",
    "official_differential_lifecycle",
]
TIER_VALUES = {"MET", "PARTIAL", "NOT_RUN", "NOT_APPLICABLE"}

# Free-text markers meaning "this is not closed".
UNRESOLVED = re.compile(
    r"\b(OPEN|NOT_RUN|OPEN_AUDIT|IMPLEMENTED_UNVERIFIED|TODO|FIXME|PENDING)\b"
    r"|未验收|未闭环|仍需|尚未|待补|待验证|not yet|remains? open|still open"
)
# Markers that legitimately describe a *scoped* remainder.
SCOPE_KEYS = ("accepted_scope", "verified_scope", "scope")

# Test files that only prove parsing/projection, never a live consumer.
PARSE_ONLY_FILES = ("crates/lk-config/tests/config_leaf_parsing.rs",)

# Evidence that documents an upstream reading or a projection ledger. These can
# attest tier 1/2 but never a live consumer, whatever the item's prose claims.
CONTRACT_ONLY_EVIDENCE = (
    "f62-config-source-audit.json",
    "config-consumer-audio-room-network-ledger.json",
    "config-consumer-agents-room-api-ledger.json",
    "config-consumer-limits-logging-presets-ledger.json",
)


def non_parse_only_evidence(refs: list[str], repo_root: str) -> list[str]:
    """Evidence that is neither prose nor a parse/projection-only test."""
    out = []
    for ref in refs:
        if not is_evidence_path(ref, repo_root)[0]:
            continue
        if any(p in ref for p in PARSE_ONLY_FILES):
            continue
        if any(c in ref for c in CONTRACT_ONLY_EVIDENCE):
            continue
        out.append(ref)
    return out


GO_IDENTITY = ("oracle_binary", "reference_binary", "go_binary")
RUST_IDENTITY = ("candidate_binary", "rust_binary")

# A name is not a hash. Only a hex digest of a plausible length counts as a binary
# identity, so `{"oracle_binary_sha256": ""}` or a placeholder cannot certify a run.
DIGEST_LENGTHS = (40, 64)


def valid_digest(value) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip().lower()
    return (len(text) in DIGEST_LENGTHS
            and all(char in "0123456789abcdef" for char in text))


def find_digests(text: str) -> set[str]:
    hits = set()
    for length in DIGEST_LENGTHS:
        for match in re.finditer(r"[0-9a-fA-F]{" + str(length) + r"}", text):
            hits.add(match.group(0).lower())
    return hits


# Path/key fragments that identify which side a hashed binary came from.
GO_PATH_HINTS = ("-go", "official", "oracle", "reference", "livekit-stable",
                 "livekit-server", "/go/")
RUST_PATH_HINTS = ("livecloud", "target/debug", "target/release", "livekit-rs")


def identities_from_binaries_map(binaries: dict) -> tuple[bool, bool]:
    """Classify a `binaries` map into (rust_seen, go_seen).

    Two shapes occur in real receipts: a flat `{path: sha256}` map, and
    `{label: {path: …, sha256: …}}`. Either way the path says which side the image
    came from and the value must be a real digest — a path key paired with an
    empty or malformed hash certifies nothing.
    """
    rust = go = False
    for key, value in binaries.items():
        path = str(key)
        digest = value
        if isinstance(value, dict):
            for field in ("sha256", "sha512", "sha", "digest", "hash"):
                if isinstance(value.get(field), str):
                    digest = value[field]
                    break
            else:
                # any hex-looking value in the entry is the identity it claims
                digest = next((v for v in value.values() if valid_digest(v)), None)
            # a labelled entry names its side in the label too (/tmp/…-go, livecloud)
            path = f"{path} {value.get('path', '')}"
        if not valid_digest(digest):
            continue
        low = path.lower()
        # a Rust path can also contain "livekit-rs"; check Go markers first
        if any(h in low for h in GO_PATH_HINTS):
            go = True
        elif any(h in low for h in RUST_PATH_HINTS):
            rust = True
    return rust, go


def identity_values(data, depth: int = 0) -> tuple[bool, bool, int]:
    """(rust_seen, go_seen, digests) from identity *keys*, requiring a real digest.

    `{"oracle_binary_sha256": "…64 hex…"}` is a hash of the original server. The key
    alone is not: identity strings appearing in prose or an empty field must not
    attest a paired run.
    """
    rust = go = False
    digests = set()

    def walk(node, seen_key=None, depth=0):
        nonlocal rust, go
        if depth > 6:
            return
        if isinstance(node, dict):
            for key, value in node.items():
                lowered = str(key).lower()
                if isinstance(value, str) and valid_digest(value):
                    if any(marker in lowered for marker in GO_IDENTITY):
                        go = True
                        digests.add(value.strip().lower())
                    elif any(marker in lowered for marker in RUST_IDENTITY):
                        rust = True
                        digests.add(value.strip().lower())
                walk(value, key, depth + 1)
        elif isinstance(node, list):
            for entry in node:
                walk(entry, seen_key, depth + 1)

    walk(data)
    return rust, go, len(digests)


def pass_status(data: dict) -> bool:
    """The receipt reports its own run as passed, under any of its status spellings.

    A paired artifact that never records success is not acceptance evidence; the
    checker's existing `failed` flag only excluded an explicit FAIL.
    """
    for key in ("status", "aggregate_status", "result"):
        value = data.get(key)
        if isinstance(value, str) and value.strip().upper().startswith("PASS"):
            return True
    return False


def artifact_text(abs_path: str) -> str | None:
    try:
        with open(abs_path, encoding="utf-8") as handle:
            return handle.read()
    except (OSError, UnicodeDecodeError):
        return None


def item_match_keys(item_id: str) -> list[str]:
    """Patterns that would identify this item inside an evidence artifact.

    Ids of the form `API:RoomService.ListEgress` also match on the unambiguous
    tail, because a differential receipt names the service+method it exercised,
    not the ledger prefix. A bare tail like `port` is never used: it would match
    anything.

    Matching is delimited on both sides, because these ids nest: an artifact that
    mentions `generated_flags:--logging.level` does *not* thereby mention the
    distinct items `YAML:logging.level` (a config file leaf) or `YAML:logging`.
    """
    keys = [item_id]
    if ":" in item_id:
        tail = item_id.split(":", 1)[1].strip()
        if tail and ("." in tail or tail.startswith("--")):
            keys.append(tail)
    return keys


def naming_pattern(key: str) -> re.Pattern:
    """Delimited match for an id inside artifact text.

    The tail must not grow on either end: `YAML:room` may not be satisfied by
    `YAML:room.enable_remote_unmute` (a different item), and the tail `logging.level`
    of `YAML:logging.level` may not be satisfied by the flag spelling
    `--logging.level`, which belongs to its own `generated_flags:` item.
    """
    return re.compile(r"(?<![\w-])" + re.escape(key) + r"(?![\w.])")


def names_item_in_text(text: str, key: str) -> bool:
    """Delimited match of an item id inside artifact text.

    These ids nest, so plain substring matching would let a run of
    `generated_flags:--logging.level` speak for the distinct item
    `YAML:logging.level`, or `YAML:room` for `YAML:room.enable_remote_unmute`.
    """
    return naming_pattern(key).search(text) is not None


def artifact_names_item(abs_path: str, keys: list[str]) -> bool:
    """Whether an artifact identifies *this* item rather than only a run total.

    Batch receipts f06/f09/f27 hash both binaries and report case counts, but
    contain no item-identifying string, so they cannot show that the leaf a ledger
    item names was one of the cases that ran.
    """
    text = artifact_text(abs_path)
    if text is None:
        return False
    return any(names_item_in_text(text, key) for key in keys)


def classify_artifact(abs_path: str) -> dict:
    """Read an evidence artifact and report what it can attest.

    A genuine paired differential must carry *both* the original Go binary
    identity and the Rust candidate identity as **real digests**, and it must
    report the run as passed: that is the only way the run proves the two servers
    were compared successfully, rather than Rust being checked against a
    hand-written expectation — or a marker string asserting a comparison that
    never produced bytes.
    """
    out = {"paired": False, "failed": False, "rust_identity": False,
           "passed": False, "identity_digests": 0}
    text = artifact_text(abs_path)
    if text is None:
        return out
    saw_rust = saw_go = False
    digests = 0
    data = None
    if abs_path.endswith(".json"):
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            data = None
    if isinstance(data, dict):
        keys_rust, keys_go, key_digests = identity_values(data)
        saw_rust, saw_go, digests = keys_rust, keys_go, key_digests
        binaries = data.get("binaries")
        if isinstance(binaries, dict):
            r, g = identities_from_binaries_map(binaries)
            # a path-keyed map hashes the images it names
            mapped = sum(1 for value in binaries.values() if valid_digest(value))
            saw_rust = saw_rust or r
            saw_go = saw_go or g
            digests += mapped
        status = str(data.get("status", ""))
        out["failed"] = status.upper().startswith("FAIL")
        out["passed"] = pass_status(data)
    else:
        # Free-form receipt (log, README, jsonl): the side markers alone are only
        # prose. Two *distinct* real digests alongside both markers is the floor
        # for calling it a paired run.
        low_all = text.lower()
        found = find_digests(text)
        if any(k in low_all for k in RUST_IDENTITY) and any(k in low_all for k in GO_IDENTITY):
            saw_rust = saw_go = len(found) >= 2
            digests = len(found)
    out["rust_identity"] = saw_rust
    out["identity_digests"] = digests
    out["paired"] = saw_rust and saw_go
    return out


def slot_receipts(abs_path: str) -> list[str]:
    """The artifact itself plus the receipt governing its evidence slot.

    A batch writes one receipt per slot (`BATCH.json`) next to many per-case files,
    so an item citing a log or pair file in that directory refers to the same run
    the receipt summarises. Credit still requires the receipt to name the item.
    """
    out = [abs_path]
    if os.sep + "evidence" + os.sep in abs_path:
        receipt = os.path.join(os.path.dirname(abs_path), "BATCH.json")
        if receipt != abs_path and os.path.isfile(receipt):
            out.append(receipt)
    return out


def server_root(repo_root: str) -> str:
    legacy = os.path.join(repo_root, "livekit-rs-server")
    return legacy if os.path.isdir(legacy) else repo_root


def docs_root(repo_root: str) -> str:
    legacy = os.path.join(repo_root, "livekit-rs-docs")
    if os.path.isdir(legacy):
        return legacy
    sibling = os.path.join(os.path.dirname(repo_root), "livekit-docs")
    return os.path.join(os.environ.get("LIVEKIT_DOCS_ROOT", sibling), "server")


def citation_paths(repo_root: str, path: str):
    if path.startswith("livekit-rs-docs/"):
        yield os.path.join(docs_root(repo_root), path.removeprefix("livekit-rs-docs/"))
    elif path.startswith("livekit-rs-server/"):
        yield os.path.join(server_root(repo_root), path.removeprefix("livekit-rs-server/"))
    else:
        yield os.path.join(repo_root, path)
        if server_root(repo_root) != repo_root:
            yield os.path.join(server_root(repo_root), path)


def artifact_field(refs: list[str], repo_root: str, field: str,
                   item_id: str | None = None,
                   receipts: bool = False) -> list[str]:
    """Refs resolving to an artifact with `field` set.

    When `item_id` is given the artifact must also name that item: a run total
    that never says which leaf it exercised supports no specific item.
    """
    out = []
    keys = item_match_keys(item_id) if item_id else None
    for ref in refs:
        file_part = ref.split("::", 1)[0]
        for abs_path in citation_paths(repo_root, file_part):
            if os.path.isfile(abs_path):
                candidates = slot_receipts(abs_path) if receipts else [abs_path]
                for candidate in candidates:
                    if classify_artifact(candidate).get(field) and (
                            keys is None or artifact_names_item(candidate, keys)):
                        out.append(ref)
                        break
                break
    return out


def paired_differential_evidence(refs: list[str], repo_root: str) -> list[str]:
    return artifact_field(refs, repo_root, "paired")


def item_paired_evidence(refs: list[str], repo_root: str, item_id: str) -> list[str]:
    return artifact_field(refs, repo_root, "paired", item_id, receipts=True)


def item_paired_pass_evidence(refs: list[str], repo_root: str, item_id: str) -> list[str]:
    """Paired, item-naming *and* self-reported as passed.

    A receipt that hashes both binaries but leaves its status blank, or records an
    inconclusive outcome, has not accepted anything.
    """
    return artifact_field(refs, repo_root, "passed", item_id, receipts=True)


def empty_case_inventory_evidence(refs: list[str], repo_root: str, item_id: str) -> list[str]:
    """Receipts that announce a case inventory and then report none.

    `{"cases": []}` or `{"case_count": 0}` next to a PASS status is self-contradictory:
    nothing was actually compared, whatever the header claims.
    """
    keys = item_match_keys(item_id)
    out = []
    for ref in refs:
        file_part = ref.split("::", 1)[0]
        for abs_path in citation_paths(repo_root, file_part):
            if not os.path.isfile(abs_path):
                continue
            for candidate in slot_receipts(abs_path):
                info = classify_artifact(candidate)
                if not (info.get("paired") and info.get("passed")):
                    continue
                if not artifact_names_item(candidate, keys):
                    continue
                try:
                    with open(candidate, encoding="utf-8") as handle:
                        data = json.load(handle)
                except (OSError, json.JSONDecodeError):
                    continue
                if not isinstance(data, dict):
                    continue
                for field in ("cases", "profiles", "results"):
                    value = data.get(field)
                    if isinstance(value, list) and not value:
                        out.append(f"{ref} ({field} is empty)")
                        break
                else:
                    count = data.get("case_count")
                    if isinstance(count, int) and count == 0:
                        out.append(f"{ref} (case_count is 0)")
            break
    return out


def item_named_evidence(refs: list[str], repo_root: str, item_id: str) -> list[str]:
    """Support that is neither a parse-only test nor a source audit *and* names
    this item.

    SPEC-0075 §2.7 keeps a declared consumer tier off parsing; this adds the other
    half: an aggregate receipt that reports only totals ("464 cases, 0 differences")
    does not show that the leaf this item names was among them.
    """
    keys = item_match_keys(item_id)
    out = []
    for ref in non_parse_only_evidence(refs, repo_root):
        file_part = ref.split("::", 1)[0].rstrip("/")
        for abs_path in citation_paths(repo_root, file_part):
            if os.path.isfile(abs_path):
                if artifact_names_item(abs_path, keys):
                    out.append(ref)
                break
            if os.path.isdir(abs_path):
                if any(artifact_names_item(os.path.join(where, name), keys)
                       for where, _, names in os.walk(abs_path) for name in names):
                    out.append(ref)
                break
    return out


def rust_binary_evidence(refs: list[str], repo_root: str) -> list[str]:
    return artifact_field(refs, repo_root, "rust_identity")


def interop_fixture_evidence(refs: list[str], repo_root: str,
                             item_id: str) -> list[str]:
    """`tests/interop/` citations whose harness actually drives this item.

    Those fixtures boot the assembled candidate binary, which is tier-4 support,
    but only for the leaf the harness names.
    """
    keys = item_match_keys(item_id)
    out = []
    for ref in refs:
        if "tests/interop/" not in ref:
            continue
        file_part = ref.split("::", 1)[0].rstrip("/")
        for abs_path in citation_paths(repo_root, file_part):
            if os.path.isfile(abs_path):
                if artifact_names_item(abs_path, keys):
                    out.append(ref)
                break
            if os.path.isdir(abs_path):
                if any(artifact_names_item(os.path.join(where, name), keys)
                       for where, _, names in os.walk(abs_path) for name in names):
                    out.append(ref)
                break
    return out


def failing_evidence(refs: list[str], repo_root: str) -> list[str]:
    out = []
    for ref in refs:
        file_part = ref.split("::", 1)[0]
        for abs_path in citation_paths(repo_root, file_part):
            if os.path.isfile(abs_path):
                if classify_artifact(abs_path)["failed"]:
                    out.append(ref)
                break
    return out

# Parent prefixes that must carry a coverage_map before they may close.
PARENT_PREFIXES = ("FUNC:",)


def is_evidence_path(ref: str, repo_root: str) -> tuple[bool, str | None]:
    """Return (resolves, missing_path). `path::test` splits off the test name.

    References are mostly repo-root relative; some batches used a path relative
    to livekit-rs-server/. Both bases are tried so a real file is never reported
    as missing.
    """
    file_part = ref.split("::", 1)[0]
    if not file_part:
        return False, ref
    for abs_path in citation_paths(repo_root, file_part):
        if os.path.exists(abs_path):
            return True, None
    # Prose evidence ("All 21 children VERIFIED") never resolves under either base.
    return False, ref


def free_text(item: dict) -> str:
    parts = []
    for key, value in item.items():
        if key in ("id", "state", "evidence", "milestone", "workstream", "surface",
                   "m1_class", "verification_levels"):
            continue
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, list):
            parts.extend(v for v in value if isinstance(v, str))
    return "\n".join(parts)


def scope_text(item: dict) -> str:
    return "\n".join(str(item.get(k, "")) for k in SCOPE_KEYS)


def check(ledger_path: str, repo_root: str) -> dict:
    with open(ledger_path) as handle:
        ledger = json.load(handle)
    items = ledger["items"]

    findings: dict[str, list[str]] = collections.defaultdict(list)
    affected: set[str] = set()

    def flag(kind: str, iid: str, detail: str | None = None) -> None:
        affected.add(iid)
        findings[kind].append(f"{iid} :: {detail}" if detail else iid)

    # ---- per-item rules -------------------------------------------------
    parse_only_ids: set[str] = set()
    paired_verified: set[str] = set()
    item_named_paired_verified: set[str] = set()
    rust_verified: set[str] = set()
    for it in items:
        iid, state, ev = it["id"], it["state"], it.get("evidence") or []
        levels = it.get("verification_levels") or {}

        for tier in TIERS:
            val = levels.get(tier)
            if val is not None and val not in TIER_VALUES:
                flag("BAD_TIER_VALUE", iid, f"{tier}={val}")

        unresolved = [ref for ref in ev if not is_evidence_path(ref, repo_root)[0]]
        if state == "VERIFIED" and unresolved:
            flag("EVIDENCE_PATH_MISSING", iid, unresolved[0][:110])

        if state == "VERIFIED" and not ev:
            flag("VERIFIED_WITHOUT_EVIDENCE", iid)

        # evidence that is entirely prose
        if state == "VERIFIED" and ev and all(
            not is_evidence_path(ref, repo_root)[0] for ref in ev
        ):
            flag("VERIFIED_PROSE_ONLY_EVIDENCE", iid)

        # Parse-test-only support. Prose references add no support, so only the
        # refs that resolve to real artifacts are considered.
        supported = [ref for ref in ev if is_evidence_path(ref, repo_root)[0]]
        if supported and all(
            any(p in ref for p in PARSE_ONLY_FILES) for ref in supported
        ):
            parse_only_ids.add(iid)
            if state == "VERIFIED":
                flag("VERIFIED_PARSE_TEST_ONLY", iid)

        # self-contradiction
        if state == "VERIFIED":
            text = free_text(it)
            if UNRESOLVED.search(text):
                scoped = scope_text(it)
                if not scoped or not UNRESOLVED.search(scoped):
                    flag("VERIFIED_TEXT_CONTRADICTS_STATE", iid)

        # tier/state consistency
        if levels and state == "VERIFIED":
            consumer = levels.get("main_server_consumer")
            differential = levels.get("official_differential_lifecycle")
            if consumer != "MET":
                flag("VERIFIED_MISSING_MAIN_CONSUMER", iid, str(consumer))
            if differential not in ("MET", "NOT_APPLICABLE"):
                flag("VERIFIED_MISSING_OFFICIAL_DIFFERENTIAL", iid, str(differential))
            for tier in TIERS:
                if tier not in levels:
                    flag("TIER_FIELD_INCOMPLETE", iid, f"missing {tier}")

        # SPEC-0075 §2: a VERIFIED item must state which tiers its evidence attests
        if state == "VERIFIED" and not levels:
            flag("VERIFIED_WITHOUT_DECLARED_TIERS", iid)

        # a declared consumer tier needs evidence beyond parsing/projection
        if levels.get("main_server_consumer") == "MET":
            if not non_parse_only_evidence(ev, repo_root):
                flag("CONSUMER_TIER_WITHOUT_CONSUMER_EVIDENCE", iid)
            elif not item_named_evidence(ev, repo_root, iid):
                flag("CONSUMER_TIER_ARTIFACT_DOES_NOT_NAME_ITEM", iid)

        # an item may only claim tier 5 on an artifact carrying both identities
        if levels.get("official_differential_lifecycle") == "MET":
            if not paired_differential_evidence(ev, repo_root):
                flag("TIER5_WITHOUT_PAIRED_BINARY_ARTIFACT", iid)
            elif not item_paired_evidence(ev, repo_root, iid):
                # a paired run that never names this leaf proves nothing about it
                flag("TIER5_ARTIFACT_DOES_NOT_NAME_ITEM", iid)
            elif not item_paired_pass_evidence(ev, repo_root, iid):
                # two hashes and an item name still do not report a passed assertion
                flag("TIER5_PAIRED_ARTIFACT_NOT_REPORTED_PASS", iid)
            elif empty_case_inventory_evidence(ev, repo_root, iid):
                flag("TIER5_PAIRED_RECEIPT_REPORTS_NO_CASES", iid,
                     empty_case_inventory_evidence(ev, repo_root, iid)[0])

        if state == "VERIFIED":
            bad = failing_evidence(ev, repo_root)
            if bad:
                flag("VERIFIED_CITES_FAILING_EVIDENCE", iid, bad[0])

        if state == "VERIFIED" and paired_differential_evidence(ev, repo_root):
            paired_verified.add(iid)
            if item_paired_evidence(ev, repo_root, iid):
                item_named_paired_verified.add(iid)
        if state == "VERIFIED" and rust_binary_evidence(ev, repo_root):
            rust_verified.add(iid)

    # ---- parent / child scope -------------------------------------------
    for it in items:
        if not it["id"].startswith(PARENT_PREFIXES) or it["state"] != "VERIFIED":
            continue
        cov = it.get("coverage_map")
        if not cov:
            flag("PARENT_CLOSED_WITHOUT_COVERAGE_MAP", it["id"])
            continue
        gaps = [c for c in cov if c.get("status") not in ("covered", "not_applicable")]
        if gaps:
            flag("PARENT_COVERAGE_MAP_HAS_GAPS", it["id"],
                 f"{len(gaps)} open, e.g. {gaps[0].get('id')}")

    for d, n in collections.Counter(i["id"] for i in items).items():
        if n > 1:
            flag("DUPLICATE_ID", d, f"appears {n}x")

    counts = collections.Counter(i["state"] for i in items)
    verified = {i["id"] for i in items if i["state"] == "VERIFIED"}
    return {
        "ledger_items": len(items),
        "state_counts": dict(counts),
        "verified_total": len(verified),
        "verified_with_paired_binary_evidence": len(paired_verified),
        "verified_with_item_named_paired_evidence": len(item_named_paired_verified),
        "verified_with_rust_binary_evidence": len(rust_verified),
        "verified_unsupported": len(verified & affected),
        "verified_supported": len(verified - affected),
        "parse_only_verified": len(parse_only_ids),
        "with_verification_levels": sum(1 for i in items if i.get("verification_levels")),
        "findings": {k: sorted(v) for k, v in findings.items()},
        "finding_counts": {k: len(v) for k, v in sorted(findings.items())},
    }


def main() -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(here)
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", default=os.path.join(
        docs_root(repo_root), "specs/0011-functional-parity-todo.json"))
    ap.add_argument("--root", default=repo_root)
    ap.add_argument("--json-out")
    ap.add_argument("--max-examples", type=int, default=8)
    args = ap.parse_args()

    result = check(args.ledger, args.root)
    total = sum(result["finding_counts"].values())

    print(f"ledger items: {result['ledger_items']}")
    print(f"states: {result['state_counts']}")
    print(f"VERIFIED with no SPEC-0075 finding: "
          f"{result['verified_supported']} / {result['verified_total']}")
    print(f"VERIFIED citing a paired Go+Rust binary artifact: "
          f"{result['verified_with_paired_binary_evidence']} / {result['verified_total']}")
    print(f"  of those, artifact also names the item: "
          f"{result['verified_with_item_named_paired_evidence']}")
    print(f"VERIFIED citing an artifact with a Rust binary identity: "
          f"{result['verified_with_rust_binary_evidence']} / {result['verified_total']}")
    print(f"items with verification_levels: {result['with_verification_levels']}")
    print()
    if not result["finding_counts"]:
        print("OK: no SPEC-0075 findings")
    for kind, ids in sorted(result["findings"].items()):
        print(f"[{len(ids):5d}] {kind}")
        for example in ids[: args.max_examples]:
            print(f"          e.g. {example}")
    print(f"\nTOTAL FINDINGS: {total}")

    if args.json_out:
        with open(args.json_out, "w") as handle:
            json.dump(result, handle, indent=2, sort_keys=True)
            handle.write("\n")
        print(f"wrote {args.json_out}")

    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())
