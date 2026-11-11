#!/usr/bin/env python3
"""Compare Go and Rust config observer results.

Usage: compare.py <cases.json> <go-results.json> <rust-results.json> <go-leaves.json> <report.json>

Compares the Go "run" outcome (getConfig + ValidateKeys + LoadTURNSecrets)
with the Rust "run" outcome (parse() -> RunServer -> LoadedConfig.official).

Equivalence: identical text, or (benign, reported separately, not a mismatch)
Go nil vs Rust empty collection: "null" vs "[]"/"{}" at any depth, because the
Rust tree uses Vec/BTreeMap where Go keeps a nil slice/map.
"""
import collections
import json
import sys


def norm(v):
    """JSON value with null/[]/{} collapsed to None."""
    if v is None or v == [] or v == {}:
        return None
    if isinstance(v, list):
        return [norm(x) for x in v]
    if isinstance(v, dict):
        return {k: norm(x) for k, x in v.items() if norm(x) is not None}
    return v


def equivalent(a, b):
    try:
        ja, jb = json.loads(a), json.loads(b)
    except (ValueError, TypeError):
        return False
    if not isinstance(ja, (list, dict, type(None))) or not isinstance(jb, (list, dict, type(None))):
        return False
    return norm(ja) == norm(jb)


def kind(o):
    if o is None:
        return "missing"
    if "ok" in o:
        return "ok"
    if "error" in o:
        return "usage" if o.get("usage") else "error"
    return "no_action"


def diff_trees(g, r):
    mism, benign = {}, {}
    for p in sorted(set(g) | set(r)):
        if p not in r:
            mism[p] = {"go": g[p], "rust": None, "cause": "go_only_path"}
        elif p not in g:
            mism[p] = {"go": None, "rust": r[p], "cause": "rust_only_path"}
        elif g[p] != r[p]:
            if equivalent(g[p], r[p]):
                benign[p] = {"go": g[p], "rust": r[p]}
            else:
                mism[p] = {"go": g[p], "rust": r[p], "cause": "value"}
    return mism, benign


def section(path):
    return path.split(".")[0]


def first_line(s, n=110):
    s = (s or "").split("\n")[0]
    return s[:n]


def main():
    cases_doc, go_doc, rs_doc, leaves_doc = (json.load(open(p)) for p in sys.argv[1:5])
    out_path = sys.argv[5]
    cases = {c["id"]: c for c in cases_doc["cases"]}
    go = {x["id"]: x for x in go_doc["results"]}
    rs = {x["id"]: x for x in rs_doc["results"]}

    # Leaf table comparison
    gl = {l["name"]: l for l in leaves_doc["leaves"]}
    rl = {l["name"]: l for l in rs_doc["leaves"]}
    leaf_diff = {
        "go_count": len(gl), "rust_count": len(rl),
        "go_only": sorted(set(gl) - set(rl)), "rust_only": sorted(set(rl) - set(gl)),
        "kind_diff": {n: {"go": gl[n]["kind"], "rust": rl[n]["kind"]} for n in sorted(set(gl) & set(rl)) if gl[n]["kind"] != rl[n]["kind"]},
        "default_text_diff": {n: {"go": gl[n]["default"], "rust": rl[n]["default"]} for n in sorted(set(gl) & set(rl)) if gl[n]["default"] != rl[n]["default"]},
    }
    dt_m, dt_b = diff_trees(leaves_doc["default_tree"], rs_doc["default_tree"])
    default_tree_diff = {"mismatch": dt_m, "benign_nil_vs_empty": sorted(dt_b)}

    baseline_id = "base.defaults.keys"
    base_m = {}
    if go[baseline_id]["run"].get("ok") and rs[baseline_id]["run"].get("ok"):
        base_m, _ = diff_trees(go[baseline_id]["run"]["ok"], rs[baseline_id]["run"]["ok"])

    per_case = []
    path_stats = collections.defaultdict(lambda: {"cases": 0, "cause": None, "baseline": False, "examples": []})
    by_cause = collections.Counter()
    by_section = collections.Counter()
    by_group = collections.defaultdict(lambda: collections.Counter())
    outcome_groups = collections.defaultdict(list)
    error_text_groups = collections.defaultdict(list)
    benign_paths = collections.Counter()
    matching = matching_ex_baseline = 0
    for cid, c in cases.items():
        g = go.get(cid, {}).get("run")
        r = rs.get(cid, {}).get("run")
        kg, kr = kind(g), kind(r)
        entry = {"id": cid, "group": c["group"], "go_kind": kg, "rust_kind": kr}
        status = "match"
        sec = section(c.get("leaf", "")) if c.get("leaf") else c["group"]
        if {kg, kr} == {"error", "usage"} and g["error"] == r["error"]:
            # Same error text; only the usage presentation ("Incorrect Usage" + help) differs.
            status = "usage_classification_mismatch"
            entry["go"], entry["rust"] = g["error"], r["error"]
            by_cause[f"usage_classification:go_{kg}/rust_{kr}"] += 1
            by_section[sec] += 1
            by_group[c["group"]][f"usage_classification:go_{kg}/rust_{kr}"] += 1
        elif kg != kr:
            status = "outcome_mismatch"
            cause = f"outcome:go_{kg}/rust_{kr}"
            entry["go"] = g.get("error") if g and "error" in g else (g.get("no_action") if g and "no_action" in g else "<ok>")
            entry["rust"] = r.get("error") if r and "error" in r else (r.get("no_action") if r and "no_action" in r else "<ok>")
            key = cause + " :: " + (first_line(entry["rust"]) if kr in ("error", "usage") else first_line(entry["go"]))
            outcome_groups[key].append(cid)
            by_cause[cause] += 1
            by_section[sec] += 1
            by_group[c["group"]][cause] += 1
        elif kg in ("error", "usage"):
            # same kind
            if g["error"] != r["error"]:
                status = "error_text_mismatch"
                entry["go"], entry["rust"] = g["error"], r["error"]
                error_text_groups[first_line(g["error"], 70)].append({"id": cid, "go": g["error"], "rust": r["error"]})
                by_cause["error_text"] += 1
                by_section[sec] += 1
                by_group[c["group"]]["error_text"] += 1
        elif kg == "no_action":
            if g["no_action"] != r["no_action"]:
                status = "no_action_mismatch"
                entry["go"], entry["rust"] = g["no_action"], r["no_action"]
                by_cause["no_action"] += 1
        elif kg == "ok":
            m, b = diff_trees(g["ok"], r["ok"])
            for p in b:
                benign_paths[p] += 1
            if m:
                status = "value_mismatch"
                entry["paths"] = m
                only_baseline = all(p in base_m and base_m[p] == v for p, v in m.items())
                entry["only_baseline_paths"] = only_baseline
                for p, v in m.items():
                    st = path_stats[p]
                    st["cases"] += 1
                    st["cause"] = v["cause"]
                    st["baseline"] = p in base_m
                    if len(st["examples"]) < 4 and (not st["examples"] or st["examples"][-1]["go"] != v["go"] or st["examples"][-1]["rust"] != v["rust"]):
                        st["examples"].append({"case": cid, "go": v["go"], "rust": v["rust"]})
                if only_baseline:
                    matching_ex_baseline += 1
        entry["status"] = status
        if status == "match":
            matching += 1
            matching_ex_baseline += 1
        per_case.append(entry)

    # path-level causes
    path_by_section = collections.Counter()
    path_by_cause = collections.Counter()
    for p, st in path_stats.items():
        path_by_section[section(p)] += 1
        path_by_cause[st["cause"] + (":baseline" if st["baseline"] else ":case-specific")] += 1

    leaf_cases = [c for c in cases.values() if c.get("leaf")]
    leaf_status = collections.defaultdict(dict)
    for e in per_case:
        c = cases[e["id"]]
        if c.get("leaf"):
            st = e["status"]
            if st == "value_mismatch" and e.get("only_baseline_paths"):
                st = "match_except_baseline"
            leaf_status[c["leaf"]][c["group"]] = st
    leaves_all_match = sum(1 for v in leaf_status.values() if all(s == "match" for s in v.values()))
    leaves_all_match_ex_baseline = sum(
        1 for v in leaf_status.values() if all(s in ("match", "match_except_baseline") for s in v.values()))
    leaves_failing = {k: v for k, v in sorted(leaf_status.items())
                      if not all(s in ("match", "match_except_baseline") for s in v.values())}

    summary = {
        "cases": len(cases),
        "leaves_go": len(gl), "leaves_rust": len(rl),
        "leaves_covered": len(leaf_status), "leaf_cases": len(leaf_cases),
        "leaves_all_three_forms_match": leaves_all_match,
        "leaves_all_three_forms_match_ignoring_baseline_value_diffs": leaves_all_match_ex_baseline,
        "matching_cases": matching,
        "matching_cases_ignoring_baseline_value_diffs": matching_ex_baseline,
        "mismatching_cases": len(cases) - matching,
        "status_counts": dict(collections.Counter(e["status"] for e in per_case)),
        "case_mismatch_by_cause": dict(by_cause),
        "case_mismatch_by_section_or_group": dict(by_section.most_common()),
        "case_mismatch_by_group": {k: dict(v) for k, v in by_group.items()},
        "mismatching_paths": len(path_stats),
        "mismatching_paths_by_section": dict(path_by_section.most_common()),
        "mismatching_paths_by_cause": dict(path_by_cause),
        "baseline_mismatching_paths": len(base_m),
        "benign_nil_vs_empty_paths": dict(benign_paths.most_common()),
    }
    report = {
        "summary": summary,
        "leaf_table_diff": leaf_diff,
        "default_tree_diff": default_tree_diff,
        "baseline_case": baseline_id,
        "baseline_mismatch": base_m,
        "leaves_failing": leaves_failing,
        "path_mismatches": dict(sorted(path_stats.items(), key=lambda kv: (-kv[1]["cases"], kv[0]))),
        "outcome_mismatch_groups": {k: v for k, v in sorted(outcome_groups.items(), key=lambda kv: -len(kv[1]))},
        "error_text_mismatch_groups": {k: v for k, v in sorted(error_text_groups.items(), key=lambda kv: -len(kv[1]))},
        "cases": per_case,
    }
    with open(out_path, "w") as f:
        json.dump(report, f, indent=1, ensure_ascii=False)
        f.write("\n")
    summary_path = out_path.rsplit("/", 1)[0] + "/summary.json" if "/" in out_path else "summary.json"
    with open(summary_path, "w") as f:
        json.dump({"summary": summary, "baseline_mismatch": base_m,
                   "outcome_mismatch_groups": {k: len(v) for k, v in report["outcome_mismatch_groups"].items()},
                   "error_text_mismatch_groups": {k: len(v) for k, v in report["error_text_mismatch_groups"].items()},
                   "path_mismatches": {p: {"cases": v["cases"], "cause": v["cause"], "baseline": v["baseline"],
                                           "example": v["examples"][0]} for p, v in report["path_mismatches"].items()}},
                  f, indent=1, ensure_ascii=False)
        f.write("\n")
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
