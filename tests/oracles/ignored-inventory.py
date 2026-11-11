#!/usr/bin/env python3
"""Reconcile static #[ignore] sites against the runtime ignored-test inventory.

A static site count and a runtime entry count are different measurements: oracle
payload copies are staged source, not compiled tests; feature-gated modules do
not build by default; and one source site can be compiled into several test
targets. This tool reports each discrepancy with its file:line and only closes
the arithmetic against an explicit, evidenced exclusion list.
"""
import argparse
import collections
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
SKIP = ('vendor', 'payload', 'target', 'references', 'node_modules')
IGNORE = re.compile(r'\s*#\[ignore')
FN = re.compile(r'\bfn\s+([A-Za-z0-9_]+)')


def static_sites():
    sites = []
    for base in ('crates', 'tests'):
        for path in sorted((ROOT / base).rglob('*.rs')):
            if any(part in SKIP for part in path.parts):
                continue
            lines = path.read_text(errors='replace').splitlines()
            for i, line in enumerate(lines):
                if not IGNORE.match(line):
                    continue
                name = None
                for j in range(i + 1, min(i + 12, len(lines))):
                    m = FN.search(lines[j])
                    if m:
                        name = m.group(1)
                        break
                sites.append({'file': str(path.relative_to(ROOT)), 'line': i + 1,
                              'fn': name, 'ignore': line.strip()})
    return sites


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--runtime', type=pathlib.Path, required=True,
                   help='inventory JSON: list of {test, binary}')
    p.add_argument('--exclusions', type=pathlib.Path, required=True,
                   help='JSON list of {file, line, reason} for non-live static sites')
    p.add_argument('--output', type=pathlib.Path, required=True)
    a = p.parse_args()

    sites = static_sites()
    runtime = [e['test'] for e in json.loads(a.runtime.read_text())]
    distinct = sorted(set(runtime))
    counts = collections.Counter(runtime)
    duplicated = {n: c for n, c in counts.items() if c > 1}
    exclusions = json.loads(a.exclusions.read_text())
    excluded_keys = {(e['file'], e['line']) for e in exclusions}
    for e in exclusions:
        if not any(s['file'] == e['file'] and s['line'] == e['line'] for s in sites):
            print('exclusion does not match a static site: %s:%s' % (e['file'], e['line']))
            return 1
    live = [s for s in sites if (s['file'], s['line']) not in excluded_keys]

    rt_by_fn = collections.defaultdict(set)
    for n in distinct:
        rt_by_fn[n.split('::')[-1]].add(n)
    st_by_fn = collections.defaultdict(list)
    for s in live:
        st_by_fn[s['fn']].append(s)
    unmatched = []
    for fn, group in sorted(st_by_fn.items()):
        if len(group) != len(rt_by_fn.get(fn, set())):
            unmatched.append({'fn': fn, 'live_sites': group,
                              'runtime_names': sorted(rt_by_fn.get(fn, set()))})

    closed = len(live) == len(distinct) and not unmatched
    out = {'static_ignore_sites': len(sites),
           'runtime_ignored_entries': len(runtime),
           'distinct_runtime_names': len(distinct),
           'recompiled_names': duplicated,
           'excluded_static_sites': exclusions,
           'live_static_sites': len(live),
           'arithmetic_closed': closed,
           'fn_name_discrepancies': unmatched,
           'note': 'static sites minus evidenced exclusions must equal distinct runtime '
                   'names; runtime entries may exceed that when one source site is '
                   'compiled into several test targets'}
    a.output.write_text(json.dumps(out, indent=2) + '\n')
    print('STATIC_SITES %d' % len(sites))
    print('RUNTIME_ENTRIES %d DISTINCT %d' % (len(runtime), len(distinct)))
    print('EXCLUDED %d LIVE %d' % (len(exclusions), len(live)))
    print('RECOMPILED %s' % json.dumps(duplicated))
    print('FN_DISCREPANCIES %d' % len(unmatched))
    for u in unmatched:
        print('   ', u['fn'], len(u['live_sites']), len(u['runtime_names']))
    print('ARITHMETIC_CLOSED %s' % closed)
    return 0 if closed else 1


if __name__ == '__main__':
    sys.exit(main())