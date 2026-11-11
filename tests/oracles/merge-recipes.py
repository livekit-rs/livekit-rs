#!/usr/bin/env python3
"""Merge recipe tables into one entry per test, preserving first-wins order.

disposition.py and rebind-workspace.py both keep the FIRST recipe recorded for a
test name, and that order is load-bearing: one entry is COVERED_VIA_PARENT in the
table that wins and RUNNABLE_NOW in the one that loses. This tool reproduces that
order instead of silently reversing it.

stage-producers.py runs every entry it is given, so passing a base table plus a
correction table would stage the same producer twice and the second run would
fail on its fresh-directory invariant. --override names the tables that are
allowed to replace an earlier entry, and the merge is recorded per entry.
"""
import argparse
import json
import pathlib
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--recipes', required=True, action='append',
                    help='repeatable; the first table recording a test wins')
    ap.add_argument('--override', action='append', default=[],
                    help='repeatable; these tables replace an earlier entry')
    ap.add_argument('--output', required=True)
    args = ap.parse_args()

    merged = {}
    for path in args.recipes:
        name = pathlib.Path(path).name
        for row in json.loads(pathlib.Path(path).read_text()):
            test = row['test']
            if test in merged:
                merged[test]['recipe_sources'].append(name + ':ignored-first-wins')
                continue
            row = dict(row)
            row['recipe_sources'] = [name]
            merged[test] = row
    for path in args.override:
        name = pathlib.Path(path).name
        for row in json.loads(pathlib.Path(path).read_text()):
            test = row['test']
            row = dict(row)
            prior = merged.get(test, {}).get('recipe_sources', [])
            row['recipe_sources'] = prior + [name + ':override']
            merged[test] = row

    rows = [merged[k] for k in sorted(merged)]
    pathlib.Path(args.output).write_text(json.dumps(rows, indent=2) + '\n')
    replaced = [r['test'] for r in rows
                if any(s.endswith(':override') for s in r['recipe_sources'])]
    print('MERGED_ENTRIES', len(rows))
    print('OVERRIDDEN', len(replaced))
    for test in replaced:
        print('  ', test, '<-', ','.join(merged[test]['recipe_sources']))
    return 0


if __name__ == '__main__':
    sys.exit(main())