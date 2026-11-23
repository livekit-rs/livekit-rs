#!/usr/bin/env python3
"""Enumerate the runtime ignored-test inventory of the workspace test binaries.

The inventory is what `cargo test --workspace` counts as `ignored`: the exact
names libtest registers in each binary it runs, with the `#[ignore]` flag set.
Static `#[ignore]` sites in the tree are a different measurement — oracle payload
copies are staged source, feature-gated modules do not build by default, and one
source site can be compiled into several targets — so this driver reads the
compiled binaries themselves and never infers names from the sources.

Output shape matches the historical batch22e `runtime-inventory.json` (a list of
`{"test": ..., "binary": ...}`) so the two inventories can be diffed item by item.
"""
import argparse
import json
import os
import pathlib
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
RUNNING = re.compile(r'^\s+(?:Running|Compile tests)\s+(.+ \((?:target|\.)/[^)]*/deps/[^)]+\))\s*$')
RESULT = re.compile(r'test result:\s+\w+\.\s+(\d+) passed;\s+(\d+) failed;\s+(\d+) ignored')
HASHED = re.compile(r'^(.*)-[0-9a-f]{16}$')


def stem(deps_path):
    """The un-hashed identity of a built test binary, stable across rebuilds."""
    name = pathlib.Path(deps_path).name
    m = HASHED.match(name)
    return m.group(1) if m else name


def key(label):
    """(source path, binary stem) for a `unittests src/lib.rs (target/.../deps/x-h)` label."""
    m = re.match(r'^(.*?) \((.*/deps/[^)]+)\)$', label)
    return (m.group(1), stem(m.group(2))) if m else (label, label)


def artifacts(by_cargo):
    """Every test-eligible binary cargo builds for the workspace, with its label."""
    proc = subprocess.run(['cargo', 'test', '--workspace', '--no-run', '--message-format=json'],
                          cwd=ROOT, env=by_cargo, check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    seen = {}
    for line in proc.stdout.splitlines():
        if not line.startswith('{'):
            continue
        msg = json.loads(line)
        executable = msg.get('executable')
        if not executable:
            continue
        target = msg['target']
        kind = target['kind'][0]
        if kind not in ('lib', 'bin', 'test'):
            continue
        src = pathlib.Path(target['src_path'])
        package_root = pathlib.Path(msg.get('manifest_path') or
                                    msg.get('manifest_dir') or ROOT).parent
        try:
            rel = str(src.relative_to(package_root))
        except ValueError:
            rel = str(src)
        label = ('unittests %s' % rel) if kind in ('lib', 'bin') else rel
        seen[executable] = '%s (%s)' % (label, pathlib.Path(executable).relative_to(ROOT))
    return seen


def ignored_names(binary, timeout):
    """The ignored test names libtest registers in one binary.

    `--list --format json` is nightly-only, so the stable text listing is parsed:
    one `<name>: test` line per registered test, plus the total libtest prints.
    """
    proc = subprocess.run([binary, '--ignored', '--list'], cwd=ROOT,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, timeout=timeout)
    names = [line.rsplit(':', 1)[0] for line in proc.stdout.splitlines()
             if line.endswith(': test')]
    total = re.search(r'^(\d+) tests?(?: and \d+ benchmarks?)?(?: found)? in total',
                      proc.stdout, re.M)
    if proc.returncode != 0:
        raise RuntimeError('%s exited %d: %s' % (binary, proc.returncode,
                                                 proc.stderr.strip()[:300]))
    if total and int(total.group(1)) != len(names):
        raise RuntimeError('%s listed %d names but reports %s in total'
                           % (binary, len(names), total.group(1)))
    return names


def log_distribution(path):
    """Per-binary ignored counts as the workspace run itself reported them."""
    text = pathlib.Path(path).read_text(errors='replace')
    rows, current = [], None
    for line in text.splitlines():
        m = RUNNING.match(line)
        if m:
            current = m.group(1)
            continue
        r = RESULT.search(line)
        if r and current:
            rows.append((current, int(r.group(3))))
            current = None
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--output', required=True, type=pathlib.Path)
    ap.add_argument('--cross-check-log', help='workspace test log whose ignored counts '
                                             'the inventory must reproduce')
    ap.add_argument('--timeout', type=int, default=120)
    args = ap.parse_args()
    out = args.output.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    by_cargo = dict(os.environ)
    rows = []
    for executable, label in sorted(artifacts(by_cargo).items(), key=lambda kv: kv[1]):
        for name in ignored_names(executable, args.timeout):
            rows.append({'test': name, 'binary': label})
    rows.sort(key=lambda r: (r['binary'], r['test']))
    out.write_text(json.dumps(rows, indent=2) + '\n')

    per = {}
    for row in rows:
        per[key(row['binary'])] = per.get(key(row['binary']), 0) + 1
    summary = {'entries': len(rows), 'binaries_with_ignored': len(per)}
    if args.cross_check_log:
        expected = {}
        for label, count in log_distribution(args.cross_check_log):
            k = key(label)
            expected[k] = expected.get(k, 0) + count
        mismatched = {'%s %s' % k: (expected.get(k, 0), per.get(k, 0))
                      for k in set(expected) | set(per)
                      if expected.get(k, 0) != per.get(k, 0)}
        summary['log_total'] = sum(expected.values())
        summary['log_mismatched_binaries'] = mismatched
    (out.parent / (out.stem + '.summary.json')).write_text(
        json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary))
    if args.cross_check_log and (summary['log_mismatched_binaries']
                                 or summary['log_total'] != summary['entries']):
        raise SystemExit('inventory does not reproduce the workspace run counts')


if __name__ == '__main__':
    main()
