#!/usr/bin/env python3
"""Map the historical ignored-test registrations onto the current runtime inventory.

`211` and `182` are not the same measurement taken twice: the historical number is
the batch22e registration table, the current number is what the workspace binaries
register now. This driver pairs them item by item and, for each item that is new,
attributes the commit that introduced its test function so the growth is traceable
instead of asserted. Items that disappeared are reported as such rather than
silently dropped from the count.
"""
import argparse
import json
import pathlib
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
CRATE_OF = re.compile(r'crates/([^/]+)/src/(.+\.rs)')


def git(*args):
    return subprocess.run(['git', *args], cwd=ROOT, capture_output=True, text=True)


def static_sites():
    """Every `#[ignore]` test site in the tree, keyed by function name.

    `vendor/` is scanned on purpose: the vendored str0m carries patched parity
    tests that the workspace binaries really register, so leaving it out would
    report those items as having no source.
    """
    sites = {}
    for base in ('crates', 'tests', 'vendor'):
        for path in sorted((ROOT / base).rglob('*.rs')):
            if any(part in ('payload', 'target', 'references', 'node_modules')
                   for part in path.parts):
                continue
            lines = path.read_text(errors='replace').splitlines()
            for i, line in enumerate(lines):
                if not re.match(r'\s*#\[ignore', line):
                    continue
                for j in range(i + 1, min(i + 12, len(lines))):
                    m = re.search(r'\bfn\s+([A-Za-z0-9_]+)', lines[j])
                    if m:
                        sites.setdefault(m.group(1), []).append(
                            '%s:%d' % (path.relative_to(ROOT), i + 1))
                        break
    return sites


def introduced_by(site, fn):
    """The commit whose diff first contains this test function name."""
    path, _, line = site.partition(':')
    proc = git('log', '-1', '--format=%H%x09%ad%x09%s', '--date=short', '-S', fn, '--', path)
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    sha, date, subject = proc.stdout.strip().split('\t', 2)
    return {'commit': sha, 'date': date, 'subject': subject,
            'file': path, 'line': int(line),
            'blame': blame_line(path, fn)}


def blame_line(path, fn):
    """The authoring commit of the `fn` line itself, when the site can be located."""
    proc = git('grep', '-n', '--', 'fn %s' % fn, path)
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    parts = proc.stdout.splitlines()[0].split(':')
    if len(parts) < 2 or not parts[1].isdigit():
        return None
    blame = git('blame', '-L', parts[1], '--porcelain', path)
    lines = blame.stdout.splitlines()
    if not lines:
        return None
    summary = next((l.split('\t', 1)[1] for l in lines
                    if '\t' in l and l.startswith('summary')), '?')
    return '%s %s' % (lines[0].split()[0][:12], summary)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--history', required=True, help='historical runtime inventory JSON')
    ap.add_argument('--history-table', help='historical disposition table JSON, for attribution')
    ap.add_argument('--current', required=True, help='current runtime inventory JSON')
    ap.add_argument('--output', required=True, type=pathlib.Path)
    args = ap.parse_args()

    history = json.loads(pathlib.Path(args.history).read_text())
    current = json.loads(pathlib.Path(args.current).read_text())
    table = {}
    if args.history_table:
        rows = json.loads(pathlib.Path(args.history_table).read_text())
        if isinstance(rows, dict):
            rows = rows['table']
        for row in rows:
            table[row['test']] = row

    old = {row['test'] for row in history}
    new = {row['test'] for row in current}
    sites = static_sites()

    added, kept = [], sorted(old & new)
    binaries = {}
    for row in current:
        binaries.setdefault(row['test'], []).append(row['binary'])
    for name in sorted(new - old):
        fn = name.split('::')[-1]
        site_list = sites.get(fn, [])
        origin = introduced_by(site_list[0], fn) if site_list else None
        added.append({'test': name, 'fn': fn, 'binary': binaries[name],
                      'sites': site_list, 'introduced': origin})
    removed = []
    for name in sorted(old - new):
        fn = name.split('::')[-1]
        successors = [n for n in new if n.split('::')[-1] == fn]
        removed.append({'test': name, 'fn': fn, 'historical': table.get(name, {}),
                        'same_fn_now': successors})

    out = {
        'history_total': len(history),
        'history_unique_names': len(old),
        'history_pass': sum(1 for row in history
                           if table.get(row['test'], {}).get('disposition') == 'PASS'),
        'current_total': len(current),
        'current_unique_names': len(new),
        'current_names_in_several_binaries': sorted(k for k, v in binaries.items() if len(v) > 1),
        'exact_carried': len(kept),
        'added_since_history': added,
        'removed_since_history': removed,
        'carried_names': kept,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(out, indent=2) + '\n')
    print(json.dumps({'history_total': out['history_total'],
                      'history_pass': out['history_pass'],
                      'current_total': out['current_total'],
                      'exact_carried': out['exact_carried'],
                      'added': len(added),
                      'removed': len(removed),
                      'added_without_source_site': sum(1 for a in added if not a['sites']),
                      'added_without_commit': sum(1 for a in added if not a['introduced'])}))


if __name__ == '__main__':
    main()
