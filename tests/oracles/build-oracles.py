#!/usr/bin/env python3
"""Build every oracle Go module with the toolchain its own go.mod requires.

Each module is built with -mod=readonly and GOPROXY=off so a missing requirement
fails loudly instead of being fetched. Output goes to a per-module directory so a
package name can never collide with a sibling source directory.
"""
import argparse
import json
import os
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
CACHE = pathlib.Path.home() / 'go/pkg/mod/golang.org'


def cached_toolchains():
    found = {}
    for entry in CACHE.glob('toolchain@v0.0.1-go*.darwin-arm64'):
        m = re.match(r'toolchain@v0\.0\.1-go([0-9.]+)\.', entry.name)
        binary = entry / 'bin/go'
        if m and binary.is_file():
            found[tuple(int(p) for p in m.group(1).split('.'))] = binary
    return found


def pick(required, available):
    if required in available:
        return available[required], 'exact'
    higher = sorted(v for v in available if v >= required)
    if higher:
        return available[higher[0]], 'newer'
    return None, 'absent'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--output', required=True)
    ap.add_argument('--only', action='append', default=[])
    args = ap.parse_args()
    out = pathlib.Path(args.output).resolve()
    out.mkdir(parents=True, exist_ok=True)
    available = cached_toolchains()
    rows = []
    for gm in sorted(ROOT.glob('tests/oracles/**/go.mod')):
        rel = str(gm.parent.relative_to(ROOT))
        if args.only and rel not in args.only:
            continue
        m = re.search(r'^go\s+([0-9]+(?:\.[0-9]+)*)\s*$', gm.read_text(), re.M)
        required = tuple(int(p) for p in m.group(1).split('.')) if m else (1, 24, 0)
        while len(required) < 3:
            required += (0,)
        binary, match = pick(required, available)
        label = rel.replace('/', '_')
        log = out / (label + '.log')
        bin_dir = out / 'bin' / label
        row = {'module': rel, 'go_directive': '.'.join(map(str, required)),
               'toolchain_match': match}
        if binary is None:
            row.update(exit_code=None, status='NOT_RUN',
                       blocker='no cached toolchain >= go' + row['go_directive'])
            rows.append(row)
            continue
        bin_dir.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, GOTOOLCHAIN='local', GOPROXY='off')
        cmd = [str(binary), 'build', '-mod=readonly', '-o', str(bin_dir) + '/', './...']
        with log.open('w') as handle:
            proc = subprocess.run(cmd, cwd=gm.parent, env=env,
                                  stdout=handle, stderr=subprocess.STDOUT)
        mode = 'build'
        if proc.returncode != 0 and 'no main packages to build' in log.read_text():
            mode = 'test-compile'
            cmd = [str(binary), 'test', '-mod=readonly', '-run', '^$', './...']
            with log.open('w') as handle:
                proc = subprocess.run(cmd, cwd=gm.parent, env=env,
                                      stdout=handle, stderr=subprocess.STDOUT)
        row.update(toolchain=str(binary), exit_code=proc.returncode, mode=mode,
                   status='BUILD_OK' if proc.returncode == 0 else 'BUILD_FAIL',
                   command=' '.join(cmd), log=log.name)
        rows.append(row)
    failures = [r for r in rows if r['status'] != 'BUILD_OK']
    summary = {'modules': len(rows), 'build_ok': len(rows) - len(failures),
               'not_ok': len(failures), 'rows': rows}
    (out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    with (out / 'commands.log').open('w') as handle:
        for r in rows:
            handle.write('%s EXIT=%s TOOLCHAIN=%s\n'
                         % (r['module'], r['exit_code'], r.get('toolchain', 'none')))
        handle.write('ORACLE_BUILD_FAILURES=%d\n' % len(failures))
    print('ORACLE_MODULES=%d BUILD_OK=%d NOT_OK=%d'
          % (len(rows), len(rows) - len(failures), len(failures)))
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())