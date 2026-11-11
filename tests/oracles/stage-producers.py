#!/usr/bin/env python3
"""Stage the Go oracle producers a recipe table needs, then emit a runnable recipe.

Each producer script runs under bash with `set -e`, so `cd` persists within one
entry and the first failing command aborts it. A staging failure is recorded as
STAGE_FAILED with its original exit code; it is never retried into a pass.
"""
import argparse
import json
import pathlib
import re
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
ENV_PREFIX = re.compile(r'^([A-Z][A-Z0-9_]*)=(\S+)\s+')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--recipes', required=True, action='append')
    ap.add_argument('--output', required=True)
    ap.add_argument('--timeout', type=float, default=1800.0)
    args = ap.parse_args()
    out = pathlib.Path(args.output).resolve()
    out.mkdir(parents=True, exist_ok=True)

    entries = []
    for path in args.recipes:
        entries.extend(json.loads(pathlib.Path(path).read_text()))

    done = {}
    rows = []
    for e in entries:
        name = e['test']
        label = name.replace('::', '__').replace('/', '_')
        row = dict(e)
        row['staging'] = []
        if e.get('classification') != 'RUNNABLE_NOW':
            rows.append(row)
            continue
        script = []
        for cmd in e.get('producer_commands') or []:
            if cmd.strip().startswith('#'):
                continue
            # `cd` is stateful and every entry starts in a fresh bash at ROOT, so
            # reusing another entry's `cd` would run this one in the wrong tree.
            if cmd in done and not cmd.strip().startswith('cd '):
                row['staging'].append({'command': cmd, 'reused': True,
                                       'exit_code': done[cmd]})
                continue
            script.append(cmd)
        if script:
            body = 'set -e\n' + '\n'.join(script) + '\n'
            log = out / (label + '.stage.log')
            started = time.time()
            with log.open('w') as handle:
                handle.write(body)
                handle.flush()
                proc = subprocess.run(['/bin/bash', '-c', body], cwd=ROOT,
                                      stdout=handle, stderr=subprocess.STDOUT,
                                      timeout=args.timeout)
            for cmd in script:
                done[cmd] = proc.returncode
            row['staging'].append({'command': '\n'.join(script),
                                   'exit_code': proc.returncode,
                                   'log': log.name,
                                   'seconds': round(time.time() - started, 2)})
            print('STAGE %s EXIT=%s' % (name, proc.returncode), flush=True)
            if proc.returncode != 0:
                row['classification'] = 'STAGE_FAILED'
                rows.append(row)
                continue

        cmd = e.get('cargo_command') or ''
        env_values = dict(e.get('env_values') or {})
        while True:
            m = ENV_PREFIX.match(cmd)
            if not m:
                break
            env_values[m.group(1)] = m.group(2)
            cmd = cmd[m.end():]
        row['cargo_command'] = cmd
        row['env_values'] = env_values
        rows.append(row)

    (out / 'runnable.json').write_text(json.dumps(rows, indent=2) + '\n')
    counts = {}
    for r in rows:
        counts[r['classification']] = counts.get(r['classification'], 0) + 1
    print('CLASS_COUNTS %s' % json.dumps(counts))
    return 1 if 'STAGE_FAILED' in counts else 0


if __name__ == '__main__':
    sys.exit(main())