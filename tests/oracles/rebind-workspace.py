#!/usr/bin/env python3
"""Re-bind ignored-test measurements to the workspace-unified test binaries.

`cargo test -p X --lib` and `cargo test --workspace` build the same source with
different feature unification, so they produce different test binaries. A
disposition assembled from both is stitched from two candidates. This driver
re-executes every selected entry against the binaries cargo built for the whole
workspace, and records each row's executed binary sha256.

Harness-driven entries keep their own harness (fresh owned fixtures, real
refusals); only the --test-binary and --output arguments are substituted.
"""
import argparse
import hashlib
import json
import os
import pathlib
import re
import shlex
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
ENV_PREFIX = re.compile(r'^([A-Z][A-Z0-9_]*)=(\S+)\s+')
DEPS = re.compile(r'target/debug/deps/([A-Za-z0-9_.-]+)')
HARNESS = re.compile(r'tests/[\w./-]*run[\w./-]*\.py')
PLACEHOLDER = re.compile(r'\{[a-z_]+\}')
WRITESIDE = re.compile(r'RUST|_OUT$|_OUT_|^OUT_|_DIR$|EVIDENCE')
PROOF = ('running 1 test', '1 passed; 0 failed')


def sha256(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def split_env(cmd):
    env = {}
    while True:
        m = ENV_PREFIX.match(cmd)
        if not m:
            return cmd, env
        env[m.group(1)] = m.group(2)
        cmd = cmd[m.end():]


def workspace_binary(entry, binaries):
    m = DEPS.search(entry['binary'])
    if not m:
        raise SystemExit('cannot parse inventory binary label: %r' % entry['binary'])
    label = m.group(1)
    for row in binaries:
        if pathlib.Path(row['path']).name == label:
            return row
    raise SystemExit('no workspace binary for %s' % label)


def harness_command(recipe, binary_path, out_dir):
    """The recorded harness invocation, re-pointed at the workspace binary."""
    for cmd in recipe.get('producer_commands') or []:
        if cmd.strip().startswith('#') or not HARNESS.search(cmd):
            continue
        body, env = split_env(cmd.strip())
        tokens = shlex.split(body)
        binary_flag = next((f for f in ('--test-binary', '--binary', '--rust-binary')
                            if f in tokens), None)
        out_flag = next((f for f in ('--output', '--out', '--evidence-dir')
                         if f in tokens), None)
        if binary_flag is None or out_flag is None:
            continue
        i = tokens.index(binary_flag)
        tokens[i + 1] = str(binary_path)
        i = tokens.index(out_flag)
        tokens[i + 1] = str(out_dir)
        prefix = ''.join('%s=%s ' % (k, shlex.quote(v)) for k, v in env.items())
        return prefix + ' '.join(shlex.quote(t) for t in tokens)
    return None


def missing_command_inputs(tokens, skip):
    """Absolute inputs a harness command reads but that are no longer staged.

    The scratch roots harnesses point at (a compiled Go client, a staged corpus)
    are temporary by design. A harness that cannot find its input aborts before
    the Rust binary is exercised, so that is a blocked measurement, not a Rust
    failure.
    """
    return sorted({t for t in tokens if t.startswith('/') and t not in skip
                   and not pathlib.Path(t).exists()})


def consumer_env_names(recipe):
    """The env names the Rust consumer actually reads.

    A recipe's `env` table also documents producer-side variables (the Go oracle's
    own inputs and outputs). Requiring those would block a consumer that never
    reads them, so only names appearing as literals in the consumer source count.
    """
    env = recipe.get('env') or {}
    # `env` is a NAME -> value table in every validated recipe; where a row carries
    # prose instead, only the upper-case tokens it names are candidates.
    if isinstance(env, dict):
        declared = set(env.keys())
    else:
        declared = {t for t in re.findall(r'\b[A-Z][A-Z0-9_]{2,}\b', str(env))}
    if not declared:
        return set()
    source = ROOT / (recipe.get('source_file') or '')
    if not source.is_file():
        return declared
    text = source.read_text(errors='replace')
    return {n for n in declared if '"%s"' % n in text}


def absent_paths(env):
    """Env values that name a file which is not there.

    A recipe records the staged producer output it was measured against; once that
    scratch tree is gone the consumer cannot execute, and running it anyway only
    produces a panic in `fs::read_to_string`. That is a blocked measurement, not a
    Rust failure, so it is reported as NOT_RUN with the exact variables.

    Write-side names are excluded: those paths are the consumer's own fresh result
    file, opened with create_new, so requiring them to pre-exist would block every
    differential row.
    """
    return sorted(name for name, value in env.items()
                  if value.startswith(('/', './')) and not WRITESIDE.search(name)
                  and not pathlib.Path(value).exists())


def proof_in(directory):
    """Every per-test log under a harness output dir that proves one test ran."""
    hits = []
    for path in sorted(pathlib.Path(directory).rglob('*')):
        if not path.is_file() or path.stat().st_size > 4_000_000:
            continue
        if path.suffix not in {'.stdout', '.stderr', '.log', '.txt'}:
            continue
        text = path.read_text(errors='replace')
        if all(p in text for p in PROOF):
            hits.append(path.name)
    return hits


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--disposition', required=True)
    ap.add_argument('--inventory', required=True)
    ap.add_argument('--binaries', required=True)
    ap.add_argument('--recipes', required=True, action='append')
    ap.add_argument('--env-map', action='append', default=[],
                    help='JSON object of test name -> {ENV: value} for direct runs')
    ap.add_argument('--output', required=True)
    ap.add_argument('--select', default='PASS',
                    help='comma-separated dispositions to re-bind')
    ap.add_argument('--only', action='append', default=[])
    ap.add_argument('--timeout', type=float, default=1800.0)
    args = ap.parse_args()

    out = pathlib.Path(args.output).resolve()
    out.mkdir(parents=True, exist_ok=True)
    binaries = json.loads(pathlib.Path(args.binaries).read_text())
    inventory = {r['test']: r for r in json.loads(pathlib.Path(args.inventory).read_text())}
    table = json.loads(pathlib.Path(args.disposition).read_text())['table']
    recipes = {}
    for path in args.recipes:
        for row in json.loads(pathlib.Path(path).read_text()):
            recipes.setdefault(row['test'], row)
    env_map = {}
    for path in args.env_map:
        for name, values in json.loads(pathlib.Path(path).read_text()).items():
            env_map.setdefault(name, {}).update(values)

    select = set(args.select.split(','))
    rows = []
    harness_done = {}
    seen_labels = {}
    for entry in table:
        name = entry['test']
        if entry['disposition'] not in select:
            continue
        if args.only and name not in args.only:
            continue
        recipe = recipes.get(name)
        if recipe is None:
            rows.append({'test': name, 'status': 'NOT_RUN',
                         'reason': 'no recipe recorded for this entry'})
            continue
        binary = workspace_binary(inventory[name], binaries)
        label = name.replace('::', '__').replace('/', '_')
        row = {'test': name, 'binary_path': binary['path'],
               'binary_sha256': binary['sha256'],
               'prior_disposition': entry['disposition']}
        harness = harness_command(recipe, binary['path'], out / (label + '.harness'))
        if harness:
            row['mode'] = 'harness'
            row['command'] = harness
            body, henv = split_env(harness)
            missing_input = missing_command_inputs(
                shlex.split(body),
                {binary['path'], str(out / (label + '.harness'))})
            missing_input += ['%s=%s' % (n, henv[n]) for n in absent_paths(henv)]
            if missing_input:
                row.update(status='NOT_RUN', exit_code=None,
                           reason='harness input absent: %s' % ', '.join(missing_input))
                rows.append(row)
                print('NOT_RUN %s ABSENT_HARNESS_INPUTS=%s'
                      % (name, ','.join(missing_input)), flush=True)
                continue
            if harness in harness_done:
                prior = harness_done[harness]
                row.update(prior)
                row['shared_harness_run'] = True
                rows.append(row)
                continue
            log = out / (label + '.harness.log')
            started = time.time()
            with log.open('w') as handle:
                handle.write(harness + '\n')
                handle.flush()
                proc = subprocess.run(['/bin/bash', '-c', harness], cwd=ROOT,
                                      stdout=handle, stderr=subprocess.STDOUT,
                                      timeout=args.timeout)
            directory = out / (label + '.harness')
            hits = proof_in(directory) if directory.exists() else []
            result = {'exit_code': proc.returncode, 'log': log.name,
                      'seconds': round(time.time() - started, 2),
                      'proof_logs': hits,
                      'status': 'PASS' if proc.returncode == 0 and hits else 'FAIL'}
            harness_done[harness] = result
            row.update(result)
            rows.append(row)
            print('%s %s EXIT=%s PROOFS=%d'
                  % (row['status'], name, proc.returncode, len(hits)), flush=True)
            continue

        cmd, env = split_env((recipe.get('cargo_command') or '').strip())
        env.update({k: str(v) for k, v in (recipe.get('env_values') or {}).items()})
        env.update({k: str(v) for k, v in env_map.get(name, {}).items()})
        missing = sorted(consumer_env_names(recipe) - set(env))
        if missing:
            row.update(mode='direct', status='NOT_RUN', exit_code=None,
                       reason='no recorded value for required env: %s' % ', '.join(missing))
            rows.append(row)
            print('NOT_RUN %s MISSING_ENV=%s' % (name, ','.join(missing)), flush=True)
            continue
        # Consumers write their fresh results under the recipe's `{out}` scratch token
        # and open those files with create_new, so each entry needs its own empty
        # directory; a shared one turns the second compilation of a name into a false
        # AlreadyExists failure.
        seq = seen_labels.get(label, 0)
        seen_labels[label] = seq + 1
        scratch = out / ('%s.out%s' % (label, seq))
        scratch.mkdir(parents=True, exist_ok=True)
        env = {k: (v.replace('{out}', str(scratch)) if isinstance(v, str) else v)
               for k, v in env.items()}
        for value in env.values():
            if isinstance(value, str) and value.startswith(('/', './')):
                pathlib.Path(value).parent.mkdir(parents=True, exist_ok=True)
        stuck = sorted('%s=%s' % (k, v) for k, v in env.items()
                       if k in consumer_env_names(recipe) and PLACEHOLDER.search(v))
        if stuck:
            row.update(mode='direct', status='NOT_RUN', exit_code=None,
                       reason='recipe placeholder unresolved: %s' % ', '.join(stuck))
            rows.append(row)
            print('NOT_RUN %s UNRESOLVED_PLACEHOLDER=%s' % (name, ','.join(stuck)), flush=True)
            continue
        absent = absent_paths(env)
        if absent:
            row.update(mode='direct', status='NOT_RUN', exit_code=None,
                       reason='staged producer output absent: %s' % ', '.join(absent))
            rows.append(row)
            print('NOT_RUN %s ABSENT_INPUTS=%s' % (name, ','.join(absent)), flush=True)
            continue
        argv = [binary['path'], '--ignored', '--exact', name,
                '--nocapture', '--test-threads=1']
        row['mode'] = 'direct'
        row['command'] = ' '.join(shlex.quote(a) for a in argv)
        row['env_names'] = sorted(env)
        log = out / ('%s%s.log' % (label, '' if seq == 0 else '.%d' % seq))
        started = time.time()
        with log.open('w') as handle:
            handle.write(row['command'] + '\n')
            handle.flush()
            proc = subprocess.run(argv, cwd=ROOT, env=dict(os.environ, **env),
                                  stdout=handle, stderr=subprocess.STDOUT,
                                  timeout=args.timeout)
        text = log.read_text(errors='replace')
        proved = all(p in text for p in PROOF)
        row.update(exit_code=proc.returncode, log=log.name, executed_one=proved,
                   seconds=round(time.time() - started, 2),
                   status='PASS' if proc.returncode == 0 and proved else 'FAIL')
        rows.append(row)
        print('%s %s EXIT=%s' % (row['status'], name, proc.returncode), flush=True)

    counts = {}
    for r in rows:
        counts[r['status']] = counts.get(r['status'], 0) + 1
    identities = sorted({(r.get('binary_path'), r.get('binary_sha256'))
                         for r in rows if r.get('binary_sha256')})
    summary = {'entries': len(rows), 'status_counts': counts,
               'workspace_binaries': [{'path': p, 'sha256': s} for p, s in identities],
               'rows': rows}
    (out / 'rebind.json').write_text(json.dumps(summary, indent=2) + '\n')
    print('REBIND_ENTRIES', len(rows))
    print('REBIND_COUNTS', json.dumps(counts))
    print('REBIND_BINARIES', len(identities))
    return 1 if counts.get('FAIL') or counts.get('NOT_RUN') else 0


if __name__ == '__main__':
    sys.exit(main())