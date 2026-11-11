#!/usr/bin/env python3
"""Derive the consumer env-map from the producer commands a recipe already records.

rebind-workspace.py needs a value for every env name the Rust consumer reads.
Those values live in the producer invocations, not in the recipe's prose `env`
table, and an earlier batch supplied them through an --env-map that was never
persisted. This tool reconstructs that map from the recorded commands alone, so
the measurement is reproducible from committed inputs.

Rules are applied strongest first and every value carries its derivation:
  env-prefix   the producer was literally invoked with NAME=value
  normalized   a later command read this corpus and wrote the one the consumer parses
  redirection  a shell `>` or `<` (the producer's own result stream)
  flag         an explicit --output/--input style flag
  argument     a bare data-file argument to the producer

Compiled oracles, directories and `2>&1` are not corpora and are filtered out.
A name that no rule can pin to exactly one path is reported UNRESOLVED. It is
never guessed, because a wrong fixture path turns a real comparison into a
comparison against the wrong corpus.
"""
import argparse
import json
import pathlib
import re
import shlex
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
ENV_PREFIX = re.compile(r'^([A-Z][A-Z0-9_]*)=(\S+)\s+')
HARNESS = re.compile(r'tests/[\w./-]*run[\w./-]*\.py')
OUT_FLAGS = ('--output', '--out', '--results', '--result', '--log', '-o')
IN_FLAGS = ('--input', '--cases', '--source', '--vectors', '--stages', '-i')
OUT_SUFFIX = ('_RESULTS', '_RESULT', '_OUTPUT', '_LOG', '_ORACLE', '_ACTUAL',
              '_RECEIPT', '_REPORT')
IN_SUFFIX = ('_CASES', '_INPUT', '_VECTORS', '_SOURCE', '_STAGES')
DATA_SUFFIX = {'.json', '.jsonl', '.tsv', '.log', '.out', '.txt', '.csv', '.yaml'}


def split_env(cmd):
    env = {}
    while True:
        m = ENV_PREFIX.match(cmd)
        if not m:
            return cmd, env
        env[m.group(1)] = m.group(2)
        cmd = cmd[m.end():]


def harness_mode(recipe):
    """True when rebind-workspace.py will drive this entry through its harness."""
    for cmd in recipe.get('producer_commands') or []:
        if cmd.strip().startswith('#') or not HARNESS.search(cmd):
            continue
        body, _ = split_env(cmd.strip())
        try:
            tokens = shlex.split(body)
        except ValueError:
            continue
        has_binary = any(f in tokens for f in ('--test-binary', '--binary', '--rust-binary'))
        has_out = any(f in tokens for f in ('--output', '--out', '--evidence-dir'))
        if has_binary and has_out:
            return True
    return False


def consumer_env_names(recipe):
    """Only names the Rust consumer source actually reads as a literal."""
    declared = set((recipe.get('env') or {}).keys())
    if not declared:
        return set()
    source = ROOT / recipe.get('source_file', '')
    if not source.is_file():
        return declared
    text = source.read_text(errors='replace')
    return {n for n in declared if '"%s"' % n in text}


def is_corpus(token, cwd, allow_program=False):
    """A corpus is a data file, never a directory or `&1`.

    `go build -o` writes a program, not a corpus; it is only admissible when the
    consumer is meant to execute that program, and it ranks below every data file.
    """
    if token.startswith('&') or token.startswith('-'):
        return None
    token = token.replace('${PWD}', str(cwd)).replace('$PWD', str(cwd))
    path = pathlib.Path(token)
    if not path.is_absolute():
        path = cwd / path
    if path.suffix not in DATA_SUFFIX:
        if not allow_program or path.exists() and not path.is_file():
            return None
    elif path.exists() and not path.is_file():
        return None
    return str(path)


def producer_role(name):
    if name.endswith(OUT_SUFFIX):
        return 'out'
    if name.endswith(IN_SUFFIX):
        return 'in'
    return None


SEPARATORS = {'&&', '||', ';'}
ASSIGN = re.compile(r'^([A-Z][A-Z0-9_]*)=(.*)$', re.S)
HEREDOC = re.compile(r'<<-?\s*[\'"]?([A-Za-z_][A-Za-z0-9_]*)[\'"]?')


def segments(raw):
    """One recorded command split into bash segments, honouring quotes.

    A recipe command is often `cd DIR && NAME=value program …`, so the env
    assignment is not at the start of the string. Splitting on the shell
    operators after tokenising keeps quoted paths intact.
    """
    lines = raw.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        i += 1
        if not line or line.startswith('#'):
            continue
        marker = HEREDOC.search(line)
        if marker:
            while i < len(lines) and lines[i].strip() != marker.group(1):
                i += 1
            i += 1
        try:
            tokens = shlex.split(line)
        except ValueError:
            continue
        current = []
        for token in tokens:
            if token in SEPARATORS:
                if current:
                    yield current
                current = []
            else:
                current.append(token)
        if current:
            yield current


def candidates(recipe):
    """(path, rule, command, direction) for every corpus a producer command names.

    `cd` is stateful inside one entry's bash script, so a relative flag value
    resolves against the directory that command actually ran in.
    """
    found = []
    transforms = []
    cwd = ROOT
    for raw in recipe.get('producer_commands') or []:
        if raw.strip().startswith('#'):
            continue
        for seg in segments(raw):
            env = {}
            while seg and ASSIGN.match(seg[0]):
                m = ASSIGN.match(seg[0])
                env[m.group(1)] = m.group(2)
                seg = seg[1:]
            for name, value in env.items():
                resolved = is_corpus(value, cwd)
                if resolved:
                    found.append((resolved, 'env-prefix', raw, name))
            if not seg:
                continue
            if seg[0] == 'cd':
                if len(seg) > 1:
                    target = pathlib.Path(seg[1].replace('$PWD', str(cwd)))
                    cwd = target if target.is_absolute() else cwd / target
                continue
            seen = {f[0] for f in found}

            def add(value, rule, direction, allow_program=False):
                resolved = is_corpus(value, cwd, allow_program=allow_program)
                if resolved and resolved not in seen:
                    seen.add(resolved)
                    found.append((resolved, rule, raw, direction))

            builds = 'build' in seg
            consumed = set()
            ins, outs = [], []
            for i, token in enumerate(seg):
                if token in ('>', '>>') and i + 1 < len(seg):
                    add(seg[i + 1], 'redirection', 'out')
                    consumed.add(i + 1)
                elif token == '<' and i + 1 < len(seg):
                    add(seg[i + 1], 'redirection', 'in')
                    consumed.add(i + 1)
                elif token in OUT_FLAGS and i + 1 < len(seg):
                    program = builds and token == '-o'
                    add(seg[i + 1], 'program' if program else 'flag',
                        'program' if program else 'out', allow_program=program)
                    consumed.add(i + 1)
                    if not program:
                        outs.append(seg[i + 1])
                elif token in IN_FLAGS and i + 1 < len(seg):
                    add(seg[i + 1], 'flag', 'in')
                    consumed.add(i + 1)
                    ins.append(seg[i + 1])
            for src in ins:
                for dst in outs:
                    a, b = is_corpus(src, cwd), is_corpus(dst, cwd)
                    if a and b and a != b:
                        transforms.append((a, b, raw))
            # `test -f X` asserts a committed corpus exists; X serves either role.
            if seg[0] == 'test' and len(seg) > 2 and seg[1] in ('-f', '-e', '-s'):
                add(seg[2], 'committed', 'any')
                consumed.add(2)
            for i, token in enumerate(seg):
                if i == 0 or i in consumed or seg[i - 1].startswith('-'):
                    continue
                add(token, 'argument', 'in')
    # A normalizer reads one producer's raw stream and writes the corpus the Rust
    # consumer actually parses, so the downstream artifact outranks the stream it
    # was derived from. Binding the raw stream compares against the wrong corpus.
    known = {f[0] for f in found}
    downstream = {dst for src, dst, _ in transforms if src in known}
    if downstream:
        found = [(p, 'normalized' if p in downstream else r, c, d)
                 for p, r, c, d in found]
        for src, dst, raw in transforms:
            if src in known and dst not in known:
                known.add(dst)
                found.append((dst, 'normalized', raw, 'out'))
    return found


def wanted_class(name):
    if name.endswith(OUT_SUFFIX):
        return 'out'
    if name.endswith(IN_SUFFIX):
        return 'in'
    return None


def tokens_of(text):
    return set(re.findall(r'[a-z0-9]+', text.lower()))


RULE_RANK = {'env-prefix': 0, 'normalized': 1, 'committed': 2, 'redirection': 3,
             'flag': 4, 'argument': 5, 'program': 6}


def resolve(name, pool):
    """Pin one env name to one path, or explain why it cannot be pinned."""
    exact = [(p, r, c) for p, r, c, d in pool if d == name]
    if exact:
        uniq = {}
        for p, r, c in exact:
            uniq.setdefault(p, (r, c))
        if len(uniq) == 1:
            p = next(iter(uniq))
            return p, uniq[p][0], uniq[p][1], 'env-prefix'
        return None, None, None, 'AMBIGUOUS_ENV_PREFIX:' + ','.join(sorted(uniq))

    cls = wanted_class(name)
    scoped = [(p, r, c) for p, r, c, d in pool
              if cls is None or d == cls or d == name or d == 'any']
    uniq = {}
    for p, r, c in scoped:
        uniq.setdefault(p, (r, c))
    if not uniq:
        return None, None, None, 'NO_CANDIDATE'
    if len(uniq) == 1:
        p = next(iter(uniq))
        return p, uniq[p][0], uniq[p][1], 'single-candidate'
    # A producer's redirected stdout is its result; a flag or a bare argument may
    # just name an intermediate, and a built program is never a corpus.
    for rule in sorted(RULE_RANK, key=RULE_RANK.get):
        at_rule = [p for p in uniq if uniq[p][0] == rule]
        if len(at_rule) == 1:
            p = at_rule[0]
            return p, uniq[p][0], uniq[p][1], 'rule-priority:' + rule
    want = tokens_of(name)
    scored = []
    for p in uniq:
        path = pathlib.Path(p)
        have = tokens_of(path.name) | tokens_of(path.parent.name)
        scored.append((len(want & have), p))
    scored.sort(key=lambda x: (-x[0], x[1]))
    if scored[0][0] > 0 and (len(scored) == 1 or scored[0][0] > scored[1][0]):
        p = scored[0][1]
        return p, uniq[p][0], uniq[p][1], 'token-score:%d' % scored[0][0]
    return None, None, None, 'AMBIGUOUS:' + ','.join(sorted(uniq))


def sole_role_alias(name, pool, pending):
    """Bind a consumer name to the producer's own env name for the same file.

    Producers and consumers do not always agree on the variable name
    (`PION_SENT_SIZE_OUTPUT` vs `PION_SENT_SIZE_RESULTS`). When one entry has
    exactly one unresolved consumer name of a role and its producer commands
    declare exactly one env-prefix value of that role, the binding is forced by
    counting, not by name similarity. The producer's name is recorded.
    """
    role = wanted_class(name)
    if role is None or sum(1 for n in pending if wanted_class(n) == role) != 1:
        return None
    declared = {}
    for path, rule, command, direction in pool:
        if rule != 'env-prefix' or producer_role(direction) != role:
            continue
        declared.setdefault(direction, (path, command))
    if len(declared) != 1:
        return None
    producer_name, (path, command) = next(iter(declared.items()))
    return path, 'env-prefix', command, 'sole-role-alias:%s' % producer_name


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--recipes', required=True, action='append')
    ap.add_argument('--runtime', required=True)
    ap.add_argument('--output', required=True, help='env-map JSON for rebind --env-map')
    ap.add_argument('--derivation', required=True, help='per-value rule and command')
    ap.add_argument('--require-on-disk', action='store_true',
                    help='fail when a derived value does not exist (stage first)')
    args = ap.parse_args()

    recipes = {}
    for path in args.recipes:
        for row in json.loads(pathlib.Path(path).read_text()):
            recipes.setdefault(row['test'], row)
    runtime = json.loads(pathlib.Path(args.runtime).read_text())

    env_map, derivation, unresolved = {}, [], []
    counts = {'harness_mode': 0, 'no_consumer_env': 0, 'already_supplied': 0,
              'entries_with_derivation': 0}
    for entry in runtime:
        name = entry['test']
        recipe = recipes.get(name)
        if recipe is None:
            continue
        if harness_mode(recipe):
            counts['harness_mode'] += 1
            continue
        names = consumer_env_names(recipe)
        if not names:
            counts['no_consumer_env'] += 1
            continue
        _, env = split_env((recipe.get('cargo_command') or '').strip())
        env.update({k: str(v) for k, v in (recipe.get('env_values') or {}).items()})
        missing = sorted(names - set(env))
        if not missing:
            counts['already_supplied'] += 1
            continue
        pool = candidates(recipe)
        got = {}
        pending = list(missing)
        reasons = {}
        for env_name in missing:
            value, rule, command, how = resolve(env_name, pool)
            if value is None:
                reasons[env_name] = how
                continue
            got[env_name] = value
            pending.remove(env_name)
            derivation.append({'test': name, 'env': env_name, 'value': value,
                               'rule': rule, 'selection': how, 'command': command,
                               'on_disk': pathlib.Path(value).exists()})
        for env_name in list(pending):
            aliased = sole_role_alias(env_name, pool, pending)
            if aliased is None:
                continue
            value, rule, command, how = aliased
            got[env_name] = value
            pending.remove(env_name)
            derivation.append({'test': name, 'env': env_name, 'value': value,
                               'rule': rule, 'selection': how, 'command': command,
                               'on_disk': pathlib.Path(value).exists()})
        for env_name in pending:
            unresolved.append({'test': name, 'env': env_name,
                               'reason': reasons.get(env_name, 'NO_CANDIDATE')})
        if got:
            env_map[name] = got
            counts['entries_with_derivation'] += 1

    off_disk = [d for d in derivation if not d['on_disk']]
    for target in (args.output, args.derivation):
        pathlib.Path(target).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(args.output).write_text(json.dumps(env_map, indent=2) + '\n')
    pathlib.Path(args.derivation).write_text(json.dumps(
        {'counts': counts, 'values': len(derivation), 'unresolved': unresolved,
         'not_on_disk': [{'test': d['test'], 'env': d['env'], 'value': d['value']}
                         for d in off_disk],
         'derivation': derivation}, indent=2) + '\n')
    print('DERIVE_COUNTS', json.dumps(counts))
    print('DERIVED_VALUES', len(derivation))
    print('UNRESOLVED', len(unresolved))
    for row in unresolved:
        print('  ', row['test'], row['env'], row['reason'][:160])
    print('NOT_ON_DISK', len(off_disk))
    for row in off_disk:
        print('  ', row['test'], row['env'], row['value'])
    if args.require_on_disk and off_disk:
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())