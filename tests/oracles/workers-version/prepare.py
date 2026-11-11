#!/usr/bin/env python3
"""Stage the pinned hashicorp/go-version oracle; never execute Go or edit references.

The 60-vector corpus is extracted from the pinned upstream version_test.go at
staging time, so no expected value is hand-authored and the corpus cannot drift
from the source it claims to come from.
"""
import argparse
import hashlib
import json
import re
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
TOKEN = re.compile(r'"((?:[^"\\]|\\.)*)"|(-?\d+)|(true|false)')
ESCAPES = {'n': '\n', 't': '\t', 'r': '\r', 'a': '\a', 'b': '\b', 'f': '\f',
           'v': '\v', '\\': '\\', '"': '"', "'": "'"}


def unquote(literal):
    out = []
    i = 0
    while i < len(literal):
        ch = literal[i]
        if ch != '\\':
            out.append(ch)
            i += 1
            continue
        esc = literal[i + 1]
        if esc in ESCAPES:
            out.append(ESCAPES[esc])
            i += 2
        elif esc in 'xuU':
            width = {'x': 2, 'u': 4, 'U': 8}[esc]
            out.append(chr(int(literal[i + 2:i + 2 + width], 16)))
            i += 2 + width
        elif esc in '01234567':
            out.append(chr(int(literal[i + 1:i + 4], 8)))
            i += 4
        else:
            raise SystemExit('unsupported Go escape: \\' + esc)
    return ''.join(out)


def table(text, function):
    """Rows of one `cases := []struct{...}{...}` literal, in source order."""
    start = text.index('func %s(t *testing.T) {' % function)
    head = text.index('cases := []struct {', start)
    body = text[text.index('\t}{\n', head) + 4:]
    body = body[:body.index('\n\t}\n')]
    rows = []
    for line in body.splitlines():
        line = line.strip()
        if not line.startswith('{'):
            continue
        values = []
        for match in TOKEN.finditer(line):
            if match.group(1) is not None:
                values.append(unquote(match.group(1)))
            elif match.group(2) is not None:
                values.append(int(match.group(2)))
            else:
                values.append(match.group(3) == 'true')
        rows.append(values)
    return rows


def corpus(repository):
    pinned = repository / 'references/go-modules/hashicorp-go-version-v1.9.0'
    text = (pinned / 'version_test.go').read_text()
    # RedisStore.Start reads the stored version, so the validity probes go on the
    # `existing` side against the real server constant; the comparison tables put
    # v1 on the `current` side because that is the operand GreaterThan is called on.
    server_version = re.search(
        r'^const Version = "([^"]+)"$',
        (repository / 'references/livekit-stable/version/version.go').read_text(),
        re.M).group(1)
    vectors = []

    def add(table_name, rows, operands):
        for index, values in enumerate(rows):
            existing, current = operands(values)
            vectors.append({'existing': existing, 'current': current,
                            'source': '%s[%d]' % (table_name, index)})

    add('TestGreaterThan', table(text, 'TestGreaterThan'), lambda v: (v[1], v[0]))
    add('TestComparePreReleases', table(text, 'TestComparePreReleases'), lambda v: (v[1], v[0]))
    add('TestNewVersion', table(text, 'TestNewVersion'), lambda v: (v[0], server_version))
    for index, vector in enumerate(vectors):
        vector['id'] = 'v%02d' % index
    return vectors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    repository = Path(args.repository).resolve()
    target = Path(args.out).resolve()
    target.mkdir(exist_ok=False)

    expected = json.loads((HERE / 'SOURCE.json').read_text())['source_sha256']
    for rel, want in expected.items():
        actual = hashlib.sha256((repository / rel).read_bytes()).hexdigest()
        if actual != want:
            raise SystemExit('Source drift: ' + rel)

    vectors = corpus(repository)
    if len(vectors) != 60:
        raise SystemExit('expected 60 upstream vectors, extracted %d' % len(vectors))
    (target / 'cases.json').write_text(json.dumps(vectors, indent=1) + '\n')

    mod = (repository / 'references/livekit-stable/go.mod').read_text()
    needle = 'module github.com/livekit/livekit-server\n'
    if mod.count(needle) != 1:
        raise SystemExit('unexpected module line in the pinned server go.mod')
    mod = mod.replace(needle, 'module local.test/livekit-workers-version\n', 1)
    mod += '\nreplace github.com/hashicorp/go-version => %s\n' % (
        repository / 'references/go-modules/hashicorp-go-version-v1.9.0')
    (target / 'go.mod').write_text(mod)
    shutil.copyfile(repository / 'references/livekit-stable/go.sum', target / 'go.sum')
    shutil.copyfile(HERE / 'main.go', target / 'main.go')

    inputs = ['go.mod', 'go.sum', 'main.go', 'cases.json']
    (target / 'STAGED.json').write_text(json.dumps({
        'status': 'STAGED_NOT_RUN',
        'source_manifest': hashlib.sha256((HERE / 'SOURCE.json').read_bytes()).hexdigest(),
        'vectors': len(vectors),
        'inputs': {n: hashlib.sha256((target / n).read_bytes()).hexdigest() for n in inputs},
    }, indent=2) + '\n')
    print(str(target))


if __name__ == '__main__':
    main()