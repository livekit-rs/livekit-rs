#!/usr/bin/env python3
"""Stage, build and test offline; all writable caches/evidence stay in task tmp."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
from prepare import FIXED_HEADS, HERE, checked_parent, git, mkdir, prepare, require, sha256
from validate import load

GO = Path('/Users/zoujiaqing/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin/go')
MODULE_CACHE = Path('/Users/zoujiaqing/go/pkg/mod')


def escape(value):
    return ''.join('!' + c.lower() if c.isupper() else c for c in value)


def seed_cache(stage, dest):
    # Copy immutable cached modules, never build with a writable shared cache.
    # No download or synthetic dependency replacement is allowed.
    identities = set()
    for relative in FIXED_HEADS:
        for line in (stage / 'sources' / relative / 'go.sum').read_text().splitlines():
            name, version, _ = line.split()
            identities.add((escape(name), escape(version.removesuffix('/go.mod'))))
    copied = []
    for name, version in sorted(identities):
        relative = name + '@' + version
        source = MODULE_CACHE / relative
        if source.is_dir():
            target = dest / relative
            mkdir(target.parent)
            checked_parent(target)
            # macOS APFS clone keeps isolated staging inexpensive; no hard links.
            subprocess.run(['/bin/cp', '-cR', str(source), str(target)], check=True)
            copied.append(relative)
        metadata = Path('cache/download') / name / '@v'
        for suffix in ('.mod', '.info', '.ziphash'):
            source = MODULE_CACHE / metadata / (version + suffix)
            if source.is_file():
                target = dest / metadata / source.name
                mkdir(target.parent)
                checked_parent(target)
                shutil.copyfile(source, target)
    return copied


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixed-source-root', type=Path, default=HERE.parents[2])
    args = parser.parse_args()
    checked_parent(Path('/tmp/c57-go-new'))
    work = Path(tempfile.mkdtemp(prefix='c57-go-', dir='/tmp')).resolve()
    print(work, flush=True)
    stage = prepare(args.fixed_source_root, work / 'stage')
    for name in ('cache', 'tmp', 'modcache', 'gopath'):
        mkdir(work / name)
    env = dict(os.environ)
    # Observation output is enabled only for the separate count=1 invocation.
    env.pop('C57_GO_JSONL', None)
    overrides = {
        'GOTOOLCHAIN': 'local', 'GOWORK': 'off', 'GOPROXY': 'off', 'GOSUMDB': 'off',
        'GOENV': 'off', 'GOFLAGS': '-mod=readonly -p=4 -buildvcs=false',
        'GOCACHE': str(work / 'cache'), 'GOMODCACHE': str(work / 'modcache'),
        'GOPATH': str(work / 'gopath'), 'GOTMPDIR': str(work / 'tmp'),
        'TMPDIR': str(work / 'tmp'), 'GOTELEMETRY': 'off', 'GOMAXPROCS': '4',
        'GOROOT': str(GO.parent.parent), 'GOOS': 'darwin', 'GOARCH': 'arm64',
        'CGO_ENABLED': '1', 'GOEXPERIMENT': '', 'GODEBUG': '',
        'PYTHONDONTWRITEBYTECODE': '1',
    }
    env.update(overrides)
    commands = []

    def run(name, command, timeout=600, extra_env=None, allow_failure=False):
        checked_parent(work / (name + '.stdout'))
        entry = {'name': name, 'command': list(map(str, command)), 'environment_override': extra_env or {}, 'timeout_seconds': timeout}
        commands.append(entry)
        (work / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        try:
            with (work / (name + '.stdout')).open('wb') as out, (work / (name + '.stderr')).open('wb') as err:
                result = subprocess.run(entry['command'], cwd=stage, env=dict(env, **(extra_env or {})), stdout=out, stderr=err, timeout=timeout)
            entry['exit_code'] = result.returncode
            if not allow_failure:
                result.check_returncode()
        except subprocess.TimeoutExpired:
            entry['timed_out'] = True
            raise
        finally:
            (work / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        return (work / (name + '.stdout')).read_text()

    provenance = json.loads((stage / 'provenance.json').read_text())
    provenance['environment'] = overrides
    provenance['go_binary'] = {'path': str(GO), 'sha256': sha256(GO)}
    version = run('go-version', [GO, 'version']).strip()
    require(version == 'go version go1.26.0 darwin/arm64', 'unexpected Go toolchain: ' + version)
    provenance['go_binary']['version'] = version
    provenance['seeded_modules'] = seed_cache(stage, work / 'modcache')
    (work / 'provenance.json').write_text(json.dumps(provenance, indent=2, sort_keys=True) + '\n')
    run('go-env', [GO, 'env', '-json'])
    # Preserve actual compiled package source identities, including Pion's MIME
    # constants (protocol MimeType.String delegates to that pinned replacement).
    packages = run('dependencies', [GO, 'list', '-deps', '-test', '-json', '.'])
    decoder, offset, dependency_hashes = json.JSONDecoder(), 0, {}
    while offset < len(packages):
        while offset < len(packages) and packages[offset].isspace():
            offset += 1
        if offset == len(packages):
            break
        package, offset = decoder.raw_decode(packages, offset)
        for key in ('GoFiles', 'TestGoFiles', 'XTestGoFiles', 'CgoFiles', 'CFiles', 'HFiles', 'SFiles', 'SysoFiles', 'EmbedFiles'):
            for name in package.get(key, []):
                path = Path(package['Dir']) / name
                dependency_hashes[str(path)] = sha256(path)
    (work / 'dependency-source-sha256.json').write_text(json.dumps(dependency_hashes, indent=2, sort_keys=True) + '\n')
    run('build-tests', [GO, 'test', '-race', '-c', '-o', work / 'observer.test', '.'])
    run('tests', [work / 'observer.test', '-test.run=^TestInitialUpdate$', '-test.v', '-test.count=20', '-test.timeout=60s'], allow_failure=True)
    run('tests-build-info', [GO, 'version', '-m', work / 'observer.test'])
    output = work / 'observations.jsonl'
    run('observer', [work / 'observer.test', '-test.run=^TestObserve$', '-test.v', '-test.count=1', '-test.timeout=60s'],
        extra_env={'C57_GO_JSONL': str(output)})
    raw = output.read_bytes()
    # Do not suppress independent observation if race assertions disagree.
    # The strict validator remains unchanged and retains its nonzero exit.
    rows = [json.loads(line) for line in raw.splitlines()]
    run('validate', ['/usr/bin/python3', '-B', HERE / 'validate.py', '--input', output, '--self-test'], allow_failure=True)
    validated = commands[-1]['exit_code'] == 0
    if validated:
        load(raw)
        run('project-codecs', ['/usr/bin/python3', '-B', HERE / 'project-codecs.py', '--fixed-source-root', args.fixed_source_root.resolve(),
                               '--input', output, '--out', work / 'observations.projected.jsonl'])
    require(output.read_bytes() == raw, 'raw observer output changed')
    # An existing output must be rejected without altering the original evidence.
    duplicate = [str(work / 'observer.test'), '-test.run=^TestObserve$', '-test.count=1', '-test.timeout=60s']
    try:
        run('existing-output-control', duplicate, extra_env={'C57_GO_JSONL': str(output)})
    except subprocess.CalledProcessError as error:
        require(error.returncode == 1, 'unexpected existing-output exit')
        require('file exists' in (work / 'existing-output-control.stdout').read_text(), 'missing O_EXCL rejection')
    else:
        raise ValueError('observer accepted existing output')
    require(output.read_bytes() == raw, 'existing-output control modified evidence')
    # Confirm all pinned sources and staged module inputs remained unchanged.
    for relative, expected in provenance['source_sha256'].items():
        require(sha256(stage / 'sources' / relative) == expected, 'source changed during build: ' + relative)
    for name, expected in provenance['staged_sha256'].items():
        require(sha256(stage / name) == expected, 'staged input changed: ' + name)
    provenance['binary_sha256'] = {'observer.test': sha256(work / 'observer.test')}
    provenance['evidence_sha256'] = {p.name: sha256(p) for p in work.iterdir() if p.is_file() and p.name != 'provenance.json'}
    for relative, expected in FIXED_HEADS.items():
        for root in (args.fixed_source_root.resolve(), stage / 'sources'):
            require(git(root / relative, 'rev-parse', 'HEAD') == expected, 'reference HEAD changed')
            require(not git(root / relative, 'status', '--porcelain', '--untracked-files=all'), 'reference changed during run')
    provenance['observed_counts'] = {
        'rows': len(rows),
        'checkpoints': sum(len(r['observations']) for r in rows),
        'events': sum(len(o['events']) for r in rows for o in r['observations']),
    }
    passed = validated and all(c.get('exit_code') == 0 for c in commands if c['name'] != 'existing-output-control')
    provenance['outcome'] = ('PASS' if passed else 'FAIL') + ': 20 race-enabled matrix repetitions, separate observer, strict literal validator; see commands and logs for actual exits'
    provenance['expected_counts'] = {'rows': 14, 'checkpoints': 140, 'events': 10}
    provenance['emitted_codec_strings'] = sorted({c['codec'] for row in rows for o in row['observations'] for e in o['events'] for c in e['codecs']})
    (work / 'provenance.json').write_text(json.dumps(provenance, indent=2, sort_keys=True) + '\n')
    print(provenance['outcome'])
    print(work / 'observations.jsonl')
    if not passed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
