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
from prepare import FIXED_HEADS, HERE, prepare, require, sha256

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
            target.parent.mkdir(parents=True, exist_ok=True)
            # macOS APFS clone keeps isolated staging inexpensive; no hard links.
            subprocess.run(['/bin/cp', '-cR', str(source), str(target)], check=True)
            copied.append(relative)
        metadata = Path('cache/download') / name / '@v'
        for suffix in ('.mod', '.info', '.ziphash'):
            source = MODULE_CACHE / metadata / (version + suffix)
            if source.is_file():
                target = dest / metadata / source.name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
    return copied


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixed-source-root', type=Path, default=HERE.parents[2])
    args = parser.parse_args()
    work = Path(tempfile.mkdtemp(prefix='c55-go-', dir='/tmp')).resolve()
    print(work, flush=True)
    stage = prepare(args.fixed_source_root, work / 'stage')
    for name in ('cache', 'tmp', 'modcache', 'gopath'):
        (work / name).mkdir()
    env = dict(os.environ)
    overrides = {
        'GOTOOLCHAIN': 'local', 'GOWORK': 'off', 'GOPROXY': 'off', 'GOSUMDB': 'off',
        'GOENV': 'off', 'GOFLAGS': '-mod=readonly -p=4 -buildvcs=false',
        'GOCACHE': str(work / 'cache'), 'GOMODCACHE': str(work / 'modcache'),
        'GOPATH': str(work / 'gopath'), 'GOTMPDIR': str(work / 'tmp'),
        'TMPDIR': str(work / 'tmp'), 'GOTELEMETRY': 'off', 'GOMAXPROCS': '4',
        'GOROOT': str(GO.parent.parent), 'GOOS': 'darwin', 'GOARCH': 'arm64',
        'CGO_ENABLED': '1',
    }
    env.update(overrides)
    commands = []

    def run(name, command, timeout=600):
        commands.append({'name': name, 'command': list(map(str, command))})
        (work / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        with (work / (name + '.stdout')).open('wb') as out, (work / (name + '.stderr')).open('wb') as err:
            subprocess.run(list(map(str, command)), cwd=stage, env=env, stdout=out, stderr=err, timeout=timeout, check=True)
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
    packages = run('dependencies', [GO, 'list', '-deps', '-json', '.'])
    decoder, offset, dependency_hashes = json.JSONDecoder(), 0, {}
    while offset < len(packages):
        while offset < len(packages) and packages[offset].isspace():
            offset += 1
        if offset == len(packages):
            break
        package, offset = decoder.raw_decode(packages, offset)
        for key in ('GoFiles', 'CgoFiles', 'CFiles', 'HFiles', 'SFiles', 'SysoFiles', 'EmbedFiles'):
            for name in package.get(key, []):
                path = Path(package['Dir']) / name
                dependency_hashes[str(path)] = sha256(path)
    (work / 'dependency-source-sha256.json').write_text(json.dumps(dependency_hashes, indent=2, sort_keys=True) + '\n')
    run('build-tests', [GO, 'test', '-race', '-c', '-o', work / 'observer.test', '.'])
    run('tests', [work / 'observer.test', '-test.v', '-test.count=20', '-test.timeout=60s'])
    run('build-observer', [GO, 'build', '-o', work / 'observer', '.'])
    run('observer-build-info', [GO, 'version', '-m', work / 'observer'])
    run('tests-build-info', [GO, 'version', '-m', work / 'observer.test'])
    raw = run('observer', [work / 'observer'])
    rows = [json.loads(line) for line in raw.splitlines()]
    require(len(rows) == 6, 'expected six JSONL rows')
    for index, row in enumerate(rows):
        require(set(row) == {'codec_count', 'batch', 'events'}, 'row schema')
        require(row['codec_count'] == index // 2 + 1 and row['batch'] == bool(index % 2), 'case order')
        require(len(row['events']) == 5 * row['codec_count'], 'event count')
        for event in row['events']:
            require(set(event) == {'codecs'} and len(event['codecs']) == row['codec_count'], 'event schema')
            for codec in event['codecs']:
                require(set(codec) == {'codec', 'qualities'} and len(codec['qualities']) == 3, 'codec schema')
                for q, quality in enumerate(codec['qualities']):
                    require(set(quality) == {'quality', 'enabled'} and quality['quality'] == q and type(quality['enabled']) is bool, 'quality schema')
        if index % 2:
            require(row['events'] == rows[index - 1]['events'], 'batch changed snapshots')
    (work / 'observations.jsonl').write_text(raw)
    # Confirm all pinned sources and staged module inputs remained unchanged.
    for relative, expected in provenance['source_sha256'].items():
        require(sha256(stage / 'sources' / relative) == expected, 'source changed during build: ' + relative)
    for name, expected in provenance['staged_sha256'].items():
        require(sha256(stage / name) == expected, 'staged input changed: ' + name)
    provenance['binary_sha256'] = {n: sha256(work / n) for n in ('observer', 'observer.test')}
    provenance['evidence_sha256'] = {p.name: sha256(p) for p in work.iterdir() if p.is_file() and p.name != 'provenance.json'}
    provenance['outcome'] = 'PASS: 6 rows, 60 measured events; 20 race-enabled repetitions of all observer tests'
    provenance['emitted_codec_strings'] = sorted({c['codec'] for row in rows for e in row['events'] for c in e['codecs']})
    (work / 'provenance.json').write_text(json.dumps(provenance, indent=2, sort_keys=True) + '\n')
    print(provenance['outcome'])
    print(work / 'observations.jsonl')


if __name__ == '__main__':
    main()
