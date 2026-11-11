#!/usr/bin/env python3
"""Restore locked reference sources; --verify is read-only and offline.
Existing checkouts or package directories are never reset or overwritten.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile
import tempfile
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
SPEC = ROOT / 'references'
LOCK = json.loads((SPEC / '0001-references-lock.json').read_text())
CATALOG = json.loads((SPEC / '0001-dependency-catalog.json').read_text())


def local(path):
    result = (ROOT / path).resolve()
    if ROOT / 'references' not in result.parents:
        raise ValueError('Path outside references: ' + path)
    return result


def git(path, *args):
    return subprocess.check_output(['git', '-C', str(path), *args], text=True).strip()


def check_files(base, files):
    for entry in files:
        path = (base / entry['path']).resolve()
        if base.resolve() not in path.parents:
            raise ValueError('Unsafe evidence path')
        if hashlib.sha256(path.read_bytes()).hexdigest() != entry['sha256']:
            raise ValueError('Source hash mismatch: ' + str(path))


def restore_repo(row, verify):
    path = local(row['path'])
    if not path.exists():
        if verify:
            raise ValueError('Missing repository: ' + row['path'])
        path.parent.mkdir(parents=True, exist_ok=True)
        # Stage elsewhere so an interrupted download does not occupy the target.
        with tempfile.TemporaryDirectory(prefix='.fetch-', dir=path.parent) as temp:
            stage = Path(temp) / 'repo'
            subprocess.run(['git', 'clone', '--depth', '1', '--no-checkout', row['repository'], str(stage)], check=True)
            git(stage, 'fetch', '--depth', '1', 'origin', row['checkout_commit'])
            git(stage, 'checkout', '--detach', row['checkout_commit'])
            stage.rename(path)
    if git(path, 'remote', 'get-url', 'origin').rstrip('/') != row['repository'].rstrip('/'):
        raise ValueError('Remote mismatch: ' + row['path'])
    if git(path, 'rev-parse', 'HEAD') != row['checkout_commit']:
        raise ValueError('Checkout differs; refusing to reset: ' + row['path'])
    if git(path, 'status', '--porcelain'):
        raise ValueError('Working tree changed; preserving it: ' + row['path'])
    check_files(path, row['selected_files'])


def restore_package(row, verify):
    path = local(row['local_package'])
    if not path.exists():
        if verify:
            raise ValueError('Missing package: ' + row['local_package'])
        name = row['crate']
        version = row.get('latest_stable') or row['version']
        prefix = name + '-' + version
        url = 'https://static.crates.io/crates/' + name + '/' + prefix + '.crate'
        request = urllib.request.Request(url, headers={'User-Agent': 'livekit-rs-reference-restore/0.5'})
        with urllib.request.urlopen(request, timeout=90) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != row['checksum']:
            raise ValueError('Archive checksum mismatch: ' + name)
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='.fetch-', dir=path.parent) as temp:
            stage = Path(temp)
            with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
                for member in archive.getmembers():
                    rel = PurePosixPath(member.name)
                    if rel.is_absolute() or '..' in rel.parts or not rel.parts or rel.parts[0] != prefix:
                        raise ValueError('Unsafe archive path: ' + member.name)
                    if member.isdir():
                        continue
                    if not member.isfile():
                        raise ValueError('Unsupported archive entry: ' + member.name)
                    output = stage.joinpath(*rel.parts)
                    output.parent.mkdir(parents=True, exist_ok=True)
                    output.write_bytes(archive.extractfile(member).read())
            (stage / prefix).rename(path)
    manifest_hash = row.get('package_manifest_sha256')
    if manifest_hash:
        check_files(path, [{'path': 'Cargo.toml', 'sha256': manifest_hash}])
    for evidence in LOCK['published_package_reviews']:
        if evidence['path'] == row['local_package']:
            check_files(path, evidence['files'])
    # Preserve published commits in the repository even when checkout is another version.
    rev = row.get('published_source', {}).get('git', {}).get('sha1')
    repo = row.get('local_repository')
    if rev and repo:
        present = subprocess.run(['git', '-C', str(local(repo)), 'cat-file', '-e', rev + '^{commit}'], capture_output=True).returncode == 0
        if not present:
            if verify:
                raise ValueError('Published Git object missing: ' + row['crate'])
            git(local(repo), 'fetch', '--depth', '1', 'origin', rev)



# Immutable Go zip source excerpts, including their original audit metadata.
MODULE_ARCHIVE_LOCK = SPEC / '0018-go-module-archives-lock.json'
MAX_MODULE_ZIP = 32 * 1024 * 1024
MAX_MODULE_EXPANDED = 64 * 1024 * 1024


def safe_relative(name):
    rel = PurePosixPath(name)
    if not name or '\\' in name or rel.is_absolute() or '..' in rel.parts or str(rel) != name:
        raise ValueError('Unsafe module archive path')
    return rel


def rename_noreplace(source, target):
    """Publish a complete directory atomically, never replacing even an empty one."""
    import ctypes
    import errno
    import os
    import sys
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == 'darwin':
        rc = libc.renameatx_np(-2, os.fsencode(source), -2, os.fsencode(target), 4)  # RENAME_EXCL
    elif sys.platform.startswith('linux') and hasattr(libc, 'renameat2'):
        rc = libc.renameat2(-100, os.fsencode(source), -100, os.fsencode(target), 1)  # RENAME_NOREPLACE
    else:
        raise ValueError('Atomic no-replace directory publish unavailable')
    if rc:
        code = ctypes.get_errno()
        if code == errno.EEXIST:
            raise ValueError('Reference target appeared; preserving it')
        raise OSError(code, os.strerror(code))


def module_files(row):
    files = {item['path']: item['sha256'] for item in row['selected_files']}
    if len(files) != len(row['selected_files']):
        raise ValueError('Duplicate selected module file')
    for item in row['metadata_files']:
        if item['path'] in files:
            raise ValueError('Metadata/source path collision')
        data = item['utf8'].encode('utf-8')
        if hashlib.sha256(data).hexdigest() != item['sha256']:
            raise ValueError('Locked metadata hash mismatch')
        files[item['path']] = item['sha256']
    for name in files:
        safe_relative(name)
    return files


def verify_module_archive(path, row):
    expected = module_files(row)
    actual = set()
    for entry in path.rglob('*'):
        if entry.is_symlink():
            raise ValueError('Symlink in module reference; preserving it')
        if entry.is_file():
            actual.add(entry.relative_to(path).as_posix())
        elif not entry.is_dir():
            raise ValueError('Non-regular module reference entry')
    if actual != set(expected):
        raise ValueError('Module reference file set differs; preserving it')
    check_files(path, [{'path': name, 'sha256': value} for name, value in expected.items()])


def extract_module_archive(data, row, destination):
    import stat
    import zipfile
    if len(data) > MAX_MODULE_ZIP or hashlib.sha256(data).hexdigest() != row['zip_sha256']:
        raise ValueError('Module archive checksum/size mismatch')
    selected = {item['path']: item['sha256'] for item in row['selected_files']}
    module_files(row)  # Validate metadata before any writes.
    prefix = safe_relative(row['zip_prefix'])
    seen = set()
    expanded = 0
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        entries = archive.infolist()
        if len(entries) > 10000:
            raise ValueError('Module archive entry bound exceeded')
        for entry in entries:
            name = entry.filename.rstrip('/') if entry.is_dir() else entry.filename
            rel = safe_relative(name)
            if rel.parts[:len(prefix.parts)] != prefix.parts or len(rel.parts) <= len(prefix.parts):
                raise ValueError('Unexpected Go module zip prefix')
            key = '/'.join(rel.parts[len(prefix.parts):])
            if key in seen:
                raise ValueError('Duplicate Go module zip path')
            seen.add(key)
            mode = entry.external_attr >> 16
            if stat.S_ISLNK(mode) or (stat.S_IFMT(mode) not in (0, stat.S_IFREG, stat.S_IFDIR)):
                raise ValueError('Unsupported Go module zip entry')
            if entry.flag_bits & 1:
                raise ValueError('Encrypted Go module zip entry')
            expanded += entry.file_size
            if expanded > MAX_MODULE_EXPANDED:
                raise ValueError('Expanded Go module zip bound exceeded')
            if entry.is_dir() or key not in selected:
                continue
            content = archive.read(entry)
            if len(content) != entry.file_size or hashlib.sha256(content).hexdigest() != selected[key]:
                raise ValueError('Selected Go module source hash mismatch')
            output = destination.joinpath(*PurePosixPath(key).parts)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(content)
    if not set(selected).issubset(seen):
        raise ValueError('Selected Go module source absent from zip')
    for item in row['metadata_files']:
        output = destination.joinpath(*PurePosixPath(item['path']).parts)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(item['utf8'].encode('utf-8'))
    verify_module_archive(destination, row)


def restore_module_archive(row, verify, download=None):
    path = local(row['local_path'])
    unresolved = ROOT / row['local_path']
    for part in (unresolved, *unresolved.parents):
        if part == ROOT:
            break
        if part.is_symlink():
            raise ValueError('Module path symlink; preserving it')
    if path.exists():
        verify_module_archive(path, row)
        return
    if verify:
        raise ValueError('Missing module archive: ' + row['local_path'])
    if not row['zip_url'].startswith('https://proxy.golang.org/'):
        raise ValueError('Unexpected Go module archive origin')
    if download is None:
        request = urllib.request.Request(row['zip_url'], headers={'User-Agent': 'livekit-rs-reference-restore/0.6'})
        with urllib.request.urlopen(request, timeout=90) as response:
            data = response.read(MAX_MODULE_ZIP + 1)
    else:
        data = download(row['zip_url'])
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.fetch-module-', dir=path.parent) as temp:
        stage = Path(temp) / 'source'
        stage.mkdir()
        extract_module_archive(data, row, stage)
        rename_noreplace(stage, path)
    verify_module_archive(path, row)


def test_module_restoration():
    """Offline synthetic tests: no references modified, reset or downloaded."""
    import copy
    import stat
    import unittest
    import zipfile
    def zipped(names):
        data = io.BytesIO()
        with zipfile.ZipFile(data, 'w') as archive:
            for name, content, mode in names:
                entry = zipfile.ZipInfo(name)
                entry.external_attr = mode << 16
                archive.writestr(entry, content)
        return data.getvalue()
    class RestorationTests(unittest.TestCase):
        def setUp(self):
            self.temp = tempfile.TemporaryDirectory(prefix='reference-restore-test-')
            self.addCleanup(self.temp.cleanup)
            self.base = Path(self.temp.name).resolve()
            self.data = zipped([('example.org/mod@v1.0.0/LICENSE', b'license\n', stat.S_IFREG | 0o644)])
            metadata = '{"audit":"original"}\n'
            self.row = {'zip_prefix': 'example.org/mod@v1.0.0', 'zip_sha256': hashlib.sha256(self.data).hexdigest(), 'selected_files': [{'path': 'LICENSE', 'sha256': hashlib.sha256(b'license\n').hexdigest()}], 'metadata_files': [{'path': 'SOURCE.json', 'utf8': metadata, 'sha256': hashlib.sha256(metadata.encode()).hexdigest()}]}
        def test_exact_selected_source_metadata_and_offline_verify(self):
            extract_module_archive(self.data, self.row, self.base)
            verify_module_archive(self.base, self.row)
            self.assertEqual((self.base/'SOURCE.json').read_bytes(), self.row['metadata_files'][0]['utf8'].encode())
        def test_changed_or_extra_source_is_rejected_not_reset(self):
            extract_module_archive(self.data, self.row, self.base)
            (self.base/'LICENSE').write_bytes(b'local edit')
            with self.assertRaises(ValueError): verify_module_archive(self.base, self.row)
            self.assertEqual((self.base/'LICENSE').read_bytes(), b'local edit')
            (self.base/'LICENSE').write_bytes(b'license\n'); (self.base/'extra').write_text('retain')
            with self.assertRaises(ValueError): verify_module_archive(self.base, self.row)
            self.assertTrue((self.base/'extra').exists())
        def test_existing_empty_target_is_never_replaced(self):
            source=self.base/'stage'; target=self.base/'existing'; source.mkdir();target.mkdir()
            (source/'LICENSE').write_text('new')
            with self.assertRaises(ValueError):rename_noreplace(source,target)
            self.assertEqual(list(target.iterdir()),[])
            self.assertTrue((source/'LICENSE').exists())
        def test_restore_is_staged_and_offline_verify_never_downloads(self):
            global ROOT
            old_root = ROOT
            try:
                ROOT = self.base
                (ROOT/'references').mkdir()
                row = copy.deepcopy(self.row)
                row.update(local_path='references/owned',zip_url='https://proxy.golang.org/example.org/mod/@v/v1.0.0.zip')
                bad = copy.deepcopy(row); bad['zip_sha256'] = '0' * 64
                with self.assertRaises(ValueError):restore_module_archive(bad,False,lambda _:self.data)
                self.assertFalse((ROOT/'references/owned').exists())
                restore_module_archive(row,False,lambda _:self.data)
                def forbidden(_):raise AssertionError('offline verify attempted download')
                restore_module_archive(row,True,forbidden)
                (ROOT/'references/owned/LICENSE').write_bytes(b'preserve local')
                with self.assertRaises(ValueError):restore_module_archive(row,False,forbidden)
                self.assertEqual((ROOT/'references/owned/LICENSE').read_bytes(),b'preserve local')
            finally:
                ROOT = old_root
        def test_expanded_bound_is_enforced(self):
            global MAX_MODULE_EXPANDED
            old_limit=MAX_MODULE_EXPANDED
            try:
                MAX_MODULE_EXPANDED=1
                with self.assertRaises(ValueError):extract_module_archive(self.data,self.row,self.base)
            finally:
                MAX_MODULE_EXPANDED=old_limit
        def test_checksum_failure(self):
            with self.assertRaises(ValueError):extract_module_archive(self.data+b'x',self.row,self.base)
        def test_unsafe_unknown_entries_and_duplicates_are_rejected(self):
            for bad in ('../outside','/absolute','example.org/mod@v1.0.0/../outside','other@v1.0.0/LICENSE','example.org/mod@v1.0.0/back\\slash','example.org/mod@v1.0.0/LICENSE'):
                data=zipped([('example.org/mod@v1.0.0/LICENSE',b'license\n',stat.S_IFREG|0o644),(bad,b'bad',stat.S_IFREG|0o644)])
                row=copy.deepcopy(self.row);row['zip_sha256']=hashlib.sha256(data).hexdigest()
                with tempfile.TemporaryDirectory(dir=self.base) as temp:
                    with self.assertRaises(ValueError):extract_module_archive(data,row,Path(temp))
        def test_symlink_and_missing_selected_entry_are_rejected(self):
            for name,mode in [('example.org/mod@v1.0.0/link',stat.S_IFLNK|0o777),('example.org/mod@v1.0.0/other',stat.S_IFREG|0o644)]:
                data=zipped([(name,b'target',mode)]);row=copy.deepcopy(self.row);row['zip_sha256']=hashlib.sha256(data).hexdigest()
                with self.assertRaises(ValueError):extract_module_archive(data,row,self.base)
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(RestorationTests))
    if not result.wasSuccessful():
        raise SystemExit(1)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', action='store_true', help='Offline check of identities, cleanliness, manifests and selected source hashes')
    parser.add_argument('--module-archives-only', action='store_true', help='Restore/verify only locked F06 Go source archives')
    parser.add_argument('--self-test', action='store_true', help='Offline temporary-directory module archive safety tests only')
    args = parser.parse_args()
    if args.self_test:
        test_module_restoration()
        return
    archives = json.loads(MODULE_ARCHIVE_LOCK.read_text())['archives']
    for row in archives:
        restore_module_archive(row, args.verify)
    if args.module_archives_only:
        print('Verified {} exact Go module source archives; no compilers/runtime tests executed.'.format(len(archives)))
        return
    for row in LOCK['repositories']:
        restore_repo(row, args.verify)
    packages = [r for r in CATALOG['crates'] if r.get('local_package')]
    packages += CATALOG.get('compatibility_snapshots', [])
    for row in packages:
        restore_package(row, args.verify)
    print('Verified {} Git checkouts and {} package manifests/selected sources; runtime tests not executed.'.format(len(LOCK['repositories']), len(packages)))
    print('Verified {} exact Go module source archives and audit metadata.'.format(len(archives)))


if __name__ == '__main__':
    main()
