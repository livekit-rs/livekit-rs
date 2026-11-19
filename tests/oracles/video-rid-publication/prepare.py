#!/usr/bin/env python3
"""C65 RID publication observer: offline immutable-source staging."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile

HERE = Path(__file__).resolve().parent
MODULE_CACHE = Path('/Users/zoujiaqing/go/pkg/mod')
FIXED_HEADS = {
    "references/livekit-stable": "e5655ac20267fa5005dccaf3f0e3bfc3ea688c07",
    "references/protocol-stable": "9bb708cdb81918a6c63dd25e915b4644310b45b6",
    "references/psrpc": "e9701ee1b5a981cfa3c7c69503d4775ddff13729",
}
MEDIA_PATH = "references/go-modules/mediatransportutil-f234b534b095"
MEDIA_VERSION = "v0.0.0-20260821083140-f234b534b095"
MEDIA_MANIFEST_SHA256 = "05efcdb3f2437b1cc31ca9a5e5622b454eaad4a916a4418dcdc11aca5c84fb6f"
REPLACEMENTS = (
    ("github.com/livekit/livekit-server", "references/livekit-stable"),
    ("github.com/livekit/protocol", "references/protocol-stable"),
    ("github.com/livekit/psrpc", "references/psrpc"),
    ("github.com/livekit/mediatransportutil", MEDIA_PATH),
)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()


def checked_parent(path):
    require(path.parent.is_dir(), 'missing parent: ' + str(path.parent))
    subprocess.run(['/bin/ls', '-d', str(path.parent)], check=True, stdout=subprocess.DEVNULL)


def mkdir(path):
    if not path.parent.is_dir():
        mkdir(path.parent)
    checked_parent(path)
    path.mkdir(exist_ok=True)


def prepare(root, out):
    root = root.resolve(strict=True)
    require(not out.is_symlink() and not out.exists(), "output must be fresh")
    out = out.resolve()
    require(out.parent.is_dir(), "output parent must exist")
    require(out.is_relative_to(Path('/tmp').resolve()) and
            any(p.startswith('video-rid-publication-go-') for p in out.parts), "output must be task-owned /tmp/video-rid-publication-go-* staging")
    for relative, expected in FIXED_HEADS.items():
        ref = root / relative
        require(Path(git(ref, "rev-parse", "--show-toplevel")).resolve() == ref.resolve(), "not an independent reference: " + relative)
        require(git(ref, "rev-parse", "HEAD") == expected, "reference HEAD drift: " + relative)
        require(not git(ref, "status", "--porcelain", "--untracked-files=all"), "dirty reference: " + relative)
    manifest = HERE / "PROVENANCE.json"
    provenance = json.loads(manifest.read_text())
    hashes = provenance["source_sha256"].copy()
    for relative, expected in provenance['module_source_sha256'].items():
        require(sha256(MODULE_CACHE / relative) == expected, 'module source drift: ' + relative)
    for relative, expected in hashes.items():
        require(sha256(root / relative) == expected, "source drift: " + relative)
    media = root / MEDIA_PATH
    require(sha256(media / "SOURCE-ARCHIVE.json") == MEDIA_MANIFEST_SHA256, "media manifest drift")
    archive = json.loads((media / "SOURCE-ARCHIVE.json").read_text())
    require(archive["source"] == "github.com/livekit/mediatransportutil" and archive["version"] == MEDIA_VERSION, "media identity drift")
    require(not any(p.is_symlink() for p in media.rglob('*')), "media symlink")
    require({str(p.relative_to(media)) for p in media.rglob('*') if p.is_file()} == set(archive["paths"]) | {"SOURCE-ARCHIVE.json"}, "media inventory drift")
    for relative, expected in archive["paths"].items():
        require(sha256(media / relative) == expected, "media source drift: " + relative)
        hashes[MEDIA_PATH + '/' + relative] = expected
    hashes[MEDIA_PATH + '/SOURCE-ARCHIVE.json'] = MEDIA_MANIFEST_SHA256

    mkdir(out)
    sources = out / "sources"
    archives = {}
    for relative, head in FIXED_HEADS.items():
        # APFS clone clean, pinned checkouts, including private .git metadata.
        # Verify every tracked source byte against git archive, not just selected files.
        data = subprocess.check_output(["git", "-C", str(root / relative), "archive", head])
        archives[relative] = hashlib.sha256(data).hexdigest()
        dest = sources / relative
        mkdir(dest.parent)
        checked_parent(dest)
        subprocess.run(['/bin/cp', '-cR', str(root / relative), str(dest)], check=True)
        with tarfile.open(fileobj=io.BytesIO(data)) as tar:
            for member in tar.getmembers():
                require(not member.name.startswith('/') and '..' not in Path(member.name).parts, "unsafe archive path")
                require(member.isfile() or member.isdir(), "non-regular git archive member: " + member.name)
                if member.isfile():
                    original = tar.extractfile(member).read()
                    require((dest / member.name).read_bytes() == original, 'cloned source drift: ' + member.name)
                    hashes[relative + '/' + member.name] = hashlib.sha256(original).hexdigest()
    media_dest = sources / MEDIA_PATH
    mkdir(media_dest.parent)
    checked_parent(media_dest)
    subprocess.run(['/bin/cp', '-cR', str(media), str(media_dest)], check=True)
    for relative, expected in hashes.items():
        require(sha256(sources / relative) == expected, "staged source drift: " + relative)

    fixed = sources / "references/livekit-stable"
    mod = (fixed / "go.mod").read_text()
    declaration = "module github.com/livekit/livekit-server\n"
    require(mod.startswith(declaration), "unexpected module declaration")
    mod = "module video-rid-publication-observer\n" + mod[len(declaration):]
    for name, relative in REPLACEMENTS:
        mod += '\nreplace ' + name + ' => ' + json.dumps(str(sources / relative)) + '\n'
    mod += '\nrequire github.com/livekit/livekit-server v0.0.0\n'
    (out / "go.mod").write_text(mod)
    shutil.copyfile(fixed / "go.sum", out / "go.sum")
    for name in ("main_test.go", "cases.json", "expected.json"):
        shutil.copyfile(HERE / name, out / name)
    provenance = json.loads(manifest.read_text())
    provenance.update({
        "reference_heads": FIXED_HEADS,
        "fixed_source_root": str(root),
        "git_archive_sha256": archives,
        "source_sha256": hashes,
        "media_version": MEDIA_VERSION,
        "asset_sha256": {p.name: sha256(p) for p in sorted(HERE.iterdir()) if p.is_file()},
        "staged_sha256": {n: sha256(out / n) for n in ("main_test.go", "cases.json", "expected.json", "go.mod", "go.sum")},
    })
    # Preserve scripts/expectations for every attempt, even if fixtures change later.
    mkdir(out / 'assets')
    for path in HERE.iterdir():
        if path.is_file():
            shutil.copyfile(path, out / 'assets' / path.name)
    (out / "provenance.json").write_text(json.dumps(provenance, indent=2, sort_keys=True) + '\n')
    return out


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixed-source-root', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    print(prepare(args.fixed_source_root, args.out))
