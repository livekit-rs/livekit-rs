#!/usr/bin/env python3
"""Stage a fixed-Go source observer without building or accessing the network."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys


HERE = Path(__file__).resolve().parent
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
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def git(path, *args):
    return subprocess.run(
        ["git", "-C", str(path), *args], check=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ).stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixed-source-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = args.fixed_source_root.resolve(strict=True)
    require(not args.out.is_symlink(), "output must not be a symlink")
    out = args.out.resolve()
    require(not out.exists(), "output directory must be fresh: " + str(out))
    require(out.parent.is_dir(), "output parent must already exist")

    heads = {}
    for relative, expected in FIXED_HEADS.items():
        reference = root / relative
        require(Path(git(reference, "rev-parse", "--show-toplevel")).resolve()
                == reference.resolve(), "not an independent git reference: " + relative)
        actual = git(reference, "rev-parse", "HEAD")
        require(actual == expected, "reference HEAD drift: " + relative)
        require(not git(reference, "status", "--porcelain", "--untracked-files=normal"),
                "dirty reference (including untracked files): " + relative)
        heads[relative] = actual

    # Reuse only C47's fixed-source checks; never modify its assets or manifest.
    c47_manifest = HERE.parent / "video-quality-source" / "PROVENANCE.json"
    expected_sources = json.loads(c47_manifest.read_text(encoding="utf-8"))["source_sha256"]
    local_manifest = HERE / "PROVENANCE.json"
    expected_sources.update(json.loads(local_manifest.read_text(encoding="utf-8"))["source_sha256"])
    source_hashes = {}
    for relative, expected in expected_sources.items():
        actual = sha256(root / relative)
        require(actual == expected, "fixed source drift: " + relative)
        source_hashes[relative] = actual

    # This dependency is an archive, not a git checkout: pin and verify its inventory.
    media = root / MEDIA_PATH
    archive_path = media / "SOURCE-ARCHIVE.json"
    require(sha256(archive_path) == MEDIA_MANIFEST_SHA256, "media archive manifest drift")
    archive = json.loads(archive_path.read_text(encoding="utf-8"))
    require(archive["source"] == "github.com/livekit/mediatransportutil"
            and archive["version"] == MEDIA_VERSION, "media archive identity drift")
    inventory = {str(path.relative_to(media)) for path in media.rglob("*") if path.is_file()}
    require(inventory == set(archive["paths"]) | {"SOURCE-ARCHIVE.json"},
            "media archive file inventory drift")
    for relative, expected in archive["paths"].items():
        actual = sha256(media / relative)
        require(actual == expected, "media archive source drift: " + relative)
        source_hashes[MEDIA_PATH + "/" + relative] = actual
    source_hashes[MEDIA_PATH + "/SOURCE-ARCHIVE.json"] = MEDIA_MANIFEST_SHA256
    for relative in FIXED_HEADS:
        for name in ("go.mod", "go.sum"):
            source_hashes[relative + "/" + name] = sha256(root / relative / name)

    fixed = root / "references/livekit-stable"
    mod = (fixed / "go.mod").read_text(encoding="utf-8")
    module_line = "module github.com/livekit/livekit-server\n"
    require(mod.startswith(module_line), "unexpected fixed Go module declaration")
    mod = "module dynacast-reference-sr-observer\n" + mod[len(module_line):]
    for name, relative in REPLACEMENTS:
        mod += "\nreplace " + name + " => " + json.dumps(str((root / relative).resolve())) + "\n"
    mod += "\nrequire github.com/livekit/livekit-server v0.0.0\n"

    observer_hash = sha256(HERE / "main.go")
    out.mkdir()
    shutil.copyfile(HERE / "main.go", out / "main.go")
    shutil.copyfile(HERE / "main_test.go", out / "main_test.go")
    (out / "go.mod").write_text(mod, encoding="utf-8")
    shutil.copyfile(fixed / "go.sum", out / "go.sum")
    staged_hashes = {name: sha256(out / name) for name in ("main.go", "main_test.go", "go.mod", "go.sum")}
    require(staged_hashes["main.go"] == observer_hash, "observer changed while staging")
    provenance = {
        "kind": "fixed-go-source-contract-observer",
        "scope": "public Forwarder API with testutils RTP packets and ForceLayerForTest current/target; not allocator, MAIN, decoder, SDP or SRTP evidence",
        "fixed_source_root": str(root),
        "reference_heads": heads,
        "archive_reference": {
            "path": MEDIA_PATH,
            "source": archive["source"],
            "version": MEDIA_VERSION,
            "manifest_sha256": MEDIA_MANIFEST_SHA256,
        },
        "source_sha256": source_hashes,
        "source_checks_manifest_sha256": sha256(c47_manifest),
        "forwarder_checks_manifest_sha256": sha256(local_manifest),
        "main_go_sha256": observer_hash,
        "prepare_py_sha256": sha256(Path(__file__).resolve()),
        "staged_sha256": staged_hashes,
        "rows": 90,
        "observations_per_row": 20,
        "schema": 1,
        "case_order": "codec_mode [h264-simulcast,vp9-svc], reference [0,1,2], max_spatial [-1,0,2], report [none,wrong-layer,matching-zero,matching-valid,premature-zero]",
        "wire_schema_sha256": sha256(HERE / "schema.json"),
    }
    (out / "provenance.json").write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    print(out)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print("prepare: " + str(error), file=sys.stderr)
        sys.exit(1)
