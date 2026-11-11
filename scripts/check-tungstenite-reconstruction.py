#!/usr/bin/env python3
"""Exact published tungstenite + narrow patch reconstruction; no compiler."""
import argparse
import hashlib
import importlib.util
import io
import json
from pathlib import Path, PurePosixPath
import tarfile
import tempfile
import urllib.request

SERVER=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location("locked_patch_parser", SERVER/"scripts/check-dimpl-reconstruction.py")
parser=importlib.util.module_from_spec(spec)
spec.loader.exec_module(parser)
sha=lambda b:hashlib.sha256(b).hexdigest()

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--restore-reference",action="store_true",help="download ONLY missing pinned package; preserve all existing inputs")
    args=ap.parse_args()
    vendor=SERVER/"vendor/tungstenite"
    source=json.loads((vendor/"SOURCE.json").read_text())
    ref=SERVER/source["reference"]
    if not ref.exists() and args.restore_reference:
        url="https://static.crates.io/crates/tungstenite/tungstenite-0.29.0.crate"
        with urllib.request.urlopen(url,timeout=60) as response:
            raw=response.read(2*1024*1024+1)
        assert len(raw)<=2*1024*1024 and sha(raw)==source["registry_checksum"], "archive identity/size mismatch"
        ref.parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".tungstenite-restore-",dir=ref.parent) as temp:
            stage=Path(temp)/"package";stage.mkdir();seen=set();total=0
            with tarfile.open(fileobj=io.BytesIO(raw),mode="r:gz") as archive:
                for member in archive:
                    p=PurePosixPath(member.name)
                    assert p.parts[0]=="tungstenite-0.29.0" and not p.is_absolute() and ".." not in p.parts
                    if member.isdir():continue
                    assert member.isfile() and len(p.parts)>1,"unsupported entry"
                    name=PurePosixPath(*p.parts[1:]).as_posix()
                    assert name not in seen and name in source["upstream_files_sha256"],"unexpected/duplicate entry"
                    total+=member.size;assert total<=8*1024*1024,"expanded package bound"
                    data=archive.extractfile(member).read();assert len(data)==member.size and sha(data)==source["upstream_files_sha256"][name]
                    dest=stage/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data);seen.add(name)
            assert seen==set(source["upstream_files_sha256"])
            assert not ref.exists(),"refusing overwrite"
            stage.rename(ref)
    assert ref.is_dir(),"missing pinned package; explicit --restore-reference may restore it"
    assert not any(p.is_symlink()for p in ref.rglob("*")),"reference symlink"
    tree={p.relative_to(ref).as_posix():p.read_bytes()for p in ref.rglob("*")if p.is_file()}
    assert {n:sha(b)for n,b in tree.items()}==source["upstream_files_sha256"],"original package drift"
    parser.parser_self_check()
    touched=set()
    for name in source["reconstruction"]["sequential_patches"]:
        data=(vendor/name).read_bytes();assert sha(data)==source["patch_sha256"][name]
        touched.update(parser.apply_patch(tree,data))
    assert touched==set(source["modified_native_files"])
    assert {n:sha(b)for n,b in tree.items()}==source["exact_files_sha256"]
    for name,data in tree.items():assert (vendor/name).read_bytes()==data,"vendor drift: "+name
    expected=set(tree)|set(source["metadata_files"])|set(source["patch_sha256"])
    assert not any(p.is_symlink()for p in vendor.rglob("*")),"vendor symlink"
    assert {p.relative_to(vendor).as_posix()for p in vendor.rglob("*")if p.is_file()}==expected
    for name,value in source["license_sha256"].items():assert sha(tree[name])==value
    print(json.dumps({"status":"PASS","package":source["package"],"version":source["version"],"original_files":len(tree),"patched_files":len(touched),"registry_checksum":source["registry_checksum"],"source_sha256":sha((vendor/"SOURCE.json").read_bytes())},indent=2))
if __name__=="__main__":main()
