#!/usr/bin/env python3
"""Stage the original locked server tree plus one RED-encode observer; no build and no run."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--root', required=True, type=Path)
parser.add_argument('--out', required=True, type=Path)
args = parser.parse_args()

root = args.root.resolve()
out = args.out.resolve()
here = Path(__file__).resolve().parent
ref = root / 'references' / 'livekit-stable'
manifest = json.loads((here / 'SOURCE.json').read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


for name, digest in manifest['original_inputs'].items():
    actual = sha(ref / name)
    assert actual == digest, 'original input drift: %s %s != %s' % (name, actual, digest)

assert not out.exists(), 'fresh isolated stage required'
out.mkdir(parents=True)
shutil.copytree(ref, out / 'source', ignore=shutil.ignore_patterns('.git'))
shutil.copyfile(here / 'observer_test.go',
                out / 'source' / 'pkg' / 'sfu' / 'source_red_encode_observer_test.go')

mod = out / 'source' / 'go.mod'
mod.write_text(mod.read_text()
               + '\nreplace github.com/livekit/protocol => '
               + json.dumps(str(root / 'references' / 'protocol-stable')) + '\n'
               + 'replace github.com/livekit/mediatransportutil => '
               + json.dumps(str(root / 'references' / 'go-modules'
                                / 'mediatransportutil-f234b534b095')) + '\n')

(out / 'STAGED.json').write_text(json.dumps({
    'status': 'STAGED_NOT_RUN',
    'source': manifest,
    'observer_sha256': sha(here / 'observer_test.go'),
    'vector_origin': 'frozen pkg/sfu/redreceiver_test.go TestRedReceiver vectors plus one '
                     'maxRedPayload bypass vector; generatePkts and dummyDowntrack are the '
                     'locked helpers, not reimplemented',
    'algorithm_substitutions': [],
    'go_mod_staged_sha256': sha(mod),
}, indent=2) + '\n')
print(out)
