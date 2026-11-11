#!/usr/bin/env python3
"""Draft real-process official/Rust ports comparison. NOT_RUN by author.

Usage: compare-ports.py --official /absolute/pinned-go --rust /absolute/livecloud
Output intentionally contains only the synthetic corpus and public port outputs.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--official', required=True)
    parser.add_argument('--rust', required=True)
    parser.add_argument('--output', required=True)
    options = parser.parse_args()
    for binary in (options.official, options.rust):
        if not Path(binary).is_absolute() or not Path(binary).is_file():
            parser.error('both executable paths must be absolute existing files')
    cases = json.loads(Path(__file__).with_name('ports-cases.json').read_text())['cases']
    rows = []
    # No inherited LIVEKIT_*, key material, Redis credentials, or user config.
    env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'NODE_IP': '127.0.0.1'}
    for fixture in cases:
        pair = {}
        for side, binary in [('official', options.official), ('rust', options.rust)]:
            try:
                result = subprocess.run([binary, *fixture['args']], env=env,
                                        capture_output=True, timeout=15, check=False)
                pair[side] = {'exit': result.returncode,
                              'stdout': result.stdout.decode('utf-8', 'strict'),
                              'stderr': result.stderr.decode('utf-8', 'strict')}
            except subprocess.TimeoutExpired:
                pair[side] = {'timeout': True}
        # Preserve raw stderr; timestamped logger output is visible, not silently
        # erased. Any future normalization requires a reviewed narrower contract.
        rows.append({'name': fixture['name'], 'args': fixture['args'], **pair,
                     'equal': pair['official'] == pair['rust']})
    document = {'status': 'PASS' if all(r['equal'] for r in rows) else 'FAIL',
                'scope': 'real process ports corpus, exact streams, no normalization',
                'cases': rows}
    Path(options.output).write_text(json.dumps(document, indent=2) + '\n')
    return 0 if document['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
