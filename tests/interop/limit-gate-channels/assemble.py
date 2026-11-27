#!/usr/bin/env python3
"""Rebuild run-paired.py from part1..part7 (parts are the source of truth)."""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

PARTS = ['part1.py', 'part2.py', 'part3.py', 'part4.py', 'part5.py', 'part6.py', 'part7.py']
HEAD = ('#!/usr/bin/env python3\n'
        '"""C106 limit-gate channels: the `limit.*` numbers judged on the refusals they cause.\n'
        '\n'
        'Assembled by ./assemble.py from part1..part7 - edit the parts, not this file.\n'
        'See README.md for the profile inventory, the grading rules and how to reproduce the receipt.\n'
        '"""\n'
        'from __future__ import annotations\n')


def strip_part_imports(text: str) -> str:
    """Drop `from partN import ...` statements (including parenthesised continuations)."""
    kept, depth = [], 0
    for line in text.splitlines():
        if depth:
            depth += line.count('(') - line.count(')')
            continue
        if re.match(r'^from part\d+ import', line):
            depth = line.count('(') - line.count(')')
            continue
        if line.strip() == 'from __future__ import annotations':
            continue
        kept.append(line.rstrip())
    return '\n'.join(kept)


def main() -> int:
    here = Path(__file__).resolve().parent
    blocks = [HEAD]
    for name in PARTS:
        path = here / name
        if not path.is_file():
            print(f'MISSING {name}', file=sys.stderr)
            return 2
        blocks.append(f'# ---- {name} ----\n' + strip_part_imports(path.read_text(encoding='utf-8')))
    src = '\n\n\n'.join(blocks) + '\n'
    ast.parse(src)
    (here / 'run-paired.py').write_text(src, encoding='utf-8')
    (here / 'run-paired.py').chmod(0o755)
    print(f'assembled run-paired.py: {len(src.encode())} B, {len(src.splitlines())} lines')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
