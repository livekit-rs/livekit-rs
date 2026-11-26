#!/usr/bin/env python3
"""Regenerate the C99 lane-C raw per-leg exit table from an execution receipt.

Usage: python3 raw-exit-table.py <run-dir>/execution.json [out]. The harness records
server_exit/probe_exit per leg; this turns them into the committed table so the
table cannot drift from the receipt it claims to summarise.

Verbatim fields only (server_exit, probe_exit, second_sigterm_sent, sigkill_required,
server_ports_released) — no summarisation, so the table can be re-derived from the
receipt by any reader.
"""
from __future__ import annotations

import json
import pathlib
import sys

HDR = ('profile', 'side', 'daemon', 'probe', '2nd_sigterm', 'sigkill', 'ports_released')
FMT = '{:<26} {:<6} {:>7} {:>6} {:>12} {:>8} {:>15}'


def main() -> int:
    src = pathlib.Path(sys.argv[1])
    dst = pathlib.Path(sys.argv[2]) if len(sys.argv) > 2 else src.parent / 'raw-exits.txt'
    exe = json.loads(src.read_text())
    rows = []
    for name in exe['profiles']:
        for side in ('go', 'rust'):
            rec = exe['profiles'][name][side]
            rows.append((name, side, rec.get('server_exit'), rec.get('probe_exit'),
                         str(rec.get('second_sigterm_sent')), str(rec.get('sigkill_required')),
                         str(rec.get('server_ports_released'))))
    lines = [
        f'# raw per-leg exits for SPEC-0077 C99 lane C (from {src})',
        '# daemon_exit = livekit-server/livecloud exit after the SIGTERM ladder;',
        '# probe_exit = node probe.mjs exit. Verbatim fields, no summarisation.',
        '',
        FMT.format(*HDR),
    ]
    for r in rows:
        lines.append(FMT.format(r[0], r[1], r[2], r[3], r[4], r[5], r[6]))
    dst.write_text('\n'.join(lines) + '\n')
    print(f'{dst}: {len(rows)} legs, exits '
          f'{sorted({(r[2], r[3]) for r in rows})}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
