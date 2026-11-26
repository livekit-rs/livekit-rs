#!/usr/bin/env python3
"""SPEC-0077 launched-behaviour evidence redactor.

A launched daemon prints the machine's real interface addresses: the observer's ICE gather
picks up the host's LAN and tunnel addresses, and an `rtc.node_ip` leg echoes whatever the
OS offers into an SDP that lands in a WARN line. Those bytes are genuine routing state for
one user's laptop and must not reach a public repository, while the loopback and
documentation addresses the fixtures use ARE the evidence. This tool rewrites only the
addresses that can identify a real machine and reports every change, so a filing step can
prove what it altered.

Usage:
  redact-evidence.py selftest              # assert the classifier, no files touched
  redact-evidence.py check FILE...         # exit 1 if a forbidden address is present
  redact-evidence.py write DIR_OR_FILE...  # rewrite in place, print a change table

Kept (fixture vocabulary, defined by the specs and the harness configs):
  127.0.0.0/8 (and any ::ffff:127/8 spelling) and ::1   loopback, the whole lane runs on it
  0.0.0.0, 255.255.255.255                              wildcard / broadcast sentinels
  192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24         RFC 5737 documentation prefixes
  2001:db8::/32                                         RFC 8200 documentation prefix
Redacted (a real machine can own these):
  every other literal address, including 198.18.0.0/15 (RFC 2544 benchmarking, which this
  host really carries on a tunnel) and fe80::/10 (link-local, which encodes an interface).

Two deliberate non-rules, both learned from this repo's logs:
  * No dotted-decimal-integer rule. A 10-digit field is a byte counter or a nanosecond
    stamp far more often than it is a packed address, and masking one silently destroys
    the compared value.
  * A token is only ever classified by `ipaddress`, never by shape: `e:1` (the tail of
    `a=candidate:1`), `1F:FF:D9:DD` (a DTLS fingerprint) and `1.13.10` (a version) all look
    address-shaped to a regex and must stay verbatim.
"""
from __future__ import annotations

import ipaddress
import re
import sys
import pathlib

# Exactly four decimal groups, not embedded in a longer dotted or word run.
IPV4_RE = re.compile(r'(?<![\w.])(\d{1,3}(?:\.\d{1,3}){3})(?![\w.])')
# A maximal hex-and-colon run (so a 47-character DTLS fingerprint cannot yield a shorter
# "valid" IPv6 prefix), with an optional %zone. The run is only ever an address candidate:
# `ipaddress` decides, and a token it rejects stays verbatim.
IPV6_RE = re.compile(r'(?<![\w:.%])([0-9A-Fa-f:]{3,45}(?:%[0-9A-Za-z._-]+)?)(?![\w:.%])')

MARK = '<redacted-ip>'


def _parse(token: str):
    try:
        return ipaddress.ip_address(token)
    except ValueError:
        return None


def classify(token: str) -> str:
    """'keep' for fixture vocabulary, 'mask' for a machine address, 'none' if not one."""
    ip = _parse(token.split('%')[0].strip('[]'))
    if ip is None:
        return 'none'
    mapped = getattr(ip, 'ipv4_mapped', None)
    if mapped is not None:
        ip = mapped
    if ip.version == 4:
        if ip.is_loopback or ip.is_unspecified or str(ip) == '255.255.255.255':
            return 'keep'
        return 'keep' if any(ip in net for net in (
            ipaddress.ip_network('192.0.2.0/24'),
            ipaddress.ip_network('198.51.100.0/24'),
            ipaddress.ip_network('203.0.113.0/24'))) else 'mask'
    if ip.is_loopback or ip.is_unspecified:
        return 'keep'
    return 'keep' if ip in ipaddress.ip_network('2001:db8::/32') else 'mask'


def keeps(token: str) -> bool:
    return classify(token) == 'keep'


def _sub(regex):
    def repl(match):
        token = match.group(1)
        return MARK if classify(token) == 'mask' else match.group(0)
    return repl


def redact_text(text: str) -> str:
    return IPV6_RE.sub(_sub(IPV6_RE), IPV4_RE.sub(_sub(IPV4_RE), text))


def find_forbidden(text: str) -> list[str]:
    hits = []
    for regex in (IPV4_RE, IPV6_RE):
        for match in regex.finditer(text):
            if classify(match.group(1)) == 'mask':
                hits.append(match.group(1))
    return sorted(set(hits))


# Each case pins one direction of the classifier: a tool that masks everything fails the
# keep list, one that masks nothing fails the mask list, and a shape-matching regex fails
# the untouched list. All three must hold for the redaction to be usable as evidence.
SELFTEST_KEEP = ['127.0.0.1', '127.0.0.2', '0.0.0.0', '255.255.255.255', '203.0.113.7',
                 '192.0.2.1', '198.51.100.9', '::1', '2001:db8::7', '2001:db8::ffff',
                 '::ffff:127.0.0.1', '::ffff:127.0.0.2']
SELFTEST_MASK = ['172.16.16.245', '198.18.0.1', '10.0.0.5', '192.168.1.8', 'fe80::1%en0',
                 '2606:4700:4700::1111', '169.254.3.4', 'fd12:3456::9', '::ffff:198.18.0.1']
SELFTEST_UNTOUCHED = [
    'a=candidate:2330131441 1 udp 2130706431 127.0.0.1 50081 typ host ufrag AbCd',
    'duration: 2.455125ms', '"protocol": 17', '13:33:52.449+0800',
    'a=fingerprint:sha-256 1F:FF:D9:DD:69:6F:D9:8A:B2:A6:2F:F0:04:22:4B:1A',
    'version: 1.13.10, protocol: 17', 'nodeId: ND_ifXawGFbCHHc', '0123456789',
    'bytesSent: 1234567890', 'ICE:TLS', 'port_range_start: 50000',
    '"address":"127.0.0.1"', '2026-11-26T13:33:52.449+0800',
]


def selftest() -> int:
    failures = []
    for token in SELFTEST_KEEP:
        if not keeps(token):
            failures.append(f'keep-list miss: {token} classified as a machine address')
    for token in SELFTEST_MASK:
        if keeps(token) or redact_text(f'x {token} y') == f'x {token} y':
            failures.append(f'redaction miss: {token} left verbatim')
    for line in SELFTEST_UNTOUCHED:
        after = redact_text(line)
        if after != line:
            failures.append(f'collateral damage: {line!r} -> {after!r}')
    sample = ('udp4 host 172.16.16.245:49169 (resolved: 172.16.16.245:49169), '
              'remote udp host 198.18.0.1:65535, local 127.0.0.1:57285, '
              'explicit node IP 203.0.113.7 ::ffff:127.0.0.2 and 2001:db8::7, '
              'link fe80::2%en0, tcp 10.0.0.5:7881, bytesSent: 1234567890')
    expect = ('udp4 host ' + MARK + ':49169 (resolved: ' + MARK + ':49169), '
              'remote udp host ' + MARK + ':65535, local 127.0.0.1:57285, '
              'explicit node IP 203.0.113.7 ::ffff:127.0.0.2 and 2001:db8::7, '
              'link ' + MARK + ', tcp ' + MARK + ':7881, bytesSent: 1234567890')
    got = redact_text(sample)
    if got != expect:
        failures.append(f'composite mismatch:\n  got    {got}\n  expect {expect}')
    if failures:
        for f in failures:
            print('FAIL', f)
        return 1
    print(f'selftest ok: {len(SELFTEST_KEEP)} kept, {len(SELFTEST_MASK)} masked, '
          f'{len(SELFTEST_UNTOUCHED)} untouched lines, composite rewrite exact')
    return 0


def paths(argv: list[str]) -> list[pathlib.Path]:
    out = []
    for item in argv:
        p = pathlib.Path(item)
        if p.is_dir():
            out.extend(sorted(q for q in p.rglob('*') if q.is_file()))
        elif p.is_file():
            out.append(p)
        else:
            raise SystemExit(f'no such path: {p}')
    return out


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] == 'selftest':
        return selftest()
    mode = sys.argv[1]
    if mode not in ('check', 'write'):
        raise SystemExit('usage: redact-evidence.py selftest|check|write PATH...')
    changed = 0
    total = 0
    for path in paths(sys.argv[2:]):
        raw = path.read_text(errors='replace')
        hits = find_forbidden(raw)
        if not hits:
            continue
        total += len(hits)
        if mode == 'check':
            print(f'{path}: {len(hits)} machine address(es): {", ".join(hits[:6])}')
        else:
            path.write_text(redact_text(raw))
            print(f'{path}: rewrote {len(hits)} distinct address(es)')
        changed += 1
    if mode == 'check':
        print(f'check: {changed} file(s) still carry a machine address'
              if changed else 'check: no machine address in scope')
        return 1 if changed else 0
    print(f'write: {total} occurrence(s) across {changed} file(s)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
