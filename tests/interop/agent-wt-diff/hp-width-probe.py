#!/usr/bin/env python3
"""Header-protection width probe (C15 t2b closing evidence).

`quic-relay.py` decodes Initial packets with the RFC 9001 §5.4.1 rule: header
protection covers only the low bits of the first byte plus exactly `pn_len`
packet-number bytes. quinn-proto 0.11.19 (`src/crypto/rustls.rs`, mirrored
verbatim by `HeaderKeyWrap` in `crates/rtc-tls/src/quinn.rs`) instead masks the
slice `rest[pn_offset - 1 .. min(pn_offset + 3, rest.len())]`, whose length is
`pn_len + 3` whenever the packet number is shorter than four bytes -- so three
bytes of ciphertext get XORed with the mask before the AEAD tag is checked.

This probe decodes the *same* packet under both rules and prints which one
opens, so the direction of the deviation is measured rather than argued:

    Go client (quic-go) -> only the RFC rule opens it
    Rust stack (quinn)  -> only the 4-byte rule opens it

The key schedule, header parser and AEAD come from `quic-relay.py` itself (the
relay is pinned against RFC 9001 Appendix A by `relay-kat.py`), so the two
rules differ in exactly one thing: how many bytes are un-protected.

Usage:
    hp-width-probe.py <dir> [--side c|s] [--seed <hex>] [--widths rfc,4]
        [--pattern 'NNN-c.bin'] [--max N]

`--widths rfc` means "mask exactly pn_len bytes" and a number means "mask that
many bytes". The seed DCID defaults to the DCID of the first packet in the
directory, which is the Initial key seed for *both* directions (RFC 9001 §5.2).
"""
import argparse
import glob
import importlib.util
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def load_relay():
    """Import `quic-relay.py` (hyphenated name) as a module."""
    spec = importlib.util.spec_from_file_location("quic_relay",
                                                  os.path.join(HERE, "quic-relay.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def probe(k, data, width):
    """Decode one Initial, masking `width` bytes (None = exactly pn_len, RFC rule).

    Returns `(info, plaintext)` on success or `(None, reason)`; `info` carries
    the packet-number length so the log shows which width was correct.
    """
    relay = RELAY
    if len(data) < 20 or not data[0] & 0x80:
        return None, {"why": "not a long header"}
    try:
        dcid_len = data[5]
        off = 6 + dcid_len
        scid_len = data[off]
        off += 1 + scid_len
        token_len, n = relay.varint(data, off)
        off += n + token_len
        length, plen_n = relay.varint(data, off)
    except (ValueError, IndexError):
        return None, {"why": "truncated header"}
    pn_off = off + plen_n
    sample_off = pn_off + 4
    if pn_off >= len(data) or sample_off + 16 > len(data):
        return None, {"why": "no HP sample"}
    mask = relay.hp_mask(k["hp"], data[sample_off:])

    first = data[0] ^ (mask[0] & 0x0F)
    pn_len = (first & 0x03) + 1
    if pn_off + pn_len > len(data):
        return None, {"why": "pn out of bounds"}
    w = pn_len if width is None else width
    if length < pn_len or pn_off + length > len(data):
        return None, {"why": f"length {length} outside datagram"}
    pn_field = bytearray(data[pn_off:pn_off + pn_len])
    for i in range(pn_len):                   # the packet number itself: always
        pn_field[i] ^= mask[1 + i]
    # The payload starts with the first `w - pn_len` ciphertext bytes, which the
    # quinn rule also XORs -- that is the whole difference between the two rules.
    # AES-128 produces a 5-byte mask, so at most 4 extra bytes are available.
    body = bytearray(data[pn_off + pn_len:pn_off + length])
    for i in range(min(max(0, w - pn_len), 5 - pn_len)):
        body[i] ^= mask[1 + pn_len + i]
    if len(body) < 16:
        return None, {"why": "no AEAD tag"}
    # AAD = the header with the *unprotected* first byte and the decoded PN.
    aad = bytes([first]) + data[1:pn_off] + bytes(pn_field)
    nonce = k["iv"][:4] + bytes(a ^ b for a, b in zip(
        k["iv"][4:], int.from_bytes(pn_field, "big").to_bytes(8, "big")))
    label = "rfc" if width is None else str(width)
    try:
        plain = relay.AESGCM(k["key"]).decrypt(nonce, bytes(body), aad)
    except Exception as e:  # noqa: BLE001 - the failure *is* the finding
        return None, {"why": f"AEAD failed ({type(e).__name__})", "pn_len": pn_len,
                      "width": label, "first": first}
    return {"pn": int.from_bytes(pn_field, "big"), "pn_len": pn_len, "first": first,
            "width": label, "extra_masked": max(0, w - pn_len)}, plain


RELAY = load_relay()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir", help="directory of NNN-c.bin / NNN-s.bin dumps")
    ap.add_argument("--side", default="c", choices=["c", "s"],
                    help="which direction's keys to use (c=client->server)")
    ap.add_argument("--seed", help="odcid (hex) seeding the Initial keys")
    ap.add_argument("--widths", default="rfc,4",
                    help="comma list: 'rfc' = mask pn_len only, N = mask N bytes")
    ap.add_argument("--max", type=int, default=6, help="max packets to probe")
    a = ap.parse_args()

    files = sorted(glob.glob(os.path.join(a.dir, f"*-{a.side}.bin")))
    if not files:
        print(f"no *-{a.side}.bin in {a.dir}", file=sys.stderr)
        return 1
    widths = [None if w == "rfc" else int(w) for w in a.widths.split(",")]

    first = open(files[0], "rb").read()
    seed = bytes.fromhex(a.seed) if a.seed else first[6:6 + first[5]]
    keys = RELAY.initial_keys(seed)
    side = keys["c"] if a.side == "c" else keys["s"]
    print(f"seed odcid={seed.hex()} side={a.side} packets={len(files)} "
          f"widths={['rfc' if w is None else w for w in widths]}")

    opens = {"rfc" if w is None else str(w): 0 for w in widths}
    for path in files[:a.max]:
        data = open(path, "rb").read()
        row = [os.path.basename(path)]
        for w in widths:
            key = "rfc" if w is None else str(w)
            info, out = probe(side, data, w)
            if info is None:
                row.append(f"{key}:FAIL({out['why']})")
            else:
                opens[key] += 1
                row.append(f"{key}:OPEN pn={info['pn']} pn_len={info['pn_len']} "
                           f"masked={info['extra_masked'] + 1 + info['pn_len']}B "
                           f"plain={out[:8].hex()}..({len(out)}B)")
        print("  " + "\n      ".join(row))
    print(f"  summary: " + "  ".join(f"{k} opens {v}/{min(len(files), a.max)}"
                                     for k, v in opens.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
