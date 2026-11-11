#!/usr/bin/env python3
"""Known-answer test for quic-relay.py's Initial decoder against RFC 9001 A.2/A.3.

The relay may only be used to make claims about a server's packets after this
passes: it unprotects the two packets the RFC publishes byte-for-byte, using the
same key schedule and the same `pn_offset + 4` sample rule as `lk-quic-crypto`
(which pins the identical vectors in its own test suite).
"""
import importlib.util
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("relay", os.path.join(HERE, "quic-relay.py"))
relay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(relay)

# Vectors are read from the crate that ships the Rust-side implementation, so the
# Python tool and the production code are checked against the same published bytes.
VECTORS = os.path.normpath(os.path.join(
    HERE, "..", "..", "..", "crates", "lk-quic-crypto", "src", "vectors.rs"))
const_src = open(VECTORS).read()


def vector(name):
    m = re.search(rf'pub const {name}: &str = "([0-9a-f]+)";', const_src)
    if not m:
        raise SystemExit(f"vector {name} not found in {VECTORS}")
    return bytes.fromhex(m.group(1))


A2_CLIENT = vector("A2_CLIENT_INITIAL_PROTECTED")
A2_PLAIN = vector("A2_CLIENT_INITIAL_PAYLOAD")
A3_SERVER = vector("A3_SERVER_INITIAL_PROTECTED")
A3_PLAIN = vector("A3_SERVER_INITIAL_PAYLOAD")

# RFC 9001 Appendix A.1/A.2/A.3: both sample packets are keyed from this DCID.
DCID = bytes.fromhex("8394c8f03e515708")

keys = relay.initial_keys(DCID)
ok = True

# RFC 9001 Appendix A.1 Initial key schedule for DCID 0x8394c8f03e515708 -- the
# same values `lk-quic-crypto`'s `rfc9001_a1_initial_keys` test pins. Matching them
# proves the HKDF-Expand-Label encoding and both directional secrets before any
# whole-packet claim is made.
A1 = {
    "c": {"key": "1f369613dd76d5467730efcbe3b1a22d",
          "iv": "fa044b2f42a3fd3b46fb255c",
          "hp": "9f50449e04a0e810283a1e9933adedd2"},
    "s": {"key": "cf3a5331653c364c88f0f379b6067e37",
          "iv": "0ac1493ca1905853b0bba03e",
          "hp": "c206b8d9b9f0f37644430b490eeaa314"},
}
for side, want in A1.items():
    for field, hexval in want.items():
        got = keys[side][field].hex()
        good = got == hexval
        ok = ok and good
        print(f"{'PASS' if good else 'FAIL'} A.1 {side} {field}: {got} (want {hexval})")

# (name, key set, packet, expected header fields, expected decrypted payload).
# The payload comparison is the strongest check available: it fails unless the
# varint offsets, header-protection sample position, pn_len derivation, Length
# semantics and AEAD AAD are all simultaneously right.
CASES = (
    ("A.2 client Initial", keys["c"], A2_CLIENT, 2, 1182, "c3", A2_PLAIN),
    # A.3's server packet carries an empty DCID: the keys always come from the
    # client's original DCID, using the "quic server in" secret.
    ("A.3 server Initial", keys["s"], A3_SERVER, 1, 117, "c1", A3_PLAIN),
)
for label, k, pkt, want_pn, want_len, want_first, want_plain in CASES:
    info, plain = relay.open_packet(k, pkt)
    if info is None:
        print(f"FAIL {label}: {plain}")
        ok = False
        continue
    # A.2's packet is padded to the 1200-byte QUIC minimum, and PADDING (0x00)
    # lives inside the encrypted payload, so the decrypted plaintext is the RFC
    # body plus trailing zero bytes -- the published payload must match it
    # exactly up to that padding.
    stripped = plain.rstrip(b"\x00")
    good = (info["pn"] == want_pn and info["len"] == want_len
            and f"{info['first']:02x}" == want_first and stripped == want_plain)
    frames = relay.walk_frames(plain, {})
    print(f"{'PASS' if good else 'FAIL'} {label}: first=0x{info['first']:02x} "
          f"(want 0x{want_first}) pn={info['pn']} (want {want_pn}) "
          f"len={info['len']} (want {want_len}) fixed_bit={info['fixed_bit']} "
          f"dcid={info['dcid']} scid={info['scid']} token={info['token'] or '-'} "
          f"plain={len(plain)}B body={len(stripped)}B/{len(want_plain)}B "
          f"frames={frames}")
    if not good and stripped != want_plain:
        print(f"  plain got ={stripped[:32].hex()}\n  plain want={want_plain[:32].hex()}")
    ok = ok and good
print("relay KAT:", "PASS" if ok else "FAIL")
raise SystemExit(0 if ok else 1)
