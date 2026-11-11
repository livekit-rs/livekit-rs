#!/usr/bin/env python3
"""C15 cross-stack QUIC wire forensics relay (SPEC-0074 §2.4 C15).

A userspace UDP relay on loopback that forwards between a QUIC client and the
Rust/Go WebTransport listener while decoding every datagram. It answers the
question a client-side error string cannot: *does the server answer the client's
Initial packets at all*, and what exactly is inside those Initial packets.

QUIC v1 Initial packets are unprotectable by design (RFC 9001 §5.2): the keys
are derived from the packet's Destination Connection ID with the public v1 salt.
This tool derives them itself -- HKDF-Extract/Expand-SHA-256, AES-128-GCM packet
protection and AES-128-ECB header protection, i.e. the same schedule
`lk-quic-crypto` implements and the RFC 9001 A.1/A.2 vectors pin -- so the
decoded ClientHello is the byte-exact one the client put on the wire.

Output: one section per datagram (packet header, frame list), the reassembled
CRYPTO stream parsed as a TLS ClientHello (cipher suites, supported groups,
key_share, supported_versions, ALPN, every QUIC transport parameter) and an
explicit marker whenever the server sends anything back.

Usage:
    quic-relay.py --listen 127.0.0.1:44430 --target 127.0.0.1:44431 \
                  [--out relay.log] [--ch-out clienthello.bin] [--budget 45]
"""
import argparse
import hashlib
import hmac
import select
import signal
import socket
import struct
import sys
import time

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

INITIAL_SALT_V1 = bytes.fromhex("38762cf7f55934b34d179ae6a4c80cadccbb7f0a")
QUIC_V1 = 0x00000001
TLS_CIPHER_SUITE = {
    0x1301: "TLS_AES_128_GCM_SHA256",
    0x1302: "TLS_AES_256_GCM_SHA384",
    0x1303: "TLS_CHACHA20_POLY1305_SHA256",
}
TLS_GROUP = {
    0x001d: "x25519", 0x0017: "secp256r1", 0x0018: "secp384r1",
    0x0019: "secp521r1", 0x001e: "x448", 0x11ec: "X25519MLKEM768",
    0x0029: "brainpoolP256r1tls13",
}
EXT_NAME = {
    0x0000: "server_name", 0x000a: "supported_groups", 0x000b: "ec_point_formats",
    0x000d: "signature_algorithms", 0x0015: "padding", 0x0016: "psk_ke_modes",
    0x002a: "supported_versions", 0x002b: "key_share", 0x0034: "alpn",
    0x0039: "quic_transport_parameters", 0xff01: "renegotiation_info",
}
TP_NAME = {
    0x00: "original_dest_cid", 0x01: "max_idle_timeout", 0x02: "stateless_reset_token",
    0x03: "max_udp_payload_size", 0x04: "initial_max_data",
    0x05: "max_stream_data_bidi_local", 0x06: "max_stream_data_bidi_remote",
    0x07: "max_stream_data_uni", 0x08: "max_streams_bidi", 0x09: "max_streams_uni",
    0x0A: "ack_delay_exponent", 0x0B: "max_ack_delay", 0x0C: "disable_active_migration",
    0x0D: "preferred_address", 0x0E: "active_connection_id_limit",
    0x0F: "initial_source_cid", 0x10: "retry_source_cid", 0x1D: "reset_stream_at",
    0x20: "max_datagram_frame_size", 0x2AB2: "grease_quic_bit",
}


def is_grease(v):
    """TLS/QUIC GREASE values: 0x?a?a? with equal outer nibbles."""
    return v <= 0xFFFF and (v & 0x0F0F) == 0x0A0A


def suite_name(v):
    return TLS_CIPHER_SUITE.get(v, "GREASE" if is_grease(v) else "unknown")


def group_name(v):
    return TLS_GROUP.get(v, "GREASE" if is_grease(v) else "unknown")


def ext_name(v):
    return EXT_NAME.get(v, "GREASE" if is_grease(v) else "unknown")


def varint(buf, off):
    """Decode one RFC 9000 variable-length integer: (value, bytes consumed).

    The two most significant bits encode the width, so the value always starts
    from `first & 0x3f` -- masking anything else (e.g. 6 bits per extra byte)
    corrupts every 2/4/8-byte field, which is how a Length of 0x449e silently
    became 17566 and made the packet look unreadable.
    """
    if off >= len(buf):
        raise ValueError("varint past end")
    n = 1 << (buf[off] >> 6)
    if off + n > len(buf):
        raise ValueError("truncated varint")
    return int.from_bytes(buf[off:off + n], "big") & ((1 << (8 * n - 2)) - 1), n


def hkdf(secret, label, length):
    """RFC 8446 HKDF-Expand-Label over HMAC-SHA-256 (no AEAD dependency).

    HkdfLabel = uint16 length || uint8 label.length || label || uint8
    context.length (0 here); omitting the trailing context byte silently yields
    the wrong key schedule, so RFC 9001 A.1 values are pinned by relay-kat.py.
    """
    full = b"tls13 " + label
    info = struct.pack(">H", length) + bytes([len(full)]) + full + b"\x00"
    out, block, i = b"", b"", 1
    while len(out) < length:
        block = hmac.new(secret, block + info + bytes([i]), hashlib.sha256).digest()
        out += block
        i += 1
    return out[:length]


def initial_keys(dcid):
    """RFC 9001 §5.2: the two Initial key sets, both keyed from the client's DCID.

    Both endpoints use the *same* DCID (the one the client chose) but different
    directional secrets -- "quic client in" for C->S packets and "quic server in"
    for S->C packets -- so decoding a server Initial with the client set can never
    succeed.
    """
    secret = hmac.new(INITIAL_SALT_V1, dcid, hashlib.sha256).digest()

    def suite(direction_secret):
        return {"key": hkdf(direction_secret, b"quic key", 16),
                "iv": hkdf(direction_secret, b"quic iv", 12),
                "hp": hkdf(direction_secret, b"quic hp", 16)}

    return {"c": suite(hkdf(secret, b"client in", 32)),
            "s": suite(hkdf(secret, b"server in", 32))}


def hp_mask(hp_key, sample):
    return Cipher(algorithms.AES(hp_key), modes.ECB()).encryptor().update(sample[:16])


def open_packet(k, data):
    """Strip header protection and AEAD-decode one long-header Initial packet.

    RFC 9000 §17.2 layout: first byte, version, DCID (len-prefixed), SCID,
    **Token (varint length + value)**, Length, Packet Number. Header protection
    (RFC 9001 §5.4.1) covers only the low bits of the first byte and the packet
    number -- the Length field keeps its encoding prefix *and* its value in the
    clear, which is what makes this parser possible.

    Returns `(info, plaintext)` on success and `(None, reason)` otherwise, so the
    log can tell a header that fails to parse apart from a packet that is
    well-formed but encrypted with different keys (an AEAD tag failure).
    """
    if len(data) < 20:
        return None, "packet shorter than a long header"
    if not data[0] & 0x80:
        return None, "short header"
    if int.from_bytes(data[1:5], "big") != QUIC_V1:
        return None, f"version 0x{int.from_bytes(data[1:5], 'big'):08x}"
    try:
        dcid_len = data[5]
        dcid = data[6:6 + dcid_len]
        off = 6 + dcid_len
        scid_len = data[off]
        scid = data[off + 1:off + 1 + scid_len]
        off += 1 + scid_len
        token_len, n = varint(data, off)          # Initial-only Token field
        off += n
        token = data[off:off + token_len]
        off += token_len
        length, plen_n = varint(data, off)        # unprotected by RFC 9001
    except (ValueError, IndexError):
        return None, "truncated header fields"
    len_off, len_end = off, off + plen_n
    pn_off = len_end
    # The sample offset is `pn_offset + 4`, and pn_offset is fully determined by
    # the *unprotected* Length field width -- the packet number length is not
    # needed, and cannot be used here because those bits are themselves protected
    # (RFC 9001 §5.4/§5.4.2).
    sample_off = pn_off + 4
    if pn_off >= len(data) or sample_off + 16 > len(data):
        return None, "no 16-byte header-protection sample"
    mask = hp_mask(k["hp"], data[sample_off:])

    first = data[0] ^ (mask[0] & 0x0F)            # long header: 4 protected bits
    pn_len = (first & 0x03) + 1                   # now, and only now, knowable
    if pn_off + pn_len > len(data):
        return None, "packet number out of bounds"
    pn_field = bytearray(data[pn_off:pn_off + pn_len])
    for i in range(pn_len):
        pn_field[i] ^= mask[1 + i]
    pn = int.from_bytes(pn_field, "big")
    # RFC 9000 §17.2: Length counts the packet number field *plus* the payload and
    # the 16-byte tag, so the protected packet ends at `pn_offset + length`.
    if length < pn_len or pn_off + length > len(data):
        return None, f"declared length {length} outside the datagram"
    aad = bytes([first]) + data[1:len_off] + data[len_off:len_end] + bytes(pn_field)
    body = data[pn_off + pn_len:pn_off + length]
    nonce = k["iv"][:4] + bytes(a ^ b for a, b in zip(k["iv"][4:], pn.to_bytes(8, "big")))
    try:
        # `body` is already ciphertext||tag; the header goes in as AAD only.
        plain = AESGCM(k["key"]).decrypt(nonce, body, aad)
    except Exception:  # noqa: BLE001 - AEAD failure is a finding, not a crash
        return None, f"AEAD open failed (pn={pn}, ct={len(body)}B, aad={len(aad)}B)"
    return {"first": first, "fixed_bit": bool(data[0] & 0x40), "dcid": dcid.hex(),
            "scid": scid.hex(), "pn": pn, "token": token.hex(), "len": length,
            "consumed": pn_off + length}, plain


def walk_frames(plain, crypto):
    """Report the frame list of a decrypted payload, collecting CRYPTO bytes."""
    seen, off = [], 0
    fixed = {0x00: "PADDING", 0x01: "PING", 0x0C: "disable_active_migration"}
    while off < len(plain):
        try:
            t, n = varint(plain, off)
        except ValueError:
            seen.append("truncated")
            break
        off += n
        if t == 0x00:
            while off < len(plain) and plain[off] == 0x00:
                off += 1
            seen.append("PADDING")
            continue
        if t == 0x01:
            seen.append("PING")
            continue
        if t == 0x06:  # CRYPTO {offset, length, data}
            try:
                co, n1 = varint(plain, off)
                cl, n2 = varint(plain, off + n1)
            except ValueError:
                seen.append("CRYPTO?")
                break
            off += n1 + n2
            crypto[co] = plain[off:off + cl]
            off += cl
            seen.append(f"CRYPTO(off={co},len={cl})")
            continue
        if t in (0x02, 0x03, 0x1c, 0x1d, 0x1e, 0x3f42):
            seen.append(f"frame:0x{t:x}")
            off = len(plain)  # ACK/close/etc: not needed for this analysis
            break
        seen.append(f"frame:0x{t:x}(rest {len(plain) - off}B)")
        break
    return seen


def parse_transport_params(blob):
    out, off = [], 0
    while off < len(blob):
        try:
            tid, n1 = varint(blob, off)
            tlen, n2 = varint(blob, off + n1)
        except ValueError:
            out.append("truncated")
            break
        val = blob[off + n1 + n2:off + n1 + n2 + tlen]
        off += n1 + n2 + tlen
        name = TP_NAME.get(tid, f"0x{tid:x}" + ("(GREASE)" if tid % 31 == 27 else ""))
        out.append(f"{name}={val.hex() if tlen else 'empty'}")
    return out


def parse_client_hello(blob, log):
    """Dump the ClientHello fields a server gates on."""
    try:
        if blob[0] != 0x01:
            print(f"[crypto] not a ClientHello (handshake type 0x{blob[0]:02x})",
                  file=log, flush=True)
            return
        p = 4
        legacy = int.from_bytes(blob[p:p + 2], "big")
        p += 2 + 32
        p += 1 + blob[p]                                    # legacy_session_id
        cs_len = int.from_bytes(blob[p:p + 2], "big")
        p += 2
        suites = [int.from_bytes(blob[q:q + 2], "big") for q in range(p, p + cs_len, 2)]
        print(f"[ClientHello] legacy_version=0x{legacy:04x} cipher_suites="
              f"{[f'0x{s:04x}:{suite_name(s)}' for s in suites]}", file=log, flush=True)
        p += cs_len
        p += 1 + blob[p]                                    # compression methods
        ext_total = int.from_bytes(blob[p:p + 2], "big")
        p += 2
        exts, q = {}, 0
        while q < ext_total:
            et = int.from_bytes(blob[p + q:p + q + 2], "big")
            el = int.from_bytes(blob[p + q + 2:p + q + 4], "big")
            exts[et] = blob[p + q + 4:p + q + 4 + el]
            q += 4 + el
        names = [f"0x{e:04x}:{ext_name(e)}" for e in sorted(exts)]
        print(f"  extensions={names}", file=log, flush=True)
        if 0x000A in exts:
            g = exts[0x000A]
            n = int.from_bytes(g[:2], "big")
            ids = [int.from_bytes(g[q:q + 2], "big") for q in range(2, 2 + n, 2)]
            print(f"  supported_groups="
                  f"{[f'0x{i:04x}:{group_name(i)}' for i in ids]}", file=log, flush=True)
        if 0x002B in exts:
            k, n = exts[0x002B], int.from_bytes(exts[0x002B][:2], "big")
            q, shares = 2, []
            while q < 2 + n:
                gid = int.from_bytes(k[q:q + 2], "big")
                kl = int.from_bytes(k[q + 2:q + 4], "big")
                shares.append(f"0x{gid:04x}:{group_name(gid)}({kl}B)")
                q += 4 + kl
            print(f"  key_share={shares}", file=log, flush=True)
        if 0x002A in exts:
            v = exts[0x002A]
            print(f"  supported_versions="
                  f"{[f'0x{int.from_bytes(v[q:q + 2], 'big'):04x}' for q in range(1, len(v), 2)]}",
                  file=log, flush=True)
        if 0x0034 in exts:
            a, q, alpn = exts[0x0034], 0, []
            while q < len(a):
                alpn.append(a[q + 1:q + 1 + a[q]].decode("utf-8", "replace"))
                q += 1 + a[q]
            print(f"  alpn={alpn}", file=log, flush=True)
        if 0x000D in exts:
            s, n = exts[0x000D], int.from_bytes(exts[0x000D][:2], "big")
            print(f"  signature_algorithms="
                  f"{[f'0x{int.from_bytes(s[q:q + 2], 'big'):04x}' for q in range(2, 2 + n, 2)]}",
                  file=log, flush=True)
        if 0x0039 in exts:
            print(f"  quic_transport_parameters="
                  f"{parse_transport_params(exts[0x0039])}", file=log, flush=True)
    except Exception as e:  # noqa: BLE001 - diagnostic tool
        print(f"[ClientHello] parse error {e!r} on {blob[:32].hex()}", file=log, flush=True)


def hp(s):
    """'host:port' -> (host, port) tuple for bind/sendto."""
    host, _, port = s.rpartition(":")
    return host or "0.0.0.0", int(port)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", required=True, help="relay bind host:port")
    ap.add_argument("--target", required=True, help="server host:port")
    ap.add_argument("--out", help="log file (default stdout)")
    ap.add_argument("--ch-out", help="write the reassembled CRYPTO stream here")
    ap.add_argument("--dump", help="directory for raw Initial datagrams (pcap + bin)")
    ap.add_argument("--budget", type=float, default=45, help="relay lifetime seconds")
    a = ap.parse_args()

    log = open(a.out, "w") if a.out else sys.stdout
    front = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    front.bind(hp(a.listen))
    back = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    back.bind(("127.0.0.1", 0))
    target = hp(a.target)
    print(f"relay {a.listen} -> {a.target}", file=log, flush=True)

    client = None
    keys, crypto, reported = {}, {}, {}
    odcid = None                       # client's first Initial DCID: the key seed
    dump = [] if a.dump else None      # raw datagrams, for an independent oracle
    seq = 0
    stop = []                          # set by SIGTERM so the dump still gets written
    signal.signal(signal.SIGTERM, lambda *_: stop.append(True))
    end = time.time() + a.budget
    while time.time() < end and not stop:
        budget = max(0.05, end - time.time())
        r, _, _ = select.select([front, back], [], [], budget)
        if not r:
            continue
        sock = r[0]
        data, addr = sock.recvfrom(65536)
        from_front = sock is front
        if from_front:
            client = addr
            print(f"\n--> client datagram {len(data)}B  head={data[:48].hex()}",
                  file=log, flush=True)
        else:
            print(f"\n<-- SERVER REPLY datagram {len(data)}B from {addr}  "
                  f"head={data[:48].hex()}", file=log, flush=True)
        rest = data
        if dump is not None and rest[0] & 0x80:
            seq += 1
            # ports: use the *endpoint* tuple (client <-> real listener), not the
            # relay's own sockets, so the pcap shows one coherent conversation
            sport, dport = ((addr[1], target[1]) if from_front
                            else (target[1], client[1] if client else 0))
            dump.append((seq, from_front, bytes(data), sport, dport))
        while len(rest) > 16:
            if not rest[0] & 0x80:
                print(f"    short-header packet {len(rest)}B (1-RTT, needs handshake keys)",
                      file=log, flush=True)
                break
            ver = int.from_bytes(rest[1:5], "big")
            if ver == 0:
                print("    Version Negotiation packet", file=log, flush=True)
                break
            if ver != QUIC_V1:
                print(f"    unsupported QUIC version 0x{ver:08x}", file=log, flush=True)
                break
            dcid_len = rest[5]
            dcid = rest[6:6 + dcid_len]
            # The two long-packet-type bits are outside the 4 header-protected
            # bits, so the packet type is readable before unprotecting anything.
            ptype = rest[0] & 0x30
            if from_front and ptype == 0x00 and odcid is None:
                # RFC 9001 §5.2: both directions are keyed from the DCID the client
                # chose in its *first* Initial. Later client Initials carry the
                # server's CID as DCID, so re-seeding on them would be a bug.
                odcid = dcid.hex()
            if ptype != 0x00:
                label = {0x10: "0-RTT", 0x20: "Handshake", 0x30: "Retry"}[ptype]
                print(f"    {label} packet {len(rest)}B "
                      f"(needs TLS handshake keys, not decoded here)",
                      file=log, flush=True)
                break
            if not odcid:
                print(f"    Initial before any client Initial (dcid={dcid.hex()})",
                      file=log, flush=True)
                break
            keys.setdefault(odcid, initial_keys(bytes.fromhex(odcid)))
            side = "c" if from_front else "s"
            info, plain = open_packet(keys[odcid][side], rest)
            if info is None:
                off = 6 + dcid_len + rest[6 + dcid_len]
                print(f"    Initial {len(rest)}B odcid={odcid} dcid={dcid.hex()} "
                      f"-> NOT DECODABLE ({side}-> {plain}; "
                      f"first_byte=0x{rest[0]:02x} hdr@{off}={rest[off:off + 8].hex()})",
                      file=log, flush=True)
                break
            tag = "C->S" if from_front else "S->C"
            print(f"    [{tag}] Initial pn={info['pn']} len={info['len']} "
                  f"type={info['first'] & 0xF0:02x} fixed_bit={info['fixed_bit']} "
                  f"dcid={info['dcid']} scid={info['scid']} "
                  f"token={info['token'] or '-'} frames={walk_frames(plain, crypto)}",
                  file=log, flush=True)
            rest = rest[info["consumed"]:]
        if not from_front:
            if client:
                front.sendto(data, client)
            continue
        if crypto:
            blob = b"".join(crypto[k] for k in sorted(crypto))
            if not reported.get("ch") and blob[:1] == b"\x01" and len(blob) > 4:
                hlen = int.from_bytes(blob[1:4], "big") + 4
                if len(blob) >= hlen:
                    parse_client_hello(blob, log)
                    reported["ch"] = True
                    if a.ch_out:
                        with open(a.ch_out, "wb") as f:
                            f.write(blob)
                        print(f"    ClientHello written to {a.ch_out} "
                              f"({len(blob)}B)", file=log, flush=True)
        back.sendto(data, target)

    print("\nrelay done", file=log, flush=True)
    if dump is not None:
        import dump_pcap  # noqa: PLC0415  (local helper next to this script)
        dump_pcap.write(a.dump, dump, log)
    if log is not sys.stdout:
        log.close()


if __name__ == "__main__":
    main()
