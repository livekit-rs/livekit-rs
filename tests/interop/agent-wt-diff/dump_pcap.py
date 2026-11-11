#!/usr/bin/env python3
"""Write relay-captured UDP datagrams as a pcap so an *independent* dissector can
verify `quic-relay.py`'s own Initial decoding.

Wireshark/tshark implement RFC 9001 Initial keys themselves, so a packet that
tshark decrypts and this relay does not (or the other way round) is a tool bug,
not a finding about the server. LINKTYPE_RAW (101) keeps the file format trivial:
one synthetic IPv4 + UDP header per datagram, addresses/ports taken from the real
sockets so the conversation is coherent.

Records: (seq, from_client, payload, src_port, dst_port).
"""
import os
import socket
import struct

LOOPBACK = "127.0.0.1"
IP_UDP = 20 + 8


def ipv4(src, dst, payload_len):
    """Minimal IPv4 header (checksum computed) for a UDP payload."""
    header = struct.pack(">BBHHHBB", 0x45, 0, IP_UDP + payload_len, 0x0000, 0x4000, 64, 17)
    checksum = 0
    for i in range(0, len(header), 2):
        checksum += int.from_bytes(header[i:i + 2], "big")
    checksum = (~((checksum & 0xFFFF) + (checksum >> 16))) & 0xFFFF
    return header + struct.pack(">H", checksum) + socket.inet_aton(src) + socket.inet_aton(dst)


def write(directory, records, log, src=LOOPBACK, dst=LOOPBACK):
    os.makedirs(directory, exist_ok=True)
    pcap = os.path.join(directory, "capture.pcap")
    with open(pcap, "wb") as f:
        # global header: magic, v2.4, thiszone, sigfigs, snaplen, network=101 (RAW)
        f.write(struct.pack("<IHHiIII", 0xA1B2C3D4, 2, 4, 0, 0, 65535, 101))
        for seq, from_client, data, sport, dport in records:
            with open(os.path.join(directory, f"{seq:03d}-{'c' if from_client else 's'}.bin"),
                      "wb") as raw:
                raw.write(data)
            s, d = (src, dst) if from_client else (dst, src)
            sp, dp = (sport, dport) if from_client else (dport, sport)
            udp = struct.pack(">HHHH", sp, dp, IP_UDP + len(data), 0) + data
            blob = ipv4(s, d, len(udp)) + udp
            f.write(struct.pack("<IIII", seq, 0, len(blob), len(blob)) + blob)
    ports = sorted({p for _, _, _, a, b in records for p in (a, b)})
    print(f"[dump] {len(records)} datagrams -> {pcap} "
          f"(force QUIC: -d udp.port=={ports[0] if ports else 443},quic)",
          file=log, flush=True)
