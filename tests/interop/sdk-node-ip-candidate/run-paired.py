#!/usr/bin/env python3
"""SPEC-0077 C99 — launched-MAIN `rtc.node_ip` rewrite as seen by the OFFICIAL SDK.

Leafs named: `YAML:rtc.node_ip`, `generated_flags:--rtc.node_ip.ipv4` at the
`official_differential_lifecycle` tier — the tier above the WHIP/pion observer leg in
`../rtc-node-ip-family/`, which records what a daemon *answers* but never that a real
client can *connect* through the rewritten address. Here the client is the pinned
official `livekit-client@2.23.0` UMD build (served byte-for-byte, unpatched) running in
headless Chromium, and the observables are what that client was offered, what it
nominated, and whether audio actually egressed.

Contract under test (frozen Go oracle e5655ac / mediatransportutil f234b534b095):
  * `pkg/rtcconfig/config.go:235-263` `NodeIP.UnmarshalString` normalises the scalar
    text (`net.ParseIP` then `ip.String()`), so `::ffff:127.0.0.2` lands in the **V4**
    slot; `PrimaryIP()` (:280-285) prefers V4 and `ToStringSlice()` (:269-278) emits it
    first. The generated dotted flag writes RAW text into the named leaf and bypasses
    that parser entirely.
  * `pkg/rtcconfig/webrtc_config.go:91-116` installs the rewrite when the node IP is
    set and not auto-generated; `nat1To1AddressRewriteRules` (:257-286) keys each rule
    by the external address's OWN family, so a V6-only node IP must leave the IPv4
    candidate untouched.
  * The rewrite is applied to the per-peer transport addresses gathered from
    `port_range_start/end`; with a single mux `rtc.udp_port` Pion's `UDPMuxDefault`
    advertises only the mux endpoint, so the rewritten address reaches the SDP but no
    socket listens on it (`pkg/rtc/engine.go:322-326`, `iceTCPMux` is set only when
    `TCPPort != 0`).

Gate shape (non-vacuous by construction):
  * every profile carries a **declared row derived from the Go oracle run**, checked on
    each side independently (`go_declared_ok`, `rust_declared_ok`) before cross-side
    equality, so a matching failure cannot pass;
  * `baseline-no-node-ip` is the positive control for the whole lane: it proves the
    harness connects, publishes and gathers traffic when nothing is rewritten, and the
    `scalar-v4-mapped-self` leg proves it over a candidate the rewrite produced. The
    remaining legs are fenced rather than negative: on this host the rewritten
    destination `127.0.0.2` is not deliverable at all (the probe socket cannot bind it),
    so those legs advertise the rewrite into the client's remote description and then
    complete over a learned address. The baseline is what makes them discriminate the
    mechanism rather than a broken probe;
  * a `fence` profile records a **measured** Rust divergence that this batch did not
    fix. A fence passes only while the divergence reproduces exactly as recorded; it is
    never absorbed into the parity claim.
"""
import argparse
import hashlib
import ipaddress
import json
import os
import pathlib
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

FIXED_GO_SHA256 = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
FIXED_GO_COMMIT = 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07'
REPO = pathlib.Path('/Users/zoujiaqing/projects/livekit-rs')
HERE = REPO / 'tests/interop/sdk-node-ip-candidate'
COVERS = ['YAML:rtc.node_ip', 'generated_flags:--rtc.node_ip.ipv4']
API_KEY = 'ownedc99sdk'
API_SECRET = 'fixture-secret-only-for-local-loopback-tests-32b'
UDP_RANGE = 40
# Addresses this lane may persist verbatim: loopback plus the documentation ranges the
# profiles deliberately use. Anything else the daemon auto-discovers is redacted.
ALLOWED_TEXT = {'', '0.0.0.0', '::', '255.255.255.255'}
ALLOWED_NETS = [ipaddress.ip_network(n) for n in
                ('127.0.0.0/8', '::1/128', '203.0.113.0/24', '192.0.2.0/24',
                 '198.51.100.0/24', '2001:db8::/32', 'fe80::/10')]
IP_TOKEN_RE = re.compile(r'[0-9A-Fa-f:.]+(?:%[0-9A-Za-z._-]+)?')
PROFILES = []
FIELDS = ()


def as_address(token):
    """Parse an address literal, unwrapping the IPv4-mapped form Go stores verbatim."""
    core = token.split('%')[0].strip('[]')
    try:
        ip = ipaddress.ip_address(core)
    except ValueError:
        return None
    if ip.version == 6 and getattr(ip, 'ipv4_mapped', None) is not None:
        return ip.ipv4_mapped
    return ip


def keeps(addr_text):
    if addr_text in ALLOWED_TEXT:
        return True
    ip = as_address(addr_text)
    return ip is not None and any(ip.version == n.version and ip in n for n in ALLOWED_NETS)


def leaks(text):
    """Every real address literal in `text` that must not be persisted."""
    return [m.group(0) for m in IP_TOKEN_RE.finditer(text or '')
            if as_address(m.group(0)) is not None and not keeps(m.group(0))]


def redact(value):
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, dict):
        return {k: redact(v) for k, v in value.items()}
    if not isinstance(value, str):
        return value

    def sub(match):
        token = match.group(0)
        # Only genuine address literals are candidates; a timestamp or a `key: value`
        # colon must survive untouched, otherwise redaction destroys compared text.
        return token if as_address(token) is None or keeps(token) else '<discovered-address>'

    return IP_TOKEN_RE.sub(sub, value)


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def norm_ports(text):
    """Strip port digits so the two sides' YAML texts compare as one config."""
    return re.sub(r'\b\d{2,5}\b', '<port>', text)


def base_env():
    return {k: v for k, v in os.environ.items()
            if not k.startswith(('LIVEKIT_', 'LIVECLOUD_', 'OTEL_'))}


def tcp_bound(port, host='127.0.0.1'):
    try:
        with socket.create_connection((host, port), timeout=0.4):
            return True
    except OSError:
        return False


def udp_free(port, host='127.0.0.1'):
    """True when nothing holds this UDP endpoint (Go binds without SO_REUSEADDR)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((host, port))
        s.close()
        return True
    except OSError:
        return False


RESERVED = []


def reserve_tcp(host='127.0.0.1'):
    """A port in a non-ephemeral band, bind-tested and HELD by this process.

    Allocating from the ephemeral range and closing immediately is not enough: macOS
    handed the released port to an unrelated outbound socket between allocation and
    daemon start, which reads as `bind: address already in use` on the next case. The
    reservation is released the moment the daemon is launched.
    """
    for _ in range(400):
        port = 21000 + random.randrange(0, 8000)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind((host, port))
        except OSError:
            sock.close()
            continue
        sock.listen(1)
        RESERVED.append(sock)
        return port
    raise SystemExit('no free tcp port in band')


def reserve_udp(count, host='127.0.0.1'):
    """A contiguous UDP block, bind-tested and HELD by this process."""
    for _ in range(400):
        start = 50000 + random.randrange(0, 13000)
        if start + count >= 64000:
            continue
        socks = []
        try:
            for offset in range(count):
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind((host, start + offset))
                socks.append(sock)
        except OSError:
            for sock in socks:
                sock.close()
            continue
        RESERVED.extend(socks)
        return start
    raise SystemExit('no contiguous free UDP range')


def free_band_tcp(host='127.0.0.1'):
    """Bind-tested but NOT held: for a service (redis) we start ourselves."""
    for _ in range(400):
        port = 21000 + random.randrange(0, 8000)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind((host, port))
            sock.close()
            return port
        except OSError:
            sock.close()
    raise SystemExit('no free tcp port for redis')


def release_reservations():
    while RESERVED:
        RESERVED.pop().close()


def wait_ready(proc, port, timeout=30):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if proc.poll() is not None:
            return False
        if tcp_bound(port):
            return True
        time.sleep(0.05)
    return False


def terminate(proc, window=8):
    """Escalating shutdown. Upstream needs a second SIGTERM, and that is a compared
    field: a candidate that exits on the first signal diverges from the oracle."""
    started = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    second = False
    try:
        code = proc.wait(timeout=window)
    except subprocess.TimeoutExpired:
        proc.send_signal(signal.SIGTERM)
        second = True
        try:
            code = proc.wait(timeout=window)
        except subprocess.TimeoutExpired:
            os_kill_group(proc.pid)
            code = proc.wait()
    return {'server_exit': code, 'sigterm_to_exit_s': round(time.monotonic() - started, 3),
            'second_sigterm_sent': second, 'sigkill_required': code == -signal.SIGKILL}


def os_kill_group(pid):
    import os
    os.killpg(pid, signal.SIGKILL)


# ------------------------------------------------------------------ profiles
# `mode`: 'range' (per-peer transports from port_range_start/end — the shape the rewrite
# is designed for) or 'mux' (a single rtc.udp_port).  `node_ip` is either absent, a YAML
# scalar, or ['--rtc.node_ip.ipv4', text] for the generated-flag channel.
# `scheme`: the URL handed to the SDK, which selects the daemon's signal handler.
PROFILES.extend([
    {'name': 'baseline-no-node-ip', 'group': 'parity', 'mode': 'range', 'scheme': 'ws',
     'why': 'positive control for the whole lane: nothing is rewritten, so the SDK connects, publishes and nominates a host pair whose counters grow. Every other row here is only meaningful because this one is green -- a broken probe would make the negative legs look like parity',
     'expect': {
         'sdk_connected': True, 'room_state': 'connected',
         'can_update_subscriptions': None, 'tracks_published': 1, 'gum_result_class': 'ok',
         'connect_error_class': None, 'publish_error_class': None,
         'peer_connection_count': 1, 'pc_connection_state': 'connected',
         'pc_ice_connection_state': 'connected',
         'ice_states_seen': ['checking', 'connected'], 'host_candidate_ips': ['127.0.0.1'],
         'candidate_types': ['host'], 'has_tcpcandidate': True, 'tcp_candidate_count': 2,
         'udp_candidate_count': 2, 'prflx_signalled_count': 0, 'srflx_candidate_count': 0,
         'selected_pair_count': 1, 'nominated_pair_state': 'succeeded',
         'nominated_remote_type': 'host', 'nominated_remote_is_advertised': True,
         'transport_bytes_sent_positive': True, 'rtp_out_bytes_sent_positive': True,
         'counters_grew': True, 'ready': True, 'server_exit': 0, 'probe_exit': 0,
         'rewrite_log_present': False, 'node_ip_slots': None, 'tcp_mux_zero_log': False,
         'signal_url_scheme': 'ws', 'signal_url_path': '/rtc/v1',
         'server_ports_released': True, 'second_sigterm_sent': True,
         'sigkill_required': False, 'mux_alt_bindable': None,
     },
    },
    {'name': 'scalar-v4-mapped', 'group': 'fence', 'mode': 'range', 'scheme': 'ws',
     'node_ip': '::ffff:127.0.0.2',
     'why': 'the review row for the YAML scalar channel. `rtc.node_ip: "::ffff:127.0.0.2"` goes through `NodeIP.UnmarshalString`, which re-renders the parsed IP, so the V4 slot records `127.0.0.2` and the official SDK\'s remote description carries 127.0.0.2 in both the udp and the tcp host candidate lines. That is advertisement proven from the client side. What the row does NOT claim: this host refuses to bind 127.0.0.2, so no media flows over the rewritten address and the pair built on it is never fixed -- consumption is carried by scalar-v4-mapped-self. Fenced on the one measured cross-side divergence: when the rewritten target is not deliverable on this host the Rust daemon also trickles a peer-reflexive udp candidate for the address its checks arrived from (udp_candidate_count 2->3, prflx_signalled_count 0->1, candidate_types gains prflx) which Pion never signals. Everything the row claims about the advertised text holds on both sides; the fence records the residue, it does not absorb it.',
     'expect': {
         'sdk_connected': True, 'room_state': 'connected',
         'can_update_subscriptions': None, 'tracks_published': 1, 'gum_result_class': 'ok',
         'connect_error_class': None, 'publish_error_class': None,
         'peer_connection_count': 1, 'pc_connection_state': 'connected',
         'pc_ice_connection_state': 'connected',
         'ice_states_seen': ['checking', 'connected'], 'host_candidate_ips': ['127.0.0.2'],
         'has_tcpcandidate': True, 'tcp_candidate_count': 2, 'srflx_candidate_count': 0,
         'selected_pair_count': 1, 'nominated_pair_state': 'succeeded',
         'nominated_remote_type': 'prflx', 'nominated_remote_is_advertised': True,
         'transport_bytes_sent_positive': True, 'rtp_out_bytes_sent_positive': True,
         'counters_grew': True, 'ready': True, 'server_exit': 0, 'probe_exit': 0,
         'rewrite_log_present': True, 'node_ip_slots': '{"V4":"127.0.0.2","V6":""}',
         'tcp_mux_zero_log': False, 'signal_url_scheme': 'ws',
         'signal_url_path': '/rtc/v1', 'server_ports_released': True,
         'second_sigterm_sent': True, 'sigkill_required': False, 'mux_alt_bindable': None,
     },
     'divergence': {
         'go': {'candidate_types': ['host'], 'prflx_signalled_count': 0, 'udp_candidate_count': 2},
         'rust': {'candidate_types': ['host', 'prflx'], 'prflx_signalled_count': 1, 'udp_candidate_count': 3},
     }},
    {'name': 'scalar-v4-mapped-self', 'group': 'parity', 'mode': 'range', 'scheme': 'ws',
     'node_ip': '::ffff:127.0.0.1',
     'why': 'the actual-consumption half of the review row, and the only leg where the rewrite is genuinely reachable: Go normalises `::ffff:127.0.0.1` to the address the daemon really binds, so the SDK nominates a host candidate carrying the rewritten text and RTP egresses through it (rtp_out_bytes_sent_positive and counters_grew both true, with the two samples taken 2 s apart). Because the rewrite is an identity here, this leg alone would not catch a dropped v4 mapping -- the binary positive control does that, and the 127.0.0.2 legs discriminate the advertised text',
     'expect': {
         'sdk_connected': True, 'room_state': 'connected',
         'can_update_subscriptions': None, 'tracks_published': 1, 'gum_result_class': 'ok',
         'connect_error_class': None, 'publish_error_class': None,
         'peer_connection_count': 1, 'pc_connection_state': 'connected',
         'pc_ice_connection_state': 'connected',
         'ice_states_seen': ['checking', 'connected'], 'host_candidate_ips': ['127.0.0.1'],
         'candidate_types': ['host'], 'has_tcpcandidate': True, 'tcp_candidate_count': 2,
         'udp_candidate_count': 2, 'prflx_signalled_count': 0, 'srflx_candidate_count': 0,
         'selected_pair_count': 1, 'nominated_pair_state': 'succeeded',
         'nominated_remote_type': 'host', 'nominated_remote_is_advertised': True,
         'transport_bytes_sent_positive': True, 'rtp_out_bytes_sent_positive': True,
         'counters_grew': True, 'ready': True, 'server_exit': 0, 'probe_exit': 0,
         'rewrite_log_present': True, 'node_ip_slots': '{"V4":"127.0.0.1","V6":""}',
         'tcp_mux_zero_log': False, 'signal_url_scheme': 'ws',
         'signal_url_path': '/rtc/v1', 'server_ports_released': True,
         'second_sigterm_sent': True, 'sigkill_required': False, 'mux_alt_bindable': None,
     },
    },
    {'name': 'flag-v4-mapped', 'group': 'fence', 'mode': 'range', 'scheme': 'ws',
     'node_ip': ['--rtc.node_ip.ipv4', '::ffff:127.0.0.2'],
     'why': 'the generated dotted-flag channel, which writes RAW text and bypasses `UnmarshalString`: the slot log renders `"V4":"::ffff:127.0.0.2"` verbatim, yet the candidates offered to the SDK carry `127.0.0.2` -- the same client-visible text as the scalar leg. So the two arrival channels differ in what they record and agree in what they advertise, and any parity claim about the slot rendering has to say which channel it covers. Fenced on the one measured cross-side divergence: when the rewritten target is not deliverable on this host the Rust daemon also trickles a peer-reflexive udp candidate for the address its checks arrived from (udp_candidate_count 2->3, prflx_signalled_count 0->1, candidate_types gains prflx) which Pion never signals. Everything the row claims about the advertised text holds on both sides; the fence records the residue, it does not absorb it.',
     'expect': {
         'sdk_connected': True, 'room_state': 'connected',
         'can_update_subscriptions': None, 'tracks_published': 1, 'gum_result_class': 'ok',
         'connect_error_class': None, 'publish_error_class': None,
         'peer_connection_count': 1, 'pc_connection_state': 'connected',
         'pc_ice_connection_state': 'connected',
         'ice_states_seen': ['checking', 'connected'], 'host_candidate_ips': ['127.0.0.2'],
         'has_tcpcandidate': True, 'tcp_candidate_count': 2, 'srflx_candidate_count': 0,
         'selected_pair_count': 1, 'nominated_pair_state': 'succeeded',
         'nominated_remote_type': 'prflx', 'nominated_remote_is_advertised': True,
         'transport_bytes_sent_positive': True, 'rtp_out_bytes_sent_positive': True,
         'counters_grew': True, 'ready': True, 'server_exit': 0, 'probe_exit': 0,
         'rewrite_log_present': True, 'node_ip_slots': '{"V4":"::ffff:127.0.0.2","V6":""}',
         'tcp_mux_zero_log': False, 'signal_url_scheme': 'ws',
         'signal_url_path': '/rtc/v1', 'server_ports_released': True,
         'second_sigterm_sent': True, 'sigkill_required': False, 'mux_alt_bindable': None,
     },
     'divergence': {
         'go': {'candidate_types': ['host'], 'prflx_signalled_count': 0, 'udp_candidate_count': 2},
         'rust': {'candidate_types': ['host', 'prflx'], 'prflx_signalled_count': 1, 'udp_candidate_count': 3},
     }},
    {'name': 'pair-v6-first-precedence', 'group': 'fence', 'mode': 'range', 'scheme': 'ws',
     'node_ip': '2001:db8::7,127.0.0.2',
     'why': 'the explicit IPv4 precedence row. `node_ip: "2001:db8::7,127.0.0.2"` fills BOTH slots (`{"V4":"127.0.0.2","V6":"2001:db8::7"}`) with the v6 text listed first, and the only candidates the SDK ever sees are the IPv4 udp/tcp pair rewritten to 127.0.0.2 -- no v6 address reaches the client at all, which is the consumer-side consequence of `PrimaryIP()` preferring v4. Fenced on the one measured cross-side divergence: when the rewritten target is not deliverable on this host the Rust daemon also trickles a peer-reflexive udp candidate for the address its checks arrived from (udp_candidate_count 2->3, prflx_signalled_count 0->1, candidate_types gains prflx) which Pion never signals. Everything the row claims about the advertised text holds on both sides; the fence records the residue, it does not absorb it.',
     'expect': {
         'sdk_connected': True, 'room_state': 'connected',
         'can_update_subscriptions': None, 'tracks_published': 1, 'gum_result_class': 'ok',
         'connect_error_class': None, 'publish_error_class': None,
         'peer_connection_count': 1, 'pc_connection_state': 'connected',
         'pc_ice_connection_state': 'connected',
         'ice_states_seen': ['checking', 'connected'], 'host_candidate_ips': ['127.0.0.2'],
         'has_tcpcandidate': True, 'tcp_candidate_count': 2, 'srflx_candidate_count': 0,
         'selected_pair_count': 1, 'nominated_pair_state': 'succeeded',
         'nominated_remote_type': 'prflx', 'nominated_remote_is_advertised': True,
         'transport_bytes_sent_positive': True, 'rtp_out_bytes_sent_positive': True,
         'counters_grew': True, 'ready': True, 'server_exit': 0, 'probe_exit': 0,
         'rewrite_log_present': True,
         'node_ip_slots': '{"V4":"127.0.0.2","V6":"2001:db8::7"}',
         'tcp_mux_zero_log': False, 'signal_url_scheme': 'ws',
         'signal_url_path': '/rtc/v1', 'server_ports_released': True,
         'second_sigterm_sent': True, 'sigkill_required': False, 'mux_alt_bindable': None,
     },
     'divergence': {
         'go': {'candidate_types': ['host'], 'prflx_signalled_count': 0, 'udp_candidate_count': 2},
         'rust': {'candidate_types': ['host', 'prflx'], 'prflx_signalled_count': 1, 'udp_candidate_count': 3},
     }},
    {'name': 'v6-only-control', 'group': 'parity', 'mode': 'range', 'scheme': 'ws',
     'node_ip': '2001:db8::7',
     'why': "the normal-IPv6 control: a V6-only node IP fills only the V6 slot and the IPv4 host candidates stay at the gathered 127.0.0.1, because each NAT1To1 rule is keyed by the external address's own family. Without this row the 127.0.0.2 legs could be explained by a family-blind rewrite",
     'expect': {
         'sdk_connected': True, 'room_state': 'connected',
         'can_update_subscriptions': None, 'tracks_published': 1, 'gum_result_class': 'ok',
         'connect_error_class': None, 'publish_error_class': None,
         'peer_connection_count': 1, 'pc_connection_state': 'connected',
         'pc_ice_connection_state': 'connected',
         'ice_states_seen': ['checking', 'connected'], 'host_candidate_ips': ['127.0.0.1'],
         'candidate_types': ['host'], 'has_tcpcandidate': True, 'tcp_candidate_count': 2,
         'udp_candidate_count': 2, 'prflx_signalled_count': 0, 'srflx_candidate_count': 0,
         'selected_pair_count': 1, 'nominated_pair_state': 'succeeded',
         'nominated_remote_type': 'host', 'nominated_remote_is_advertised': True,
         'transport_bytes_sent_positive': True, 'rtp_out_bytes_sent_positive': True,
         'counters_grew': True, 'ready': True, 'server_exit': 0, 'probe_exit': 0,
         'rewrite_log_present': True, 'node_ip_slots': '{"V4":"","V6":"2001:db8::7"}',
         'tcp_mux_zero_log': False, 'signal_url_scheme': 'ws',
         'signal_url_path': '/rtc/v1', 'server_ports_released': True,
         'second_sigterm_sent': True, 'sigkill_required': False, 'mux_alt_bindable': None,
     },
    },
    {'name': 'mux-single-port', 'group': 'fence', 'mode': 'mux', 'scheme': 'ws',
     'node_ip': '::ffff:127.0.0.2',
     'why': 'the single-mux consequence, measured rather than assumed. With `rtc.udp_port: P` and no range, both daemons advertise the rewritten 127.0.0.2:P and the bind probe confirms nothing on this host holds that address (mux_alt_bindable false on both sides). What the batch expected to follow -- that the client therefore cannot connect -- is refuted by these receipts: the session still reaches connected and pushes RTP, because ICE recovers through a peer-reflexive observation of the real path (nominated_remote_type prflx on both sides). The row stands as evidence that an advertised-but-dead candidate does not by itself deny connectivity over loopback. Fenced on the one measured cross-side divergence: when the rewritten target is not deliverable on this host the Rust daemon also trickles a peer-reflexive udp candidate for the address its checks arrived from (udp_candidate_count 2->3, prflx_signalled_count 0->1, candidate_types gains prflx) which Pion never signals. Everything the row claims about the advertised text holds on both sides; the fence records the residue, it does not absorb it.',
     'expect': {
         'sdk_connected': True, 'room_state': 'connected',
         'can_update_subscriptions': None, 'tracks_published': 1, 'gum_result_class': 'ok',
         'connect_error_class': None, 'publish_error_class': None,
         'peer_connection_count': 1, 'pc_connection_state': 'connected',
         'pc_ice_connection_state': 'connected',
         'ice_states_seen': ['checking', 'connected'], 'host_candidate_ips': ['127.0.0.2'],
         'has_tcpcandidate': True, 'tcp_candidate_count': 2, 'srflx_candidate_count': 0,
         'selected_pair_count': 1, 'nominated_pair_state': 'succeeded',
         'nominated_remote_type': 'prflx', 'nominated_remote_is_advertised': True,
         'transport_bytes_sent_positive': True, 'rtp_out_bytes_sent_positive': True,
         'counters_grew': True, 'ready': True, 'server_exit': 0, 'probe_exit': 0,
         'rewrite_log_present': True, 'node_ip_slots': '{"V4":"127.0.0.2","V6":""}',
         'tcp_mux_zero_log': False, 'signal_url_scheme': 'ws',
         'signal_url_path': '/rtc/v1', 'server_ports_released': True,
         'second_sigterm_sent': True, 'sigkill_required': False, 'mux_alt_bindable': False,
     },
     'divergence': {
         'go': {'candidate_types': ['host'], 'prflx_signalled_count': 0, 'udp_candidate_count': 2},
         'rust': {'candidate_types': ['host', 'prflx'], 'prflx_signalled_count': 1, 'udp_candidate_count': 3},
     }},
    {'name': 'scheme-http-origin', 'group': 'fence', 'mode': 'range', 'scheme': 'http',
     'node_ip': '::ffff:127.0.0.2',
     'why': 'the signal-scheme row, corrected by measurement. `room.connect("http://127.0.0.1:P", ...)` on livekit-client@2.23.0 logs `signal connecting to ws://127.0.0.1:P/rtc/v1`: the SDK converts the http origin into its WebSocket join, so this client never issues the Twirp POST that the earlier reading of this row assumed, and no `invalid Websocket upgrade request` appears on either daemon. The scheme the client actually dials is read from its own log line rather than from the launch argument, which is what makes the claim checkable. Fenced on the one measured cross-side divergence: when the rewritten target is not deliverable on this host the Rust daemon also trickles a peer-reflexive udp candidate for the address its checks arrived from (udp_candidate_count 2->3, prflx_signalled_count 0->1, candidate_types gains prflx) which Pion never signals. Everything the row claims about the advertised text holds on both sides; the fence records the residue, it does not absorb it.',
     'expect': {
         'sdk_connected': True, 'room_state': 'connected',
         'can_update_subscriptions': None, 'tracks_published': 1, 'gum_result_class': 'ok',
         'connect_error_class': None, 'publish_error_class': None,
         'peer_connection_count': 1, 'pc_connection_state': 'connected',
         'pc_ice_connection_state': 'connected',
         'ice_states_seen': ['checking', 'connected'], 'host_candidate_ips': ['127.0.0.2'],
         'has_tcpcandidate': True, 'tcp_candidate_count': 2, 'srflx_candidate_count': 0,
         'selected_pair_count': 1, 'nominated_pair_state': 'succeeded',
         'nominated_remote_type': 'prflx', 'nominated_remote_is_advertised': True,
         'transport_bytes_sent_positive': True, 'rtp_out_bytes_sent_positive': True,
         'counters_grew': True, 'ready': True, 'server_exit': 0, 'probe_exit': 0,
         'rewrite_log_present': True, 'node_ip_slots': '{"V4":"127.0.0.2","V6":""}',
         'tcp_mux_zero_log': False, 'signal_url_scheme': 'ws',
         'signal_url_path': '/rtc/v1', 'server_ports_released': True,
         'second_sigterm_sent': True, 'sigkill_required': False, 'mux_alt_bindable': None,
     },
     'divergence': {
         'go': {'candidate_types': ['host'], 'prflx_signalled_count': 0, 'udp_candidate_count': 2},
         'rust': {'candidate_types': ['host', 'prflx'], 'prflx_signalled_count': 1, 'udp_candidate_count': 3},
     }},
    {'name': 'tcp-port-zero-surface', 'group': 'parity', 'mode': 'range', 'scheme': 'ws',
     'tcp_port': 0,
     'why': '`rtc.tcp_port: 0` observed at the client edge: the baseline leg advertises two tcp host candidates (`1 tcp ... typ host tcptype passive`) and this leg advertises none, while the two udp candidates and the successful nomination are unchanged. The batch predicted this as a Rust fence (an ephemeral tcp candidate surviving, plus an upstream `use of closed file` line); the receipts refute both -- tcp_candidate_count is 0 on each side and neither log contains that message -- so it is a parity row instead',
     'expect': {
         'sdk_connected': True, 'room_state': 'connected',
         'can_update_subscriptions': None, 'tracks_published': 1, 'gum_result_class': 'ok',
         'connect_error_class': None, 'publish_error_class': None,
         'peer_connection_count': 1, 'pc_connection_state': 'connected',
         'pc_ice_connection_state': 'connected',
         'ice_states_seen': ['checking', 'connected'], 'host_candidate_ips': ['127.0.0.1'],
         'candidate_types': ['host'], 'has_tcpcandidate': False, 'tcp_candidate_count': 0,
         'udp_candidate_count': 2, 'prflx_signalled_count': 0, 'srflx_candidate_count': 0,
         'selected_pair_count': 1, 'nominated_pair_state': 'succeeded',
         'nominated_remote_type': 'host', 'nominated_remote_is_advertised': True,
         'transport_bytes_sent_positive': True, 'rtp_out_bytes_sent_positive': True,
         'counters_grew': True, 'ready': True, 'server_exit': 0, 'probe_exit': 0,
         'rewrite_log_present': False, 'node_ip_slots': None, 'tcp_mux_zero_log': False,
         'signal_url_scheme': 'ws', 'signal_url_path': '/rtc/v1',
         'server_ports_released': True, 'second_sigterm_sent': True,
         'sigkill_required': False, 'mux_alt_bindable': None,
     },
    },
])

COMPARE = ('sdk_connected', 'room_state', 'can_update_subscriptions',
           'tracks_published', 'audio_tracks', 'video_tracks',
           'media_kind', 'gum_result_class',
           'connect_error_class', 'publish_error_class',
           'peer_connection_count', 'pc_connection_state', 'pc_ice_connection_state',
           'ice_states_seen', 'host_candidate_ips', 'candidate_types',
           'has_tcpcandidate', 'tcp_candidate_count', 'udp_candidate_count',
           'prflx_signalled_count', 'srflx_candidate_count', 'selected_pair_count',
           'nominated_pair_state', 'nominated_remote_ip', 'nominated_remote_type',
           'nominated_remote_is_advertised',
           'signal_url_scheme', 'signal_url_path',
           'selected_remote_ip', 'selected_remote_in_range', 'selected_local_ip',
           'transport_bytes_sent_positive', 'transport_bytes_received_positive',
           'rtp_out_bytes_sent_positive', 'rtp_in_bytes_received_positive',
           'ready', 'rewrite_log_present', 'node_ip_slots', 'tcp_mux_zero_log',
           'probe_exit', 'server_ports_released', 'binary_unchanged',
           'second_sigterm_sent', 'sigkill_required', 'mux_alt_bindable')
# Recorded for provenance but never compared, because each is a per-session random or
# ephemeral value that legitimately differs between two runs of the same daemon:
# timings, the negotiated ports, the minted join token and the SDK track SID. Keeping
# them out of COMPARE is asserted below so an edit cannot quietly make the lane flaky.
RECORDED_ONLY = ('connected_ms', 'selected_remote_port', 'sample_grow_ms',
                 'track_sources', 'token_sha256', 'ports', 'client', 'browser',
                 'server_sdk_version', 'page_sha256', 'probe_error')


def config_yaml(profile, ports, redis_port):
    mode = profile['mode']
    lines = ['redis:', f'  address: 127.0.0.1:{redis_port}', f'port: {ports["http"]}',
             # Loopback-only listeners: the lane never exposes the daemon on a real
             # interface, and the bind list is not what carries the node_ip rewrite.
             'bind_addresses: ["127.0.0.1"]',
             'keys:', f'  {API_KEY}: {API_SECRET}',
             'logging:', '  level: info', 'rtc:']
    if mode == 'range':
        # `udp_port: 0` disables the single mux port so the per-peer transports really
        # come from the range; leaving it unset would keep the 7882 default and make the
        # range a no-op.
        lines += [f'  udp_port: 0', f'  port_range_start: {ports["udp"]}',
                  f'  port_range_end: {ports["udp"] + UDP_RANGE - 1}']
    else:
        lines.append(f'  udp_port: {ports["udp"]}')
    lines.append(f'  tcp_port: {ports["tcp"]}')
    lines += ['  use_external_ip: false', '  enable_loopback_candidate: true',
              '  interfaces:', '    includes: [lo0]']
    node_ip = profile.get('node_ip')
    if isinstance(node_ip, str):
        lines.append(f'  node_ip: "{node_ip}"')
    return '\n'.join(lines) + '\n'


def daemon_argv(binary, profile, cfg_path, ports):
    argv = [str(binary), '--config', str(cfg_path)]
    node_ip = profile.get('node_ip')
    if isinstance(node_ip, list):
        argv += [f'{node_ip[0]}={node_ip[1]}']
    return argv


def first_line(text):
    return (text or '').split('\n')[0].strip()


def signal_url(receipt):
    """(scheme, path) of the join the SDK reports dialing, or (None, None)."""
    for line in (receipt or {}).get('console_lines') or []:
        m = re.search(r'signal connecting to (\w+)://[^/]+(/[^?\s]*)', line)
        if m:
            return m.group(1), m.group(2)
    return None, None


def error_class(text):
    """A stable identity for a JS rejection: the message, without stack or URLs."""
    line = first_line(text)
    line = re.sub(r'https?://\S+', '<url>', line)
    return line or None


def slots_from_log(text):
    """The oracle's `{"V4":..,"V6":..}` slot rendering, verbatim where present."""
    m = re.search(r'\{"V4":"[^"]*","V6":"[^"]*"\}', text or '')
    return redact(m.group(0)) if m else None


def candidate_surface(stats):
    """The candidate set the SDK's remote description actually carries.

    The `typ` token is the candidate CLASS (`host`/`srflx`/`prflx`/`relay`); the transport
    is a separate field, so an ICE-TCP host candidate reads `1 tcp ... typ host
    tcptype passive` and never contains the word `tcpcandidate`. Counting the transport, not
    a guessed token, is what makes the `rtc.tcp_port: 0` row discriminate.
    """
    hosts, types, srflx = [], set(), 0
    tcp = udp = prflx = 0
    for cand in (stats or {}).get('remote_candidates') or []:
        ip = redact(cand.get('ip'))
        typ = cand.get('type')
        types.add(typ)
        if cand.get('transport') == 'tcp':
            tcp += 1
        elif cand.get('transport') == 'udp':
            udp += 1
        if typ == 'host' and ip not in hosts:
            hosts.append(ip)
        if typ == 'srflx':
            srflx += 1
        if typ == 'prflx':
            prflx += 1
    return sorted(hosts), sorted(types), srflx, tcp, udp, prflx


def advertised_ports(stats):
    return sorted({int(c['port']) for c in (stats or {}).get('remote_candidates') or []
                   if str(c.get('port') or '').isdigit()})


def nominated_pair(stats):
    """The pair the SDK's own transport selected, i.e. the path actually consumed.

    `selected_pairs` is read through the transport's `selectedCandidatePairId`, so this is
    not a candidate the daemon merely offered.
    """
    for pair in (stats or {}).get('selected_pairs') or []:
        remote = pair.get('remote') or {}
        return {'state': pair.get('state'), 'remote_ip': redact(remote.get('ip')),
                'remote_type': remote.get('candidateType'),
                'remote_port': remote.get('port')}
    return {'state': None, 'remote_ip': None, 'remote_type': None, 'remote_port': None}


def any_positive(stats, key, entry_types=None, sub_key=None):
    """True when some recorded stats entry has a positive counter."""
    if not stats:
        return False
    for group in ('transports', 'candidate_pairs') if sub_key is None else (sub_key,):
        for entry in stats.get(group) or []:
            if entry_types and entry.get('entry_type') not in entry_types:
                continue
            if isinstance(entry.get(key), (int, float)) and entry[key] > 0:
                return True
    return False


def normalise(case_dir, daemon, probe, profile, ports):
    receipt = {}
    receipt_path = case_dir / 'sdk-receipt.json'
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_text())
    result = receipt.get('result') or {}
    stats = result.get('stats_second') or result.get('stats_first')
    hosts, types, srflx, tcp_count, udp_count, prflx_count = candidate_surface(stats)
    selected = (stats or {}).get('selected_pairs') or []
    remote = selected[0].get('remote') if selected else None
    local = selected[0].get('local') if selected else None
    log = (case_dir / 'server.log').read_text(errors='replace') if (case_dir / 'server.log').exists() else ''
    out = {
        'sdk_connected': bool(result) and result.get('connect_error') is None
                           and result.get('room_state') == 'connected',
        'room_state': result.get('room_state'),
        'can_update_subscriptions': result.get('can_update_subscriptions'),
        'tracks_published': result.get('tracks_published'),
        'audio_tracks': result.get('audio_tracks'),
        'video_tracks': result.get('video_tracks'),
        'track_sources': redact(result.get('track_sources') or []),
        'media_kind': result.get('media_kind'),
        'gum_result_class': ('ok' if str(result.get('gum_result') or '').startswith('ok:')
                             else error_class(result.get('gum_result'))),
        'connect_error_class': error_class(result.get('connect_error')),
        'publish_error_class': error_class(result.get('publish_error')),
        'peer_connection_count': (stats or {}).get('peer_connection_count', 0),
        'pc_connection_state': (stats or {}).get('pc_connection_state'),
        'pc_ice_connection_state': (stats or {}).get('pc_ice_connection_state'),
        'ice_states_seen': redact((stats or {}).get('pc_ice_states_seen') or []),
        'host_candidate_ips': hosts,
        'candidate_types': types,
        'has_tcpcandidate': tcp_count > 0,
        'tcp_candidate_count': tcp_count,
        'udp_candidate_count': udp_count,
        'prflx_signalled_count': prflx_count,
        'srflx_candidate_count': srflx,
        'nominated_pair_state': nominated_pair(stats)['state'],
        'nominated_remote_ip': nominated_pair(stats)['remote_ip'],
        'nominated_remote_type': nominated_pair(stats)['remote_type'],
        'selected_pair_count': len(selected),
        'selected_remote_ip': redact((remote or {}).get('ip')),
        'selected_remote_port': (remote or {}).get('port'),
        'selected_remote_in_range': bool(remote) and ports is not None
            and isinstance(remote.get('port'), int)
            and ports['udp'] <= remote['port'] < ports['udp'] + UDP_RANGE,
        'selected_local_ip': redact((local or {}).get('ip')),
        'transport_bytes_sent_positive': any_positive(stats, 'bytes_sent',
                                                     None, 'transports'),
        'transport_bytes_received_positive': any_positive(stats, 'bytes_received',
                                                          None, 'transports'),
        'rtp_out_bytes_sent_positive': any_positive(stats, 'bytes_sent',
                                                    {'outbound-rtp'}, 'rtp'),
        'rtp_in_bytes_received_positive': any_positive(stats, 'bytes_received',
                                                       {'inbound-rtp'}, 'rtp'),
        # The SDK logs the URL it actually dials, so the signal transport the row claims
        # to exercise is read from the client rather than inferred from the `--server`
        # argument: livekit-client@2.23.0 turns an `http://` origin into a `ws://` join.
        'signal_url_scheme': signal_url(receipt)[0],
        'signal_url_path': signal_url(receipt)[1],
        # Port numbers are ephemeral, so the consumption claim is expressed as identity:
        # is the port the SDK nominated one of the ports the daemon actually advertised?
        'nominated_remote_is_advertised': bool(
            nominated_pair(stats)['remote_port']
            and nominated_pair(stats)['remote_port'] in advertised_ports(stats)),
        'ports': dict(ports or {}),
        'ready': daemon.get('ready'),
        'rewrite_log_present': 'using explicit node IP for NAT1To1Ips' in log
                               or 'using explicit node IP' in log,
        'node_ip_slots': slots_from_log(log),
        'tcp_mux_zero_log': 'use of closed file' in log,
        'probe_exit': probe.get('exit'),
        'server_exit': daemon.get('server_exit'),
        'server_ports_released': daemon.get('server_ports_released'),
        'binary_unchanged': daemon.get('binary_unchanged'),
        'second_sigterm_sent': daemon.get('second_sigterm_sent'),
        'sigkill_required': daemon.get('sigkill_required'),
        'mux_alt_bindable': daemon.get('mux_alt_bindable'),
        'connected_ms': result.get('connected_ms'),
        'probe_error': error_class(receipt.get('error')),
        'client': {k: (receipt.get('client') or {}).get(k)
                   for k in ('package', 'version', 'sha256', 'patched')},
        'browser': {k: (receipt.get('browser') or {}).get(k)
                    for k in ('name', 'version', 'executable')},
        'server_sdk_version': ((receipt.get('server_sdk') or {}).get('version')),
        'page_sha256': (receipt.get('page') or {}).get('sha256'),
        'token_sha256': receipt.get('token_sha256'),
        'sample_grow_ms': None,
    }
    # The two samples exist to prove counters MOVE; a single positive counter could be a
    # stale pre-existing value.
    first = result.get('stats_first') or {}
    second = result.get('stats_second') or {}
    if first and second:
        out['sample_grow_ms'] = 2000
        out['counters_grew'] = (json.dumps(first.get('transports'))
                                != json.dumps(second.get('transports')))
    else:
        out['counters_grew'] = False
    return redact(out)


def run_case(binary, profile, side, work, redis_port, ports):
    case = work / f'{profile["name"]}-{side}'
    case.mkdir(parents=True)
    cfg_text = config_yaml(profile, ports, redis_port)
    cfg = case / 'server.yaml'
    cfg.write_text(cfg_text)
    (case / 'config.sha256').write_text(hashlib.sha256(cfg_text.encode()).hexdigest() + '\n')
    argv = daemon_argv(binary, profile, cfg, ports)
    (case / 'argv.txt').write_text(' '.join(argv) + '\n')
    before = sha(binary)
    log_path = case / 'server.log'
    out_path = case / 'server.stdout'
    daemon = {}
    with log_path.open('wb') as err, out_path.open('wb') as out:
        proc = subprocess.Popen(argv, cwd=str(case), stdout=out, stderr=err,
                                env=base_env(), stdin=subprocess.DEVNULL,
                                start_new_session=True)
        # The daemon can only bind the ports once this process stops holding them.
        release_reservations()
        daemon['ready'] = wait_ready(proc, ports['http'])
        if profile['mode'] == 'mux' and daemon['ready']:
            # Mechanical witness for the C30 consequence: is the rewritten address:port
            # actually held by the daemon, or free for us to bind?
            try:
                probe_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                probe_sock.bind(('127.0.0.2', ports['udp']))
                probe_sock.close()
                daemon['mux_alt_bindable'] = True
            except OSError:
                daemon['mux_alt_bindable'] = False
        else:
            daemon['mux_alt_bindable'] = None
        probe = {}
        if daemon['ready']:
            cmd = ['node', str(HERE / 'probe.mjs'), '--server',
                   f'{profile["scheme"]}://127.0.0.1:{ports["http"]}',
                   '--out', str(case / 'sdk-receipt.json'), '--key', API_KEY,
                   '--secret', API_SECRET, '--room', f'c99-{profile["name"]}-{side}',
                   '--identity', 'c99-viewer', '--budget-ms', '70000',
                   '--media', MEDIA]
            res = subprocess.run(cmd, cwd=str(REPO / 'tests/interop'), capture_output=True,
                                 text=True, timeout=200)
            probe = {'exit': res.returncode, 'stderr': res.stderr[-2000:]}
            (case / 'probe.stderr').write_text(res.stderr)
            (case / 'probe.stdout').write_text(res.stdout)
        else:
            probe = {'exit': None, 'stderr': 'daemon never became ready'}
        daemon.update(terminate(proc))
    media_ports = (range(ports['udp'], ports['udp'] + UDP_RANGE) if profile['mode'] == 'range'
                   else [ports['udp']])
    daemon['server_ports_released'] = (not tcp_bound(ports['http'])
                                       and all(udp_free(p) for p in media_ports)
                                       and (ports['tcp'] == 0 or not tcp_bound(ports['tcp'])))
    daemon['binary_unchanged'] = sha(binary) == before
    daemon['log_bytes'] = log_path.stat().st_size
    shutil.copyfile(log_path, work.parent / f'{profile["name"]}-{side}.log')
    (work.parent / f'{profile["name"]}-{side}.yaml').write_text(cfg_text)
    return normalise(case, daemon, probe, profile, ports)


p = argparse.ArgumentParser()
p.add_argument('--go', type=pathlib.Path, help='fixed Go oracle binary')
p.add_argument('--rust', type=pathlib.Path, help='Rust candidate binary')
p.add_argument('--output', type=pathlib.Path)
p.add_argument('--reference', type=pathlib.Path, default=REPO)
p.add_argument('--only', default='', help='comma-separated profile names')
p.add_argument('--media', default='audio', choices=('audio', 'video'),
               help='getUserMedia kind the SDK publishes with; the receipt records the '
                    'kind and the gum outcome per leg, and the gate declares gum_result_class '
                    'so a capture failure cannot be misread as an ICE failure')
p.add_argument('--report', type=pathlib.Path, default=None,
               help='recompute the verdict from a persisted execution.json; no daemon runs')
p.add_argument('--selftest-gate', type=pathlib.Path, default=None,
               help='re-derive every row from a persisted execution.json and inject one '
                    'fault per gate branch; proves the verdict is a function of the '
                    'measurements. Writes gate-falsification.json, never touches the '
                    'receipt. No daemon runs.')
a = p.parse_args()
if a.selftest_gate and a.report:
    p.error('--selftest-gate and --report are mutually exclusive offline modes')
if a.selftest_gate:
    # Both offline modes read a persisted receipt, so they share that plumbing.
    a.selftest_gate = a.selftest_gate.resolve()
    a.report = a.selftest_gate
    a.output = a.report.parent
elif a.report:
    a.report = a.report.resolve()
    a.output = a.report.parent
else:
    for name in ('go', 'rust', 'output'):
        if getattr(a, name) is None:
            p.error(f'--{name} is required unless --report/--selftest-gate is used')
    a.output = a.output.resolve()
    a.output.mkdir(parents=True, exist_ok=False)

PINNED_SDK_SHA256 = 'fb646f2260d84f91238d0a2dc10726c456631d6a26c7cafc43cfee5959b4d31d'
MEDIA = a.media


def start_redis(work):
    port = free_band_tcp()
    conf = work / 'redis.conf'
    conf.write_text(f'port {port}\nbind 127.0.0.1\nsave ""\ndir {work}\n'
                    f'dbfilename owned-c99-sdk.rdb\nlogfile {work}/redis.log\n')
    proc = subprocess.Popen(['redis-server', str(conf)], stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, start_new_session=True)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if subprocess.run(['redis-cli', '-p', str(port), 'PING'],
                          capture_output=True).stdout.strip() == b'PONG':
            return proc, port
        time.sleep(0.1)
    raise SystemExit('owned redis never answered PING')


def redis_dbsize(port):
    out = subprocess.run(['redis-cli', '-p', str(port), 'DBSIZE'],
                         capture_output=True, text=True).stdout.strip()
    return out


def finish(manifest, out, write=True):
    """Recompute the verdict from the per-profile records in `manifest`.

    Kept separate from the launch loop so a persisted execution.json can be re-checked
    without starting a single daemon or browser.
    """
    if write:
        refuse_filed_slot(out)
    P = manifest['profiles']
    parity = [n for n, e in P.items() if e['group'] == 'parity']
    fences = [n for n, e in P.items() if e['group'] == 'fence']
    missing = [n for n, e in P.items() if not e.get('declared')]
    failed_parity, failed_fences = [], []
    for name in parity:
        e = P[name]
        if e['declared_mismatches'] or e['cross_mismatches']:
            failed_parity.append(name)
    for name in fences:
        if not P[name]['fence_reproduces']:
            failed_fences.append(name)
    controls = manifest.setdefault('controls', {})

    def all_cases():
        for name in P:
            for side in ('go', 'rust'):
                yield name, side, P[name][side]

    for name, side, rec in all_cases():
        controls[f'probe_ran_{name}_{side}'] = rec.get('probe_exit') == 0

    controls['all_profiles_measured'] = len(P) == len(PROFILES)
    controls['volatile_fields_excluded_from_compare'] = not (
        set(COMPARE) & set(RECORDED_ONLY))
    # A run whose declared rows are still empty is a measurement, never a PASS.
    controls['declared_expectations_present'] = not missing and bool(P)
    for side in ('go', 'rust'):
        controls[f'lane_connects_positive_control_{side}'] = (
            P.get('baseline-no-node-ip', {}).get(side, {}).get('sdk_connected') is True)
        controls[f'rewrite_changes_advertised_text_{side}'] = (
            P.get('baseline-no-node-ip', {}).get(side, {}).get('host_candidate_ips')
            != P.get('scalar-v4-mapped', {}).get(side, {}).get('host_candidate_ips')
            and bool(P.get('scalar-v4-mapped', {}).get(side, {}).get('host_candidate_ips')))
        controls[f'media_over_rewritten_candidate_{side}'] = bool(
            P.get('scalar-v4-mapped-self', {}).get(side, {}).get('rtp_out_bytes_sent_positive')
            and P.get('scalar-v4-mapped-self', {}).get(side, {}).get('counters_grew')
            and P.get('scalar-v4-mapped-self', {}).get(side, {}).get('nominated_remote_type')
            == 'host'
            and P.get('scalar-v4-mapped-self', {}).get(side, {}).get('host_candidate_ips')
            != P.get('scalar-v4-mapped', {}).get(side, {}).get('host_candidate_ips'))
        controls[f'flag_channel_reaches_client_{side}'] = (
            P.get('flag-v4-mapped', {}).get(side, {}).get('host_candidate_ips')
            == P.get('scalar-v4-mapped', {}).get(side, {}).get('host_candidate_ips')
            and bool(P.get('flag-v4-mapped', {}).get(side, {}).get('host_candidate_ips')))
        controls[f'family_scoping_control_{side}'] = (
            P.get('v6-only-control', {}).get(side, {}).get('host_candidate_ips')
            == P.get('baseline-no-node-ip', {}).get(side, {}).get('host_candidate_ips')
            and P.get('v6-only-control', {}).get(side, {}).get('sdk_connected') is True)
        # Measured, not assumed: the single-mux leg DOES connect. What the leg proves is
        # that the daemon advertises a rewritten endpoint no socket holds on this host
        # (mux_alt_bindable False) and the client then finishes ICE through a candidate
        # that was never signalled. Asserting a failed connect here would fail on correct
        # behaviour, so the control asserts the observed shape instead.
        controls[f'mux_advertises_unbindable_target_and_connects_via_prflx_{side}'] = (
            P.get('mux-single-port', {}).get(side, {}).get('host_candidate_ips')
            == ['127.0.0.2']
            and P.get('mux-single-port', {}).get(side, {}).get('mux_alt_bindable') is False
            and P.get('mux-single-port', {}).get(side, {}).get('sdk_connected') is True
            and P.get('mux-single-port', {}).get(side, {}).get('nominated_remote_type')
            == 'prflx')
        controls[f'sdk_normalises_http_origin_to_ws_{side}'] = (
            P.get('scheme-http-origin', {}).get(side, {}).get('signal_url_scheme') == 'ws'
            and P.get('scheme-http-origin', {}).get(side, {}).get('peer_connection_count') == 1
            and P.get('scheme-http-origin', {}).get(side, {}).get('sdk_connected') is True)
    for side in ('go', 'rust'):
        controls[f'advertised_rewrite_text_matches_node_ip_{side}'] = (
            P.get('scalar-v4-mapped', {}).get(side, {}).get('host_candidate_ips')
            == ['127.0.0.2']
            and P.get('flag-v4-mapped', {}).get(side, {}).get('host_candidate_ips')
            == ['127.0.0.2'])
        # The row is about the leaf being consumed, so the default surface must offer an
        # ICE-TCP candidate for the zero case to mean anything.
        controls[f'tcp_port_zero_removes_ice_tcp_candidate_{side}'] = (
            (P.get('baseline-no-node-ip', {}).get(side, {}).get('tcp_candidate_count') or 0) > 0
            and P.get('tcp-port-zero-surface', {}).get(side, {}).get('tcp_candidate_count') == 0)
    controls['sdk_client_pinned_and_unpatched'] = all(
        rec.get('client', {}).get('sha256') == PINNED_SDK_SHA256
        and rec.get('client', {}).get('patched') is False
        and rec.get('client', {}).get('version') == '2.23.0'
        for _, _, rec in all_cases())
    controls['same_config_across_sides'] = all(
        norm_ports(e['go_yaml_text']) == norm_ports(e['rust_yaml_text'])
        for e in P.values() if 'go_yaml_text' in e)
    controls['go_binary_is_fixed_oracle'] = (
        manifest['binaries']['go']['sha256'] == FIXED_GO_SHA256)
    # The launch recorded each binary's hash. Re-derive it on every finish, launched or offline,
    # so a receipt that is re-read after a rebuild names the moved identity instead of reporting
    # a green gate over a binary that is no longer the one under test.
    manifest['binary_hash_now'] = {
        side: (sha(manifest['binaries'][side]['path'])
               if pathlib.Path(manifest['binaries'][side].get('path', '')).exists() else None)
        for side in ('go', 'rust')}
    controls['binary_hash_now_matches_recorded'] = all(
        manifest['binary_hash_now'][side] == manifest['binaries'][side]['sha256']
        for side in ('go', 'rust'))
    # Same reasoning for the harness itself: this file defines the declarations the rows compare,
    # so a receipt read back after it was edited is provenance about an older lane, and the only
    # way a reader learns that is if the receipt says so.
    manifest['harness_hash_now'] = sha(HERE / 'run-paired.py')
    recorded_harness = (manifest.get('harness') or {}).get('sha256', '')
    controls['harness_hash_now_matches_recorded'] = bool(recorded_harness) and (
        manifest['harness_hash_now'] == recorded_harness)
    controls['binaries_immutable'] = all(rec.get('binary_unchanged') is True
                                        for _, _, rec in all_cases())
    controls['clean_startup_and_teardown'] = all(
        rec.get('ready') is True and rec.get('server_exit') == 0
        and rec.get('server_ports_released') is True for _, _, rec in all_cases())
    controls['shutdown_shape_matches_oracle'] = all(
        (e['go'].get('second_sigterm_sent'), e['go'].get('sigkill_required'))
        == (e['rust'].get('second_sigterm_sent'), e['rust'].get('sigkill_required'))
        for e in P.values())
    # macOS gives 127.0.0.0/8 to lo0 as a whole, so an unassigned loopback address cannot
    # be bound and a socket bound to 127.0.0.1 never receives traffic for it. Both sides
    # therefore advertise a target this host cannot deliver to, which is why the fenced
    # legs claim ADVERTISEMENT only and consumption is carried by the mapped-self leg.
    controls['mux_alternate_address_not_bindable_on_host'] = bool(
        P.get('mux-single-port')) and all(
        v is False for v in P['mux-single-port']['side_data']['mux_single_bindable'].values())
    controls['fence_divergence_reproduces'] = all(
        P[n]['fence_reproduces'] for n in fences) and bool(fences)
    text = json.dumps({k: v for k, v in manifest['profiles'].items()}, ensure_ascii=False)
    controls['receipt_has_no_machine_address'] = not leaks(text)
    manifest['counts'] = {'parity': len(parity), 'fences': len(fences),
                          'failed_parity': failed_parity, 'failed_fences': failed_fences,
                          'controls_total': len(controls),
                          'controls_failed': [k for k, v in controls.items() if not v]}
    if missing:
        manifest['status'] = 'MEASURED_SHAPE_NEEDS_DECLARATIONS'
    elif failed_parity:
        manifest['status'] = 'FAIL_SDK_NODE_IP_PARITY'
    elif failed_fences:
        # Checked before the controls on purpose: `fence_divergence_reproduces` aggregates
        # every fence, so a single stale fence would otherwise always be reported as a
        # generic control failure and this branch could never fire.
        manifest['status'] = 'FAIL_SDK_NODE_IP_FENCES'
    elif manifest['counts']['controls_failed']:
        manifest['status'] = 'FAIL_SDK_NODE_IP_CONTROLS'
    elif fences:
        manifest['status'] = 'PARTIAL_LAUNCHED_MAIN_SDK_NODE_IP_WITH_FENCED_DIVERGENCES'
    else:
        manifest['status'] = 'PASS_LAUNCHED_MAIN_SDK_NODE_IP_CANDIDATE'
    if write:
        tmp = out / 'execution.tmp'
        tmp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
        tmp.replace(out / 'execution.json')
    print(json.dumps({'status': manifest['status'], 'counts': manifest['counts'],
                      'missing_declarations': missing}, indent=2))
    return 0 if manifest['status'].startswith('PASS') or manifest['status'].startswith('PARTIAL') else 1


def declare(profile, entry, side, declared):
    mismatches = {}
    for key, want in declared.items():
        got = entry[side].get(key)
        if got != want:
            mismatches[key] = {'want': want, 'got': got}
    entry[side + '_declared_ok'] = not mismatches
    entry[side + '_declared_mismatches'] = mismatches


PROFILES_BY_NAME = {pr['name']: pr for pr in PROFILES}
BASE_COMPARE = tuple(COMPARE)
COMPARE_GLOBALS = globals()


def derive_entry(profile, yaml_sources):
    """Turn the two per-side records into the declared/cross/fence verdict for one row.

    Kept as one function so `--selftest-gate` re-derives real persisted receipts through
    the SAME code the launch loop uses, instead of maintaining a second copy of the rules
    that could drift green.
    """
    entry = {'name': profile['name'], 'group': profile['group'], 'mode': profile['mode'],
             'scheme': profile['scheme'], 'node_ip': redact(profile.get('node_ip')),
             'why': profile['why'], 'declared': redact(profile.get('expect') or {}),
             'side_data': {}}
    for side in ('go', 'rust'):
        src = yaml_sources[side]
        entry[side] = src['rec']
        entry[side + '_yaml_text'] = redact(src['yaml'])
    entry['side_data']['mux_single_bindable'] = {
        side: entry[side].get('mux_alt_bindable') for side in ('go', 'rust')}
    declared = redact(profile.get('expect') or {})
    for side in ('go', 'rust'):
        declare(profile, entry, side, declared)
    entry['declared_mismatches'] = {
        s: entry[f'{s}_declared_mismatches'] for s in ('go', 'rust')
        if entry[f'{s}_declared_mismatches']}
    entry['declared_ok'] = (entry['go_declared_ok'] and entry['rust_declared_ok'])
    cross = {}
    for key in COMPARE:
        if entry['go'].get(key) != entry['rust'].get(key):
            cross[key] = {'go': entry['go'].get(key), 'rust': entry['rust'].get(key)}
    entry['cross_equal'] = not cross
    entry['cross_mismatches'] = cross
    if profile['group'] == 'fence':
        want = redact(profile['divergence'])
        entry['fence_expected'] = want
        diverging = {k for kv in want.values() for k in kv}
        entry['fence_divergence_reproduces'] = all(
            entry[side].get(k) == v for side, kv in want.items()
            for k, v in kv.items())
        entry['fence_other_fields_equal'] = not (set(entry['cross_mismatches']) - diverging)
        entry['fence_shared_claims_hold'] = not any(
            entry[f'{side}_declared_mismatches'] for side in ('go', 'rust'))
        entry['fence_reproduces'] = (entry['fence_divergence_reproduces']
                                     and entry['fence_other_fields_equal']
                                     and entry['fence_shared_claims_hold'])
    return entry


GATE_INJECTIONS = (
    ('declared_rows_emptied', 'MEASURED_SHAPE_NEEDS_DECLARATIONS',
     'a gate with nothing declared must never read as a pass'),
    ('parity_declaration_wrong', 'FAIL_SDK_NODE_IP_PARITY',
     'the Go-derived row is checked per side, so a wrong claim fails both lanes'),
    ('fence_stops_reproducing', 'FAIL_SDK_NODE_IP_FENCES',
     'an absorbed or stale fence cannot silently become parity'),
    ('client_bundle_tampered', 'FAIL_SDK_NODE_IP_CONTROLS',
     'the lane only claims the ORIGINAL SDK, so a patched bundle invalidates it'),
    ('oracle_identity_broken', 'FAIL_SDK_NODE_IP_CONTROLS',
     'the comparison is only meaningful against the locked Go binary'),
    ('binary_hash_drifted', 'FAIL_SDK_NODE_IP_CONTROLS',
     'a receipt re-read after the candidate was rebuilt must name the moved binary'),
    ('harness_hash_drifted', 'FAIL_SDK_NODE_IP_CONTROLS',
     'a receipt re-read after the harness was edited describes an older lane, not this one'),
    ('rust_needs_sigkill', 'FAIL_SDK_NODE_IP_PARITY',
     'an unclean shutdown is a behavioural divergence, not noise'),
    ('volatile_field_compared', 'FAIL_SDK_NODE_IP_CONTROLS',
     'comparing a per-session random value would make the lane flaky'),
    ('unredacted_address_in_receipt', 'FAIL_SDK_NODE_IP_CONTROLS',
     'committed evidence must not carry a machine-local address'),
)


def refuse_filed_slot(path):
    """--report rewrites the receipt beside itself and --selftest-gate drops
    gate-falsification.json next to it, so either one aimed at a committed evidence slot destroys
    the evidence it was meant to check. Filed slots are immutable: copy and re-derive the copy."""
    resolved = pathlib.Path(path).resolve()
    if '/server/evidence/' in str(resolved):
        raise SystemExit(json.dumps({
            'error': 'refusing_to_touch_filed_evidence', 'path': str(resolved),
            'why': 'finish() writes execution.json beside the receipt it reads and the gate sweep '
                   'writes gate-falsification.json beside it',
            'use': 'cp the receipt to a scratch dir and run --report/--selftest-gate on the copy'}))


def gate_selftest(manifest, out):
    """Prove each gate branch can fail, using the persisted records as the input.

    Every injection is applied to a fresh rebuild of the entries (derive_entry over the
    stored per-side records), so a passing self-test shows the verdict is a function of the
    measurements rather than a constant.
    """
    refuse_filed_slot(out)
    def rebuild(mutate=None):
        by_name = {pr['name']: pr for pr in PROFILES}
        fresh = {'binaries': json.loads(json.dumps(manifest['binaries'])),
                 'harness': json.loads(json.dumps(manifest.get('harness', {}))),
                 'profiles': {}}
        for name, stored in manifest['profiles'].items():
            pr = by_name.get(name)
            if pr is None:
                continue
            entry = derive_entry(pr, {side: {'rec': json.loads(json.dumps(stored[side])),
                                             'yaml': stored.get(side + '_yaml_text', '')}
                                     for side in ('go', 'rust')})
            fresh['profiles'][name] = entry
        if mutate:
            mutate(fresh)
        return fresh

    def base(_):
        return None

    untouched = rebuild(base)
    finish(untouched, out, write=False)
    rows = [{'case': 'untouched_measurement', 'injected': 'nothing',
             'expect_prefix': 'PARTIAL_LAUNCHED_MAIN_SDK_NODE_IP',
             'status': untouched['status'],
             'controls_failed': untouched['counts']['controls_failed']}]

    def inj_declared(m):
        for e in m['profiles'].values():
            e['declared'] = {}

    def inj_parity(m):
        e = m['profiles']['v6-only-control']
        e['declared']['host_candidate_ips'] = ['127.0.0.9']
        for side in ('go', 'rust'):
            declare(PROFILES_BY_NAME['v6-only-control'], e, side, e['declared'])
        e['declared_mismatches'] = {s: e[f'{s}_declared_mismatches']
                                   for s in ('go', 'rust') if e[f'{s}_declared_mismatches']}

    def inj_fence(m):
        for e in m['profiles'].values():
            if e['group'] == 'fence':
                e['fence_reproduces'] = False

    def inj_client(m):
        for e in m['profiles'].values():
            for side in ('go', 'rust'):
                e[side]['client'] = {'sha256': 'tampered', 'patched': True,
                                     'version': '2.23.0'}

    def inj_binaries(m):
        m['binaries']['go']['sha256'] = 'not-the-fixed-oracle'
        for e in m['profiles'].values():
            for side in ('go', 'rust'):
                e[side]['binary_unchanged'] = False

    def inj_hash_drift(m):
        m['binaries']['rust']['sha256'] = 'stale-hash-from-an-older-build'

    def inj_harness_drift(m):
        m['harness']['sha256'] = 'stale-hash-from-an-older-harness'

    def inj_shutdown(m):
        for e in m['profiles'].values():
            for side in ('go', 'rust'):
                e[side]['server_exit'] = -9
                e[side]['sigkill_required'] = True
            declare(PROFILES_BY_NAME[e['name']], e, 'rust', e['declared'])
            e['declared_mismatches'] = {s: e[f'{s}_declared_mismatches']
                                       for s in ('go', 'rust') if e[f'{s}_declared_mismatches']}
            e['declared_ok'] = not e['declared_mismatches']

    def inj_volatile(_m):
        COMPARE_GLOBALS['COMPARE'] = tuple(COMPARE_GLOBALS['COMPARE']) + ('connected_ms',)

    def inj_leak(m):
        e = m['profiles']['baseline-no-node-ip']
        e['rust']['host_candidate_ips'] = ['198.18.0.7']

    mutators = {'declared_rows_emptied': inj_declared, 'parity_declaration_wrong': inj_parity,
                'fence_stops_reproducing': inj_fence, 'client_bundle_tampered': inj_client,
                'oracle_identity_broken': inj_binaries,
                'binary_hash_drifted': inj_hash_drift,
                'harness_hash_drifted': inj_harness_drift,
                'rust_needs_sigkill': inj_shutdown,
                'volatile_field_compared': inj_volatile,
                'unredacted_address_in_receipt': inj_leak}
    for name, expect, why in GATE_INJECTIONS:
        mutate = mutators[name]
        COMPARE_GLOBALS['COMPARE'] = tuple(BASE_COMPARE)
        fresh = rebuild(mutate)
        finish(fresh, out, write=False)
        status = fresh['status']
        failed = fresh['counts']['controls_failed']
        rows.append({'case': name, 'injected': why, 'expect_prefix': expect,
                     'status': status, 'controls_failed': failed,
                     'discriminated': status.startswith(expect)})
    COMPARE_GLOBALS['COMPARE'] = tuple(BASE_COMPARE)
    rows[0]['discriminated'] = rows[0]['status'].startswith(
        'PARTIAL_LAUNCHED_MAIN_SDK_NODE_IP')
    outd = out / 'gate-falsification.json'
    outd.write_text(json.dumps({'cases': rows,
                                'all_discriminated': all(r['discriminated'] for r in rows)},
                               indent=2) + '\n')
    print(json.dumps({'all_discriminated': all(r['discriminated'] for r in rows),
                      'cases': {r['case']: r['discriminated'] for r in rows}}, indent=2))
    return 0 if all(r['discriminated'] for r in rows) else 1


def main():
    work = a.output / 'work'
    work.mkdir()
    go, rust = a.go.resolve(), a.rust.resolve()
    ref = a.reference.resolve()
    redis_proc, redis_port = start_redis(work)
    page_bytes = (HERE / 'page.html').read_bytes()
    probe_bytes = (HERE / 'probe.mjs').read_bytes()
    manifest = {
        'status': 'INCOMPLETE',
        'leafs': COVERS,
        'tier': 'official_differential_lifecycle',
        'scope': ('launched-MAIN rtc.node_ip consumption observed by the pinned official '
                  'livekit-client UMD build in headless Chromium: the IPv4-mapped scalar '
                  'and generated-flag rewrites as they appear in the SDP the CLIENT '
                  'receives, the candidate the client NOMINATES, and whether RTP '
                  'actually egresses through it (the published media kind is recorded per '
                  'launch); explicit IPv4 precedence in a comma pair; '
                  'the V6-only family-scoping control; the single-mux unroutable-candidate '
                  'consequence; and the SDK@2.23.0 ws-vs-http signal-scheme row. Loopback '
                  'only, no TURN, no public network.'),
        'binaries': {
            'go': {'path': str(go), 'sha256': sha(go),
                   'is_fixed_oracle': sha(go) == FIXED_GO_SHA256,
                   'reference_commit': FIXED_GO_COMMIT},
            'rust': {'path': str(rust), 'sha256': sha(rust)},
        },
        'client': {
            'package': 'livekit-client', 'version': '2.23.0',
            'path': str(REPO / 'tests/interop/node_modules/livekit-client/dist/livekit-client.umd.js'),
            'sha256': sha(REPO / 'tests/interop/node_modules/livekit-client/dist/livekit-client.umd.js'),
            'served_byte_for_byte': True, 'patched': False,
            'page_html': {'path': str(HERE / 'page.html'),
                          'sha256': hashlib.sha256(page_bytes).hexdigest()},
            'probe_mjs': {'path': str(HERE / 'probe.mjs'),
                          'sha256': hashlib.sha256(probe_bytes).hexdigest()},
            'observation_only_tap': 'RTCPeerConnection constructor records the object the '
                                    'SDK created; no SDK or page method is replaced',
        },
        'harness': {'path': str(HERE / 'run-paired.py'),
                    'sha256': sha(HERE / 'run-paired.py')},
        'reference': {
            'rust_head': subprocess.run(['git', '-C', str(ref), 'rev-parse', 'HEAD'],
                                        capture_output=True, text=True).stdout.strip(),
            'rust_dirty_paths': subprocess.run(['git', '-C', str(ref), 'status', '--short'],
                                               capture_output=True, text=True).stdout.splitlines(),
            'go_checkout': str(ref / 'references/livekit-stable'),
            'go_checkout_commit': subprocess.run(
                ['git', '-C', str(ref / 'references/livekit-stable'), 'rev-parse', 'HEAD'],
                capture_output=True, text=True).stdout.strip(),
        },
        'profiles': {},
    }
    path = a.output / 'execution.json'

    def save():
        tmp = a.output / 'execution.tmp'
        tmp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
        tmp.replace(path)

    save()
    selected = [pr for pr in PROFILES if not a.only or pr['name'] in a.only.split(',')]
    try:
        for profile in selected:
            case_data = {}
            for side, binary in (('go', go), ('rust', rust)):
                ports = {'http': reserve_tcp(), 'tcp': reserve_tcp(),
                         'udp': reserve_udp(UDP_RANGE)}
                if profile.get('tcp_port') == 0:
                    ports['tcp'] = 0
                case_work = work / profile['name']
                case_work.mkdir(exist_ok=True)
                case_data[side] = {
                    'rec': run_case(binary, profile, side, case_work, redis_port, ports),
                    'yaml': (case_work / f'{profile["name"]}-{side}' /
                             'server.yaml').read_text()}
            entry = derive_entry(profile, case_data)
            manifest['profiles'][profile['name']] = entry
            save()
    finally:
        manifest['redis_dbsize_before_flush'] = redis_dbsize(redis_port)
        subprocess.run(['redis-cli', '-p', str(redis_port), 'FLUSHALL'], capture_output=True)
        manifest['redis_dbsize_after_flush'] = redis_dbsize(redis_port)
        os_kill_group(redis_proc.pid)
        redis_proc.wait(timeout=20)
    return finish(manifest, a.output)


if __name__ == '__main__':
    if a.selftest_gate:
        target = a.selftest_gate.resolve()
        sys.exit(gate_selftest(json.loads(target.read_text()), target.parent))
    if a.report:
        sys.exit(finish(json.loads(a.report.read_text()), a.report.parent))
    sys.exit(main())

