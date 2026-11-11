#!/usr/bin/env python3
"""Generate the W1 config differential case matrix.

Usage: gen_cases.py <go-leaves.json> <cases.json>

<go-leaves.json> is written by the Go observer in leaves mode
(CONFIG_OBSERVER_LEAVES), so the per-leaf matrix covers every generated flag
of the upstream build automatically.

Case schema: {"id", "group", "leaf"?, "argv": [...], "env": {...},
"files": {name: body}, "file_modes": {name: "0600"}}. "{dir}" in argv, env
values and file bodies is replaced by the case's temp directory.
"""
import json
import sys

KEY = "devkey: secretsecretsecretsecretsecretsecret"
KEYS_ENV = {"LIVEKIT_KEYS": KEY}
SECRET = "secretsecretsecretsecretsecretsecret"


def case(cid, group, argv=(), env=None, files=None, modes=None, leaf=None):
    c = {"id": cid, "group": group, "argv": list(argv), "env": dict(env or {}),
         "files": dict(files or {}), "file_modes": dict(modes or {})}
    if leaf:
        c["leaf"] = leaf
    return c


def fmt_num(x):
    return str(int(x)) if float(x).is_integer() and "e" not in repr(x) else repr(x)


def value_for(leaf):
    """Non-default valid value as (flag/env text, YAML scalar text)."""
    kind, d, name = leaf["kind"], leaf["default"], leaf["name"]
    if kind == "Bool":
        v = "false" if d == "true" else "true"
        return v, v
    if kind in ("Int", "Uint", "Uint64"):
        n = int(d) if d not in ("",) else 0
        v = str(7 if n == 0 else n + 1)
        return v, v
    if kind == "Float":
        x = float(d) if d else 0.0
        v = "1.25" if x == 0 else repr(x + 0.5)
        return v, v
    if kind == "Duration":
        v = "2m" if d == "1m30s" else "1m30s"
        return v, json.dumps(v)
    if kind == "Str":
        v = "x-" + name
        return v, json.dumps(v)
    raise SystemExit("unknown kind " + kind)


def yaml_nested(path, scalar):
    parts = path.split(".")
    lines = []
    for i, p in enumerate(parts):
        ind = "  " * i
        lines.append(f"{ind}{p}:" + (f" {scalar}" if i == len(parts) - 1 else ""))
    return "\n".join(lines) + "\n"


def leaf_cases(leaves):
    out = []
    for leaf in leaves:
        name = leaf["name"]
        flag_v, yaml_v = value_for(leaf)
        env = leaf["env"] or "LIVEKIT_" + name.upper().replace(".", "_")
        out.append(case(f"leaf.yaml.{name}", "leaf-yaml", ["--config", "{dir}/c.yaml"], KEYS_ENV,
                        {"c.yaml": yaml_nested(name, yaml_v)}, leaf=name))
        out.append(case(f"leaf.flag.{name}", "leaf-flag", [f"--{name}={flag_v}"], KEYS_ENV, leaf=name))
        out.append(case(f"leaf.env.{name}", "leaf-env", [], {**KEYS_ENV, env: flag_v}, leaf=name))
    return out


def baseline_cases():
    return [
        case("base.defaults", "baseline"),
        case("base.defaults.keys", "baseline", env=KEYS_ENV),
        case("base.dev", "baseline", ["--dev"]),
        case("base.dev.keys", "baseline", ["--dev", "--keys", KEY]),
        case("base.dev.bind_lan", "baseline", ["--dev", "--bind", "192.168.1.10"]),
        case("base.dev.bind_loopback", "baseline", ["--dev", "--bind", "127.0.0.1"]),
        case("base.dev.yaml_bind_empty", "baseline", ["--dev", "--config", "{dir}/c.yaml"],
             files={"c.yaml": "bind_addresses: []\n"}),
        case("base.development_yaml", "baseline", ["--config", "{dir}/c.yaml"],
             files={"c.yaml": "development: true\n"}),
        case("base.help", "baseline", ["--help"]),
        case("base.version", "baseline", ["--version"]),
    ]


def precedence_cases():
    c = []
    # one leaf per kind: YAML vs flag vs env
    trio = [("rtc.tcp_port", "7001", "7002", "7003"),
            ("rtc.use_ice_lite", "true", "false", "true"),
            ("logging.level", '"warn"', "error", "info"),
            ("rtc.pli_throttle.low_quality", '"1s"', "2s", "3s"),
            ("node_selector.cpu_load_limit", "0.5", "0.6", "0.7"),
            ("rtc.data_channel_max_buffered_amount", "11", "12", "13")]
    for path, y, f, e in trio:
        env = "LIVEKIT_" + path.upper().replace(".", "_")
        files = {"c.yaml": yaml_nested(path, y)}
        c.append(case(f"prec.yaml_flag.{path}", "precedence", ["--config", "{dir}/c.yaml", f"--{path}={f}"], KEYS_ENV, files))
        c.append(case(f"prec.yaml_env.{path}", "precedence", ["--config", "{dir}/c.yaml"], {**KEYS_ENV, env: e}, files))
        c.append(case(f"prec.flag_env.{path}", "precedence", [f"--{path}={f}"], {**KEYS_ENV, env: e}))
        c.append(case(f"prec.all.{path}", "precedence", ["--config", "{dir}/c.yaml", f"--{path}={f}"], {**KEYS_ENV, env: e}, files))
    # config-body vs config file
    c.append(case("prec.body_vs_file", "precedence", ["--config", "{dir}/c.yaml", "--config-body", "port: 7002\n"], KEYS_ENV,
                  {"c.yaml": "port: 7001\n"}))
    c.append(case("prec.body_env_vs_file", "precedence", ["--config", "{dir}/c.yaml"], {**KEYS_ENV, "LIVEKIT_CONFIG": "port: 7003\n"},
                  {"c.yaml": "port: 7001\n"}))
    c.append(case("prec.body_empty_vs_file", "precedence", ["--config", "{dir}/c.yaml", "--config-body", ""], KEYS_ENV,
                  {"c.yaml": "port: 7001\n"}))
    c.append(case("prec.body_flag_vs_env", "precedence", ["--config-body", "port: 7004\n"], {**KEYS_ENV, "LIVEKIT_CONFIG": "port: 7005\n"}))
    # base flags vs YAML vs env
    y = {"c.yaml": ("keys:\n  ykey: " + SECRET + "\nregion: yregion\nredis:\n  address: y:6379\n  password: ypw\n"
                    "rtc:\n  udp_port: 7100-7200\n  node_ip: 10.0.0.1\nturn:\n  cert_file: y.crt\n  key_file: y.key\nbind_addresses: [10.0.0.2]\n")}
    base_argv = ["--config", "{dir}/c.yaml"]
    flagset = [("--keys", "fkey: " + SECRET), ("--region", "fregion"), ("--redis-host", "f:6379"),
               ("--redis-password", "fpw"), ("--udp-port", "7300-7400"), ("--node-ip", "10.0.0.3"),
               ("--turn-cert", "f.crt"), ("--turn-key", "f.key"), ("--bind", "10.0.0.4")]
    envset = {"LIVEKIT_KEYS": "ekey: " + SECRET, "LIVEKIT_REGION": "eregion", "REDIS_HOST": "e:6379",
              "REDIS_PASSWORD": "epw", "UDP_PORT": "7500", "NODE_IP": "10.0.0.5",
              "LIVEKIT_TURN_CERT": "e.crt", "LIVEKIT_TURN_KEY": "e.key"}
    c.append(case("prec.base.yaml_only", "precedence-base", base_argv, {}, y))
    flags = [x for kv in flagset for x in (kv[0], kv[1])]
    c.append(case("prec.base.yaml_flags", "precedence-base", base_argv + flags, {}, y))
    c.append(case("prec.base.yaml_env", "precedence-base", base_argv, envset, y))
    c.append(case("prec.base.yaml_flags_env", "precedence-base", base_argv + flags, envset, y))
    for fl, v in flagset:
        c.append(case(f"prec.base.flag{fl}", "precedence-base", [fl, v], {} if fl == "--keys" else KEYS_ENV))
    for k, v in envset.items():
        c.append(case(f"prec.base.env.{k}", "precedence-base", [], {**({} if k == "LIVEKIT_KEYS" else KEYS_ENV), k: v}))
    c.append(case("prec.base.bind_twice", "precedence-base", ["--bind", "10.0.0.6", "--bind", "::1"], KEYS_ENV))
    c.append(case("prec.base.udp_single_over_yaml_range", "precedence-base", ["--config", "{dir}/c.yaml", "--udp-port", "7600"], KEYS_ENV,
                  {"c.yaml": "rtc:\n  udp_port: 7100-7200\n"}))
    c.append(case("prec.base.node_ip_merge_v6", "precedence-base", ["--config", "{dir}/c.yaml", "--node-ip", "::2"], KEYS_ENV,
                  {"c.yaml": "rtc:\n  node_ip: 10.0.0.1\n"}))
    c.append(case("prec.base.node_ip_dup_v4", "precedence-base", ["--config", "{dir}/c.yaml", "--node-ip", "10.0.0.9"], KEYS_ENV,
                  {"c.yaml": "rtc:\n  node_ip: 10.0.0.1\n"}))
    c.append(case("prec.base.generated_udp_start_vs_base", "precedence-base", ["--rtc.udp_port.start=7700", "--udp-port", "7800"], KEYS_ENV))
    c.append(case("prec.base.key_file_flag", "precedence-base", ["--key-file", "{dir}/k.yaml"], {},
                  {"k.yaml": "kfkey: " + SECRET + "\n"}))
    c.append(case("prec.base.key_file_yaml", "precedence-base", ["--config", "{dir}/c.yaml"], {},
                  {"c.yaml": "key_file: {dir}/k.yaml\n", "k.yaml": "kykey: " + SECRET + "\n"}))
    c.append(case("prec.base.key_file_vs_keys", "precedence-base", ["--key-file", "{dir}/k.yaml", "--keys", "kk: " + SECRET], {},
                  {"k.yaml": "kfkey: " + SECRET + "\n"}))
    c.append(case("prec.base.key_file_bad_perm", "precedence-base", ["--key-file", "{dir}/k.yaml"], {},
                  {"k.yaml": "kfkey: " + SECRET + "\n"}, {"k.yaml": "0644"}))
    c.append(case("prec.base.key_file_missing", "precedence-base", ["--key-file", "{dir}/none.yaml"], {}))
    c.append(case("prec.base.key_file_env_expand", "precedence-base", ["--key-file", "$CFGOBS_DIR/k.yaml"], {"CFGOBS_DIR": "{dir}"},
                  {"k.yaml": "kfkey: " + SECRET + "\n"}))
    c.append(case("prec.base.keys_bad", "precedence-base", ["--keys", "nocolon"], {}))
    c.append(case("prec.base.keys_short_secret", "precedence-base", ["--keys", "k: short"], {}))
    c.append(case("prec.base.dev_false_over_yaml", "precedence-base", ["--config", "{dir}/c.yaml", "--dev=false"], KEYS_ENV,
                  {"c.yaml": "development: true\n"}))
    c.append(case("prec.base.dev_vs_generated_development", "precedence-base", ["--development=true", "--dev=false"], KEYS_ENV))
    c.append(case("prec.base.log_level_legacy", "precedence-base", ["--config", "{dir}/c.yaml"], KEYS_ENV,
                  {"c.yaml": "log_level: warn\nlogging:\n  level: error\n  pion_level: info\n"}))
    c.append(case("prec.base.legacy_room_limits", "precedence-base", ["--config", "{dir}/c.yaml"], KEYS_ENV,
                  {"c.yaml": "room:\n  max_metadata_size: 100\n  max_room_name_length: 10\n  max_participant_identity_length: 11\n"}))
    c.append(case("prec.base.turn_relay_range_partial", "precedence-base", ["--config", "{dir}/c.yaml"], KEYS_ENV,
                  {"c.yaml": "turn:\n  relay_range_start: 31000\n"}))
    c.append(case("prec.base.turn_ttl_clamp", "precedence-base", ["--config", "{dir}/c.yaml"], KEYS_ENV,
                  {"c.yaml": "turn:\n  ttl_seconds: 0\n"}))
    c.append(case("prec.base.prometheus_port_legacy", "precedence-base", ["--config", "{dir}/c.yaml"], KEYS_ENV,
                  {"c.yaml": "prometheus_port: 9100\n"}))
    return c


def invalid_cases():
    c = []
    K = KEYS_ENV
    y = lambda body: {"c.yaml": body}
    cfg = ["--config", "{dir}/c.yaml"]
    # bad int / uint / bool / float / duration via flag, env, yaml
    bad = [("rtc.tcp_port", "abc", "abc"), ("rtc.tcp_port", "-1", "-1"), ("rtc.tcp_port", "0x1F", "0x1F"),
           ("audio.active_level", "300", "300"), ("rtc.packet_buffer_size", "1.5", "1.5"),
           ("rtc.packet_buffer_size", "99999999999999999999", "99999999999999999999"),
           ("rtc.use_ice_lite", "maybe", "maybe"), ("rtc.use_ice_lite", "1", "1"), ("rtc.use_ice_lite", "T", "T"),
           ("node_selector.cpu_load_limit", "abc", "abc"), ("node_selector.cpu_load_limit", "1e3", "1e3"),
           ("rtc.pli_throttle.low_quality", "500", "500"), ("rtc.pli_throttle.low_quality", "5x", "5x"),
           ("rtc.pli_throttle.low_quality", "-5s", "-5s"), ("rtc.pli_throttle.low_quality", "1.5h", "1.5h"),
           ("port", "", "")]
    for path, fv, yv in bad:
        env = "LIVEKIT_" + path.upper().replace(".", "_")
        tag = fv.replace(".", "_") or "empty"
        c.append(case(f"bad.flag.{path}.{tag}", "invalid", [f"--{path}={fv}"], K))
        c.append(case(f"bad.env.{path}.{tag}", "invalid", [], {**K, env: fv}))
        c.append(case(f"bad.yaml.{path}.{tag}", "invalid", cfg, K, y(yaml_nested(path, yv if yv else '""'))))
    # port ranges
    for i, v in enumerate(["7890-7880", "a-b", "1-2-3", "7882-", "abc", "7882 - 7890", "0"]):
        c.append(case(f"bad.udp_port.flag.{i}", "invalid", ["--udp-port", v], K))
        c.append(case(f"bad.udp_port.yaml.{i}", "invalid", cfg, K, y(f"rtc:\n  udp_port: {json.dumps(v)}\n")))
    c.append(case("udp_port.yaml.int", "invalid", cfg, K, y("rtc:\n  udp_port: 7882\n")))
    c.append(case("udp_port.yaml.mapping", "invalid", cfg, K, y("rtc:\n  udp_port:\n    start: 7882\n    end: 7890\n")))
    c.append(case("udp_port.port_range_start_end", "invalid", cfg, K, y("rtc:\n  port_range_start: 50000\n  port_range_end: 60000\n")))
    c.append(case("udp_port.port_range_start_only", "invalid", cfg, K, y("rtc:\n  port_range_start: 50000\n")))
    # node ip
    for i, v in enumerate(["1.2.3", "nope", "1.1.1.1,2.2.2.2", "::1,::2", "1.1.1.1,::1", "1.1.1.1,::1,::2", " 1.1.1.1 "]):
        c.append(case(f"bad.node_ip.flag.{i}", "invalid", ["--node-ip", v], K))
        c.append(case(f"bad.node_ip.yaml.{i}", "invalid", cfg, K, y(f"rtc:\n  node_ip: {json.dumps(v)}\n")))
        c.append(case(f"bad.node_ip.env.{i}", "invalid", [], {**K, "NODE_IP": v}))
    c.append(case("node_ip.generated_ipv4_raw", "invalid", ["--rtc.node_ip.ipv4=999.1.1.1"], K))
    # unknown keys / strictness / file shapes
    c.append(case("strict.unknown_top", "strict", cfg, K, y("nope: 1\n")))
    c.append(case("strict.unknown_top.disabled", "strict", cfg + ["--disable-strict-config"], K, y("nope: 1\n")))
    c.append(case("strict.unknown_nested", "strict", cfg, K, y("rtc:\n  nope: 1\n")))
    c.append(case("strict.unknown_nested.disabled", "strict", cfg + ["--disable-strict-config"], K, y("rtc:\n  nope: 1\n")))
    c.append(case("strict.unknown_deep", "strict", cfg, K, y("rtc:\n  congestion_control:\n    stream_allocator:\n      nope: 1\n")))
    c.append(case("strict.sip_child", "strict", cfg, K, y("sip:\n  x: 1\n")))
    c.append(case("strict.sip_empty", "strict", cfg, K, y("sip: {}\n")))
    c.append(case("strict.unknown_in_list_item", "strict", cfg, K, y("rtc:\n  turn_servers:\n  - host: h\n    nope: 1\n")))
    c.append(case("strict.unknown_body", "strict", ["--config-body", "nope: 1\n"], K))
    c.append(case("strict.dup_key", "strict", cfg, K, y("port: 1\nport: 2\n")))
    c.append(case("file.comment_only", "file", cfg, K, y("# only a comment\n")))
    c.append(case("file.empty", "file", cfg, K, y("")))
    c.append(case("file.missing", "file", ["--config", "{dir}/missing.yaml"], K))
    c.append(case("file.syntax_error", "file", cfg, K, y("rtc: [\n")))
    c.append(case("file.scalar_root", "file", cfg, K, y("hello\n")))
    c.append(case("file.list_root", "file", cfg, K, y("- a\n")))
    c.append(case("file.multi_doc", "file", cfg, K, y("port: 1\n---\nport: 2\n")))
    c.append(case("file.null_section", "file", cfg, K, y("rtc:\nroom: ~\n")))
    c.append(case("file.type_mismatch_struct", "file", cfg, K, y("rtc: 5\n")))
    c.append(case("file.type_mismatch_list", "file", cfg, K, y("rtc:\n  stun_servers: abc\n")))
    c.append(case("file.body_comment_only", "file", ["--config-body", "# c\n"], K))
    # usage errors
    c.append(case("usage.unknown_flag", "usage", ["--nope"], K))
    c.append(case("usage.missing_value", "usage", ["--rtc.tcp_port"], K))
    c.append(case("usage.positional", "usage", ["extra"], K))
    c.append(case("usage.bool_value_bad", "usage", ["--dev=maybe"], K))
    c.append(case("usage.single_dash", "usage", ["-port=7001"], K))
    c.append(case("usage.subcommand_ports", "usage", ["ports"], K))
    # congestion pacer validation / misc validators
    c.append(case("validate.pacer_bad", "validate", ["--rtc.congestion_control.send_side_bwe_pacer=bogus"], K))
    c.append(case("validate.pacer_empty", "validate", ["--rtc.congestion_control.send_side_bwe_pacer="], K))
    return c


def structured_cases():
    c = []
    K = KEYS_ENV
    cfg = ["--config", "{dir}/c.yaml"]
    y = lambda body, extra=None: {"c.yaml": body, **(extra or {})}
    c.append(case("struct.turn_servers.secret", "structured", cfg, K, y(
        "rtc:\n  turn_servers:\n  - host: turn.example.com\n    port: 443\n    protocol: tls\n    secret: s1\n    ttl: 100\n")))
    c.append(case("struct.turn_servers.static", "structured", cfg, K, y(
        "rtc:\n  turn_servers:\n  - host: t\n    port: 3478\n    protocol: udp\n    username: u\n    credential: c\n")))
    c.append(case("struct.turn_servers.secret_file", "structured", cfg, K, y(
        "rtc:\n  turn_servers:\n  - host: t\n    secret_file: {dir}/s.txt\n", {"s.txt": "  filesecret\n"})))
    c.append(case("struct.turn_servers.secret_file_bad_perm", "structured", cfg, K, y(
        "rtc:\n  turn_servers:\n  - host: t\n    secret_file: {dir}/s.txt\n", {"s.txt": "filesecret\n"}), {"s.txt": "0644"}))
    c.append(case("struct.turn_servers.secret_file_empty", "structured", cfg, K, y(
        "rtc:\n  turn_servers:\n  - host: t\n    secret_file: {dir}/s.txt\n", {"s.txt": "  \n"})))
    c.append(case("struct.turn_servers.both", "structured", cfg, K, y(
        "rtc:\n  turn_servers:\n  - host: t\n    secret: ' inl '\n    secret_file: {dir}/missing\n")))
    c.append(case("struct.turn_servers.no_creds", "structured", cfg, K, y("rtc:\n  turn_servers:\n  - host: t\n")))
    c.append(case("struct.turn_servers.ttl_overflow", "structured", cfg, K, y(
        "rtc:\n  turn_servers:\n  - host: t\n    secret: s\n    ttl: 99999999\n")))
    c.append(case("struct.turn_servers.null", "structured", cfg, K, y("rtc:\n  turn_servers: ~\n")))
    c.append(case("struct.turn_servers.empty", "structured", cfg, K, y("rtc:\n  turn_servers: []\n")))
    c.append(case("struct.keys.yaml", "structured", cfg, {}, y("keys:\n  a: " + SECRET + "\n  b: " + SECRET + "x\n")))
    c.append(case("struct.keys.yaml_null_value", "structured", cfg, {}, y("keys:\n  a:\n")))
    c.append(case("struct.keys.yaml_int_value", "structured", cfg, {}, y("keys:\n  a: 123\n")))
    c.append(case("struct.keys.yaml_plus_flag", "structured", cfg + ["--keys", "f: " + SECRET], {}, y("keys:\n  a: " + SECRET + "\n")))
    c.append(case("struct.keys.flag_multi", "structured", ["--keys", "a: " + SECRET + "\nb: " + SECRET], {}))
    c.append(case("struct.keys.flag_nonstring", "structured", ["--keys", "a: 1\nb: " + SECRET], {}))
    c.append(case("struct.enabled_codecs", "structured", cfg, K, y(
        "room:\n  enabled_codecs:\n  - mime: audio/opus\n  - mime: video/VP8\n    fmtp_line: x=1\n")))
    c.append(case("struct.enabled_codecs.empty", "structured", cfg, K, y("room:\n  enabled_codecs: []\n")))
    c.append(case("struct.enabled_codecs.null", "structured", cfg, K, y("room:\n  enabled_codecs: ~\n")))
    c.append(case("struct.ips", "structured", cfg, K, y("rtc:\n  ips:\n    includes: [10.0.0.0/8]\n    excludes: [10.1.0.0/16]\n")))
    c.append(case("struct.ips.bad_cidr", "structured", cfg, K, y("rtc:\n  ips:\n    includes: [nope]\n")))
    c.append(case("struct.interfaces", "structured", cfg, K, y("rtc:\n  interfaces:\n    includes: [en0]\n    excludes: [lo0]\n")))
    c.append(case("struct.interfaces.exclude_all", "structured", cfg, K, y("rtc:\n  interfaces:\n    includes: [nonexistent0]\n")))
    c.append(case("struct.stun_servers", "structured", cfg, K, y("rtc:\n  stun_servers: [stun.example.com:3478]\n")))
    c.append(case("struct.webhook", "structured", cfg, K, y(
        "webhook:\n  api_key: devkey\n  urls: [http://a/b]\n  filter_params:\n    include_events: [room_started]\n")))
    c.append(case("struct.webhook.bad_key", "structured", cfg, K, y("webhook:\n  api_key: other\n  urls: [http://a/b]\n")))
    c.append(case("struct.node_selector.regions", "structured", cfg, K, y(
        "node_selector:\n  kind: regionaware\n  regions:\n  - name: r1\n    lat: 1.5\n    lon: -2.25\n")))
    c.append(case("struct.logging.component_levels", "structured", cfg, K, y(
        "logging:\n  component_levels:\n    rtc: debug\n    pion: warn\n")))
    c.append(case("struct.bind_addresses.list", "structured", cfg, K, y("bind_addresses: [127.0.0.1, '::1']\n")))
    c.append(case("struct.bind_addresses.empty", "structured", cfg, K, y("bind_addresses: []\n")))
    c.append(case("struct.redis.tls", "structured", cfg, K, y(
        "redis:\n  address: localhost:6379\n  tls:\n    enabled: true\n    server_name: r\n")))
    c.append(case("struct.redis.tls_null", "structured", cfg, K, y("redis:\n  tls: ~\n")))
    c.append(case("struct.redis.sentinel", "structured", cfg, K, y(
        "redis:\n  sentinel_master_name: m\n  sentinel_addresses: [a:1, b:2]\n  cluster_addresses: [c:3]\n")))
    c.append(case("struct.node_stats.intervals", "structured", cfg, K, y(
        "node_stats:\n  stats_rate_measurement_intervals: [5s, 1m]\n")))
    c.append(case("struct.stream_tracker.map_merge", "structured", cfg, K, y(
        "video:\n  stream_tracker_manager:\n    video:\n      bitrate_report_interval:\n        1: 3s\n      packet_tracker:\n        0:\n          samples_required: 9\n")))
    c.append(case("struct.turn.cidrs", "structured", cfg, K, y(
        "turn:\n  bind_addresses: [127.0.0.1]\n  proxy_protocol_trusted_cidrs: []\n  deny_peer_cidrs: [10.0.0.0/8]\n")))
    c.append(case("struct.room_configurations", "structured", cfg, K, y(
        "room:\n  room_configurations:\n    rc1:\n      name: rc1\n      empty_timeout: 10\n      max_participants: 3\n"
        "      metadata: m\n      sync_streams: true\n      agents:\n      - agent_name: a1\n        metadata: am\n")))
    c.append(case("struct.room_configurations.unknown", "structured", cfg, K, y(
        "room:\n  room_configurations:\n    rc1:\n      nope: 1\n")))
    c.append(case("struct.room_configurations.egress", "structured", cfg, K, y(
        "room:\n  room_configurations:\n    rc1:\n      egress:\n        tracks:\n          filepath: x/{track_id}\n")))
    c.append(case("struct.room_configurations.null", "structured", cfg, K, y("room:\n  room_configurations:\n    rc1: ~\n")))
    c.append(case("struct.dev.bind_lan_ips", "structured", ["--dev", "--bind", "192.168.1.10", "--bind", "0.0.0.0"], {}))
    c.append(case("struct.dev.yaml_ips_plus_bind", "structured", ["--dev", "--bind", "192.168.1.10", "--config", "{dir}/c.yaml"], {},
                  y("rtc:\n  ips:\n    includes: [10.0.0.0/8]\n")))
    c.append(case("struct.node_ip.yaml_dual", "structured", cfg, K, y("rtc:\n  node_ip: 1.2.3.4,2001:db8::1\n")))
    c.append(case("struct.node_ip.mapped_v6", "structured", ["--node-ip", "::ffff:1.2.3.4"], K))
    c.append(case("struct.limit.overrides", "structured", cfg, K, y(
        "limit:\n  num_tracks: 3\n  bytes_per_sec: 1000.5\n  max_metadata_size: 10\n")))
    return c


def yaml_semantics_cases():
    """yaml.v3 decoder semantics: resolution, conversions, aggregation of type
    errors, merges/aliases/tags, syntax errors and key files (W1-G3)."""
    c = []
    K = KEYS_ENV
    cfg = ["--config", "{dir}/c.yaml"]
    y = lambda body, extra=None: {"c.yaml": body, **(extra or {})}
    bodies = {
        # numeric resolution and conversion
        "int.hex": "port: 0x1F90\n",
        "int.octal_legacy": "port: 017\n",
        "int.octal_0o": "port: 0o17\n",
        "int.binary": "port: 0b101\n",
        "int.underscore": "port: 7_880\n",
        "int.float_truncates": "rtc:\n  packet_buffer_size: 2.9\n",
        "int.negative_float_into_uint": "port: -1.5\n",
        "int.exponent_into_uint": "port: 1e3\n",
        "int.overflow_uint32": "port: 4294967296\n",
        "int.overflow_uint16": "turn:\n  relay_range_start: 70000\n",
        "int.overflow_int32": "limit:\n  num_tracks: 3000000000\n",
        "int.into_string": "region: 007\n",
        "int.bool_into_int": "port: true\n",
        "int.timestamp_into_int": "port: 2001-01-01\n",
        "int.inf_into_int": "port: .inf\n",
        "int.nan_into_float": "node_selector:\n  cpu_load_limit: .nan\n",
        "float.int_into_float32": "limit:\n  bytes_per_sec: 7\n",
        "float.precision32": "limit:\n  bytes_per_sec: 0.1\n",
        "float.precision64": "rtc:\n  congestion_control:\n    remote_bwe:\n      nack_ratio_attenuator: 0.1\n",
        "float.hex": "limit:\n  bytes_per_sec: 0x1p-2\n",
        # bools
        "bool.yes": "rtc:\n  use_ice_lite: yes\n",
        "bool.on": "rtc:\n  use_ice_lite: On\n",
        "bool.off": "development: off\n",
        "bool.y": "rtc:\n  use_ice_lite: y\n",
        "bool.quoted_true": "rtc:\n  use_ice_lite: \"true\"\n",
        "bool.null_keeps": "rtc:\n  congestion_control:\n    enabled: ~\n",
        # strings
        "str.bool_into_string": "region: true\n",
        "str.float_into_string": "region: 1.50\n",
        "str.null_keeps": "rtc:\n  congestion_control:\n    send_side_bwe_pacer: ~\n",
        "str.binary": "region: !!binary aGVsbG8=\n",
        "str.binary_bad": "region: !!binary '***'\n",
        "str.timestamp": "region: 2001-12-14t21:59:43.10-05:00\n",
        "str.long_value_error": "port: abcdefghijklmnop\n",
        "str.multibyte_error": "port: héllo-wörld-x\n",
        "str.map_into_string": "region: {a: 1}\n",
        "str.seq_into_string": "region: [a]\n",
        # durations
        "dur.int_into_duration": "room:\n  create_room_timeout: 0\n",
        "dur.float_into_duration": "room:\n  create_room_timeout: 1.5\n",
        "dur.quoted": "room:\n  create_room_timeout: '90s'\n",
        "dur.negative": "room:\n  create_room_timeout: -1m\n",
        # explicit tags
        "tag.str_into_int": "port: !!str 7000\n",
        "tag.int_mismatch": "port: !!int abc\n",
        "tag.float_from_int": "limit:\n  bytes_per_sec: !!float 3\n",
        "tag.custom": "port: !custom 7000\n",
        "tag.custom_into_string": "region: !custom r1\n",
        "tag.null_mapping": "rtc: !!null {tcp_port: 1}\n",
        "tag.null_mapping_own_field": "rtc: !!null {strict_acks: true}\n",
        "tag.null_mapping_pointer": "redis: {tls: !!null {enabled: true}}\n",
        "tag.verbatim": "region: !<tag:yaml.org,2002:str> r1\n",
        # aggregation and order of type errors
        "err.two_fields": "port: a\nrtc:\n  tcp_port: b\n",
        "err.type_and_unknown": "nope: 1\nport: a\n",
        "err.list_element": "rtc:\n  stun_servers: [a, [b], c]\n",
        "err.nested_dup": "rtc:\n  tcp_port: 1\n  tcp_port: 2\n",
        "err.dup_after_type_error": "port: a\nrtc: {x: 1, x: 2}\n",
        "err.dup_different_style": "port: 1\n'port': 2\n",
        "err.struct_key_is_map": "{a: 1}: 2\n",
        "err.map_key_int32_bad": "video:\n  stream_tracker_manager:\n    video:\n      bitrate_report_interval:\n        x: 3s\n",
        "err.map_value_bad": "logging:\n  component_levels:\n    a: [x]\n",
        "err.custom_after_type_error": "port: a\nrtc:\n  udp_port: 9-3\n",
        # null handling for collections
        "null.map_value_new": "logging:\n  component_levels:\n    a: ~\n",
        "null.map_reset": "video:\n  stream_tracker_manager:\n    video:\n      packet_tracker: ~\n",
        "null.list_reset": "turn:\n  bind_addresses: ~\n",
        "null.keys_value": "keys:\n  a: ~\n",
        "null.key": "~: 1\n",
        "null.struct_empty_value": "rtc:\n",
        # merges and aliases
        "merge.struct": "base: &b {tcp_port: 9000}\nrtc:\n  <<: *b\n  use_ice_lite: true\n",
        "merge.explicit_wins": "rtc:\n  tcp_port: 1\n  <<: {tcp_port: 2, use_ice_lite: true}\n",
        "merge.list": "rtc:\n  <<: [{tcp_port: 3}, {tcp_port: 4, use_ice_lite: true}]\n",
        "merge.bad_value": "rtc:\n  <<: 5\n",
        "merge.map": "logging:\n  component_levels:\n    <<: {a: debug, b: info}\n    a: warn\n",
        "alias.scalar": "x: &p 9001\nport: *p\n",
        "alias.struct": "rtc: &r {tcp_port: 9002}\nx: *r\n",
        "alias.unknown": "port: *nope\n",
        "alias.recursive": "a: &a [*a]\nregion: x\n",
        "alias.in_list": "s: &s stun.example.com:3478\nrtc:\n  stun_servers: [*s, *s]\n",
        # documents and encodings
        "doc.explicit_start": "---\nport: 7001\n",
        "doc.end_marker": "port: 7002\n...\n",
        "doc.null_document": "--- ~\n",
        "doc.empty_explicit": "---\n",
        "doc.second_broken": "port: 7003\n---\nrtc: [\n",
        "doc.second_unknown": "port: 7004\n---\nnope: 1\n",
        "doc.bom": "\ufeffport: 7005\n",
        "doc.crlf": "port: 7006\r\nrtc:\r\n  tcp_port: 7007\r\n",
        "doc.directive": "%YAML 1.1\n---\nport: 7008\n",
        "doc.tag_directive": "%TAG !e! tag:example.com,2000:\n---\nregion: !e!x r\n",
        "doc.flow_root": "{port: 7009, rtc: {tcp_port: 7010}}\n",
        "doc.block_scalar": "region: |\n  multi\n  line\n",
        "doc.folded_scalar": "region: >-\n  a\n  b\n",
        "doc.multiline_plain": "region: a\n  b\n",
        "doc.comment_tail": "rtc:\n  tcp_port: 7011\n  # tail\nport: 7012 # c\n",
        # syntax errors
        "syn.unclosed_flow_map": "rtc: {tcp_port: 1\n",
        "syn.unclosed_quote": "region: \"abc\n",
        "syn.unclosed_single": "region: 'abc\n",
        "syn.bad_indent": "rtc:\n  tcp_port: 1\n udp_port: 2\n",
        "syn.tab_indent": "rtc:\n\ttcp_port: 1\n",
        "syn.mapping_values": "a: b: c\n",
        "syn.seq_after_map": "port: 1\n- a\n",
        "syn.map_after_seq": "- a\nb: c\n",
        "syn.stray_bracket": "port: 1]\n",
        "syn.reserved_at": "region: @x\n",
        "syn.reserved_backtick": "region: `x\n",
        "syn.bad_escape": "region: \"\\q\"\n",
        "syn.dup_yaml_directive": "%YAML 1.1\n%YAML 1.1\n---\nport: 1\n",
        "syn.undefined_handle": "region: !x!y z\n",
        "syn.bad_anchor": "region: & x\n",
        "syn.long_simple_key": ("k" * 1100) + ": 1\n",
        "syn.block_in_flow": "rtc: [a, - b]\n",
        "syn.invalid_utf8_like": "region: \"\\xZZ\"\n",
        "syn.colon_in_flow_key": "{a: 1, : 2}\n",
        "syn.only_dash": "-\n",
        "syn.trailing_flow": "[a]]\n",
        "syn.block_scalar_bad_indicator": "region: |x\n  a\n",
    }
    for name, body in bodies.items():
        c.append(case(f"yaml.{name}", "yaml", cfg, K, y(body)))
        if name.startswith(("err.", "merge.", "alias.", "null.")):
            c.append(case(f"yaml.{name}.lenient", "yaml", cfg + ["--disable-strict-config"], K, y(body)))
    # invalid UTF-8 bytes cannot travel through JSON text; use a body flag
    c.append(case("yaml.body.null_doc", "yaml", ["--config-body", "~"], K))
    c.append(case("yaml.body.flow", "yaml", ["--config-body", "{port: 7013}"], K))
    # key files (yaml.NewDecoder(f).Decode(conf.Keys))
    kf = lambda body: {"keys.yaml": body}
    kcfg = ["--key-file", "{dir}/keys.yaml"]
    kmodes = {"keys.yaml": "0600"}
    for name, body in {
        "ok": "a: " + SECRET + "\n",
        "empty": "",
        "comment": "# c\n",
        "list": "- a\n",
        "scalar": "abc\n",
        "nested": "a: {b: c}\n",
        "int_value": "a: 1\n",
        "null_value": "a: ~\nb: " + SECRET + "\n",
        "dup": "a: x\na: y\n",
        "syntax": "a: [\n",
        "multi_doc": "a: " + SECRET + "\n---\nb: c\n",
    }.items():
        c.append(case(f"keyfile.{name}", "keyfile", kcfg, {}, kf(body), kmodes))
    # --keys (yaml.Unmarshal into map[string]any)
    for name, v in {
        "seq": "- a",
        "nested": "a: {b: c}\nd: " + SECRET,
        "timestamp": "a: 2001-01-01\nb: " + SECRET,
        "dup": "a: x\na: y",
        "syntax": "a: [",
        "binary": "a: !!binary " + "c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0",
        "null_doc": "~",
        "multi_doc": "a: " + SECRET + "\n---\nb: [",
    }.items():
        c.append(case(f"keysflag.{name}", "keysflag", ["--keys", v], {}))
    return c


def main():
    leaves_doc = json.load(open(sys.argv[1]))
    leaves = leaves_doc["leaves"]
    cases = baseline_cases() + leaf_cases(leaves) + precedence_cases() + invalid_cases() + structured_cases() + yaml_semantics_cases()
    ids = [c["id"] for c in cases]
    dup = {i for i in ids if ids.count(i) > 1}
    if dup:
        raise SystemExit("duplicate ids: %s" % sorted(dup))
    json.dump({"generator": "tests/oracles/config-observer/gen_cases.py", "leaf_count": len(leaves),
               "cases": cases}, open(sys.argv[2], "w"), indent=1)
    print(len(cases), "cases,", len(leaves), "leaves")


if __name__ == "__main__":
    main()
