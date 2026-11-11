//! Read-only node discovery for the pinned server's `list-nodes` command.
//! Port of LiveKit commands.go formatting and RedisRouter.ListNodes, Apache-2.0.
//! Formatting draft requires the independent Go oracle before integration.
use lk_config::OfficialConfigView;
use lk_protocol::livekit::{Node, NodeStats, NodeStatsRate};
#[cfg(test)]
use prost::Message;

#[path = "redis_address.rs"]
mod redis_address;

#[path = "cli_redis_quote.rs"]
mod cli_redis_quote;
#[path = "cli_redis_resp.rs"]
mod cli_redis_resp;
#[path = "cli_redis_retry.rs"]
pub(crate) mod cli_redis_retry;
#[path = "cli_redis_session.rs"]
mod cli_redis_session;
#[path = "cli_redis_wire.rs"]
mod cli_redis_wire;

/// Separates ordinary command failures from the source's nil-stats fatal path.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NodeListError {
    Ordinary(String),
    OrdinaryBytes(Vec<u8>),
    FatalRedisPool(String),
    FatalNilStats,
}
impl std::fmt::Display for NodeListError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ordinary(message) | Self::FatalRedisPool(message) => f.write_str(message),
            Self::OrdinaryBytes(bytes) => f.write_str(&String::from_utf8_lossy(bytes)),
            Self::FatalNilStats => f.write_str(
                "panic: runtime error: invalid memory address or nil pointer dereference",
            ),
        }
    }
}
impl From<String> for NodeListError {
    fn from(message: String) -> Self {
        Self::Ordinary(message)
    }
}

pub(crate) async fn list_nodes(loaded: &impl OfficialConfigView) -> Result<String, NodeListError> {
    let settings = &loaded.official_config().redis;
    // SPEC-0074: use Redis node discovery when ANY Redis transport is configured
    // (address, sentinel_addresses, or cluster_addresses), not just the plain
    // `address` field. This matches upstream Go where the UniversalClient is
    // built from all address fields and ListNodes always queries through it.
    let has_redis = !settings.address.is_empty()
        || !settings.sentinel_addresses.is_empty()
        || !settings.cluster_addresses.is_empty();
    let nodes = if !has_redis {
        vec![local_node(loaded)]
    } else {
        let values = cli_redis_retry::read_nodes(settings).await?;
        decode_nodes(values)?
    };
    render_nodes(&nodes)
}

fn address_connection_message(
    error: &redis::RedisError,
    address: &redis_address::Address,
) -> String {
    if address.network == "tcp" || error.kind() == redis::ErrorKind::AuthenticationFailed {
        return connection_message(error, &address.endpoint);
    }
    // Only a real UNIX dial IO cause authorizes the net.OpError-style wrapper.
    // A textual Redis server/client error must never be converted into a dial.
    if error.is_io_error() {
        let kind = std::error::Error::source(error).and_then(|cause| {
            cause
                .downcast_ref::<std::io::Error>()
                .map(std::io::Error::kind)
                .or_else(|| {
                    cause
                        .downcast_ref::<std::sync::Arc<dyn std::error::Error + Send + Sync>>()
                        .and_then(|shared| shared.downcast_ref::<std::io::Error>())
                        .map(std::io::Error::kind)
                })
        });
        let cause = match kind {
            Some(std::io::ErrorKind::NotFound) => Some("no such file or directory"),
            Some(std::io::ErrorKind::ConnectionRefused) => Some("connection refused"),
            Some(std::io::ErrorKind::PermissionDenied) => Some("permission denied"),
            Some(std::io::ErrorKind::NotADirectory) => Some("not a directory"),
            Some(std::io::ErrorKind::InvalidInput) => Some("invalid argument"),
            _ => None,
        };
        if let Some(cause) = cause {
            return format!("dial unix {}: connect: {cause}", address.endpoint);
        }
    }
    redis_message(error)
}

fn connection_message(error: &redis::RedisError, endpoint: &str) -> String {
    if error.kind() == redis::ErrorKind::AuthenticationFailed {
        // The locked driver replaces RESP2 WRONGPASS with this typed error.
        // Fixed actual Redis/Go oracle establishes its source response text.
        return "WRONGPASS invalid username-password pair or user is disabled.".into();
    }
    // Redis's convenience is_connection_refusal also includes Unix NotFound.
    // This TCP command must use the actual underlying IO kind instead.
    let io_error = std::error::Error::source(error).and_then(|cause| {
        cause.downcast_ref::<std::io::Error>().or_else(|| {
            cause
                .downcast_ref::<std::sync::Arc<dyn std::error::Error + Send + Sync>>()
                .and_then(|shared| shared.downcast_ref::<std::io::Error>())
        })
    });
    if error.is_io_error()
        && let Some(message) = io_error.and_then(go_connect_cause)
    {
        return format!("dial tcp {endpoint}: connect: {message}");
    }
    redis_message(error)
}

/// Go renders a failed connect as a `net.OpError` whose `Err` is a
/// `syscall.Errno`, i.e. the OS strerror text with a lowercase first letter and
/// no `(os error N)` suffix. `io::Error` carries the same strerror but in the
/// capitalized Rust form, so only the framing differs.
fn go_connect_cause(error: &std::io::Error) -> Option<String> {
    if let Some(kind) = go_connect_errno_text(error) {
        return Some(kind.to_owned());
    }
    let text = error.to_string();
    let stripped = match text.rfind(" (os error ") {
        Some(at) => text[..at].to_owned(),
        None => text,
    };
    let mut chars = stripped.chars();
    let first = chars.next()?.to_lowercase().to_string();
    Some(format!("{first}{}", chars.as_str()))
}

/// Errnos whose Go table deliberately does not match the OS strerror, plus the
/// kinds Rust may synthesize without an OS code.
fn go_connect_errno_text(error: &std::io::Error) -> Option<&'static str> {
    match error.kind() {
        std::io::ErrorKind::ConnectionRefused => Some("connection refused"),
        std::io::ErrorKind::PermissionDenied => Some("permission denied"),
        std::io::ErrorKind::InvalidInput => Some("invalid argument"),
        std::io::ErrorKind::AddrNotAvailable => {
            if cfg!(target_os = "macos") {
                Some("can't assign requested address")
            } else {
                Some("cannot assign requested address")
            }
        }
        std::io::ErrorKind::HostUnreachable => Some("no route to host"),
        std::io::ErrorKind::NetworkUnreachable => Some("network is unreachable"),
        _ => None,
    }
}

fn redis_message(error: &redis::RedisError) -> String {
    match (error.code(), error.detail()) {
        (Some(code), Some(detail)) if !detail.is_empty() => format!("{code} {detail}"),
        (Some(code), _) => code.to_owned(),
        _ => error.to_string(),
    }
}

fn decode_nodes(values: Vec<Vec<u8>>) -> Result<Vec<Node>, String> {
    use lk_protocol::json::wire_error::{WireDecodeFailure, classify};
    values
        .into_iter()
        .map(|bytes| {
            lk_protocol::wire::decode::<Node>(&bytes).map_err(|_| {
                match classify("livekit.Node", &bytes) {
                    WireDecodeFailure::InvalidUtf8 => {
                        "string field contains invalid UTF-8".to_owned()
                    }
                    WireDecodeFailure::Malformed => {
                        "proto: cannot parse invalid wire-format data".to_owned()
                    }
                }
            })
        })
        .collect()
}

fn local_node(loaded: &impl OfficialConfigView) -> Node {
    let now = rtc_core::util::unix_s();
    let cfg = loaded.official_config();
    Node {
        id: loaded.command_node_id().to_owned(),
        ip: if cfg.rtc.node_ip.v4.is_empty() {
            cfg.rtc.node_ip.v6.clone()
        } else {
            cfg.rtc.node_ip.v4.clone()
        },
        region: cfg.region.clone(),
        // Node.num_cpus is not the displayed stats.num_cpus. Official
        // NewLocalNode initially leaves the latter zero.
        num_cpus: std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1),
        state: 1,
        stats: Some(NodeStats {
            started_at: now,
            updated_at: now,
            ..Default::default()
        }),
        ..Default::default()
    }
}

const HEADERS: [&str; 14] = [
    "ID",
    "IP Address",
    "Region",
    "CPUs",
    "CPU Usage\nLoad Avg",
    "Memory Used/Total",
    "Rooms",
    "Clients\nTracks In/Out",
    "Bytes/s In/Out\nBytes Total",
    "Packets/s In/Out\nPackets Total",
    "System Dropped Pkts/s\nPkts/s Out/Dropped",
    "Nack/s\nNack Total",
    "Retrans/s\nRetrans Total",
    "Started At\nUpdated At",
];

fn fields(node: &Node) -> Result<[String; 14], NodeListError> {
    // Go dereferences nil Stats; an exact panic/exit oracle is needed for this
    // invalid persisted object. Do not fabricate a valid zero-stat node.
    let s = node.stats.as_ref().ok_or(NodeListError::FatalNilStats)?;
    let zero = NodeStatsRate::default();
    let r = s.rates.first().unwrap_or(&zero);
    let state = match node.state {
        0 => "STARTING_UP".into(),
        1 => "SERVING".into(),
        2 => "SHUTTING_DOWN".into(),
        3 => "SUSPENDED".into(),
        other => other.to_string(),
    };
    let dropped = if r.sys_packets_out + r.sys_packets_dropped > 0.0 {
        r.sys_packets_dropped / (r.sys_packets_dropped + r.sys_packets_out)
    } else {
        0.0
    };
    Ok([
        format!("{}\n({state})", node.id),
        node.ip.clone(),
        node.region.clone(),
        s.num_cpus.to_string(),
        format!(
            "{:.2} %\n{:.2} {:.2} {:.2}",
            s.cpu_load * 100.0,
            s.load_avg_last1min,
            s.load_avg_last5min,
            s.load_avg_last15min
        ),
        format!("{} / {}", bytes(s.memory_used), bytes(s.memory_total)),
        s.num_rooms.to_string(),
        format!(
            "{}\n{} / {}",
            s.num_clients, s.num_tracks_in, s.num_tracks_out
        ),
        format!(
            "{}ps / {}ps\n{} / {}",
            bytes(r.bytes_in as u64),
            bytes(r.bytes_out as u64),
            bytes(s.bytes_in),
            bytes(s.bytes_out)
        ),
        format!(
            "{} / {}\n{} / {}",
            comma(r.packets_in as i64),
            comma(r.packets_out as i64),
            si(s.packets_in as f64),
            si(s.packets_out as f64)
        ),
        format!(
            "{:.2} %\n{} / {}",
            dropped * 100.0,
            r.sys_packets_out as f64,
            r.sys_packets_dropped as f64
        ),
        format!("{:.2}\n{}", r.nack_total, si(s.nack_total as f64)),
        format!(
            "{:.2}\n{}",
            r.retransmit_packets_out,
            si(s.retransmit_packets_out as f64)
        ),
        format!("{}\n{}", utc(s.started_at)?, utc(s.updated_at)?),
    ])
}

fn utc(seconds: i64) -> Result<String, String> {
    chrono::DateTime::from_timestamp(seconds, 0)
        .map(|v| v.format("%Y-%m-%d %H:%M:%S").to_string())
        .ok_or_else(|| "node timestamp is outside the supported formatting range".into())
}

// go-humanize v1.0.1 Bytes: decimal SI, one decimal only below ten.
fn bytes(value: u64) -> String {
    if value < 10 {
        return format!("{value} B");
    }
    let e = (value as f64).log10().div_euclid(3.0) as usize;
    let scaled = ((value as f64 / 1000_f64.powi(e as i32)) * 10.0 + 0.5).floor() / 10.0;
    let unit = ["B", "kB", "MB", "GB", "TB", "PB", "EB"][e.min(6)];
    if scaled < 10.0 {
        format!("{scaled:.1} {unit}")
    } else {
        format!("{scaled:.0} {unit}")
    }
}

fn comma(value: i64) -> String {
    let text = value.unsigned_abs().to_string();
    let mut out = String::new();
    if value < 0 {
        out.push('-');
    }
    for (i, c) in text.chars().enumerate() {
        if i > 0 && (text.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn si(value: f64) -> String {
    if value == 0.0 {
        return "0".into();
    }
    let e = (value.abs().log10() / 3.0).floor() as i32;
    let mut scaled = value / 1000_f64.powi(e);
    let mut exponent = e;
    if scaled.abs() == 1000.0 {
        exponent += 1;
        scaled = value / 1000_f64.powi(exponent);
    }
    // Go FtoaWithDigits first formats six fractional places, then truncates.
    let six = format!("{scaled:.6}");
    let (integer, fraction) = six.split_once('.').unwrap_or((&six, ""));
    let decimal = fraction
        .get(..fraction.len().min(2))
        .unwrap_or("")
        .trim_end_matches('0');
    let number = if decimal.is_empty() {
        integer.to_owned()
    } else {
        format!("{integer}.{decimal}")
    };
    let prefix = match exponent {
        0 => "",
        1 => "k",
        2 => "M",
        3 => "G",
        4 => "T",
        5 => "P",
        6 => "E",
        _ => "",
    };
    format!("{number} {prefix}").trim().to_owned()
}

// Go1.26.3 unicode.IsSpace/White_Space, tablewriter default Trimmer.
// Whole-cell trimming happens before newline splitting and width measurement.
fn go_cell_space(c: char) -> bool {
    matches!(c,'\u{0009}'..='\u{000d}'|'\u{0020}'|'\u{0085}'|'\u{00a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}')
}
fn trim_cell(value: &str) -> &str {
    value.trim_matches(go_cell_space)
}

// Fixed tablewriter twwidth uses displaywidth.Rune, where Unicode Zl/Zp
// separators are zero width. Split measurement at these grapheme boundaries;
// never remove, replace or split their original output text into visual rows.
// Port of the fixed source twwidth.Width entry, including its single-byte
// ASCII fast path. Scope here is source-default narrow plain cells; locale/tab
// discovery is not implemented by this integration (see source-bound contract).
#[path = "go_cell_width.rs"]
mod go_cell_width;
fn table_width(value: &str) -> usize {
    go_cell_width::width(
        value,
        go_cell_width::Options {
            east_asian_width: false,
            force_narrow_borders: false,
            tab_width: 8,
        },
    )
}

pub(crate) fn render_nodes(nodes: &[Node]) -> Result<String, NodeListError> {
    let rows = nodes
        .iter()
        .map(fields)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|row| row.map(|cell| trim_cell(&cell).to_owned()))
        .collect::<Vec<_>>();
    let mut widths = [0; 14];
    for i in 0..14 {
        widths[i] = HEADERS[i]
            .split('\n')
            .chain(rows.iter().flat_map(|row| row[i].split('\n')))
            .map(table_width)
            .max()
            .unwrap_or(0);
    }
    let mut out = String::new();
    border(&mut out, &widths, '┌', '┬', '┐');
    cells(&mut out, &widths, &HEADERS.map(str::to_owned), true);
    // Fixed tablewriter omits the header/body separator with no body rows.
    if !rows.is_empty() {
        border(&mut out, &widths, '├', '┼', '┤');
    }
    for (i, row) in rows.iter().enumerate() {
        cells(&mut out, &widths, row, false);
        if i + 1 < rows.len() {
            border(&mut out, &widths, '├', '┼', '┤');
        }
    }
    border(&mut out, &widths, '└', '┴', '┘');
    Ok(out)
}

fn border(out: &mut String, widths: &[usize; 14], left: char, mid: char, right: char) {
    out.push(left);
    for (i, width) in widths.iter().enumerate() {
        if i > 0 {
            out.push(mid);
        }
        out.push_str(&"─".repeat(width + 2));
    }
    out.push(right);
    out.push('\n');
}

fn cells(out: &mut String, widths: &[usize; 14], row: &[String; 14], header: bool) {
    let height = row.iter().map(|c| c.split('\n').count()).max().unwrap_or(1);
    for line in 0..height {
        // Default tablewriter TrimLine skips an entirely blank visual row
        // only after the first. Interior empty cells beside content stay put.
        if !header
            && line > 0
            && row
                .iter()
                .all(|cell| trim_cell(cell.split('\n').nth(line).unwrap_or("")).is_empty())
        {
            continue;
        }
        out.push('│');
        for i in 0..14 {
            let value = row[i].split('\n').nth(line).unwrap_or("");
            let extra = widths[i].saturating_sub(table_width(value));
            let left = if header || i < 3 || i == 13 {
                extra / 2
            } else {
                extra
            };
            out.push(' ');
            out.push_str(&" ".repeat(left));
            out.push_str(value);
            out.push_str(&" ".repeat(extra - left));
            out.push(' ');
            out.push('│');
        }
        out.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn real_wire_values_preserve_order_and_do_not_inject_local() {
        let one = Node {
            id: "ND_a".into(),
            stats: Some(NodeStats::default()),
            ..Default::default()
        };
        let two = Node {
            id: "ND_b".into(),
            state: 2,
            stats: Some(NodeStats::default()),
            ..Default::default()
        };
        assert!(decode_nodes(vec![]).unwrap().is_empty());
        assert_eq!(
            decode_nodes(vec![two.encode_to_vec(), one.encode_to_vec()]).unwrap(),
            vec![two, one]
        );
        assert_eq!(
            decode_nodes(vec![vec![255]]).unwrap_err(),
            "proto: cannot parse invalid wire-format data"
        );
        assert_eq!(
            decode_nodes(vec![vec![0x0a, 1, 0xff]]).unwrap_err(),
            "string field contains invalid UTF-8"
        );
    }
    #[test]
    fn stats_use_first_rate_and_every_official_column() {
        let n = Node {
            id: "ND_test".into(),
            state: 1,
            stats: Some(NodeStats {
                num_cpus: 7,
                num_rooms: 9,
                bytes_in: 9_007_199_254_740_993,
                rates: vec![
                    NodeStatsRate {
                        nack_total: 3.25,
                        ..Default::default()
                    },
                    NodeStatsRate {
                        nack_total: 999.0,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        let row = fields(&n).unwrap();
        assert_eq!(row.len(), 14);
        assert_eq!(row[3], "7");
        assert_eq!(row[6], "9");
        assert!(row[11].starts_with("3.25\n"));
        assert!(!row[11].contains("999"));
        assert!(row[13].contains("1970-01-01 00:00:00"));
        let rendered = render_nodes(&[n]).unwrap();
        assert!(rendered.contains("System Dropped Pkts/s"));
        assert!(rendered.contains("(SERVING)"));
    }
    #[test]
    fn humanize_boundaries() {
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(999), "999 B");
        assert_eq!(bytes(1000), "1.0 kB");
        assert_eq!(bytes(10000), "10 kB");
        assert_eq!(comma(-1234567), "-1,234,567");
        assert_eq!(si(12345.0), "12.34 k");
    }
    #[test]
    #[ignore = "requires actual pinned Go oracle via CLI_GO_ORACLE_RESULTS"]
    fn pinned_node_table_source_oracle() {
        let path =
            std::env::var("CLI_GO_ORACLE_RESULTS").expect("set actual Go oracle results path");
        let expected: Vec<serde_json::Value> = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let inputs = include_str!("../../../tests/oracles/cli/cases.jsonl");
        let mut count = 0;
        for input in inputs.lines() {
            let row: serde_json::Value = serde_json::from_str(input).unwrap();
            let Some(values) = row["nodes"].as_array() else {
                continue;
            };
            let nodes = values
                .iter()
                .map(|value| {
                    lk_protocol::json::decode::<Node>(&serde_json::to_vec(value).unwrap()).unwrap()
                })
                .collect::<Vec<_>>();
            let result = expected
                .iter()
                .find(|value| value["name"] == row["name"])
                .expect("missing actual Go vector");
            assert_eq!(
                render_nodes(&nodes).unwrap(),
                result["table"].as_str().unwrap(),
                "vector {}",
                row["name"]
            );
            count += 1;
        }
        assert_eq!(count, 3, "all source table fixtures must execute");
    }
    #[test]
    fn source_fatal_nil_stats_is_not_an_ordinary_command_error() {
        assert_eq!(
            render_nodes(&[Node::default()]),
            Err(NodeListError::FatalNilStats)
        );
        assert_eq!(
            NodeListError::FatalNilStats.to_string(),
            "panic: runtime error: invalid memory address or nil pointer dereference"
        );
    }
    #[test]
    fn redis_connection_errors_use_typed_causes_not_display_substrings() {
        let auth: redis::RedisError = (
            redis::ErrorKind::AuthenticationFailed,
            "arbitrary driver wording",
        )
            .into();
        assert_eq!(
            connection_message(&auth, "127.0.0.1:1234"),
            "WRONGPASS invalid username-password pair or user is disabled."
        );
        let refused: redis::RedisError = std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "arbitrary platform wording",
        )
        .into();
        assert_eq!(
            connection_message(&refused, "[::1]:1234"),
            "dial tcp [::1]:1234: connect: connection refused"
        );
        let not_found: redis::RedisError =
            std::io::Error::new(std::io::ErrorKind::NotFound, "no file").into();
        assert_ne!(
            connection_message(&not_found, "127.0.0.1:1234"),
            "dial tcp 127.0.0.1:1234: connect: connection refused"
        );
        // `127.0.0.1:` resolves to port 0 and the connect fails with
        // EADDRNOTAVAIL. Go renders it as a net.OpError over the syscall errno,
        // not Rust's capitalized `(os error 49)` form.
        let unavailable: redis::RedisError =
            std::io::Error::from(std::io::ErrorKind::AddrNotAvailable).into();
        let expected = if cfg!(target_os = "macos") {
            "dial tcp 127.0.0.1:0: connect: can't assign requested address"
        } else {
            "dial tcp 127.0.0.1:0: connect: cannot assign requested address"
        };
        assert_eq!(connection_message(&unavailable, "127.0.0.1:0"), expected);
        let timed_out: redis::RedisError =
            std::io::Error::from(std::io::ErrorKind::HostUnreachable).into();
        assert_eq!(
            connection_message(&timed_out, "127.0.0.1:0"),
            "dial tcp 127.0.0.1:0: connect: no route to host"
        );
        let unrelated: redis::RedisError = (redis::ErrorKind::Client, "connection refused").into();
        assert_ne!(
            connection_message(&unrelated, "127.0.0.1:1234"),
            "dial tcp 127.0.0.1:1234: connect: connection refused"
        );
    }

    #[test]
    fn stored_node_full_field_roundtrip_and_all_states_render() {
        // Full-field node: every stat populated, all states represented.
        let full = Node {
            id: "ND_full_roundtrip".into(),
            ip: "10.0.0.1".into(),
            region: "us-west-2".into(),
            num_cpus: 16,
            state: 1,
            stats: Some(NodeStats {
                started_at: 1_700_000_000,
                updated_at: 1_700_000_060,
                num_cpus: 16,
                num_rooms: 5,
                num_clients: 42,
                num_tracks_in: 80,
                num_tracks_out: 120,
                bytes_in: 1_000_000,
                bytes_out: 2_000_000,
                packets_in: 10_000,
                packets_out: 20_000,
                nack_total: 7,
                retransmit_packets_out: 3,
                memory_used: 4_294_967_296,
                memory_total: 17_179_869_184,
                cpu_load: 0.45,
                load_avg_last1min: 1.5,
                load_avg_last5min: 2.0,
                load_avg_last15min: 1.8,
                sys_packets_out: 5000,
                sys_packets_dropped: 50,
                rates: vec![NodeStatsRate {
                    bytes_in: 100_000.0,
                    bytes_out: 200_000.0,
                    packets_in: 1000.0,
                    packets_out: 2000.0,
                    nack_total: 1.5,
                    retransmit_packets_out: 0.75,
                    sys_packets_out: 500.0,
                    sys_packets_dropped: 5.0,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        // Encode → decode roundtrip preserves all fields
        let wire = full.encode_to_vec();
        let decoded = decode_nodes(vec![wire]).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].id, "ND_full_roundtrip");
        assert_eq!(decoded[0].ip, "10.0.0.1");
        assert_eq!(decoded[0].region, "us-west-2");
        assert_eq!(decoded[0].num_cpus, 16);
        assert_eq!(decoded[0].state, 1);
        let stats = decoded[0].stats.as_ref().unwrap();
        assert_eq!(stats.num_rooms, 5);
        assert_eq!(stats.num_clients, 42);
        assert_eq!(stats.rates.len(), 1);
        assert_eq!(stats.rates[0].nack_total, 1.5);

        // Render must include every official column header
        let rendered = render_nodes(&decoded).unwrap();
        assert!(rendered.contains("ID"));
        assert!(rendered.contains("IP Address"));
        assert!(rendered.contains("Region"));
        assert!(rendered.contains("CPUs"));
        assert!(rendered.contains("CPU Usage"));
        assert!(rendered.contains("Memory Used/Total"));
        assert!(rendered.contains("Rooms"));
        assert!(rendered.contains("Clients"));
        assert!(rendered.contains("Bytes/s In/Out"));
        assert!(rendered.contains("Packets/s In/Out"));
        assert!(rendered.contains("System Dropped Pkts/s"));
        assert!(rendered.contains("Nack/s"));
        assert!(rendered.contains("Retrans/s"));
        assert!(rendered.contains("Started At"));
        assert!(rendered.contains("(SERVING)"));

        // All four official states render their label
        for (state_num, label) in [
            (0, "STARTING_UP"),
            (1, "SERVING"),
            (2, "SHUTTING_DOWN"),
            (3, "SUSPENDED"),
        ] {
            let n = Node {
                id: format!("ND_s{state_num}"),
                state: state_num,
                stats: Some(NodeStats::default()),
                ..Default::default()
            };
            let out = render_nodes(&[n]).unwrap();
            assert!(
                out.contains(&format!("({label})")),
                "state {state_num} must show {label}"
            );
        }

        // Multi-node table: 3 nodes separated by borders
        let multi = vec![
            Node {
                id: "ND_a".into(),
                state: 1,
                stats: Some(NodeStats::default()),
                ..Default::default()
            },
            Node {
                id: "ND_b".into(),
                state: 2,
                stats: Some(NodeStats::default()),
                ..Default::default()
            },
            Node {
                id: "ND_c".into(),
                state: 0,
                stats: Some(NodeStats::default()),
                ..Default::default()
            },
        ];
        let multi_rendered = render_nodes(&multi).unwrap();
        assert!(multi_rendered.contains("ND_a"));
        assert!(multi_rendered.contains("ND_b"));
        assert!(multi_rendered.contains("ND_c"));
        // Header separator + 2 interior borders for 3 nodes = 3 ├ characters
        assert_eq!(multi_rendered.matches("├").count(), 3);
    }

    #[test]
    fn stored_node_known_number_wrong_wire_preserves_actual_known_values() {
        let node = Node {
            id: "ND_owned".into(),
            num_cpus: 4,
            stats: Some(NodeStats {
                num_rooms: 2,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut wire = node.encode_to_vec();
        // Correct known fields followed by legal protobuf unknown occurrences:
        // Node.id(number1) as varint, and num_cpus(number3) as opaque bytes.
        wire.extend_from_slice(&[0x08, 0x07, 0x1a, 0x02, 0xff, 0x00]);
        assert_eq!(decode_nodes(vec![wire]).unwrap(), vec![node]);
    }
    #[test]
    #[ignore = "requires actual fixed Go original proto.Unmarshal NODE_WIRE_GO_RESULTS"]
    fn actual_go_stored_node_wire_projection_and_errors_match() {
        use std::collections::BTreeMap;
        fn hex(s: &str) -> Vec<u8> {
            assert_eq!(s.len() % 2, 0);
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
                .collect()
        }
        let text = std::fs::read_to_string(
            std::env::var("NODE_WIRE_GO_RESULTS").expect("actual source output"),
        )
        .unwrap();
        let mut expected = BTreeMap::new();
        for line in text.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(
                expected
                    .insert(row["id"].as_str().unwrap().to_owned(), row)
                    .is_none()
            );
        }
        assert_eq!(expected.len(), 16);
        let inputs = include_str!("../../../tests/oracles/list-nodes-wire/cases.jsonl");
        let mut count = 0;
        for line in inputs.lines() {
            let input: serde_json::Value = serde_json::from_str(line).unwrap();
            let id = input["id"].as_str().unwrap();
            let actual = decode_nodes(vec![hex(input["hex"].as_str().unwrap())]);
            let source = expected.remove(id).expect("all actual Go cases required");
            if source["accepted"].as_bool().unwrap() {
                let node = lk_protocol::json::decode::<Node>(
                    &serde_json::to_vec(&source["known_json"]).unwrap(),
                )
                .unwrap();
                assert_eq!(actual.unwrap(), vec![node], "actual Go {id}");
            } else {
                assert_eq!(
                    actual.unwrap_err(),
                    source["error"].as_str().unwrap(),
                    "actual Go {id} error"
                );
            }
            count += 1;
        }
        assert_eq!(count, 16);
        assert!(expected.is_empty());
    }
    #[test]
    fn empty_id_matches_actual_fixed_official_cli_stdout() {
        let hex = "0a084e445f6f776e656412093132372e302e302e31180422210880e2cfaa061081e2cfaa061802200388010490028094ebdc03980280cab5ee0130013a056f776e65640a00";
        let bytes = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let nodes = decode_nodes(vec![bytes]).unwrap();
        assert_eq!(nodes[0].id, "");
        let actual = render_nodes(&nodes).unwrap();
        assert_eq!(
            actual.as_bytes(),
            include_bytes!(
                "../../../tests/oracles/list-nodes-render-trim/empty-id-fixed-official-stdout.txt"
            )
        );
    }
    #[test]
    fn whole_cell_trim_matches_go_white_space_without_removing_internal_newlines() {
        for code in [
            9, 10, 11, 12, 13, 32, 133, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199,
            8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288,
        ] {
            let c = char::from_u32(code).unwrap();
            assert_eq!(trim_cell(&format!("{c}A\n\n B{c}")), "A\n\n B");
        }
        for c in ['\u{001c}', '\u{001f}', '\u{180e}', '\u{200b}', '\u{feff}'] {
            assert_eq!(trim_cell(&format!("{c}A{c}")), format!("{c}A{c}"));
        }
    }
    #[test]
    #[ignore = "requires actual original tablewriter renderer12 via NODE_TABLE_TRIM_GO_RESULTS"]
    fn actual_go_table_cell_trim_full_output12() {
        use std::collections::BTreeMap;
        let inputs: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/oracles/list-nodes-render-trim/cases.json"
        ))
        .unwrap();
        let hex = inputs["base_node_hex"].as_str().unwrap();
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let expected = std::fs::read_to_string(
            std::env::var_os("NODE_TABLE_TRIM_GO_RESULTS").expect("actual Go renderer output"),
        )
        .unwrap();
        let mut rows = BTreeMap::new();
        for line in expected.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(
                rows.insert(row["id"].as_str().unwrap().to_owned(), row)
                    .is_none()
            );
        }
        assert_eq!(rows.len(), 12);
        for input in inputs["cases"].as_array().unwrap() {
            let mut node = decode_nodes(vec![bytes.clone()]).unwrap().pop().unwrap();
            node.id = input["node_id"].as_str().unwrap().to_owned();
            node.ip = input["ip"].as_str().unwrap().to_owned();
            node.region = input["region"].as_str().unwrap().to_owned();
            let id = input["id"].as_str().unwrap();
            let row = rows.remove(id).unwrap();
            assert_eq!(
                render_nodes(&[node]).unwrap(),
                row["stdout"].as_str().unwrap(),
                "actual source {id}"
            );
        }
        assert!(rows.is_empty());
    }

    #[test]
    #[ignore = "requires supplemental actual original decoder typed flags NODE_WIRE_TYPED_GO_RESULTS"]
    fn actual_go_stored_node_typed_failure_and_projection_match16() {
        use lk_protocol::json::wire_error::{WireDecodeFailure, classify};
        use std::collections::BTreeMap;
        let text = std::fs::read_to_string(
            std::env::var_os("NODE_WIRE_TYPED_GO_RESULTS").expect("actual typed source output"),
        )
        .unwrap();
        let mut expected = BTreeMap::new();
        for line in text.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(
                expected
                    .insert(row["id"].as_str().unwrap().to_owned(), row)
                    .is_none()
            );
        }
        assert_eq!(expected.len(), 16);
        for line in include_str!("../../../tests/oracles/list-nodes-wire/cases.jsonl").lines() {
            let input: serde_json::Value = serde_json::from_str(line).unwrap();
            let id = input["id"].as_str().unwrap();
            let hex = input["hex"].as_str().unwrap();
            let bytes: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            let actual = decode_nodes(vec![bytes.clone()]);
            let source = expected.remove(id).unwrap();
            assert_eq!(
                actual.is_ok(),
                source["accepted"].as_bool().unwrap(),
                "actual Go {id} admission"
            );
            if source["accepted"].as_bool().unwrap() {
                let node = lk_protocol::json::decode::<Node>(
                    &serde_json::to_vec(&source["known_json"]).unwrap(),
                )
                .unwrap();
                assert_eq!(actual.unwrap(), vec![node], "actual Go {id} known values");
            } else {
                let failure = classify("livekit.Node", &bytes);
                assert_eq!(
                    failure == WireDecodeFailure::InvalidUtf8,
                    source["invalid_utf8"].as_bool().unwrap(),
                    "actual Go {id} typed family"
                );
                assert!(
                    source["error_is_proto"].as_bool().unwrap(),
                    "actual Go {id} must retain original proto.Error sentinel for either binary failure family"
                );
                assert!(!actual.unwrap_err().is_empty());
            }
        }
        assert!(expected.is_empty());
    }

    #[test]
    #[ignore = "requires actual original tablewriter renderer13 via NODE_TABLE_TRIM13_GO_RESULTS"]
    fn actual_go_table_cell_trim_full_output13() {
        use std::collections::BTreeMap;
        let inputs: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/oracles/list-nodes-render-trim/cases13.json"
        ))
        .unwrap();
        let hex = inputs["base_node_hex"].as_str().unwrap();
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let expected = std::fs::read_to_string(
            std::env::var_os("NODE_TABLE_TRIM13_GO_RESULTS").expect("actual Go renderer output"),
        )
        .unwrap();
        let mut rows = BTreeMap::new();
        for line in expected.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(
                rows.insert(row["id"].as_str().unwrap().to_owned(), row)
                    .is_none()
            );
        }
        assert_eq!(rows.len(), 13);
        for input in inputs["cases"].as_array().unwrap() {
            let mut node = decode_nodes(vec![bytes.clone()]).unwrap().pop().unwrap();
            node.id = input["node_id"].as_str().unwrap().to_owned();
            node.ip = input["ip"].as_str().unwrap().to_owned();
            node.region = input["region"].as_str().unwrap().to_owned();
            let id = input["id"].as_str().unwrap();
            let row = rows.remove(id).unwrap();
            assert_eq!(
                render_nodes(&[node]).unwrap(),
                row["stdout"].as_str().unwrap(),
                "actual source {id}"
            );
        }
        assert!(rows.is_empty());
    }

    #[test]
    fn unicode_line_separators_only_change_measurement_not_output() {
        for separator in ['\u{2028}', '\u{2029}'] {
            let text = format!("owned{separator}second");
            assert_eq!(table_width(&text), 11);
            assert_eq!(
                table_width(&format!("👩{separator}\u{200d}👩")),
                4,
                "separator must remain a grapheme boundary during measurement"
            );
            let node = Node {
                id: "ND_owned".into(),
                region: text.clone(),
                state: 1,
                stats: Some(NodeStats::default()),
                ..Default::default()
            };
            let output = render_nodes(&[node]).unwrap();
            assert!(output.contains(&text));
            assert_eq!(text.matches(separator).count(), 1);
        }
    }

    #[test]
    #[ignore = "requires actual original tablewriter renderer14 via NODE_TABLE_TRIM14_GO_RESULTS"]
    fn actual_go_table_cell_trim_full_output14() {
        use std::collections::BTreeMap;
        let inputs: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/oracles/list-nodes-render-trim/cases14.json"
        ))
        .unwrap();
        let hex = inputs["base_node_hex"].as_str().unwrap();
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let expected = std::fs::read_to_string(
            std::env::var_os("NODE_TABLE_TRIM14_GO_RESULTS").expect("actual Go renderer output"),
        )
        .unwrap();
        let mut rows = BTreeMap::new();
        for line in expected.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(
                rows.insert(row["id"].as_str().unwrap().to_owned(), row)
                    .is_none()
            );
        }
        assert_eq!(rows.len(), 14);
        for input in inputs["cases"].as_array().unwrap() {
            let mut node = decode_nodes(vec![bytes.clone()]).unwrap().pop().unwrap();
            node.id = input["node_id"].as_str().unwrap().to_owned();
            node.ip = input["ip"].as_str().unwrap().to_owned();
            node.region = input["region"].as_str().unwrap().to_owned();
            let id = input["id"].as_str().unwrap();
            let row = rows.remove(id).unwrap();
            assert_eq!(
                render_nodes(&[node]).unwrap(),
                row["stdout"].as_str().unwrap(),
                "actual source {id}"
            );
        }
        assert!(rows.is_empty());
    }
    #[test]
    #[ignore = "requires actual fixed original twwidth width9 via NODE_TABLE_WIDTH_GO_RESULTS"]
    fn actual_fixed_twwidth_rune_context_width9() {
        use std::collections::BTreeMap;
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/oracles/list-nodes-go-width/width9-cases.json"
        ))
        .unwrap();
        let raw = std::fs::read_to_string(
            std::env::var_os("NODE_TABLE_WIDTH_GO_RESULTS").expect("actual source width9"),
        )
        .unwrap();
        let mut expected = BTreeMap::new();
        for line in raw.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(
                expected
                    .insert(row["id"].as_str().unwrap().to_owned(), row)
                    .is_none()
            );
        }
        assert_eq!(expected.len(), 9);
        assert_eq!(cases.as_array().unwrap().len(), 9);
        for input in cases.as_array().unwrap() {
            let id = input["id"].as_str().unwrap();
            let actual = expected.remove(id).unwrap();
            let text = input["text"].as_str().unwrap();
            assert_eq!(actual["text"].as_str().unwrap(), text);
            assert_eq!(
                table_width(text),
                actual["width"].as_u64().unwrap() as usize,
                "actual original source {id}"
            );
        }
        assert!(expected.is_empty());
    }
}
