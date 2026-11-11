//! Prometheus endpoint (`prometheus.port`). Node-level series in the text
//! exposition format; labels are bounded (no room/participant ids, §9).

use std::borrow::Cow;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use base64::Engine as _;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use rtc_runtime::RoomManager;

#[derive(Clone)]
struct MetricsState {
    manager: RoomManager,
    basic_auth: Option<(String, String)>,
    node_id: String,
}

pub fn router(
    manager: RoomManager,
    basic_auth: Option<(String, String)>,
    node_id: String,
) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .with_state(MetricsState {
            manager,
            basic_auth,
            node_id,
        })
}

// Go Request.BasicAuth uses a case-insensitive ASCII scheme and the
// non-strict StdEncoding decoder (CR/LF ignored; nonzero pad bits accepted).
// Compare decoded credential bytes: no Unicode replacement or normalization.
fn authorized_bytes(auth: &[u8], expected: &(String, String)) -> bool {
    if auth.len() < 6 || !auth[..6].eq_ignore_ascii_case(b"Basic ") {
        return false;
    }
    let encoded: Vec<u8> = auth[6..]
        .iter()
        .copied()
        .filter(|byte| *byte != b'\r' && *byte != b'\n')
        .collect();
    let decoder = base64::engine::general_purpose::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::general_purpose::GeneralPurposeConfig::new()
            .with_decode_allow_trailing_bits(true),
    );
    let Ok(decoded) = decoder.decode(encoded) else {
        return false;
    };
    let Some(colon) = decoded.iter().position(|byte| *byte == b':') else {
        return false;
    };
    decoded[..colon] == *expected.0.as_bytes() && decoded[colon + 1..] == *expected.1.as_bytes()
}

fn authorized(headers: &HeaderMap, expected: &(String, String)) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .is_some_and(|value| authorized_bytes(value.as_bytes(), expected))
}

async fn metrics(State(st): State<MetricsState>, headers: HeaderMap) -> Response {
    if let Some(cred) = &st.basic_auth
        && !authorized(&headers, cred)
    {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Basic realm=\"Protected Area\"")],
        )
            .into_response();
    }
    let s = st.manager.node_stats();
    let mut registry = Registry::with_prefix_and_labels(
        "livekit",
        [(Cow::Borrowed("node_id"), Cow::Owned(st.node_id.clone()))].into_iter(),
    );
    let mut out = String::new();
    let mut gauge = |name: &str, help: &str, value: u64| {
        let metric = Gauge::<i64>::default();
        metric.set(value.min(i64::MAX as u64) as i64);
        registry.register(name, help, metric);
    };
    gauge("room_total", "Rooms on this node", u64::from(s.rooms));
    gauge(
        "participant_total",
        "Participants on this node",
        u64::from(s.participants),
    );
    gauge(
        "track_published_total",
        "Published tracks",
        u64::from(s.tracks_in),
    );
    gauge(
        "track_subscribed_total",
        "Subscribed tracks",
        u64::from(s.tracks_out),
    );
    gauge(
        "packet_bytes_in",
        "Media bytes received in active rooms",
        s.bytes_in,
    );
    gauge(
        "packet_bytes_out",
        "Media bytes sent in active rooms",
        s.bytes_out,
    );
    gauge(
        "packet_total_in",
        "Media packets received in active rooms",
        s.packets_in,
    );
    gauge(
        "packet_total_out",
        "Media packets sent in active rooms",
        s.packets_out,
    );
    gauge(
        "estimated_capacity_bps",
        "Effective subscriber BWE capacity bits per second in active rooms",
        s.estimated_capacity_bps,
    );
    gauge("nack_total", "NACKs in active rooms", s.nacks);
    gauge(
        "allocated_bps",
        "Allocated media budget bits per second in active rooms",
        s.allocated_bps,
    );
    gauge(
        "allocation_overshoot_bps",
        "Mandatory media budget overshoot bits per second in active rooms",
        s.allocation_overshoot_bps,
    );
    gauge(
        "paused_video_tracks",
        "Bandwidth paused video subscriptions in active rooms",
        s.paused_video_tracks,
    );
    gauge(
        "probe_attempts",
        "Scheduled native TWCC probes in active rooms",
        s.probe_attempts,
    );
    gauge(
        "probe_successes",
        "Feedback confirmed probes in active rooms",
        s.probe_successes,
    );
    gauge(
        "probe_failures",
        "Failed probes in active rooms",
        s.probe_failures,
    );
    gauge(
        "congestion_events",
        "Congestion responses in active rooms",
        s.congestion_events,
    );
    if encode(&mut out, &registry).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    append_forward_histogram(&mut out, &st.node_id, &s.forward_stats);
    append_packet_bytes(&mut out, &st.node_id, &s);
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        out,
    )
        .into_response()
}

fn append_forward_histogram(
    out: &mut String,
    node_id: &str,
    snapshot: &rtc_core::forward_stats::ForwardStatsSnapshot,
) {
    // The actual shared collector already owns cumulative histogram buckets.
    // Do not replay its snapshots through Histogram::observe (double counts).
    if out.ends_with("# EOF\n") {
        out.truncate(out.len() - 6);
    }
    let node_id = node_id
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n");
    out.push_str(&format!("# HELP livekit_forward_latency Forwarding transit rolling p90 in nanoseconds\n# TYPE livekit_forward_latency gauge\nlivekit_forward_latency{{node_id=\"{node_id}\",node_type=\"SERVER\"}} {}\n",snapshot.latency_ns));
    out.push_str("# HELP livekit_forward_latency_ns Original-packet forwarding admission latency in nanoseconds\n# TYPE livekit_forward_latency_ns histogram\n");
    for (bound, count) in rtc_core::forward_stats::HISTOGRAM_BOUNDS_NS
        .iter()
        .zip(snapshot.histogram_buckets)
    {
        out.push_str(&format!(
            "livekit_forward_latency_ns_bucket{{node_id=\"{node_id}\",node_type=\"SERVER\",le=\"{bound}\"}} {count}\n"
        ));
    }
    out.push_str(&format!("livekit_forward_latency_ns_bucket{{node_id=\"{node_id}\",node_type=\"SERVER\",le=\"+Inf\"}} {}\nlivekit_forward_latency_ns_count{{node_id=\"{node_id}\",node_type=\"SERVER\"}} {}\nlivekit_forward_latency_ns_sum{{node_id=\"{node_id}\",node_type=\"SERVER\"}} {}\n# EOF\n",snapshot.histogram_count,snapshot.histogram_count,snapshot.histogram_sum_ns));
}

/// `livekit_packet_bytes` as the fixed Go server exposes it: a process-lifetime
/// counter, not the active-room gauges above. Measured against the pinned official
/// binary (sha256 4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc):
/// the sample name carries no `_total` suffix, labels are sorted, `country` is
/// always empty and the family is absent until traffic has been handled.
/// prometheus_client's OpenMetrics encoder would append `_total`, which the Region
/// Agent's `HasPrefix("livekit_packet_bytes{")` reader does not match, so the family
/// is written directly.
fn append_packet_bytes(out: &mut String, node_id: &str, s: &rtc_runtime::api::NodeStats) {
    // Go creates each direction's child lazily and independently: TrackStats calls
    // IncrementBytes per stats key (telemetry/stats.go:102), so a subscriber-only
    // participant exposes outgoing without incoming.
    let mut samples = Vec::new();
    if s.bytes_in > 0 || s.packets_in > 0 {
        samples.push(("incoming", "initial", s.bytes_in));
    }
    if s.bytes_out > 0 || s.packets_out > 0 {
        // Go splits downstream retransmissions into their own transmission label but
        // folds upstream retransmissions into the initial count (telemetry/stats.go:85-92).
        // str0m reports retransmit_bytes_out as a subset of bytes_out.
        samples.push((
            "outgoing",
            "initial",
            s.bytes_out.saturating_sub(s.retransmit_bytes_out),
        ));
        if s.retransmit_bytes_out > 0 {
            samples.push(("outgoing", "retransmit", s.retransmit_bytes_out));
        }
    }
    if samples.is_empty() {
        return;
    }
    if out.ends_with("# EOF\n") {
        out.truncate(out.len() - 6);
    }
    let node_id = node_id
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n");
    out.push_str("# HELP livekit_packet_bytes \n# TYPE livekit_packet_bytes counter\n");
    for (direction, transmission, value) in samples {
        out.push_str(&format!(
            "livekit_packet_bytes{{country=\"\",direction=\"{direction}\",node_id=\"{node_id}\",node_type=\"SERVER\",transmission=\"{transmission}\"}} {value}\n"
        ));
    }
    out.push_str("# EOF\n");
}

#[cfg(test)]
mod packet_bytes_counter_tests {
    use super::*;

    fn stats(bytes_in: u64, bytes_out: u64, retransmit: u64) -> rtc_runtime::NodeStats {
        rtc_runtime::NodeStats {
            bytes_in,
            bytes_out,
            retransmit_bytes_out: retransmit,
            packets_in: u64::from(bytes_in > 0),
            packets_out: u64::from(bytes_out > 0),
            ..Default::default()
        }
    }

    #[test]
    fn idle_process_omits_the_whole_family_like_the_lazy_go_counter() {
        let mut out = "# EOF\n".to_string();
        append_packet_bytes(&mut out, "ND_idle", &stats(0, 0, 0));
        assert_eq!(out, "# EOF\n", "no HELP, TYPE or sample before any traffic");
    }

    #[test]
    fn sample_name_has_no_total_suffix_and_labels_match_the_measured_go_order() {
        let mut out = "# EOF\n".to_string();
        append_packet_bytes(&mut out, "ND_x", &stats(11300, 15019, 0));
        assert_eq!(
            out,
            "# HELP livekit_packet_bytes \n\
             # TYPE livekit_packet_bytes counter\n\
             livekit_packet_bytes{country=\"\",direction=\"incoming\",node_id=\"ND_x\",node_type=\"SERVER\",transmission=\"initial\"} 11300\n\
             livekit_packet_bytes{country=\"\",direction=\"outgoing\",node_id=\"ND_x\",node_type=\"SERVER\",transmission=\"initial\"} 15019\n\
             # EOF\n"
        );
        assert!(!out.contains("livekit_packet_bytes_total"));
        assert_eq!(out.matches("# EOF").count(), 1);
    }

    #[test]
    fn downstream_retransmissions_are_their_own_series_and_leave_initial_disjoint() {
        let mut out = "# EOF\n".to_string();
        append_packet_bytes(&mut out, "ND_x", &stats(0, 1000, 250));
        assert!(out.contains("direction=\"outgoing\",node_id=\"ND_x\",node_type=\"SERVER\",transmission=\"initial\"} 750\n"));
        assert!(out.contains("direction=\"outgoing\",node_id=\"ND_x\",node_type=\"SERVER\",transmission=\"retransmit\"} 250\n"));
        // The Agent sums every outgoing line, so the split must not change the total.
        let total: u64 = out
            .lines()
            .filter(|l| l.starts_with("livekit_packet_bytes{") && l.contains("direction=\"outgoing\""))
            .map(|l| l.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
            .sum();
        assert_eq!(total, 1000);
    }

    #[test]
    fn upstream_retransmissions_stay_folded_into_initial() {
        let mut out = "# EOF\n".to_string();
        append_packet_bytes(&mut out, "ND_x", &stats(500, 0, 0));
        assert!(out.contains("direction=\"incoming\",node_id=\"ND_x\",node_type=\"SERVER\",transmission=\"initial\"} 500\n"));
        assert!(!out.contains("direction=\"incoming\",node_id=\"ND_x\",node_type=\"SERVER\",transmission=\"retransmit\""));
    }

    #[test]
    fn a_subscriber_only_process_exposes_outgoing_without_incoming() {
        let mut out = "# EOF\n".to_string();
        append_packet_bytes(&mut out, "ND_x", &stats(0, 900, 0));
        assert!(out.contains("direction=\"outgoing\""));
        assert!(!out.contains("direction=\"incoming\""), "Go creates each direction's child separately");
        assert_eq!(out.matches("# EOF").count(), 1);
    }

    #[test]
    fn repeated_scrapes_never_reset_or_double_count_the_process_total() {
        let s = stats(11300, 15019, 0);
        let mut first = "# EOF\n".to_string();
        let mut second = "# EOF\n".to_string();
        append_packet_bytes(&mut first, "ND_x", &s);
        append_packet_bytes(&mut second, "ND_x", &s);
        assert_eq!(first, second, "scraping is not a sampling event");
    }

    #[test]
    fn node_id_is_escaped_once_and_appends_after_an_existing_family() {
        let mut out = "livekit_room_total 1\n# EOF\n".to_string();
        append_packet_bytes(&mut out, "ND_\"x", &stats(1, 1, 0));
        assert!(out.starts_with("livekit_room_total 1\n"));
        assert!(out.contains("node_id=\"ND_\\\"x\""));
        assert_eq!(out.matches("# EOF").count(), 1);
        assert!(out.ends_with("# EOF\n"));
    }
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: shutdown::Shutdown,
) -> std::io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.wait())
        .await
}

/// Shutdown broadcast shared by the HTTP listeners.
pub mod shutdown {
    use tokio::sync::watch;

    #[derive(Clone)]
    pub struct Shutdown(pub watch::Receiver<bool>);

    impl Shutdown {
        pub async fn wait(mut self) {
            while !*self.0.borrow() {
                if self.0.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod basic_auth_public_entry_tests {
    use super::*;
    #[test]
    fn existing_authorization_entry_accepts_source_case_insensitive_scheme() {
        let expected = ("owned-user".to_owned(), "owned-pass!".to_owned());
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"owned-user:owned-pass!");
        for scheme in ["Basic", "basic", "bAsIc"] {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::AUTHORIZATION,
                format!("{scheme} {encoded}").parse().unwrap(),
            );
            assert!(
                authorized(&headers, &expected),
                "actual source BasicAuth permits ASCII scheme case"
            );
        }
    }
}

#[cfg(test)]
mod basic_auth_byte_decoder_tests {
    use super::*;
    #[test]
    fn source_decoder_preserves_bytes_and_exact_whitespace_rules() {
        let expected = ("u".to_owned(), "p!".to_owned());
        for auth in [
            b"Basic dTpwIQ==".as_slice(),
            b"Basic dTpwIR==",
            b"basic dTpw\r\nIQ==",
        ] {
            assert!(authorized_bytes(auth, &expected));
        }
        for auth in [
            b"Basic dTpwIQ".as_slice(),
            b"Basic dTpw IQ==",
            b"Basic\tdTpwIQ==",
            b" Basic dTpwIQ==",
            b"Basic  dTpwIQ==",
            b"Basic dTpwIQ===",
            b"Basic dXNlcg==",
            b"Basic dTpwIg==",
        ] {
            assert!(!authorized_bytes(auth, &expected));
        }
        assert!(
            !authorized_bytes(b"Basic dTr/", &("u".into(), "\u{fffd}".into())),
            "invalid decoded UTF8 must not become replacement character"
        );
        let expected = ("u".to_owned(), "p:q".to_owned());
        assert!(authorized_bytes(b"Basic dTpwOnE=", &expected));
        assert!(
            !authorized_bytes(b"Basic dTpwOnE=", &("u:p".into(), "q".into())),
            "split at first colon only"
        );
    }
}

#[cfg(test)]
#[path = "metrics_auth_handler_tests.rs"]
mod metrics_auth_handler_tests;

#[cfg(test)]
mod forward_histogram_tests {
    use super::*;
    #[test]
    fn real_collector_histogram_is_exported_once_with_source_units_and_escaping() {
        let now = std::time::Instant::now();
        let collector =
            rtc_core::forward_stats::ForwardStats::new(100_000_000, 100_000_000, 100_000_000, now)
                .unwrap();
        collector.record_forward_samples(
            now + std::time::Duration::from_millis(5),
            [now, now + std::time::Duration::from_millis(4)],
        );
        collector.tick(now + std::time::Duration::from_millis(100));
        let snapshot = collector.snapshot();
        let mut out = "# EOF\n".to_string();
        append_forward_histogram(&mut out, "ND_\"x", &snapshot);
        assert!(out.contains("le=\"1000000\"} 1\n"));
        assert!(out.contains("le=\"5000000\"} 2\n"));
        assert!(out.contains("le=\"+Inf\"} 2\n"));
        assert!(out.contains("} 6000000\n"));
        assert!(out.contains("node_id=\"ND_\\\"x\""));
        assert_eq!(out.matches("# EOF").count(), 1);
        let mut next = String::new();
        append_forward_histogram(&mut next, "ND_\"x", &collector.snapshot());
        assert!(
            next.contains("le=\"+Inf\"} 2\n"),
            "scraping never observes cumulative samples a second time"
        );
    }
}
