//! Frozen fixed-Go table for the RTP media counting boundary (RUNTIME:process_lifetime_source_telemetry).
//!
//! The rows come from `tests/oracles/media-counting-boundary`, which drives the locked
//! `rtpstats.RTPStatsSender`/`RTPStatsReceiver`/`toAnalyticsStream`/`toAnalyticsVideoLayer` code
//! paths over synthetic physical packets. Nothing here re-implements a Go algorithm: every
//! assertion is either (a) a decision the locked source makes about a window, or (b) an arithmetic
//! identity over the numbers the locked source printed. The direction-dependent Prometheus series
//! that the process accumulator produces is deliberately not re-derived here, because feeding a
//! Rust accumulator the numbers it just printed proves nothing; it is covered by the literal-value
//! tests in `crates/livecloud/src/metrics.rs` and by the runtime differentials named in the README.
use serde_json::Value;
use std::collections::BTreeSet;

const PREFIX: &str = "SOURCE_MEDIA_BOUNDARY ";
/// `boundarySSRC` in the observer; `toAnalyticsStream` copies it verbatim.
const SSRC: u64 = 0x1234_5678;
/// `cNumSequenceNumbers` (rtpstats_base_lite.go:33), the window-void bound used by both delta paths.
const NUM_SEQUENCE_NUMBERS: u64 = 65_536;
/// `boundaryHdr`: every packet carries a 12 byte RTP header, so a counted packet is never 0 bytes.
const HEADER_BYTES: u64 = 12;

/// Every row the locked observer must emit, exactly once. A missing or extra row means the frozen
/// table drifted, so the comparison below would no longer cover the boundary it names.
const ROWS: &[(&str, u64)] = &[
    ("downstream_padding_only_before_start", 1),
    ("downstream_padding_only_before_start", 2),
    ("downstream_padding_inside_window", 1),
    ("downstream_padding_inside_window", 2),
    ("downstream_sequence_gap", 1),
    ("downstream_sequence_gap", 2),
    ("downstream_exact_duplicate", 1),
    ("downstream_exact_duplicate", 2),
    ("downstream_late_repair_in_window", 1),
    ("downstream_late_repair_in_window", 2),
    ("downstream_late_below_start", 1),
    ("downstream_late_below_start", 2),
    ("downstream_late_beyond_cache", 1),
    ("downstream_late_beyond_cache", 2),
    ("downstream_too_many_packets_expected", 1),
    ("downstream_too_many_packets_expected", 2),
    ("downstream_zero_progress", 1),
    ("downstream_zero_progress", 2),
    ("downstream_feedback_counters", 1),
    ("rtx_starts_on_padding_only", 1),
    ("non_rtx_never_starts_on_padding", 1),
    ("downstream_padding_repairs_exceed_span", 1),
    ("rtx_reconcile", 1),
    ("upstream_primary_and_padding", 1),
    ("upstream_primary_and_padding", 2),
    ("upstream_padding_only_first", 1),
    ("upstream_padding_only_first", 2),
    ("upstream_gap_late_and_pre_start", 1),
    ("upstream_gap_late_and_pre_start", 2),
    ("upstream_duplicate", 1),
    ("upstream_duplicate", 2),
];

/// `RTPDeltaInfo` as the observer marshals it (rtpstats_base.go:39-61), minus the two timestamps.
const DELTA: &[&str] = &[
    "packets",
    "bytes",
    "header_bytes",
    "packets_duplicate",
    "bytes_duplicate",
    "packets_padding",
    "bytes_padding",
    "packets_lost",
    "packets_missing",
    "packets_out_of_order",
    "frames",
    "rtt_max",
    "jitter_max",
    "nacks",
    "nack_repeated",
    "plis",
    "firs",
];
/// `livekit.AnalyticsStream` fields the observer records (connectionstats.go:485-503).
const STREAM: &[&str] = &[
    "ssrc",
    "primary_packets",
    "primary_bytes",
    "retransmit_packets",
    "retransmit_bytes",
    "padding_packets",
    "padding_bytes",
    "packets_lost",
    "out_of_order",
    "frames",
    "rtt",
    "jitter",
    "nacks",
    "plis",
    "firs",
];
const PHYSICAL: &[&str] = &[
    "sent_primary",
    "sent_padding",
    "sent_duplicate",
    "sent_frames",
    "logical_bytes",
    "sn_span",
];
const LAYER: &[&str] = &["packets", "bytes", "frames"];

/// The delta keys that the locked `DeltaInfoSender` builds (rtpstats_sender.go:1093-1113).
const SENDER_VIEWS: &[&str] = &[
    "sender_view",
    "primary_sender_view",
    "rtx_sender_view",
    "reconciled_sender_view",
];
/// The delta keys that the locked base `deltaInfo` builds (rtpstats_base.go:535-555).
const BASE_DELTAS: &[&str] = &[
    "base_delta",
    "primary_base_delta",
    "rtx_base_delta",
    "reconciled_base_delta",
];

fn object<'a>(row: &'a Value, key: &str) -> Option<&'a Value> {
    match row.get(key) {
        Some(Value::Object(_)) | Some(Value::Array(_)) => row.get(key),
        Some(Value::Null) | None => None,
        Some(other) => panic!("{key}: expected a nullable object, got {other}"),
    }
}

fn text<'a>(row: &'a Value, key: &str, where_: &str) -> &'a str {
    row.get(key)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{where_}: missing string field {key}"))
}

/// An integral counter. Go marshals `uint32`/`uint64` as a JSON integer and `float64` jitter may
/// arrive as `0`; a fractional value where an integer is required is a schema drift, not a pass.
fn count(row: &Value, key: &str, where_: &str) -> u64 {
    match row.get(key) {
        Some(Value::Number(number)) => {
            if let Some(value) = number.as_u64() {
                return value;
            }
            let value = number
                .as_f64()
                .unwrap_or_else(|| panic!("{where_}: field {key} is not a number: {number}"));
            assert_eq!(
                value.fract(),
                0.0,
                "{where_}: field {key} must be integral, got {value}"
            );
            assert!(value >= 0.0, "{where_}: field {key} is negative: {value}");
            value as u64
        }
        Some(missing) => panic!("{where_}: field {key} is not a number: {missing}"),
        None => panic!("{where_}: missing field {key}"),
    }
}

fn real(row: &Value, key: &str, where_: &str) -> f64 {
    match row.get(key) {
        Some(Value::Number(number)) => number
            .as_f64()
            .unwrap_or_else(|| panic!("{where_}: field {key} is not a number: {number}")),
        Some(other) => panic!("{where_}: field {key} is not a number: {other}"),
        None => panic!("{where_}: missing field {key}"),
    }
}

fn shape(row: &Value, keys: &[&str], where_: &str) {
    for key in keys {
        // `JitterMax` is the only non-integral field of `RTPDeltaInfo`, so it must be present but
        // is not required to be whole.
        if *key == "jitter_max" {
            real(row, key, where_);
        } else {
            count(row, key, where_);
        }
    }
}

fn stream<'a>(row: &'a Value, key: &str, where_: &str) -> Option<&'a Value> {
    let value = object(row, key)?;
    shape(value, STREAM, &format!("{where_} {key}"));
    assert_eq!(
        count(value, "ssrc", &format!("{where_} {key}")),
        SSRC,
        "{where_}: {key} must carry the SSRC the delta was built for"
    );
    Some(value)
}

fn delta<'a>(row: &'a Value, key: &str, where_: &str) -> Option<&'a Value> {
    let value = object(row, key)?;
    shape(value, DELTA, &format!("{where_} {key}"));
    // `DeltaInfoSender` never assigns PacketsLost or RttMax, so the sender view reports the feed
    // loss only through PacketsMissing (rtpstats_sender.go:1093-1113).
    // `deltaInfo` never assigns NackRepeated because `snapshotLite`/`snapshot` do not carry it
    // (rtpstats_base.go:806-823, :535-555), so the base view can never report it.
    if SENDER_VIEWS.contains(&key) {
        for field in ["packets_lost", "rtt_max"] {
            assert_eq!(
                count(value, field, &format!("{where_} {key}")),
                0,
                "{where_}: {key}.{field} must stay 0: the sender view is feed-only \
                 (rtpstats_sender.go:1093-1113)"
            );
        }
    } else {
        assert_eq!(
            count(value, "nack_repeated", &format!("{where_} {key}")),
            0,
            "{where_}: {key}.nack_repeated must stay 0: it is not snapshotted by the base delta \
             path (rtpstats_base.go:806-823)"
        );
    }
    Some(value)
}

fn physical<'a>(row: &'a Value, where_: &str, empty_stimulus: bool) -> Option<&'a Value> {
    let value = object(row, "physical")?;
    shape(value, PHYSICAL, &format!("{where_} physical"));
    let primary = count(value, "sent_primary", where_);
    let padding = count(value, "sent_padding", where_);
    let sent = primary + padding;
    let frames = count(value, "sent_frames", where_);
    let duplicate = count(value, "sent_duplicate", where_);
    let span = count(value, "sn_span", where_);
    let bytes = count(value, "logical_bytes", where_);
    assert!(
        frames <= primary,
        "{where_}: a frame is closed by a marker on a payload packet, so frames cannot exceed \
         the primary packets handed over"
    );
    // Every handed-over packet carries the 12 byte RTP header, and only a payload packet can add
    // more than that. This is the physical fact behind the byte axis of the layer rule at
    // connectionstats.go:517: `bytes == 0` is reachable only together with `packets == 0`.
    assert!(
        bytes >= sent * HEADER_BYTES,
        "{where_}: {sent} packets cannot carry fewer than {HEADER_BYTES} bytes each"
    );
    if primary > 0 {
        assert!(
            bytes > sent * HEADER_BYTES,
            "{where_}: a stimulus with a payload packet must exceed the bare header total"
        );
    }
    if empty_stimulus {
        assert_eq!(
            sent + duplicate + bytes,
            0,
            "{where_}: the second read of a window must record an empty stimulus"
        );
    } else {
        assert!(
            sent > 0,
            "{where_}: every row with a stimulus must hand over packets"
        );
        assert!(span >= 1, "{where_}: a non-empty stimulus spans positions");
    }
    Some(value)
}

/// `toAnalyticsStream` (connectionstats.go:455-504): nil in, nil out; otherwise a field-by-field
/// mirror of the delta, with the loss/rtt/jitter source chosen by direction.
fn mirror<'a>(
    row: &'a Value,
    stream_key: &str,
    delta_key: &str,
    is_sender: bool,
    where_: &str,
) -> Option<(&'a Value, &'a Value)> {
    let source = delta(row, delta_key, where_);
    let projected = stream(row, stream_key, where_);
    assert_eq!(
        source.is_some(),
        projected.is_some(),
        "{where_}: {stream_key} must be nil exactly when {delta_key} is nil \
         (connectionstats.go:461-463)"
    );
    let (Some(d), Some(s)) = (source, projected) else {
        return None;
    };
    let at = format!("{where_} {stream_key}");
    for (field, from) in [
        ("primary_packets", "packets"),
        ("primary_bytes", "bytes"),
        ("retransmit_packets", "packets_duplicate"),
        ("retransmit_bytes", "bytes_duplicate"),
        ("padding_packets", "packets_padding"),
        ("padding_bytes", "bytes_padding"),
        ("out_of_order", "packets_out_of_order"),
        ("frames", "frames"),
        ("nacks", "nacks"),
        ("plis", "plis"),
        ("firs", "firs"),
    ] {
        assert_eq!(
            count(s, field, &at),
            count(d, from, &at),
            "{where_}: {stream_key}.{field} must mirror {delta_key}.{from} \
             (connectionstats.go:489-502)"
        );
    }
    // No row in this table ever feeds an RTCP receiver report, so `deltaStatsRemoteView` is nil and
    // the loss figure falls back to the delta itself while the sender path zeroes rtt/jitter
    // (connectionstats.go:467-483). The observer records the receiver views as nil to match.
    for key in [
        "receiver_view",
        "primary_receiver_view",
        "reconciled_receiver_view",
    ] {
        assert!(
            object(row, key).is_none(),
            "{where_}: {key} must be nil: nothing in this table feeds a receiver report \
             (rtpstats_sender.go:1118)"
        );
    }
    assert_eq!(
        count(s, "packets_lost", &at),
        count(d, "packets_lost", &at),
        "{where_}: {stream_key}.packets_lost must fall back to the delta without a remote view"
    );
    if is_sender {
        for field in ["rtt", "jitter"] {
            assert_eq!(
                count(s, field, &at),
                0,
                "{where_}: {stream_key}.{field} must be 0 downstream without a receiver report \
                 (connectionstats.go:480-482)"
            );
        }
    } else {
        assert_eq!(
            count(s, "rtt", &at),
            count(d, "rtt_max", &at),
            "{where_}: {stream_key}.rtt must keep the delta rtt upstream \
             (connectionstats.go:468)"
        );
        assert_eq!(
            count(s, "jitter", &at),
            real(d, "jitter_max", &at).trunc() as u64,
            "{where_}: {stream_key}.jitter must be the truncated delta jitter \
             (connectionstats.go:469, 499)"
        );
    }
    Some((d, s))
}

/// `toAnalyticsVideoLayer` (connectionstats.go:506-521): the three packet classes of one delta
/// fold additively into the layer, and a zero component drops the layer entirely.
fn layer_fold(row: &Value, layer_key: &str, delta_key: &str, where_: &str) {
    // A row that does not record the layer is not evidence about the projection, so it is skipped
    // rather than assumed to have evaluated it.
    if row.get(layer_key).is_none() {
        return;
    }
    let source = delta(row, delta_key, where_);
    let layer = object(row, layer_key);
    let Some(d) = source else {
        assert!(
            layer.is_none(),
            "{where_}: {layer_key} must be nil when {delta_key} is nil (:507-509)"
        );
        return;
    };
    let at = format!("{where_} {layer_key}");
    let packets = count(d, "packets", &at)
        + count(d, "packets_duplicate", &at)
        + count(d, "packets_padding", &at);
    let bytes =
        count(d, "bytes", &at) + count(d, "bytes_duplicate", &at) + count(d, "bytes_padding", &at);
    let frames = count(d, "frames", &at);
    let Some(value) = layer else {
        assert!(
            packets == 0 || bytes == 0 || frames == 0,
            "{where_}: {layer_key} is only dropped when a component is 0 (:517-519), \
             got packets={packets} bytes={bytes} frames={frames}"
        );
        return;
    };
    shape(value, LAYER, &at);
    assert_eq!(
        count(value, "packets", &at),
        packets,
        "{at}: additive packet fold"
    );
    assert_eq!(
        count(value, "bytes", &at),
        bytes,
        "{at}: additive byte fold"
    );
    assert_eq!(
        count(value, "frames", &at),
        frames,
        "{at}: frames pass through"
    );
    for field in ["packets", "bytes", "frames"] {
        assert!(
            count(value, field, &at) > 0,
            "{at}: {field} == 0 must have dropped the layer (:517-519)"
        );
    }
    // The Rust process accumulator keeps the same three classes disjoint
    // (vendor/str0m/src/media_telemetry.rs:22-34, :41-54) and its own unit test pins that
    // invariant (:79-94); how the classes become Prometheus series per direction is covered by
    // the literal-value tests in crates/livecloud/src/metrics.rs, not by this reader.
}

/// `packetsExpected` for the first read of a window whose snapshot id was taken when the highest
/// sequence number was `anchor`: the read stores `extHighestSN + 1` as the window start while the
/// id stored `extHighestSN` (rtpstats_sender.go:454 with :1245, rtpstats_base.go:311-324 with :711).
fn first_window_span(anchor: u64, highest: u64) -> u64 {
    highest - anchor + 1
}

fn check_downstream_window(row: &Value, case: &str, window: u64, where_: &str) {
    let anchor = count(row, "anchor_ext_highest_sn", where_);
    let highest = count(row, "ext_highest_sn", where_);
    let expected = first_window_span(anchor, highest);
    let seen = count(row, "seen_minus_padding", where_);
    let sender = delta(row, "sender_view", where_);
    let base = delta(row, "base_delta", where_);
    mirror(row, "stream_down", "sender_view", true, where_);
    layer_fold(row, "layer_fold", "sender_view", where_);
    if window == 2 {
        // The first read already stored the `now` window, so the second read sees
        // `packetsExpected == 0`: the sender view is dropped outright while the base delta
        // returns an all-zero object (rtpstats_sender.go:1072, rtpstats_base.go:504-510). This is
        // the asymmetry the Rust window filter mirrors at crates/rtc-media/src/receiver_traffic.rs:216.
        assert!(
            sender.is_none(),
            "{where_}: a consumed window must yield a nil sender view"
        );
        let d = base.unwrap_or_else(|| panic!("{where_}: a zero-progress base delta is not nil"));
        let at = format!("{where_} base_delta");
        for field in DELTA {
            assert_eq!(
                count(d, field, &at),
                0,
                "{where_}: base_delta.{field} must be the all-zero window result, not nil"
            );
        }
        return;
    }
    if case == "downstream_too_many_packets_expected" {
        // Both paths void the window above `cNumSequenceNumbers` and, unlike zero progress, return
        // nil rather than a zero object (rtpstats_sender.go:1062, rtpstats_base.go:493-503).
        assert!(
            expected > NUM_SEQUENCE_NUMBERS,
            "{where_}: the case only exercises the bound above {NUM_SEQUENCE_NUMBERS} packets"
        );
        assert!(
            sender.is_none() && base.is_none(),
            "{where_}: an over-long window must yield nil for both views"
        );
        assert!(
            object(row, "stream_down").is_none(),
            "{where_}: a nil delta may not project a stream"
        );
        return;
    }
    let d = sender.unwrap_or_else(|| panic!("{where_}: window 1 must produce a sender view"));
    let at = format!("{where_} sender_view");
    let padding = count(d, "packets_padding", &at);
    if case != "downstream_late_below_start" {
        // `Packets = packetsExpected - uint32(paddingDelta)` with no guard (:1096, :1245), where the
        // window start is the sequence number the sender snapshot id was created at. A late packet
        // below the stream start rewrites that anchor (:523-535), so that one row has no identity.
        assert_eq!(
            count(d, "packets", &at),
            (expected as u32).wrapping_sub(padding as u32) as u64,
            "{where_}: sender_view.packets must be the window sequence span minus padding \
             (rtpstats_sender.go:1061-1096)"
        );
    }
    if let Some(b) = base {
        let bat = format!("{where_} base_delta");
        // A base snapshot id created after the stream is initialised anchors packetsExpected on
        // that packet's position but carries no counters at all (rtpstats_base.go:311-324 with
        // :826-830), so the first read subtracts lifetime counter totals from a window span. For
        // these rows the stream started at the anchor packet, so the window span and the lifetime
        // span coincide, and the recorded lifetime seen-minus-padding is that span minus the
        // lifetime loss minus the lifetime padding (rtpstats_base.go:459-466 with
        // rtpstats_base_lite.go:308-316). Therefore packets == seen + loss, except in the one row
        // where the padding guard voided the packet count.
        if case != "downstream_padding_repairs_exceed_span" {
            assert_eq!(
                count(b, "packets", &bat),
                seen + count(b, "packets_lost", &bat),
                "{where_}: base_delta.packets must be the sequence span minus the lifetime \
                 padding, which is the recorded seen-minus-padding plus the lifetime loss \
                 (rtpstats_base.go:489, 519-538 with 459-466)"
            );
        }
    }
    match case {
        "downstream_exact_duplicate" => {
            // gapSN == 0 is a duplicate and never out-of-order (rtpstats_sender.go:559-567).
            assert_eq!(count(d, "packets_out_of_order", &at), 0, "{where_}");
            assert_eq!(count(d, "packets_duplicate", &at), 1, "{where_}");
        }
        "downstream_late_beyond_cache" => {
            // Too old for the 4 slot cache, so the repair bookkeeping is skipped and the packet
            // lands in the duplicate series while still being counted out-of-order
            // (rtpstats_sender.go:559-571 with :1317-1325).
            assert!(count(d, "packets_out_of_order", &at) >= 1, "{where_}");
            assert!(count(d, "packets_duplicate", &at) >= 1, "{where_}");
        }
        "downstream_late_below_start" => {
            // The late packet below the start moves `extStartSN` and re-anchors both snapshot kinds
            // (:523-535), so the window span no longer equals the recorded anchor distance; the
            // highest sequence number however never moves backwards.
            assert_eq!(highest, anchor + 1, "{where_}");
            assert_eq!(padding, 0, "{where_}");
        }
        "downstream_padding_repairs_exceed_span" => {
            // Four out-of-order padding repairs for holes below the anchor do not advance the
            // highest sequence number, so the padding delta exceeds the window span. The sender
            // view subtracts unguarded and shows the wrap (:1096); the base path trips its
            // `padding packets more than expected` guard, which zeroes only the packet count of an
            // otherwise fully booked delta (rtpstats_base.go:517-533), and the wrapper returns that
            // delta anyway after logging (rtpstats_receiver.go:896-900).
            assert!(
                count(d, "packets", &at) > u64::from(u32::MAX / 2),
                "{where_}: the sender view must show the unguarded underflow of {expected} - {padding}"
            );
            assert!(
                padding > expected,
                "{where_}: padding must exceed the window span"
            );
            let b = base
                .unwrap_or_else(|| panic!("{where_}: the guarded base delta is still returned"));
            let bat = format!("{where_} base_delta");
            assert_eq!(
                count(b, "packets", &bat),
                0,
                "{where_}: the guard must void only the packet count (:530)"
            );
            assert_eq!(
                count(b, "packets_padding", &bat),
                padding,
                "{where_}: the guarded delta must still book every padding packet (:544)"
            );
            assert!(
                object(row, "stream_down").is_some(),
                "{where_}: an underflowed primary count is still a delta, so the stream is projected"
            );
        }
        _ => {}
    }
}

fn check_upstream_window(row: &Value, case: &str, window: u64, where_: &str) {
    let anchor = count(row, "anchor_ext_highest_sn", where_);
    let highest = count(row, "ext_highest_sn", where_);
    let base = delta(row, "base_delta", where_);
    mirror(row, "stream_up", "base_delta", false, where_);
    layer_fold(row, "layer_fold", "base_delta", where_);
    let d = base.unwrap_or_else(|| panic!("{where_}: the receiver window must produce a delta"));
    let at = format!("{where_} base_delta");
    if window == 2 {
        for field in DELTA {
            assert_eq!(
                count(d, field, &at),
                0,
                "{where_}: the second receiver read must be the all-zero window result"
            );
        }
        assert!(
            object(row, "layer_fold").is_none(),
            "{where_}: a zero window cannot project a video layer (connectionstats.go:517-519)"
        );
        return;
    }
    let padding = count(d, "packets_padding", &at);
    if case == "upstream_padding_only_first" {
        // The receiver was still uninitialised when the snapshot id was taken, so the window is
        // anchored lazily on the live stream start at the first read (rtpstats_base.go:701-703)
        // instead of the recorded anchor, and the padding-only first packet never reached the
        // padding series because it was dropped outright (:270-278).
        assert_eq!(
            highest, 111,
            "{where_}: only the primary packet advances the receiver"
        );
        assert_eq!(
            padding, 0,
            "{where_}: a pre-start padding packet leaves no trace"
        );
        assert_eq!(
            count(d, "packets", &at),
            1,
            "{where_}: the lazily anchored window covers exactly the one primary packet"
        );
        return;
    }
    let expected = first_window_span(anchor, highest);
    assert_eq!(
        count(d, "packets", &at),
        expected.saturating_sub(padding),
        "{where_}: base_delta.packets must be the receiver sequence span minus padding \
         (rtpstats_base.go:489, 519-538)"
    );
    if case == "upstream_duplicate" {
        // gapSN == 0 is a duplicate, not out-of-order, for the receiver too
        // (rtpstats_receiver.go:529-531).
        assert_eq!(count(d, "packets_duplicate", &at), 1, "{where_}");
        assert_eq!(count(d, "packets_out_of_order", &at), 0, "{where_}");
    }
    if case == "upstream_gap_late_and_pre_start" {
        // A packet below the stream start is unhandled for the receiver, because its sequence
        // number wrap-around is created with `IsRestartAllowed: false` (rtpstats_receiver.go:203
        // with :490-523): it neither repairs the loss nor moves the start, which is why the window
        // keeps the full span of the three in-range packets.
        assert_eq!(
            highest, 122,
            "{where_}: a pre-start packet must not move the highest"
        );
        assert_eq!(padding, 0, "{where_}");
    }
}

#[test]
#[ignore = "requires actual fixed-Go SOURCE_MEDIA_BOUNDARY rows from tests/oracles/media-counting-boundary"]
fn original_media_counting_boundary_rows_match_locked_go_fold() {
    let path = std::env::var("GO_MEDIA_COUNTING_BOUNDARY_RESULTS")
        .expect("fixed-Go media counting boundary rows");
    let log = std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"));
    let rows: Vec<Value> = log
        .lines()
        .filter_map(|line| line.strip_prefix(PREFIX))
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|error| panic!("unparsable row: {error}"))
        })
        .collect();

    // ---- the frozen table is complete, in both directions ---------------
    let mut seen: BTreeSet<(String, u64)> = BTreeSet::new();
    for row in &rows {
        let case = text(row, "case", "row").to_owned();
        let window = count(row, "window", &case);
        assert!(
            seen.insert((case.clone(), window)),
            "repeated frozen row {case}/{window}"
        );
    }
    let want: BTreeSet<(String, u64)> = ROWS
        .iter()
        .map(|(case, window)| ((*case).to_owned(), *window))
        .collect();
    assert_eq!(
        seen, want,
        "every frozen (case, window) row must be present exactly once"
    );
    assert_eq!(rows.len(), ROWS.len());

    let mut voided_window = 0;
    let mut zero_window = 0;
    let mut underflowed = 0;
    let mut inflated = 0;
    let mut padding_only_stream = 0;
    for row in &rows {
        let case = text(row, "case", "row");
        let window = count(row, "window", case);
        let where_ = format!("{case}/{window}");
        assert_eq!(
            text(row, "direction", &where_),
            if case.starts_with("upstream_") {
                "upstream"
            } else {
                "downstream"
            },
            "{where_}: direction must match the object under test"
        );
        if let Some(is_rtx) = row.get("is_rtx") {
            assert_eq!(
                is_rtx.as_bool(),
                Some(case == "rtx_starts_on_padding_only"),
                "{where_}: is_rtx must name the sender the row came from"
            );
        }
        // The second read of a window is the no-progress interval, so it records an empty
        // stimulus. The one exception is the second row of the pre-start case, which is a
        // different snapshot id that is read for its first time after a real packet.
        let consumed_second_read = window == 2 && case != "downstream_padding_only_before_start";
        physical(row, &where_, consumed_second_read);
        if consumed_second_read {
            zero_window += 1;
        }
        match case {
            "downstream_padding_only_before_start" => {
                // A padding-only packet before the stream is initialised returns before any state
                // change (rtpstats_sender.go:476-480), so it cannot be seen by any view at all.
                assert_eq!(
                    count(object(row, "physical").unwrap(), "sent_primary", &where_),
                    0,
                    "{where_}: the stimulus is padding-only by construction"
                );
                assert!(
                    delta(row, "sender_view", &where_).is_none(),
                    "{where_}: the sender view must be nil"
                );
                assert!(
                    object(row, "stream_down").is_none(),
                    "{where_}: a nil delta may not project a stream"
                );
                if window == 1 {
                    assert_eq!(
                        count(row, "ext_highest_sn", &where_),
                        0,
                        "{where_}: an uninitialised sender has no highest sequence number"
                    );
                    // The lifetime helper still reports one expected position for the untouched
                    // zero/zero pair, which is why a never-started sender shows a phantom packet
                    // here (rtpstats_base.go:459-466 with base_lite.go:308-316).
                    assert_eq!(
                        count(row, "seen_minus_padding", &where_),
                        1,
                        "{where_}: seen-minus-padding is a lifetime span, not a receipt count"
                    );
                } else {
                    assert_eq!(
                        count(row, "ext_highest_sn", &where_),
                        101,
                        "{where_}: the first primary packet initialises the sender"
                    );
                    let d = delta(row, "sender_view", &where_).unwrap_or_else(|| {
                        panic!("{where_}: the primary packet must open a window")
                    });
                    assert_eq!(
                        count(d, "packets", "sender_view"),
                        1,
                        "{where_}: the whole window is the one primary packet"
                    );
                    assert_eq!(
                        count(d, "packets_padding", "sender_view"),
                        0,
                        "{where_}: the pre-start padding must not reach the padding series"
                    );
                }
                mirror(row, "stream_down", "sender_view", true, &where_);
            }
            "downstream_feedback_counters" => {
                check_downstream_window(row, case, window, &where_);
                // The feedback feeds land in the same window: nacks/plis/firs are snapshotted by
                // both paths, nackRepeated only by the sender view (base.go:806-823).
                let at = format!("{where_} sender_view");
                let d = delta(row, "sender_view", &where_).unwrap();
                assert_eq!(count(d, "nacks", &at), 3, "{where_}");
                assert_eq!(count(d, "plis", &at), 2, "{where_}");
                assert_eq!(count(d, "firs", &at), 1, "{where_}");
                assert_eq!(count(d, "nack_repeated", &at), 1, "{where_}");
                let at = format!("{where_} base_delta");
                let b = delta(row, "base_delta", &where_).unwrap();
                assert_eq!(count(b, "nacks", &at), 3, "{where_}");
                assert_eq!(count(b, "plis", &at), 2, "{where_}");
                assert_eq!(count(b, "firs", &at), 1, "{where_}");
                let s = stream(row, "stream_down", &where_).unwrap();
                let at = format!("{where_} stream_down");
                assert_eq!(count(s, "nacks", &at), 3, "{where_}");
                assert_eq!(count(s, "plis", &at), 2, "{where_}");
                assert_eq!(count(s, "firs", &at), 1, "{where_}");
                // The repetition survives in the delta but has no slot in the SDK-visible
                // series: `toAnalyticsStream` (connectionstats.go:465-503) mirrors 13 fields and
                // `nackRepeated` is not one of them, so the projection drops it.
                assert!(
                    s.get("nack_repeated").is_none(),
                    "{where_}: the projection must lose the repetition count, got {}",
                    s["nack_repeated"]
                );
            }
            "rtx_starts_on_padding_only" | "non_rtx_never_starts_on_padding" => {
                assert_eq!(
                    count(object(row, "physical").unwrap(), "sent_primary", &where_),
                    0,
                    "{where_}: the start guard is what this row is about, so the stimulus \
                     must be padding-only"
                );
                if case == "non_rtx_never_starts_on_padding" {
                    assert_eq!(
                        count(row, "ext_highest_sn", &where_),
                        0,
                        "{where_}: a non-RTX sender must never initialise on padding"
                    );
                    assert!(
                        delta(row, "sender_view", &where_).is_none()
                            && delta(row, "base_delta", &where_).is_none(),
                        "{where_}: an uninitialised sender voids both views"
                    );
                    assert_eq!(
                        count(row, "seen_minus_padding", &where_),
                        1,
                        "{where_}: the phantom lifetime position proves no counter moved"
                    );
                } else {
                    // An RTX sender does initialise on padding (:477 exempts IsRTX) and the first
                    // window then charges every padding packet of the stream to itself, because the
                    // snapshot start does not copy the live padding counter (:454 with :1248).
                    assert_eq!(
                        count(row, "ext_highest_sn", &where_),
                        1001,
                        "{where_}: an RTX sender must initialise on a padding-only packet"
                    );
                    let d = delta(row, "sender_view", &where_).unwrap_or_else(|| {
                        panic!("{where_}: the initialised RTX sender has a window")
                    });
                    let at = format!("{where_} sender_view");
                    let expected =
                        first_window_span(count(row, "anchor_ext_highest_sn", &where_), 1001);
                    assert_eq!(
                        count(d, "packets_padding", &at),
                        expected,
                        "{where_}: the whole window is lifetime padding"
                    );
                    assert_eq!(count(d, "packets", &at), 0, "{where_}");
                    assert!(
                        object(row, "stream_down").is_some(),
                        "{where_}: a padding-only window still projects a stream"
                    );
                    assert!(
                        object(row, "layer_fold").is_none(),
                        "{where_}: with no frames the layer must be dropped (:517-519)"
                    );
                }
                mirror(row, "stream_down", "sender_view", true, &where_);
                layer_fold(row, "layer_fold", "sender_view", &where_);
            }
            "downstream_padding_repairs_exceed_span"
            | "downstream_too_many_packets_expected"
            | "downstream_padding_inside_window"
            | "downstream_sequence_gap"
            | "downstream_exact_duplicate"
            | "downstream_late_repair_in_window"
            | "downstream_late_below_start"
            | "downstream_late_beyond_cache"
            | "downstream_zero_progress" => {
                check_downstream_window(row, case, window, &where_);
                let anchor = count(row, "anchor_ext_highest_sn", &where_);
                let highest = count(row, "ext_highest_sn", &where_);
                // Only the first read of a window can be the over-long one: the second read of the
                // same id sees the span that the first read consumed.
                if window == 1 && first_window_span(anchor, highest) > NUM_SEQUENCE_NUMBERS {
                    voided_window += 1;
                }
                if let Some(d) = delta(row, "sender_view", &where_) {
                    if count(d, "packets", "sender_view") > u64::from(u32::MAX / 2) {
                        underflowed += 1;
                    }
                }
                if let (Some(s), Some(p)) =
                    (stream(row, "stream_down", &where_), object(row, "physical"))
                {
                    if count(s, "primary_packets", "stream_down")
                        > count(p, "sent_primary", "physical")
                    {
                        inflated += 1;
                    }
                    if count(p, "sent_primary", "physical") == 0 {
                        padding_only_stream += 1;
                    }
                }
            }
            "rtx_reconcile" => {
                // `ReconcileRTPDeltaInfoWithRTX` (rtpstats_base.go:974-999) moves the RTX window
                // into the primary duplicate/padding series and discounts loss, and never touches
                // the primary packet or byte counts.
                let primary = delta(row, "primary_sender_view", &where_)
                    .expect("rtx_reconcile: a primary sender view");
                let rtx =
                    delta(row, "rtx_sender_view", &where_).expect("rtx_reconcile: an rtx view");
                let done = delta(row, "reconciled_sender_view", &where_)
                    .expect("rtx_reconcile: a reconciled view");
                let at = format!("{where_} reconciled_sender_view");
                for field in [
                    "packets",
                    "bytes",
                    "header_bytes",
                    "frames",
                    "nacks",
                    "nack_repeated",
                    "plis",
                    "firs",
                    "packets_out_of_order",
                ] {
                    assert_eq!(
                        count(done, field, &at),
                        count(primary, field, &at),
                        "{where_}: reconcile must leave {field} of the primary window alone"
                    );
                }
                for (field, from) in [
                    ("packets_duplicate", "packets"),
                    ("bytes_duplicate", "bytes"),
                    ("packets_padding", "packets_padding"),
                    ("bytes_padding", "bytes_padding"),
                ] {
                    assert_eq!(
                        count(done, field, &at),
                        count(primary, field, &at) + count(rtx, from, &at),
                        "{where_}: reconcile must add the RTX {from} into {field} (:979-987)"
                    );
                }
                let adjustment = (count(rtx, "packets", &at) as u32)
                    .wrapping_sub(count(rtx, "packets_lost", &at) as u32)
                    .wrapping_sub(count(primary, "nack_repeated", &at) as u32);
                let adjustment = if (adjustment as i32) < 0 {
                    0
                } else {
                    u64::from(adjustment)
                };
                let lost = count(primary, "packets_lost", &at);
                assert_eq!(
                    count(done, "packets_lost", &at),
                    if adjustment >= lost {
                        0
                    } else {
                        lost - adjustment
                    },
                    "{where_}: reconcile must discount RTX repairs from the loss (:989-998)"
                );
                // The base path reconciles the same way, and it is the `stream_up` series that the
                // upstream telemetry fold reads.
                let p = delta(row, "primary_base_delta", &where_)
                    .expect("rtx_reconcile: a primary base delta");
                let r = delta(row, "rtx_base_delta", &where_)
                    .expect("rtx_reconcile: an rtx base delta");
                let d = delta(row, "reconciled_base_delta", &where_)
                    .expect("rtx_reconcile: a reconciled base delta");
                let at = format!("{where_} reconciled_base_delta");
                assert_eq!(
                    count(d, "packets_duplicate", &at),
                    count(p, "packets_duplicate", &at) + count(r, "packets", &at),
                    "{where_}: the base path reconciles the same way (:979-987)"
                );
                assert_eq!(
                    count(d, "packets_padding", &at),
                    count(p, "packets_padding", &at) + count(r, "packets_padding", &at),
                    "{where_}"
                );
                assert_eq!(
                    count(d, "packets", &at),
                    count(p, "packets", &at),
                    "{where_}"
                );
                // The production-visible consequence: a separate RTX SSRC becomes the
                // RetransmitPackets series of the primary stream, and only after reconciliation.
                mirror(row, "stream_down", "reconciled_sender_view", true, &where_);
                mirror(
                    row,
                    "stream_down_unreconciled",
                    "primary_sender_view",
                    true,
                    &where_,
                );
                mirror(row, "stream_up", "reconciled_base_delta", false, &where_);
                layer_fold(row, "layer_fold", "reconciled_sender_view", &where_);
                let reconciled = stream(row, "stream_down", &where_).unwrap();
                let unreconciled = stream(row, "stream_down_unreconciled", &where_).unwrap();
                let at = format!("{where_} stream_down");
                assert_eq!(
                    count(reconciled, "retransmit_packets", &at)
                        - count(unreconciled, "retransmit_packets", &at),
                    count(rtx, "packets", &at),
                    "{where_}: the RTX primary packets must appear as retransmissions (:979, :491)"
                );
                assert_eq!(
                    count(reconciled, "padding_packets", &at)
                        - count(unreconciled, "padding_packets", &at),
                    count(rtx, "packets_padding", &at),
                    "{where_}: RTX padding adds to the padding series (:984, :493)"
                );
                assert_eq!(
                    count(reconciled, "primary_packets", &at),
                    count(unreconciled, "primary_packets", &at),
                    "{where_}: RTX never adds to the primary series (:979-987)"
                );
                assert!(
                    count(reconciled, "retransmit_packets", &at) > 0,
                    "{where_}: the reconcile row must actually carry retransmissions"
                );
            }
            _ if case.starts_with("upstream_") => check_upstream_window(row, case, window, &where_),
            other => panic!("unexpected frozen row {other}: the table drifted"),
        }
    }

    // ---- the frozen table really exercises every rule above -------------
    assert_eq!(
        voided_window, 1,
        "one window must exceed the sequence-number bound"
    );
    assert_eq!(
        zero_window,
        ROWS.iter()
            .filter(|(case, window)| *window == 2 && *case != "downstream_padding_only_before_start")
            .count(),
        "every windowed case must show the consumed second read"
    );
    assert_eq!(
        underflowed, 1,
        "exactly one window must show the unguarded underflow"
    );
    assert!(
        inflated >= 3,
        "sequence-number-span primaries must exceed the packets actually sent, got {inflated}"
    );
    assert!(
        padding_only_stream >= 1,
        "a padding-only stimulus must still project a stream, got {padding_only_stream}"
    );
}
