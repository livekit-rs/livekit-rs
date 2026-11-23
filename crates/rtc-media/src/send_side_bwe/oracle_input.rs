// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Normalized-input differential for the whole pinned Go stimulus campaign
//! (`operations.tsv` → `golden.tsv`).
//!
//! The oracle drives the frozen upstream sources at
//! `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07` and prints, per campaign row, both
//! the policy state it reached and the normalized input echo columns: the probe
//! cluster goal and result fields exactly as the pinned Go struct fields received
//! them, the standalone group and leaf trend detector the harness owns, and the
//! traffic aggregate each `traffic` row built. This module rebuilds the same
//! stimulus from the same declared-microsecond digits, feeds it to the Rust
//! detector and its own sidecars, and compares the two sides column by column.
//! So one pass proves two separate things: the Rust input is field-for-field the
//! input the pinned Go code got, and the Rust policy output is the output it
//! produced.
//!
//! Scope, and why it is bounded this way:
//!
//! * Every op the campaign uses is replayed except `tseed`, which has no rows
//!   and whose upstream `TrendDetector.Seed` this port deliberately does not
//!   carry (only the remote path uses it, see `trend.rs`). The op vocabulary is a
//!   hard error outside that set, so a campaign edit cannot silently shrink
//!   coverage, and [`PROBE_FAMILY_CASES`]/[`LEAF_BASELINE_CASES`] pin named cases.
//! * Every golden column is compared except the three that identify the row
//!   (`case`, `at_us`, `op`), and the compared set is taken from the oracle's own
//!   header, so a new oracle column becomes a required Rust value instead of an
//!   unchecked gap. `changes`/`committed` are the listener projections upstream
//!   only makes from inside `HandleTWCCFeedback`, which the `twcc` arm now replays.
//!
//! Representation differences the comparisons encode, each of which a case above
//! or a test below pins:
//!
//! * `trk_sn`: Go's `packetTracker.sequenceNumber` is the value the *next* send
//!   hands out and starts at the configured transport-wide start; this port's
//!   field is the last sequence it reconstructed. Before the first send the two
//!   differ by exactly that configured start, afterwards by exactly one, and the
//!   handed-out value itself is compared directly through the `sn` column.
//! * The clock: Go's `origin` is `time.Unix(1000, 0)`, an absolute wall clock,
//!   so an unset base stamp prints `-1_000_000_000`. This port uses an `Instant`
//!   for the same row zero and emits every clock-derived column as an offset from
//!   it, which is [`NO_BASE_US`] when unset. `recv_st_us` needs no conversion at
//!   all: upstream stores `pi.sendTime` as an offset from `baseSendTime` too.
//! * Case construction: the oracle builds the detector for a new case *before* it
//!   installs that row's clock, so the constructor reads the previous row's golden
//!   time. `switched_at_us` and the probe regulator's earliest-probe instant
//!   inherit it, which is why e.g. `pgmax` reports `switched_at_us=5000000` while
//!   its own first row is at 0.
//! * The implementation-defined `int64()` of a non-finite float. Upstream's
//!   `packetGroup.Traffic()` returns a `&trafficStats{}` literal, which carries a
//!   *zero* `WeightedLossConfig`, and nothing in upstream ever asks that object a
//!   rate question — only `Merge` reads its counters. Asking one anyway reaches
//!   `int64(recvDelta * WeightedLoss() * 0.0)`, and `0.0 * ±Inf` and `0.0 * NaN`
//!   are both NaN, whose `int64` conversion is implementation-defined. The oracle
//!   host (arm64 `FCVTZS`) yields 0, and that is what every non-finite row of
//!   `golden.tsv` prints. This port refuses those calls instead, under the
//!   supported-domain guard in [`super::time::float_i64`], and production stays
//!   that way: the split is part of the type design, so an unconfigured aggregate
//!   cannot answer a rate question at all. The echo therefore models the
//!   reference conversion where, and only where, the loss is non-finite
//!   ([`traffic_echo`]), and `every_non_finite_loss_row_prints_zero_conversion`
//!   pins that claim against the actual Go digits rather than assuming it.
//!   `the_echo_model_and_production_agree_on_every_finite_row` pins the other
//!   half: on a finite loss the echo *is* the production answer.
//! * `lr_values` and the float columns compare as parsed numbers, not as text:
//!   Go's `'g'` format with `-1` precision is round-trip exact, and every NaN
//!   prints as the same `NaN`, so NaN-to-NaN equality is the text comparison.
//! * The TWCC report: the oracle hands the detector a `*rtcp.TransportLayerCC`
//!   whose chunk list is one status-vector chunk plus the receive deltas, and the
//!   detector walks it in wire order. This port expands chunks at the wire
//!   boundary (`pion_gcc`), so the arm builds the flat symbol/delta stream that
//!   chunk carries, including pion's rule that a large delta belongs to symbol 2.
//!   A `sym=1` row would ask for one-bit symbol packing, which no campaign row
//!   uses and which the flat representation cannot express, so it is refused
//!   rather than approximated.

use std::collections::BTreeMap;
use std::time::Instant;

use rtc_core::config::{CongestionDetectorConfig, PolicyDuration, WeightedLossConfig};

use super::CongestionDetector;
use super::ccutils::{
    ProbeClusterGoal, ProbeClusterId, ProbeClusterInfo, ProbeClusterResult, ProbeSignal,
};
use super::detector::{CongestionState, TwccFeedbackReport};
use super::group::{GroupError, PacketGroup};
use super::measure::{CongestionReason, QueuingRegion};
use super::time::{at_instant, elapsed_micros, float_i64};
use super::tracker::{NO_BASE_US, PacketInfo};
use super::traffic::{TrafficData, TrafficStats, zero_weighted_loss};
use super::trend::{TrendDetector, TrendDirection};

/// The ops this differential rebuilds. `tseed` is the one campaign op outside
/// it: it has no rows, and this port carries no upstream `TrendDetector.Seed`.
/// Anything else takes its case out of the replay, loudly.
const REPLAYABLE_OPS: [&str; 18] = [
    "send", "msend", "twcc", "rtt", "reset", "pstart", "pdone", "pfin", "settle", "thresh", "recv",
    "traffic", "gadd", "tadd", "pgadd", "lginh", "gsel", "poll",
];

/// The cases this differential is required to cover. A campaign that renames or
/// deletes one of them fails here instead of quietly shrinking coverage.
const PROBE_FAMILY_CASES: [&str; 9] = [
    "psettle",
    "lrtt",
    "trk_sent",
    "trk_none",
    "trk_thr",
    "pgforeign",
    "pgdone",
    "pgsettle",
    "pgmax",
];

/// The leaf-baseline twins: one per config leaf the campaign exercises, each
/// replaying its parent's ops with that one leaf defaulted. Requiring them by
/// name keeps `work/check-leaf-discrimination.py`'s pairs from evaporating out of
/// the Rust side of the comparison.
const LEAF_BASELINE_CASES: [&str; 14] = [
    "tr_dw__dflt_ctr_collapse",
    "tr_dw__dflt_ctr_req",
    "tr_dw__dflt_ctr_req_min",
    "tr_dw__dflt_ctr_th",
    "tr_dw__dflt_ctr_valid",
    "tr_dw__dflt_ctr_wait",
    "deltax__dflt_grp_min",
    "deltax__dflt_grp_win",
    "prune__dflt_max_age",
    "trk_none__dflt_sn_start",
    "tr_cap__dflt_wl_base",
    "tr_cap__dflt_wl_mindur",
    "tr_cap__dflt_wl_pen",
    "tr_cap__dflt_wl_pps",
];

/// The golden columns that name the row instead of describing it. Every other
/// column of the oracle header is compared.
const IDENTITY_COLUMNS: [&str; 3] = ["case", "at_us", "op"];

/// The columns the oracle renders with Go's `strconv.FormatFloat(v, 'g', -1, 64)`.
const FLOAT_COLUMNS: [&str; 17] = [
    "rtt_s",
    "qd_coef",
    "loss_wl",
    "g_wl",
    "g_ctr",
    "pg_wl",
    "pg_ctr",
    "ctr_lowest",
    "ctr_highest",
    "lt_weighted_loss",
    "lt_ctr",
    "lg_wl",
    "lg_ctr",
    "lg_wl_raw",
    "lg_ctr_raw",
    "lr_lowest",
    "lr_highest",
];

/// The columns the oracle renders as a comma-separated list of `f(value)`.
const LIST_COLUMNS: [&str; 1] = ["lr_values"];

/// The probe-group columns the oracle fills with `-1` while no probe is running.
const NO_PROBE_GROUP: [&str; 21] = [
    "pg_max_sn",
    "pg_done_us",
    "pg_propagated_us",
    "pg_pri_pkts",
    "pg_pri_bytes",
    "pg_lost_pkts",
    "pg_lost_bytes",
    "pci_id",
    "pci_result_bytes",
    "pci_result_dur_us",
    "pci_desired_bps",
    "pci_duration_us",
    "pci_goal_available_bps",
    "pci_goal_expected_usage_bps",
    "pci_goal_desired_bytes",
    "pci_result_start_us",
    "pci_result_end_us",
    "pci_result_packets",
    "pci_result_primary_bytes",
    "pci_result_rtx_bytes",
    "pci_result_completed",
];

/// The selected-group columns the oracle fills with `-1` when no group is
/// selectable, i.e. before the first report created one.
const NO_SELECTED_GROUP: [&str; 19] = [
    "g_min_sn",
    "g_max_sn",
    "g_min_send_us",
    "g_max_send_us",
    "g_min_recv_us",
    "g_max_recv_us",
    "g_send_delta_us",
    "g_recv_delta_us",
    "g_inherited_us",
    "g_propagated_us",
    "g_pqd_us",
    "g_pri_pkts",
    "g_pri_bytes",
    "g_rtx_pkts",
    "g_rtx_bytes",
    "g_prb_pkts",
    "g_prb_bytes",
    "g_lost_pkts",
    "g_lost_bytes",
];

/// The `traffic` echo columns the oracle fills with `-2` on a row that did not
/// run that op, plus the pair of `-2` floats among them.
const NO_TRAFFIC_INT: [&str; 9] = [
    "lt_duration_us",
    "lt_send_delta_us",
    "lt_recv_delta_us",
    "lt_acked_pkts",
    "lt_acked_bytes",
    "lt_lost_pkts",
    "lt_lost_bytes",
    "lt_abr",
    "lt_loss_penalty",
];

/// The standalone-group echo columns the oracle fills with `-2` while the
/// harness group has never been touched.
const NO_LEAF_GROUP_INT: [&str; 22] = [
    "lg_err",
    "lg_finalized",
    "lg_min_send_us",
    "lg_max_send_us",
    "lg_min_recv_us",
    "lg_max_recv_us",
    "lg_propagated_us",
    "lg_pri_pkts",
    "lg_pri_bytes",
    "lg_rtx_pkts",
    "lg_rtx_bytes",
    "lg_send_delta_us",
    "lg_recv_delta_us",
    "lg_lost_pkts",
    "lg_lost_bytes",
    "lg_prb_pkts",
    "lg_prb_bytes",
    "lg_abr",
    "lg_abr_raw",
    "lg_lost_pri_pkts",
    "lg_lost_rtx_pkts",
    "lg_lost_prb_pkts",
];

/// The leaf-trend echo columns the oracle fills with `-2` while it has never
/// been fed a value.
const NO_LEAF_TREND_INT: [&str; 5] = [
    "lr_num_samples",
    "lr_direction",
    "lr_has_enough",
    "lr_len",
    "lr_span_us",
];

/// The oracle's per-row defaults for the columns an op, rather than the detector
/// state, decides.
const NOT_ATTEMPTED: i64 = -2;

// ---------------------------------------------------------------------------
// stimulus tables
// ---------------------------------------------------------------------------

/// One campaign row, as the oracle read it.
#[derive(Clone)]
struct Stimulus {
    case: String,
    config: String,
    at_us: i64,
    op: String,
    a: i64,
    b: i64,
    c: i64,
    d: i64,
    extra: String,
}

impl Stimulus {
    fn id(&self) -> String {
        format!("{}@{}us {}", self.case, self.at_us, self.op)
    }
}

/// Both tables are read the way the oracle's CSV reader reads them: `#` starts a
/// comment line, fields are tab-separated, and a row with fewer than the nine
/// stimulus columns gets padded with empty leaves, never truncated.
fn stimulus_rows(text: &str) -> Vec<Stimulus> {
    data_rows(text)
        .into_iter()
        .map(|row| Stimulus {
            case: row[0].clone(),
            config: row[1].clone(),
            at_us: integer(&row, 2),
            op: row[3].clone(),
            a: integer(&row, 4),
            b: integer(&row, 5),
            c: integer(&row, 6),
            d: integer(&row, 7),
            extra: row[8].clone(),
        })
        .collect()
}

fn data_rows(text: &str) -> Vec<Vec<String>> {
    text.lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .map(|line| {
            let mut fields = line
                .split('\t')
                .map(|field| field.trim().to_string())
                .collect::<Vec<_>>();
            while fields.len() < 9 {
                fields.push(String::new());
            }
            fields
        })
        .collect()
}

fn integer(row: &[String], index: usize) -> i64 {
    row[index]
        .parse::<i64>()
        .unwrap_or_else(|e| panic!("column {index} of {:?} is not an integer: {e}", row[0]))
}

/// The golden table as `Vec<column -> value>` per row, plus its header.
fn golden_table(text: &str) -> (Vec<String>, Vec<BTreeMap<String, String>>) {
    let mut rows = data_rows(text).into_iter();
    let header = rows.next().expect("golden.tsv has no header row");
    let body = rows
        .map(|row| {
            assert_eq!(
                row.len(),
                header.len(),
                "golden.tsv column drift: row {:?} has {} fields, header has {}",
                row.first(),
                row.len(),
                header.len()
            );
            header.iter().cloned().zip(row).collect::<BTreeMap<_, _>>()
        })
        .collect::<Vec<_>>();
    (header, body)
}

// ---------------------------------------------------------------------------
// stimulus leaf keys, mirroring the oracle's validators
// ---------------------------------------------------------------------------

fn leaves(extra: &str) -> Vec<(String, String)> {
    extra
        .split(';')
        .filter(|item| !item.is_empty())
        .map(|item| {
            let (key, value) = item
                .split_once('=')
                .unwrap_or_else(|| panic!("leaf {item} has no = separator"));
            (key.to_string(), value.to_string())
        })
        .collect()
}

fn leaf_value(extra: &str, key: &str) -> Option<i64> {
    leaves(extra)
        .into_iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| {
            value
                .parse::<i64>()
                .unwrap_or_else(|e| panic!("{key}={value} is not an integer: {e}"))
        })
}

/// `leafKey`: absent means 0, like the oracle.
fn leaf(extra: &str, key: &str) -> i64 {
    leaf_value(extra, key).unwrap_or(0)
}

fn leaf_or(extra: &str, key: &str, default: i64) -> i64 {
    leaf_value(extra, key).unwrap_or(default)
}

/// `flt`: the free-text column as a bare float, which is where the sample-valued
/// ops put their argument because it is not an integer. A `key=value` spelling is
/// a mistake here, not an absent leaf: `tadd` has one argument and no leaves.
fn flt(op: &str, extra: &str) -> f64 {
    assert!(
        !extra.contains('='),
        "{op}: the value is a bare float, not a leaf list ({extra})"
    );
    extra
        .parse::<f64>()
        .unwrap_or_else(|e| panic!("{op}: {extra} is not a float: {e}"))
}

/// `timeUs`: a time-valued leaf must be spelled exactly `<key>_us`, or the read
/// aborts instead of taking the number at an undeclared scale.
fn time_us(op: &str, extra: &str, key: &str) -> Option<i64> {
    for (name, value) in leaves(extra) {
        if name != key && !name.starts_with(&format!("{key}_")) {
            continue;
        }
        assert!(
            name == format!("{key}_us"),
            "{op}: key {name} does not declare microseconds; spell it {key}_us"
        );
        return Some(
            value
                .parse::<i64>()
                .unwrap_or_else(|e| panic!("{op}: {name}={value} is not an integer: {e}")),
        );
    }
    None
}

fn time_us_or_fail(op: &str, extra: &str, key: &str) -> i64 {
    time_us(op, extra, key).unwrap_or_else(|| panic!("{op}: missing required key {key}_us"))
}

/// `requireKeys`: a leaf no op interprets is an error, so a typo or a stale-unit
/// field can never be dropped silently.
fn require_keys(op: &str, extra: &str, allowed: &[&str]) {
    for (name, _) in leaves(extra) {
        assert!(allowed.contains(&name.as_str()), "unknown {op} key {name}");
    }
}

/// `requireUsScale`: the round trip from the declared-microsecond digit to the
/// duration the Rust port carries and back must be exact. A factor of a thousand
/// anywhere in the conversion trips it.
fn require_us_scale(op: &str, what: &str, want_us: i64, got: PolicyDuration) {
    let got_us = got.as_micros_i64();
    assert_eq!(
        got_us, want_us,
        "{op}: {what} unit scale drifted: stimulus {want_us} us, Rust {got_us} us"
    );
}

// ---------------------------------------------------------------------------
// config projection
// ---------------------------------------------------------------------------

/// The campaign's `key=value` config spec, mapped onto the same leaves the
/// oracle's `config()` applies to `DefaultSendSideBWEConfig`. Every duration key
/// is microseconds, because the oracle's `us()` helper scales it that way, and an
/// unknown key is a hard error on both sides. Returns the transport-wide sequence
/// start the oracle replaces its first random draw with.
fn campaign_config(spec: &str) -> (i64, CongestionDetectorConfig) {
    let mut sn_start = 1 << 15;
    let mut config = CongestionDetectorConfig::default();
    for item in spec.split(';') {
        if item.is_empty() {
            continue;
        }
        let (key, value) = item
            .split_once('=')
            .unwrap_or_else(|| panic!("config leaf {item} has no = separator"));
        let num = || -> i64 {
            value
                .parse()
                .unwrap_or_else(|e| panic!("config {key}={value}: {e}"))
        };
        let us = || -> PolicyDuration { PolicyDuration::from_micros(num()) };
        let flt = || -> f64 {
            value
                .parse()
                .unwrap_or_else(|e| panic!("config {key}={value}: {e}"))
        };
        match key {
            "sn_start" => sn_start = num(),
            "grp_min" => config.packet_group.min_packets = num(),
            "grp_win" => config.packet_group.max_window_duration = us(),
            "max_age" => config.packet_group_max_age = us(),
            "pgrp_min" => config.probe_packet_group.packet_group.min_packets = num(),
            "pgrp_win" => config.probe_packet_group.packet_group.max_window_duration = us(),
            "settle_rtt" => config.probe_packet_group.settle_wait_num_rtt = num() as u32,
            "settle_min" => config.probe_packet_group.settle_wait_min = us(),
            "settle_max" => config.probe_packet_group.settle_wait_max = us(),
            "wl_mindur" => config.weighted_loss.min_duration_for_loss_validity = us(),
            "wl_base" => config.weighted_loss.base_duration = us(),
            "wl_pps" => config.weighted_loss.base_pps = num(),
            "wl_pen" => config.weighted_loss.loss_penalty_factor = flt(),
            "jqr_delay" => config.jqr_min_delay = us(),
            "dqr_delay" => config.dqr_max_delay = us(),
            "jqr_coef" => config.jqr_min_trend_coefficient = flt(),
            "sig_min_bytes" => config.probe_signal.min_bytes_ratio = flt(),
            "sig_min_dur" => config.probe_signal.min_duration_ratio = flt(),
            "sig_jqr_delay" => config.probe_signal.jqr_min_delay = us(),
            "sig_dqr_delay" => config.probe_signal.dqr_max_delay = us(),
            "sig_jqr_wl" => config.probe_signal.jqr_min_weighted_loss = flt(),
            "sig_dqr_wl" => config.probe_signal.dqr_max_weighted_loss = flt(),
            "qdew_jqr_groups" => {
                config.queuing_delay_early_warning_jqr.min_number_of_groups = num()
            }
            "qdew_jqr_dur" => config.queuing_delay_early_warning_jqr.min_duration = us(),
            "qdew_dqr_groups" => {
                config.queuing_delay_early_warning_dqr.min_number_of_groups = num()
            }
            "qdew_dqr_dur" => config.queuing_delay_early_warning_dqr.min_duration = us(),
            "lew_jqr_groups" => config.loss_early_warning_jqr.min_number_of_groups = num(),
            "lew_jqr_dur" => config.loss_early_warning_jqr.min_duration = us(),
            "lew_dqr_groups" => config.loss_early_warning_dqr.min_number_of_groups = num(),
            "lew_dqr_dur" => config.loss_early_warning_dqr.min_duration = us(),
            "qdc_jqr_groups" => config.queuing_delay_congested_jqr.min_number_of_groups = num(),
            "qdc_jqr_dur" => config.queuing_delay_congested_jqr.min_duration = us(),
            "qdc_dqr_groups" => config.queuing_delay_congested_dqr.min_number_of_groups = num(),
            "qdc_dqr_dur" => config.queuing_delay_congested_dqr.min_duration = us(),
            "lc_jqr_groups" => config.loss_congested_jqr.min_number_of_groups = num(),
            "lc_jqr_dur" => config.loss_congested_jqr.min_duration = us(),
            "lc_dqr_groups" => config.loss_congested_dqr.min_number_of_groups = num(),
            "lc_dqr_dur" => config.loss_congested_dqr.min_duration = us(),
            "ctr_req" => config.congested_ctr_trend.required_samples = num(),
            "ctr_req_min" => config.congested_ctr_trend.required_samples_min = num(),
            "ctr_th" => config.congested_ctr_trend.downward_trend_threshold = flt(),
            "ctr_wait" => config.congested_ctr_trend.downward_trend_max_wait = us(),
            "ctr_collapse" => config.congested_ctr_trend.collapse_threshold = us(),
            "ctr_valid" => config.congested_ctr_trend.validity_window = us(),
            "ctr_eps" => config.congested_ctr_epsilon = flt(),
            "cg_min" => config.congested_packet_group.min_packets = num(),
            "cg_win" => config.congested_packet_group.max_window_duration = us(),
            "est_win" => config.estimation_window_duration = us(),
            "pr_base" => config.probe_regulator.base_interval = us(),
            "pr_min" => config.probe_regulator.min_duration = us(),
            "pr_backoff" => config.probe_regulator.backoff_factor = flt(),
            "pr_max_interval" => config.probe_regulator.max_interval = us(),
            "pr_max_duration" => config.probe_regulator.max_duration = us(),
            "pr_diff_factor" => config.probe_regulator.duration_increase_factor = flt(),
            other => panic!("unknown config key {other}"),
        }
    }
    (sn_start, config)
}

// ---------------------------------------------------------------------------
// replay
// ---------------------------------------------------------------------------

/// One compared column, in the kind the oracle renders it in.
#[derive(Clone, Debug)]
enum Value {
    Int(i64),
    Float(f64),
    List(Vec<f64>),
}

/// The Rust side of one row: every golden column the differential claims.
#[derive(Default)]
struct Observed {
    values: BTreeMap<&'static str, Value>,
}

impl Observed {
    fn set(&mut self, column: &'static str, value: i64) {
        self.values.insert(column, Value::Int(value));
    }

    fn set_f64(&mut self, column: &'static str, value: f64) {
        self.values.insert(column, Value::Float(value));
    }

    fn set_list(&mut self, column: &'static str, values: Vec<f64>) {
        self.values.insert(column, Value::List(values));
    }

    fn set_all(&mut self, columns: &[&'static str], value: i64) {
        for column in columns {
            self.set(column, value);
        }
    }

    fn set_floats(&mut self, columns: &[&'static str], value: f64) {
        for column in columns {
            self.set_f64(column, value);
        }
    }

    fn get(&self, column: &str) -> &Value {
        self.values
            .get(column)
            .unwrap_or_else(|| panic!("the replay claims column {column} but produces no value"))
    }
}

fn state_code(state: CongestionState) -> i64 {
    match state {
        CongestionState::None => 0,
        CongestionState::EarlyWarning => 1,
        CongestionState::Congested => 2,
    }
}

/// `int(c.congestionReason)`, in upstream's `iota` order.
fn reason_code(reason: CongestionReason) -> i64 {
    match reason {
        CongestionReason::None => 0,
        CongestionReason::QueuingDelay => 1,
        CongestionReason::Loss => 2,
    }
}

/// `int(signal)` over upstream's `iota` order.
fn signal_code(signal: ProbeSignal) -> i64 {
    match signal {
        ProbeSignal::Inconclusive => 0,
        ProbeSignal::Congesting => 1,
        ProbeSignal::NotCongesting => 2,
    }
}

/// `int(queuingRegion)` over upstream's `DqrRegion`/`Indeterminate`/`JqrRegion`
/// `iota` order.
fn region_code(region: QueuingRegion) -> i64 {
    match region {
        QueuingRegion::Dqr => 0,
        QueuingRegion::Indeterminate => 1,
        QueuingRegion::Jqr => 2,
    }
}

/// `int(direction)` over upstream's `TrendDirection` `iota` order.
fn direction_code(direction: TrendDirection) -> i64 {
    match direction {
        TrendDirection::Inconclusive => 0,
        TrendDirection::Upward => 1,
        TrendDirection::Downward => 2,
    }
}

/// `pgErr`/`lgErr`: 0 accepted, 1 finalized, 2 old packet, 3 any other refusal.
/// This port's `GroupError` has the two upstream sentinels and no other variant,
/// so 3 stays unreachable rather than being invented for a branch Go can take.
fn group_error_code(result: Result<(), GroupError>) -> i64 {
    match result {
        Ok(()) => 0,
        Err(GroupError::Finalized) => 1,
        Err(GroupError::OldPacket) => 2,
    }
}

fn bit(value: bool) -> i64 {
    i64::from(value)
}

/// The wire sequence str0m would have stamped for this send ordinal: the
/// campaign's transport-wide start plus the sends already handed to the tracker,
/// truncated into the 16-bit wire space like the RTP header does.
fn wire_sequence(sn_start: i64, sends: u64) -> u16 {
    (sn_start as u64 + sends) as u16
}

/// `goldenUnixMicro()`: the oracle's absolute clock for a row, in microseconds.
/// Its `origin` is `time.Unix(1000, 0)`, so row zero is one thousand seconds in.
const GOLDEN_ORIGIN_US: i64 = 1_000_000_000;

fn golden_unix_micro(at_us: i64) -> i64 {
    GOLDEN_ORIGIN_US + at_us
}

/// A deliberate unit drift, used only by the negative controls below. It is the
/// exact mistake this differential was created to catch: reading a declared
/// microsecond leaf at another scale. `Drift::None` is the honest read, and a
/// drifted read has to stop at the validator or, if that layer were removed, at
/// the field comparison — which is what proves the comparison is not vacuous.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Drift {
    None,
    GoalMillis,
    StampsAsNanos,
    RecvDeltaMillis,
}

/// The objects the oracle keeps per case outside its row loop: the standalone
/// group, the leaf trend detector, the selected-group index, the TWCC report
/// ordinal, and the listener the harness installs.
struct Session {
    /// `gsel`, or `-1` for "the newest group".
    gsel: i64,
    lg: PacketGroup,
    lg_has: bool,
    lr: TrendDetector,
    lr_has: bool,
    reports: i64,
    changes: i64,
    committed: i64,
}

impl Session {
    fn new(config: &CongestionDetectorConfig) -> Self {
        Self {
            gsel: -1,
            lg: PacketGroup::new(config.packet_group.clone(), config.weighted_loss, 0),
            lg_has: false,
            lr: TrendDetector::new(config.congested_ctr_trend),
            lr_has: false,
            reports: 0,
            changes: 0,
            committed: 0,
        }
    }
}

/// The columns an op itself decides, which the oracle re-defaults for every row.
struct Echo {
    sn: i64,
    bstr_us: i64,
    bstr_ok: i64,
    recv_st_us: i64,
    recv_sd_us: i64,
    recv_rd_us: i64,
    recv_found: i64,
    fin_signal: i64,
    fin_capacity: i64,
    fin_ok: i64,
    settle_ok: i64,
    lg_err: i64,
    pg_err: i64,
    traffic: Option<TrafficStats>,
}

impl Echo {
    fn fresh() -> Self {
        Self {
            sn: -1,
            bstr_us: 0,
            bstr_ok: 0,
            recv_st_us: 0,
            recv_sd_us: 0,
            recv_rd_us: 0,
            recv_found: -1,
            fin_signal: -1,
            fin_capacity: 0,
            fin_ok: 0,
            settle_ok: -1,
            lg_err: NOT_ATTEMPTED,
            pg_err: NOT_ATTEMPTED,
            traffic: None,
        }
    }
}

/// `report(spec, reports)`: the oracle's TWCC feedback packet, as this port's
/// post-unmarshal representation. The default reference time is the one pion
/// would have decoded for the row's own clock, and the default feedback count is
/// the per-case report ordinal, truncated into the wire's `uint8`.
fn feedback_report(op: &str, extra: &str, at_us: i64, reports: i64) -> TwccFeedbackReport {
    let mut base = 0_u16;
    let mut count = 0_u16;
    let mut reference_time = ((golden_unix_micro(at_us) / 64_000) as u32) & ((1 << 24) - 1);
    let mut fb_pkt_count = reports as u8;
    let mut symbols: Vec<u8> = Vec::new();
    let mut deltas_us: Vec<i64> = Vec::new();
    for (key, value) in leaves(extra) {
        match key.as_str() {
            "base" => {
                base = value
                    .parse::<i64>()
                    .unwrap_or_else(|e| panic!("{key}={value}: {e}")) as u16
            }
            "count" => {
                count = value
                    .parse::<i64>()
                    .unwrap_or_else(|e| panic!("{key}={value}: {e}")) as u16
            }
            "ref" => {
                reference_time = value
                    .parse::<i64>()
                    .unwrap_or_else(|e| panic!("{key}={value}: {e}"))
                    as u32
            }
            "fb" => {
                fb_pkt_count = value
                    .parse::<i64>()
                    .unwrap_or_else(|e| panic!("{key}={value}: {e}"))
                    as u8
            }
            "sym" => {
                assert_ne!(
                    value.parse::<i64>().unwrap(),
                    1,
                    "{op}: sym=1 asks for one-bit symbol packing, which the flat \
                     symbol stream this port walks after pion's chunk expansion \
                     cannot express; add the wire chunk layout or use two-bit symbols"
                );
            }
            "states" => {
                for state in value.split(',') {
                    if state.is_empty() {
                        continue;
                    }
                    let (state, repeat) = match state.split_once('#') {
                        Some((state, repeat)) => (state, repeat.parse::<i64>().expect("repeat")),
                        None => (state, 1),
                    };
                    let (symbol, delta) = match state.split_once(':') {
                        Some((symbol, "")) => (symbol.parse::<i64>().expect("symbol"), None),
                        Some((symbol, delta)) => (
                            symbol.parse::<i64>().expect("symbol"),
                            Some(delta.parse::<i64>().expect("delta")),
                        ),
                        None => (state.parse::<i64>().expect("symbol"), None),
                    };
                    for _ in 0..repeat {
                        symbols.push(symbol as u8);
                        if let Some(delta) = delta {
                            deltas_us.push(delta);
                        }
                    }
                }
            }
            other => panic!("unknown {op} key {other}"),
        }
    }
    if count == 0 {
        count = symbols.len() as u16;
    }
    TwccFeedbackReport {
        base_sequence_number: base,
        packet_status_count: count,
        reference_time,
        fb_pkt_count,
        symbols,
        deltas_us,
    }
}

/// The weighted-loss, acknowledged-bitrate, captured-ratio and loss-penalty the
/// oracle prints for a `trafficStats`, from the object's own numbers.
///
/// On a finite loss this is production's answer verbatim, which
/// `the_echo_model_and_production_agree_on_every_finite_row` pins over the whole
/// campaign. On a non-finite loss it substitutes the oracle host's
/// implementation-defined `int64(NaN) == 0` for the conversion production refuses
/// (see the module docs); the ratio and bitrate formulas around it are the same
/// ones `TrafficStats` implements.
fn traffic_echo(stats: &TrafficStats) -> (f64, i64, f64, i64) {
    let loss = stats.weighted_loss();
    if loss.is_finite() {
        return (
            loss,
            stats.acknowledged_bitrate(),
            stats.captured_traffic_ratio(),
            stats.loss_penalty(),
        );
    }

    let data = stats.data();
    let penalty = 0;
    let ratio = if data.recv_delta == 0 {
        0.0
    } else {
        (data.send_delta as f64 / (data.recv_delta + penalty) as f64).min(1.0)
    };
    let duration = stats.duration_us();
    let bitrate = if duration == 0 {
        0
    } else {
        float_i64(data.acked_bytes as f64 * 8.0 * 1e6 / duration as f64 * ratio)
    };
    (loss, bitrate, ratio, penalty)
}

/// The two constants one replay run fixes before its first row: the wall-clock
/// origin every `at_us` is measured from and the wire sequence base.
#[derive(Clone, Copy)]
struct Run {
    origin: Instant,
    sn_start: i64,
}

/// One row of the stimulus applied to the Rust detector, in the oracle's order
/// and with the oracle's validators.
fn apply(
    row: &Stimulus,
    detector: &mut CongestionDetector,
    session: &mut Session,
    run: Run,
    sends: &mut u64,
    echo: &mut Echo,
    drift: Drift,
) {
    let Run { origin, sn_start } = run;
    let now = at_instant(origin, row.at_us);
    let op = row.op.as_str();
    let extra = row.extra.as_str();
    // The standalone group and the leaf trend are echo arms of this row, not of
    // the case: a row that runs none of gadd/lginh or tseed/tadd prints the
    // NOT_ATTEMPTED sentinels instead of the state an earlier row left in the
    // harness objects, which is the rule the oracle applies since the armed-column
    // leak was fixed and the rule campaign case `lgarm` pins on both sides.
    session.lg_has = false;
    session.lr_has = false;
    match op {
        "send" => {
            require_keys(op, extra, &[]);
            let wire = wire_sequence(sn_start, *sends);
            *sends += 1;
            let sequence = detector.packet_tracker_mut().record_packet_send(
                now,
                row.a as usize,
                row.b == 1,
                row.c as ProbeClusterId,
                row.d == 1,
                wire,
            );
            echo.sn = i64::from(sequence as u16);
        }
        "msend" => {
            require_keys(op, extra, &["cluster", "rtx", "probe"]);
            let size = if row.d == 0 { 1_200 } else { row.d };
            let cluster = leaf(extra, "cluster") as ProbeClusterId;
            let rtx = leaf(extra, "rtx") == 1;
            let probe = leaf(extra, "probe") == 1;
            // The oracle stamps burst packet `i` at the row clock plus `i *
            // cadence` microseconds, where `i` restarts at column a for every
            // row. The sequence number is not the ordinal: Go hands it out from
            // the tracker's own counter, which keeps counting across the case.
            let mut last = -1;
            for ordinal in row.a..row.a + row.b {
                let at = at_instant(origin, row.at_us + ordinal * row.c);
                let sequence = detector.packet_tracker_mut().record_packet_send(
                    at,
                    size as usize,
                    rtx,
                    cluster,
                    probe,
                    wire_sequence(sn_start, *sends),
                );
                *sends += 1;
                last = i64::from(sequence as u16);
            }
            echo.sn = last;
        }
        "twcc" => {
            session.reports += 1;
            let report = feedback_report(op, extra, row.at_us, session.reports);
            if let Some(change) = detector.handle_twcc_feedback(&report, now) {
                session.changes += 1;
                session.committed = change.capacity_bps;
            }
        }
        "rtt" => {
            assert_eq!(row.a, 0, "rtt takes rtt_us= in the leaf keys, not column a");
            require_keys(op, extra, &["rtt_us"]);
            let rtt_us = time_us_or_fail(op, extra, "rtt");
            detector.update_rtt(rtt_us as f64 / 1_000_000.0);
        }
        "reset" => {
            require_keys(op, extra, &[]);
            detector.reset(now);
        }
        "pstart" => {
            let dur_us = time_us_or_fail(op, extra, "dur");
            require_keys(op, extra, &["bytes", "dur_us"]);
            let goal = ProbeClusterGoal {
                available_bandwidth_bps: row.c,
                expected_usage_bps: row.d,
                desired_bps: row.b,
                duration: if drift == Drift::GoalMillis {
                    PolicyDuration::from_millis(dur_us)
                } else {
                    PolicyDuration::from_micros(dur_us)
                },
                desired_bytes: leaf(extra, "bytes"),
            };
            if drift != Drift::GoalMillis {
                require_us_scale(op, "Goal.Duration", dur_us, goal.duration);
            }
            detector.probe_cluster_starting(ProbeClusterInfo {
                id: row.a as ProbeClusterId,
                created_at: Some(now),
                goal,
                result: ProbeClusterResult::default(),
            });
        }
        "pdone" => {
            let start_us = time_us_or_fail(op, extra, "start");
            let end_us = time_us_or_fail(op, extra, "end");
            require_keys(
                op,
                extra,
                &[
                    "start_us",
                    "end_us",
                    "packets",
                    "primary",
                    "rtx",
                    "completed",
                ],
            );
            let result = ProbeClusterResult {
                // The pacer's probe observer really does stamp nanoseconds, which
                // is why the stimulus declares microseconds and converts here.
                start_time_ns: if drift == Drift::StampsAsNanos {
                    start_us
                } else {
                    start_us.wrapping_mul(1_000)
                },
                end_time_ns: if drift == Drift::StampsAsNanos {
                    end_us
                } else {
                    end_us.wrapping_mul(1_000)
                },
                packets_probe: leaf(extra, "packets"),
                bytes_probe: row.b,
                packets_non_probe_primary: 0,
                bytes_non_probe_primary: leaf(extra, "primary"),
                packets_non_probe_rtx: 0,
                bytes_non_probe_rtx: leaf(extra, "rtx"),
                is_completed: leaf_or(extra, "completed", 1) == 1,
            };
            if drift != Drift::StampsAsNanos {
                require_us_scale(op, "Result.Duration", end_us - start_us, result.duration());
            }
            detector.probe_cluster_done(
                &ProbeClusterInfo {
                    id: row.a as ProbeClusterId,
                    created_at: None,
                    goal: ProbeClusterGoal::default(),
                    result,
                },
                now,
            );
        }
        "pfin" => {
            require_keys(op, extra, &[]);
            let finalized = detector.probe_cluster_finalize(now);
            let (signal, capacity) = finalized.unwrap_or((ProbeSignal::Inconclusive, 0));
            echo.fin_signal = signal_code(signal);
            echo.fin_capacity = capacity;
            echo.fin_ok = bit(finalized.is_some());
            echo.settle_ok = bit(finalized.is_some());
        }
        "settle" => {
            // Absent `rtt_us` means "do not touch the detector RTT", which keeps a
            // zero and a negative measurement expressible arms.
            if let Some(rtt_us) = time_us(op, extra, "rtt") {
                detector.set_rtt(rtt_us as f64 / 1_000_000.0);
            }
            require_keys(op, extra, &["rtt_us"]);
            let probe_max_sequence_number = detector.packet_tracker().probe_max_sequence_number();
            let rtt = detector.rtt();
            if let Some(probe_group) = detector.probe_packet_group() {
                let settled = probe_group
                    .maybe_finalize_probe(probe_max_sequence_number, rtt, now)
                    .is_some();
                echo.settle_ok = bit(settled);
            }
        }
        "thresh" => {
            require_keys(op, extra, &[]);
            let threshold = detector
                .packet_tracker()
                .base_send_time_threshold(now, row.a);
            echo.bstr_us = threshold.unwrap_or(0);
            echo.bstr_ok = bit(threshold.is_some());
        }
        "recv" => {
            require_keys(op, extra, &[]);
            let wire = row.a as u16;
            echo.recv_found = bit(detector
                .packet_tracker()
                .packet_info_existing(wire)
                .is_some());
            // Go returns the zero `packetInfo` and zero deltas when the slot holds
            // another sequence or the digit is 0, which are this port's defaults.
            if let Some((info, send_delta, recv_delta)) = detector
                .packet_tracker_mut()
                .record_packet_indication_from_remote(wire, row.b)
            {
                echo.recv_st_us = info.send_time;
                echo.recv_sd_us = send_delta;
                echo.recv_rd_us = recv_delta;
            }
        }
        "traffic" => {
            require_keys(op, extra, &["apk", "aby", "lpk", "lb", "zero_wl"]);
            let config: WeightedLossConfig = if leaf(extra, "zero_wl") == 1 {
                zero_weighted_loss()
            } else {
                detector.config().weighted_loss
            };
            let mut stats = TrafficStats::new(config);
            // The traffic arm's four time columns are positional integers, so no
            // `_us` key name can guard them; this is the read the field
            // comparison has to catch on its own.
            let recv_delta = match drift {
                Drift::RecvDeltaMillis => row.d * 1_000,
                _ => row.d,
            };
            stats.merge(&TrafficData {
                min_send_time: row.a,
                max_send_time: row.b,
                send_delta: row.c,
                recv_delta,
                acked_packets: leaf(extra, "apk"),
                acked_bytes: leaf(extra, "aby"),
                lost_packets: leaf(extra, "lpk"),
                lost_bytes: leaf(extra, "lb"),
            });
            echo.traffic = Some(stats);
        }
        "gadd" => {
            require_keys(
                op,
                extra,
                &["size", "cluster", "rd", "lost", "rtx", "probe"],
            );
            session.lg_has = true;
            let packet = PacketInfo {
                sequence_number: row.a as u64,
                send_time: row.b,
                recv_time: row.c,
                probe_cluster_id: leaf(extra, "cluster") as ProbeClusterId,
                size: leaf(extra, "size") as u16,
                is_rtx: leaf(extra, "rtx") == 1,
                is_probe: leaf(extra, "probe") == 1,
            };
            let result =
                session
                    .lg
                    .add(&packet, row.d, leaf(extra, "rd"), leaf(extra, "lost") == 1);
            echo.lg_err = group_error_code(result);
        }
        "tadd" => {
            session.lr_has = true;
            // upstream stamps samples with the clock, which this port takes from
            // the supplied per-row time, so the sample gap is the row gap.
            session.lr.add_value(flt(op, extra), now);
        }
        "pgadd" => {
            require_keys(
                op,
                extra,
                &["size", "cluster", "rd", "lost", "rtx", "probe"],
            );
            assert!(
                row.a >= 0,
                "{}: a negative sequence is not a packet",
                row.id()
            );
            let packet = PacketInfo {
                sequence_number: row.a as u64,
                send_time: row.b,
                recv_time: row.c,
                probe_cluster_id: leaf(extra, "cluster") as ProbeClusterId,
                size: leaf(extra, "size") as u16,
                is_rtx: leaf(extra, "rtx") == 1,
                is_probe: leaf(extra, "probe") == 1,
            };
            let send_delta = row.d;
            let recv_delta = leaf(extra, "rd");
            let lost = leaf(extra, "lost") == 1;
            if let Some(probe_group) = detector.probe_packet_group_mut() {
                echo.pg_err =
                    group_error_code(probe_group.add(&packet, send_delta, recv_delta, lost));
            }
        }
        "lginh" => {
            require_keys(op, extra, &[]);
            // The oracle rebuilds the standalone group with the inherited queue,
            // which is how the detector seeds a group from its predecessor.
            session.lg = PacketGroup::new(
                detector.config().packet_group.clone(),
                detector.config().weighted_loss,
                row.a,
            );
            session.lg_has = true;
        }
        "gsel" => {
            require_keys(op, extra, &[]);
            session.gsel = row.a;
        }
        "poll" => {}
        "tseed" => panic!(
            "{}: op tseed has no upstream Seed counterpart in this port; it must not \
             appear in the campaign until one is ported, and then be replayed for real",
            row.id()
        ),
        other => panic!(
            "{}: op {other} is outside the replay vocabulary; either rebuild it \
             faithfully here or the case leaves coverage",
            row.id()
        ),
    }
}

/// The columns the oracle reads back out of the detector and its own sidecars
/// after the op.
fn read_state(
    row: &Stimulus,
    detector: &CongestionDetector,
    session: &Session,
    echo: &Echo,
    run: Run,
    sends: u64,
    got: &mut Observed,
) {
    let Run { origin, sn_start } = run;
    let now = at_instant(origin, row.at_us);
    got.set("sn", echo.sn);
    got.set("state", state_code(detector.congestion_state()));
    got.set("capacity", detector.estimated_available_channel_capacity());
    got.set("changes", session.changes);
    got.set("committed", session.committed);
    got.set("can_probe", bit(detector.can_probe(now)));
    got.set(
        "probe_duration_us",
        detector.probe_duration().as_micros_i64(),
    );
    got.set(
        "goal_reached",
        bit(detector.probe_cluster_is_goal_reached()),
    );
    got.set("num_groups", detector.packet_group_count() as i64);
    got.set("fin_signal", echo.fin_signal);
    got.set("fin_capacity", echo.fin_capacity);
    got.set("fin_ok", echo.fin_ok);
    got.set_f64("rtt_s", detector.rtt());

    let tracker = detector.packet_tracker();
    let (base_send_us, base_recv_us) = tracker.base_times_us(origin);
    got.set(
        "trk_sn",
        if sends == 0 {
            sn_start
        } else {
            tracker.sequence_number() as i64 + 1
        },
    );
    got.set("trk_base_send_us", base_send_us);
    got.set("trk_base_recv_us", base_recv_us);
    got.set(
        "trk_probe_max_sn",
        tracker.probe_max_sequence_number() as i64,
    );
    got.set("bstr_us", echo.bstr_us);
    got.set("bstr_ok", echo.bstr_ok);
    got.set("recv_st_us", echo.recv_st_us);
    got.set("recv_sd_us", echo.recv_sd_us);
    got.set("recv_rd_us", echo.recv_rd_us);
    got.set("recv_found", echo.recv_found);
    // `lastRecvSn` is -1 until a live `piLastRecv` exists, and then the full
    // 64-bit sequence the aliased slot holds, so the slot can also be a reuse.
    got.set(
        "trk_last_recv_sn",
        tracker.last_recv_sequence().map_or(-1, |sn| sn as i64),
    );

    let feedback = detector.twcc_feedback();
    let (num_reports, num_out_of_order) = feedback.report_counts();
    got.set("twcc_reports", num_reports);
    got.set("twcc_ooo", num_out_of_order);
    got.set(
        "twcc_interval_us",
        feedback.estimated_feedback_interval().as_micros_i64(),
    );
    let (cycles, highest_reference) = feedback.cycles_and_highest_reference();
    got.set("twcc_cycles", cycles);
    got.set("twcc_high_ref_64ms", i64::from(highest_reference));

    got.set("reason", reason_code(detector.congestion_reason()));
    got.set(
        "switched_at_us",
        elapsed_micros(detector.congestion_state_switched_at(), origin),
    );

    match detector.qd_measurement() {
        None => {
            got.set("qd_region", NOT_ATTEMPTED);
            for column in [
                "qd_groups",
                "qd_jqr",
                "qd_dqr",
                "qd_min_idx",
                "qd_max_idx",
                "qd_sealed",
            ] {
                got.set(column, 0);
            }
            got.set_f64("qd_coef", 0.0);
        }
        Some(measure) => {
            let (num_groups, num_jqr, num_dqr) = measure.counts();
            let (min_idx, max_idx) = measure.group_range();
            got.set("qd_region", region_code(measure.queuing_region()));
            got.set("qd_groups", num_groups);
            got.set("qd_jqr", num_jqr);
            got.set("qd_dqr", num_dqr);
            got.set("qd_min_idx", min_idx);
            got.set("qd_max_idx", max_idx);
            got.set("qd_sealed", bit(measure.is_sealed()));
            got.set_f64("qd_coef", measure.trend_coefficient());
        }
    }

    match detector.loss_measurement() {
        None => {
            got.set("loss_region", NOT_ATTEMPTED);
            for column in [
                "loss_groups",
                "loss_min_idx",
                "loss_max_idx",
                "loss_jqr_sealed",
                "loss_dqr_sealed",
            ] {
                got.set(column, 0);
            }
            got.set_f64("loss_wl", 0.0);
            got.set("loss_abr", 0);
        }
        Some(measure) => {
            let (min_idx, max_idx) = measure.group_range();
            let (jqr_sealed, dqr_sealed) = measure.seals();
            let (_, abr, _, _) = traffic_echo(&measure.traffic_stats());
            got.set("loss_region", region_code(measure.queuing_region()));
            got.set("loss_groups", measure.num_groups());
            got.set("loss_min_idx", min_idx);
            got.set("loss_max_idx", max_idx);
            got.set("loss_jqr_sealed", bit(jqr_sealed));
            got.set("loss_dqr_sealed", bit(dqr_sealed));
            got.set_f64("loss_wl", measure.weighted_loss());
            got.set("loss_abr", abr);
        }
    }

    let groups = detector.packet_groups();
    let selected = if session.gsel >= 0 {
        session.gsel
    } else {
        groups.len() as i64 - 1
    };
    if selected >= 0 && (selected as usize) < groups.len() {
        let group = &groups[selected as usize];
        let (min_sn, max_sn) = group.sequence_window();
        let (min_send, max_send) = group.send_window();
        let (min_recv, max_recv) = group.recv_window();
        let (send_delta, recv_delta) = group.aggregate_deltas();
        let (wl, abr, ctr, _) = traffic_echo(&group.traffic());
        got.set("g_exists", 1);
        got.set("g_min_sn", min_sn as i64);
        got.set("g_max_sn", max_sn as i64);
        got.set("g_min_send_us", min_send);
        got.set("g_max_send_us", max_send);
        got.set("g_min_recv_us", min_recv);
        got.set("g_max_recv_us", max_recv);
        got.set("g_send_delta_us", send_delta);
        got.set("g_recv_delta_us", recv_delta);
        got.set("g_inherited_us", group.inherited_queuing_delay());
        got.set("g_propagated_us", group.propagated_queuing_delay());
        got.set(
            "g_pqd_us",
            group.finalized_propagated_queuing_delay().unwrap_or(-1),
        );
        got.set("g_is_finalized", bit(group.is_finalized()));
        got.set("g_pri_pkts", group.acked().primary().packets());
        got.set("g_pri_bytes", group.acked().primary().bytes());
        got.set("g_rtx_pkts", group.acked().rtx().packets());
        got.set("g_rtx_bytes", group.acked().rtx().bytes());
        got.set("g_prb_pkts", group.acked().probe().packets());
        got.set("g_prb_bytes", group.acked().probe().bytes());
        got.set("g_lost_pkts", group.lost().packets());
        got.set("g_lost_bytes", group.lost().bytes());
        got.set_f64("g_wl", wl);
        got.set("g_abr", abr);
        got.set_f64("g_ctr", ctr);
    } else {
        got.set("g_exists", 0);
        got.set_all(&NO_SELECTED_GROUP, -1);
        got.set("g_is_finalized", 0);
        got.set_f64("g_wl", 0.0);
        got.set("g_abr", 0);
        got.set_f64("g_ctr", 0.0);
    }

    let mut all_min_send = 0_i64;
    let mut all_max_send = 0_i64;
    let mut num_finalized = 0_i64;
    for (idx, group) in groups.iter().enumerate() {
        if idx == 0 || group.min_send_time() < all_min_send {
            all_min_send = group.min_send_time();
        }
        if group.send_window().1 > all_max_send {
            all_max_send = group.send_window().1;
        }
        if group.is_finalized() {
            num_finalized += 1;
        }
    }
    got.set("all_min_send_us", all_min_send);
    got.set("all_max_send_us", all_max_send);
    got.set("num_finalized", num_finalized);

    match detector.probe_packet_group() {
        None => {
            got.set("pg_exists", 0);
            got.set_all(&NO_PROBE_GROUP, -1);
            got.set_f64("pg_wl", 0.0);
            got.set("pg_abr", 0);
            got.set_f64("pg_ctr", 0.0);
            got.set("settle_ok", echo.settle_ok);
        }
        Some(probe_group) => {
            let group = probe_group.group();
            let (wl, abr, ctr, _) = traffic_echo(&group.traffic());
            let pci = probe_group.pci();
            got.set("pg_exists", 1);
            got.set("pg_max_sn", probe_group.max_sequence_number() as i64);
            got.set(
                "pg_done_us",
                probe_group
                    .done_at()
                    .map_or(0, |done_at| elapsed_micros(done_at, origin)),
            );
            got.set("pg_propagated_us", group.propagated_queuing_delay());
            got.set("pg_pri_pkts", group.acked().primary().packets());
            got.set("pg_pri_bytes", group.acked().primary().bytes());
            got.set("pg_lost_pkts", group.lost().packets());
            got.set("pg_lost_bytes", group.lost().bytes());
            got.set_f64("pg_wl", wl);
            got.set("pg_abr", abr);
            got.set_f64("pg_ctr", ctr);
            got.set("pci_id", i64::from(pci.id));
            got.set("pci_result_bytes", pci.result.bytes());
            got.set("pci_result_dur_us", pci.result.duration().as_micros_i64());
            got.set("pci_desired_bps", pci.goal.desired_bps);
            got.set("pci_duration_us", pci.goal.duration.as_micros_i64());
            got.set("settle_ok", echo.settle_ok);
            got.set("pci_goal_available_bps", pci.goal.available_bandwidth_bps);
            got.set("pci_goal_expected_usage_bps", pci.goal.expected_usage_bps);
            got.set("pci_goal_desired_bytes", pci.goal.desired_bytes);
            got.set("pci_result_start_us", pci.result.start_time_ns / 1_000);
            got.set("pci_result_end_us", pci.result.end_time_ns / 1_000);
            got.set("pci_result_packets", pci.result.packets_probe);
            got.set(
                "pci_result_primary_bytes",
                pci.result.bytes_non_probe_primary,
            );
            got.set("pci_result_rtx_bytes", pci.result.bytes_non_probe_rtx);
            got.set("pci_result_completed", bit(pci.result.is_completed));
        }
    }

    match detector.congested_ctr_trend() {
        None => {
            got.set("ctr_exists", 0);
            got.set("ctr_samples", -1);
            got.set("ctr_direction", -1);
            got.set("ctr_has_enough", 0);
            got.set_f64("ctr_lowest", 0.0);
            got.set_f64("ctr_highest", 0.0);
        }
        Some(trend) => {
            got.set("ctr_exists", 1);
            got.set("ctr_samples", trend.num_samples());
            got.set("ctr_direction", direction_code(trend.direction()));
            got.set("ctr_has_enough", bit(trend.has_enough_samples()));
            got.set_f64("ctr_lowest", trend.lowest());
            got.set_f64("ctr_highest", trend.highest());
        }
    }

    match detector.estimate_traffic_stats() {
        None => {
            got.set("est_ts_exists", 0);
            got.set("est_ts_abr", 0);
        }
        Some(stats) => {
            let (_, abr, _, _) = traffic_echo(stats);
            got.set("est_ts_exists", 1);
            got.set("est_ts_abr", abr);
        }
    }

    match echo.traffic {
        None => {
            got.set("l_traffic", 0);
            got.set_all(&NO_TRAFFIC_INT, NOT_ATTEMPTED);
            got.set_floats(&["lt_weighted_loss", "lt_ctr"], -2.0);
        }
        Some(stats) => {
            let data = stats.data();
            let (wl, abr, ctr, penalty) = traffic_echo(&stats);
            got.set("l_traffic", 1);
            got.set("lt_duration_us", stats.duration_us());
            got.set("lt_send_delta_us", data.send_delta);
            got.set("lt_recv_delta_us", data.recv_delta);
            got.set("lt_acked_pkts", data.acked_packets);
            got.set("lt_acked_bytes", data.acked_bytes);
            got.set("lt_lost_pkts", data.lost_packets);
            got.set("lt_lost_bytes", data.lost_bytes);
            got.set_f64("lt_weighted_loss", wl);
            got.set_f64("lt_ctr", ctr);
            got.set("lt_abr", abr);
            got.set("lt_loss_penalty", penalty);
        }
    }

    if session.lg_has {
        let group = &session.lg;
        let (wl, abr, ctr, _) = traffic_echo(&group.configured_traffic_stats());
        let (wl_raw, abr_raw, ctr_raw, _) = traffic_echo(&group.traffic());
        let (min_send, max_send) = group.send_window();
        let (min_recv, max_recv) = group.recv_window();
        let (send_delta, recv_delta) = group.aggregate_deltas();
        got.set("l_group", 1);
        got.set("lg_err", echo.lg_err);
        got.set("lg_finalized", bit(group.is_finalized()));
        got.set("lg_min_send_us", min_send);
        got.set("lg_max_send_us", max_send);
        got.set("lg_min_recv_us", min_recv);
        got.set("lg_max_recv_us", max_recv);
        got.set("lg_propagated_us", group.propagated_queuing_delay());
        got.set("lg_pri_pkts", group.acked().primary().packets());
        got.set("lg_pri_bytes", group.acked().primary().bytes());
        got.set("lg_rtx_pkts", group.acked().rtx().packets());
        got.set("lg_rtx_bytes", group.acked().rtx().bytes());
        got.set("lg_send_delta_us", send_delta);
        got.set("lg_recv_delta_us", recv_delta);
        got.set("lg_lost_pkts", group.lost().packets());
        got.set("lg_lost_bytes", group.lost().bytes());
        got.set("lg_prb_pkts", group.acked().probe().packets());
        got.set("lg_prb_bytes", group.acked().probe().bytes());
        got.set_f64("lg_wl", wl);
        got.set("lg_abr", abr);
        got.set_f64("lg_ctr", ctr);
        got.set_f64("lg_wl_raw", wl_raw);
        got.set("lg_abr_raw", abr_raw);
        got.set_f64("lg_ctr_raw", ctr_raw);
        got.set("lg_lost_pri_pkts", group.lost().primary().packets());
        got.set("lg_lost_rtx_pkts", group.lost().rtx().packets());
        got.set("lg_lost_prb_pkts", group.lost().probe().packets());
    } else {
        got.set("l_group", 0);
        got.set_all(&NO_LEAF_GROUP_INT, NOT_ATTEMPTED);
        got.set_floats(&["lg_wl", "lg_ctr", "lg_wl_raw", "lg_ctr_raw"], -2.0);
    }

    if session.lr_has {
        got.set("l_trend", 1);
        got.set("lr_num_samples", session.lr.num_samples());
        got.set("lr_direction", direction_code(session.lr.direction()));
        got.set("lr_has_enough", bit(session.lr.has_enough_samples()));
        got.set_f64("lr_lowest", session.lr.lowest());
        got.set_f64("lr_highest", session.lr.highest());
        let values = session.lr.sample_values();
        got.set("lr_len", values.len() as i64);
        got.set_list("lr_values", values);
        got.set("lr_span_us", session.lr.sample_span_us());
    } else {
        got.set("l_trend", 0);
        got.set_all(&NO_LEAF_TREND_INT, NOT_ATTEMPTED);
        got.set_floats(&["lr_lowest", "lr_highest"], -2.0);
        got.set_list("lr_values", Vec::new());
    }

    got.set("pg_err", echo.pg_err);
}

/// Compare this row's Rust columns against the Go columns. A column the replay
/// does not produce, or that the regenerated header no longer carries, is a hard
/// error rather than a silent skip.
fn compare(
    row: &Stimulus,
    compared: &[String],
    gold: &BTreeMap<String, String>,
    got: &Observed,
    diffs: &mut Vec<String>,
) {
    for column in compared {
        let want = gold.get(column.as_str()).unwrap_or_else(|| {
            panic!("golden.tsv has no column {column}: the oracle header moved")
        });
        let mismatch = match kind(column) {
            Kind::Int => match got.get(column) {
                Value::Int(value) if &value.to_string() == want => None,
                Value::Int(value) => Some(format!("rust={value} go={want}")),
                other => Some(format!("rust {:?} is not an integer column value", other)),
            },
            Kind::Float => match got.get(column) {
                Value::Float(value) => {
                    let parsed = want.parse::<f64>().unwrap_or_else(|e| {
                        panic!("golden column {column}={want} is not a float: {e}")
                    });
                    if floats_agree(*value, parsed) {
                        None
                    } else {
                        Some(format!("rust={value} go={want}"))
                    }
                }
                other => Some(format!("rust {:?} is not a float column value", other)),
            },
            Kind::List => match got.get(column) {
                Value::List(values) => {
                    let want_values = list(want);
                    if values.len() != want_values.len() {
                        Some(format!(
                            "rust has {} values {:?}, go has {} {}",
                            values.len(),
                            values,
                            want_values.len(),
                            want
                        ))
                    } else {
                        let bad = values
                            .iter()
                            .zip(&want_values)
                            .find(|(got, want)| !floats_agree(**got, **want));
                        bad.map(|(got, want)| format!("rust={got} go={want} in {want}"))
                    }
                }
                other => Some(format!("rust {:?} is not a list column value", other)),
            },
        };
        if let Some(mismatch) = mismatch {
            diffs.push(format!("{} {column}: {mismatch}", row.id()));
        }
    }
}

enum Kind {
    Int,
    Float,
    List,
}

fn kind(column: &str) -> Kind {
    if LIST_COLUMNS.contains(&column) {
        Kind::List
    } else if FLOAT_COLUMNS.contains(&column) {
        Kind::Float
    } else {
        Kind::Int
    }
}

/// Go renders a float with the shortest round-trip `'g'` form, so every NaN is
/// the same text and every other value compares exactly.
fn floats_agree(got: f64, want: f64) -> bool {
    (got.is_nan() && want.is_nan()) || got.to_bits() == want.to_bits()
}

/// The oracle's `strings.Join(vals, ",")` of `f(value)`, read back as numbers. An
/// empty field list is the unarmed and empty-list spelling alike.
fn list(text: &str) -> Vec<f64> {
    if text.is_empty() {
        return Vec::new();
    }
    text.split(',')
        .map(|value| {
            value
                .parse::<f64>()
                .unwrap_or_else(|e| panic!("list value {value} is not a float: {e}"))
        })
        .collect()
}

/// The result of a whole-table replay.
pub(super) struct Replay {
    /// Field-level differences, one per column per row.
    pub(super) diffs: Vec<String>,
    /// Names of the cases that were replayed.
    pub(super) cases: Vec<String>,
    /// Names of the cases that fell out of the replay.
    pub(super) skipped: Vec<String>,
    /// Rows whose every claimed column was compared.
    pub(super) rows: usize,
    /// Column values compared across all replayed rows.
    pub(super) fields: usize,
    /// Golden columns compared per row.
    pub(super) columns: usize,
}

/// Replay every replayable case, in campaign order, against the actual pinned Go
/// output. The stimulus and the golden table are both the committed campaign, so
/// this is a same-source comparison: nothing here re-derives a number by hand.
fn replay_campaign(operations: &str, golden: &str, drift: Drift) -> Replay {
    let stimulus = stimulus_rows(operations);
    let (header, gold) = golden_table(golden);
    assert_eq!(
        stimulus.len(),
        gold.len(),
        "operations.tsv has {} stimulus rows but golden.tsv has {} output rows",
        stimulus.len(),
        gold.len()
    );
    let compared: Vec<String> = header
        .iter()
        .filter(|column| !IDENTITY_COLUMNS.contains(&column.as_str()))
        .cloned()
        .collect();
    // The two tables describe the same stimulus, row by row. Anything else would
    // compare a Rust row against another case's Go output.
    for (row, output) in stimulus.iter().zip(&gold) {
        assert_eq!(row.case, output["case"], "campaign row alignment lost");
        assert_eq!(
            row.at_us.to_string(),
            output["at_us"],
            "{}: campaign row alignment lost",
            row.id()
        );
        assert_eq!(
            row.op,
            output["op"],
            "{}: campaign row alignment lost",
            row.id()
        );
    }

    let origin = Instant::now();
    let mut replay = Replay {
        diffs: Vec::new(),
        cases: Vec::new(),
        skipped: Vec::new(),
        rows: 0,
        fields: 0,
        columns: compared.len(),
    };
    // Each case is replayed as its own scenario: the detector is constructed at
    // this case's zero time, exactly like the oracle, which resets its package-level
    // clock before constructing a case's objects. Carrying the previous case's last
    // row instant into a fresh detector instead would let a column such as
    // switched_at_us report the order of the cases in the stimulus file, and a leaf
    // baseline could then "move" without the leaf doing anything.
    let mut start = 0;
    while start < stimulus.len() {
        let mut end = start;
        while end < stimulus.len() && stimulus[end].case == stimulus[start].case {
            end += 1;
        }
        let rows = &stimulus[start..end];
        let replayable = rows
            .iter()
            .all(|row| REPLAYABLE_OPS.contains(&row.op.as_str()));
        if replayable {
            replay.cases.push(rows[0].case.clone());
            let (sn_start, config) = campaign_config(&rows[0].config);
            let run = Run { origin, sn_start };
            let mut detector = CongestionDetector::new(config.clone(), at_instant(origin, 0));
            let mut session = Session::new(&config);
            let mut sends = 0_u64;
            for (row, output) in rows.iter().zip(&gold[start..end]) {
                let mut echo = Echo::fresh();
                apply(
                    row,
                    &mut detector,
                    &mut session,
                    run,
                    &mut sends,
                    &mut echo,
                    drift,
                );
                let mut got = Observed::default();
                read_state(row, &detector, &session, &echo, run, sends, &mut got);
                compare(row, &compared, output, &got, &mut replay.diffs);
                replay.rows += 1;
                replay.fields += compared.len();
            }
        } else {
            replay.skipped.push(rows[0].case.clone());
        }
        start = end;
    }
    replay
}

fn replay_whole_campaign() -> Replay {
    replay_campaign(
        include_str!("operations.tsv"),
        include_str!("golden.tsv"),
        Drift::None,
    )
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A stimulus row with nothing but the arguments a control needs.
    fn row(op: &str, a: i64, b: i64, at_us: i64, extra: &str) -> Stimulus {
        row4(op, a, b, 0, 0, at_us, extra)
    }

    /// The same with all four integer argument columns spelled out, which the
    /// packet-shaped ops (`msend`, `traffic`, `gadd`, `pgadd`) need.
    fn row4(op: &str, a: i64, b: i64, c: i64, d: i64, at_us: i64, extra: &str) -> Stimulus {
        Stimulus {
            case: "nc".to_string(),
            config: String::new(),
            at_us,
            op: op.to_string(),
            a,
            b,
            c,
            d,
            extra: extra.to_string(),
        }
    }

    /// A campaign detector, driven by one op at a time through the replay itself,
    /// so a control exercises the same code path the differential does.
    struct Harness {
        origin: Instant,
        detector: CongestionDetector,
        session: Session,
        sn_start: i64,
        sends: u64,
    }

    impl Harness {
        fn plain() -> Self {
            Self::with_config("")
        }

        fn with_config(spec: &str) -> Self {
            let (sn_start, config) = campaign_config(spec);
            Self {
                origin: Instant::now(),
                detector: CongestionDetector::new(config.clone(), Instant::now()),
                session: Session::new(&config),
                sn_start,
                sends: 0,
            }
        }

        fn apply(&mut self, row: &Stimulus) -> Observed {
            let mut echo = Echo::fresh();
            let run = Run {
                origin: self.origin,
                sn_start: self.sn_start,
            };
            super::apply(
                row,
                &mut self.detector,
                &mut self.session,
                run,
                &mut self.sends,
                &mut echo,
                Drift::None,
            );
            let mut got = Observed::default();
            read_state(
                row,
                &self.detector,
                &self.session,
                &echo,
                run,
                self.sends,
                &mut got,
            );
            got
        }
    }

    fn int(got: &Observed, column: &str) -> i64 {
        match got.get(column) {
            Value::Int(value) => *value,
            other => panic!("column {column} is {other:?}, not an integer"),
        }
    }

    fn float(got: &Observed, column: &str) -> f64 {
        match got.get(column) {
            Value::Float(value) => *value,
            other => panic!("column {column} is {other:?}, not a float"),
        }
    }

    fn list_of(got: &Observed, column: &str) -> Vec<f64> {
        match got.get(column) {
            Value::List(values) => values.clone(),
            other => panic!("column {column} is {other:?}, not a list"),
        }
    }

    /// The golden table as rows, for the claims a control makes about the actual
    /// pinned Go output rather than about a hand-written expectation.
    fn golden_rows() -> (Vec<String>, Vec<BTreeMap<String, String>>) {
        golden_table(include_str!("golden.tsv"))
    }

    #[test]
    fn the_whole_campaign_matches_the_go_oracle_field_by_field() {
        let replay = replay_whole_campaign();
        for required in PROBE_FAMILY_CASES {
            assert!(
                replay.cases.iter().any(|case| case == required),
                "campaign case {required} dropped out of the campaign differential"
            );
        }
        for twin in LEAF_BASELINE_CASES {
            assert!(
                replay.cases.iter().any(|case| case == twin),
                "leaf-baseline case {twin} is not replayed against the Go output"
            );
        }
        // No campaign row may fall out of the comparison: a case is skipped only
        // when one of its ops is outside the replay vocabulary, and only `tseed`
        // is, and the campaign does not use it.
        assert!(
            replay.skipped.is_empty(),
            "cases left the differential: {:?}",
            replay.skipped
        );
        let campaign = stimulus_rows(include_str!("operations.tsv")).len();
        assert_eq!(
            replay.rows, campaign,
            "the differential replayed {} of the campaign's {} rows",
            replay.rows, campaign
        );
        assert_eq!(
            replay.fields,
            replay.rows * replay.columns,
            "every replayed row must compare every claimed column"
        );
        assert_eq!(
            replay.columns + IDENTITY_COLUMNS.len(),
            golden_rows().0.len(),
            "the differential compares {} of the oracle's {} columns",
            replay.columns,
            golden_rows().0.len()
        );
        assert!(
            replay.diffs.is_empty(),
            "{} field differences over {} rows and {} columns of {} replayed cases:\n{}",
            replay.diffs.len(),
            replay.rows,
            replay.columns,
            replay.cases.len(),
            replay.diffs.join("\n")
        );
    }

    #[test]
    fn every_golden_column_is_classified_and_produced() {
        // A new oracle column must be compared, and compared in the kind the
        // oracle renders it in. Without this, a header change would silently
        // downgrade the new column to an integer text compare.
        let (header, body) = golden_rows();
        let mut classified = FLOAT_COLUMNS.to_vec();
        classified.extend(LIST_COLUMNS);
        for column in &header {
            if IDENTITY_COLUMNS.contains(&column.as_str()) {
                continue;
            }
            let kind = kind(column);
            let sample = &body[0][column.as_str()];
            match kind {
                Kind::Int => assert!(
                    sample.parse::<i64>().is_ok(),
                    "column {column} reads {sample}, which the differential treats as an integer: \
                     add it to FLOAT_COLUMNS or LIST_COLUMNS"
                ),
                Kind::Float => assert!(
                    FLOAT_COLUMNS.contains(&column.as_str()),
                    "column {column} is treated as a float but is not declared one"
                ),
                Kind::List => assert!(
                    LIST_COLUMNS.contains(&column.as_str()),
                    "column {column} is treated as a list but is not declared one"
                ),
            }
        }
        // Every replayed row reaches `read_state`, which is the only place that
        // fills columns, so the whole campaign must cover the whole header.
        let replay = replay_whole_campaign();
        assert_eq!(replay.columns, header.len() - IDENTITY_COLUMNS.len());
    }

    #[test]
    fn the_config_projection_reads_every_duration_as_the_declared_microseconds() {
        // The campaign's own probe-family spec, plus the frozen leaves it does not
        // touch, each checked in the unit the stimulus declares.
        let (sn_start, config) = campaign_config("sn_start=1;grp_min=2;grp_win=5000000");
        assert_eq!(sn_start, 1);
        assert_eq!(config.packet_group.min_packets, 2);
        assert_eq!(
            config.packet_group.max_window_duration,
            PolicyDuration::from_micros(5_000_000),
            "grp_win is microseconds: 5000000 is the 5 s group, not the 5 ms one"
        );
        assert_eq!(
            config.probe_packet_group.settle_wait_max,
            PolicyDuration::from_secs(5),
            "an untouched leaf stays the frozen upstream default"
        );
        assert_eq!(
            config.probe_regulator.min_duration,
            PolicyDuration::from_millis(200)
        );
    }

    #[test]
    fn a_millisecond_spelling_of_a_time_leaf_is_refused_not_rescaled() {
        // The pre-fix stimulus spelled these `dur_ms`/`end_ms`, and the pinned Go
        // types then read the digits as a count of milliseconds. The oracle aborts
        // on that spelling; this mirror has to abort identically, at any scale.
        for (op, key, extra) in [
            ("pstart", "dur", "bytes=20000;dur_ms=1000"),
            ("pdone", "end", "start_us=0;end_ms=600"),
            ("settle", "rtt", "rtt_ms=20"),
            ("rtt", "rtt", "rtt=100000"),
        ] {
            let panicked = std::panic::catch_unwind(|| {
                let _ = time_us(op, extra, key);
            })
            .is_err();
            assert!(panicked, "{op}: {extra} must not be read at any scale");
        }
        // The declared spelling reads exactly the digits it declares, and an
        // absent optional leaf stays absent instead of becoming a zero wait.
        assert_eq!(
            time_us("pstart", "bytes=20000;dur_us=1000000", "dur"),
            Some(1_000_000)
        );
        assert_eq!(time_us("settle", "", "rtt"), None);
    }

    #[test]
    fn a_stale_or_misspelled_leaf_is_an_error_instead_of_a_silent_default() {
        let panicked = std::panic::catch_unwind(|| {
            require_keys(
                "pdone",
                "start_us=0;end_us=600000;dur_ms=600",
                &[
                    "start_us",
                    "end_us",
                    "packets",
                    "primary",
                    "rtx",
                    "completed",
                ],
            )
        })
        .is_err();
        assert!(
            panicked,
            "a key pdone does not interpret must abort the read"
        );
        require_keys(
            "pdone",
            "start_us=0;end_us=600000;completed=1",
            &[
                "start_us",
                "end_us",
                "packets",
                "primary",
                "rtx",
                "completed",
            ],
        );
        assert!(
            std::panic::catch_unwind(|| campaign_config("sn_start=1;grp_mln=2")).is_err(),
            "an unknown config leaf is a hard error, as it is in the oracle"
        );
        // The report and traffic keys are validated the same way, because those
        // two ops carry the campaign's largest leaf vocabulary.
        assert!(
            std::panic::catch_unwind(|| {
                let mut harness = Harness::plain();
                harness.apply(&row(
                    "twcc",
                    0,
                    0,
                    0,
                    "base=32768;count=2;ref=15625;fb=1;statez=0:1000",
                ))
            })
            .is_err(),
            "a key the report builder does not interpret must abort the read"
        );
        assert!(
            std::panic::catch_unwind(|| {
                let mut harness = Harness::plain();
                harness.apply(&row("traffic", 0, 1, 2, "apk=3;ab=400"))
            })
            .is_err(),
            "a misspelled traffic byte leaf must abort the read, not default to 0"
        );
    }

    #[test]
    fn a_bare_float_argument_is_read_as_a_float_and_nothing_else() {
        // `tadd` puts its sample in the free-text column. Reading it with the
        // leaf parser would silently produce 0.0 and compare as a real sample.
        assert_eq!(flt("tadd", "1.5"), 1.5);
        assert_eq!(flt("tadd", "-0.25"), -0.25);
        assert!(std::panic::catch_unwind(|| flt("tadd", "value=1.5")).is_err());
        assert!(std::panic::catch_unwind(|| flt("tadd", "one")).is_err());
    }

    #[test]
    fn the_round_trip_guard_trips_on_a_thousandfold_scale_error() {
        // What the guard is for: the declared digit and the duration the port
        // carries have to read back identically, so a `from_millis` where
        // `from_micros` belongs cannot survive it.
        require_us_scale(
            "pstart",
            "Goal.Duration",
            1_000_000,
            PolicyDuration::from_micros(1_000_000),
        );
        assert!(
            std::panic::catch_unwind(|| require_us_scale(
                "pstart",
                "Goal.Duration",
                1_000_000,
                PolicyDuration::from_micros(1_000),
            ))
            .is_err()
        );
        assert!(
            std::panic::catch_unwind(|| require_us_scale(
                "pdone",
                "Result.Duration",
                600_000,
                PolicyDuration::from_nanos(600_000),
            ))
            .is_err(),
            "600000 ns is not 600000 us"
        );
    }

    #[test]
    fn a_unit_drift_is_rejected_by_the_field_comparison_itself_not_only_by_a_guard() {
        // The guard above refuses a drifted read. This proves the second layer:
        // with the read accepted, the campaign differential cannot go green, so
        // `the_whole_campaign_matches_the_go_oracle_field_by_field` passing is an
        // assertion that can fail rather than a table being echoed.
        for (drift, drifted_columns) in [
            (Drift::GoalMillis, ["pci_duration_us"].as_slice()),
            (
                Drift::StampsAsNanos,
                ["pci_result_end_us", "pci_result_dur_us"].as_slice(),
            ),
            (
                Drift::RecvDeltaMillis,
                ["lt_recv_delta_us", "lt_ctr", "lt_loss_penalty"].as_slice(),
            ),
        ] {
            let replay = replay_campaign(
                include_str!("operations.tsv"),
                include_str!("golden.tsv"),
                drift,
            );
            assert!(
                !replay.diffs.is_empty(),
                "a thousandfold unit drift in {drift:?} replayed clean against the Go output"
            );
            for column in drifted_columns {
                assert!(
                    replay.diffs.iter().any(|diff| diff.contains(column)),
                    "the {drift:?} drift did not move the {column} column: first diffs {}",
                    replay.diffs[..replay.diffs.len().min(5)].join("; ")
                );
            }
        }
    }

    #[test]
    fn the_twcc_report_projection_is_the_oracle_packet() {
        // The defaults: reference time is the row's own clock in 64 ms ticks, the
        // feedback count is the per-case report ordinal, the status count is the
        // symbol count, and a large delta belongs to symbol 2.
        let report = feedback_report("twcc", "base=32768;states=1:1000,0,2:32000", 64_000_000, 3);
        assert_eq!(report.base_sequence_number, 32768);
        assert_eq!(report.packet_status_count, 3);
        assert_eq!(report.reference_time, (1_000_000_000 + 64_000_000) / 64_000);
        assert_eq!(report.fb_pkt_count, 3);
        assert_eq!(report.symbols, vec![1, 0, 2]);
        assert_eq!(
            report.deltas_us,
            vec![1_000, 32_000],
            "the not-received symbol consumes no delta, exactly as the walk skips it"
        );

        // The overrides, including the wire truncations Go performs with uint16,
        // uint24 and uint8 conversions, and the repeat form, which the oracle
        // splits off with `#` before it looks for the `:` delta.
        let report = feedback_report(
            "twcc",
            "base=65539;count=5;ref=16777217;fb=258;states=1:100#3,3",
            0,
            1,
        );
        assert_eq!(
            report.base_sequence_number, 3,
            "65539 goes through Go's uint16 conversion"
        );
        assert_eq!(report.packet_status_count, 5);
        assert_eq!(
            report.reference_time, 16_777_217,
            "an explicit `ref` goes through Go's `uint32(num(v))`, which does not mask to 24 bits"
        );
        assert_eq!(report.fb_pkt_count, 2);
        assert_eq!(report.symbols, vec![1, 1, 1, 3]);
        assert_eq!(report.deltas_us, vec![100, 100, 100]);

        // The only truncation an explicit reference time gets is the u32 one.
        let report = feedback_report("twcc", "ref=4294967297;states=0", 0, 1);
        assert_eq!(report.reference_time, 1);

        // An explicit count is not the symbol count: the walk stops at it.
        let report = feedback_report("twcc", "count=1;states=1:100,1:200", 0, 1);
        assert_eq!(report.packet_status_count, 1);
        assert_eq!(report.deltas_us, vec![100, 200]);

        // `sym=1` is refused rather than quietly reinterpreted.
        assert!(
            std::panic::catch_unwind(|| feedback_report("twcc", "sym=1;states=0,1", 0, 1)).is_err()
        );

        // And the report actually drives the detector's own bookkeeping, which is
        // what makes the twcc echo columns observable at all.
        let mut harness = Harness::plain();
        let none = harness.apply(&row("poll", 0, 0, 0, ""));
        assert_eq!(int(&none, "twcc_reports"), 0);
        assert_eq!(int(&none, "twcc_high_ref_64ms"), 0);
        harness.apply(&row("send", 1_200, 0, 0, ""));
        let first = harness.apply(&row("twcc", 0, 0, 0, "base=32768;states=1:1000"));
        assert_eq!(int(&first, "twcc_reports"), 1);
        assert_eq!(int(&first, "twcc_ooo"), 0);
        assert_eq!(int(&first, "twcc_high_ref_64ms"), 15_625);
        assert_eq!(
            int(&first, "recv_st_us"),
            0,
            "the recv echo is per row, so a report-only row prints the re-defaulted zero"
        );
        assert_eq!(
            int(&first, "recv_rd_us"),
            0,
            "the delta the report builds is read through the `recv` arm, not through the twcc row"
        );
        assert_eq!(
            int(&first, "recv_found"),
            -1,
            "`recv_found` is the answer of the `recv` arm, so a report-only row prints the absent sentinel"
        );
        let second = harness.apply(&row("twcc", 0, 0, 500_000, "base=32769;states=1:1000"));
        assert_eq!(int(&second, "twcc_reports"), 2);
        assert_eq!(int(&second, "twcc_interval_us"), 500_000);
    }

    #[test]
    fn the_listener_projection_columns_only_move_for_a_state_changing_report() {
        // `changes` and `committed` are the arguments of upstream's single
        // listener call site, which only `HandleTWCCFeedback` reaches. Before any
        // report they are the harness initial state, and a probe finalize alone
        // must not move them.
        let mut harness = Harness::plain();
        let idle = harness.apply(&row("poll", 0, 0, 0, ""));
        assert_eq!(int(&idle, "changes"), 0);
        assert_eq!(int(&idle, "committed"), 0);
        // A finalize that actually finalizes: the probe cluster has to be started,
        // stamped by the tracker, filled and reported done before upstream will
        // close it, and `ProbeClusterFinalize` only reports ok once
        // `MaybeFinalizeProbe` clears the settle wait, which the campaign measures
        // at 350 ms after the `pdone` stamp (5 x the 70 ms default RTT, floored at
        // SettleWaitMin). Its listener arguments must stay untouched, because only
        // `HandleTWCCFeedback` reaches `OnCongestionStateChange`.
        harness.apply(&row("pstart", 7, 100_000, 0, "bytes=20000;dur_us=1000000"));
        harness.apply(&row("msend", 0, 1, 0, "probe=1"));
        harness.apply(&row4(
            "pgadd",
            32768,
            100,
            0,
            0,
            0,
            "size=1000;cluster=7;probe=1",
        ));
        harness.apply(&row(
            "pdone",
            7,
            12_000,
            1_000_000,
            "start_us=0;end_us=600000",
        ));
        let early = harness.apply(&row("pfin", 0, 0, 1_349_000, ""));
        assert_eq!(
            int(&early, "fin_ok"),
            0,
            "one millisecond inside the settle wait the group is not finalized"
        );
        let finalized = harness.apply(&row("pfin", 0, 0, 1_350_000, ""));
        assert_eq!(
            int(&finalized, "fin_ok"),
            1,
            "the finalize ran, which is what makes this a real control"
        );
        assert_eq!(int(&finalized, "changes"), 0);
        harness.apply(&row4("msend", 0, 4, 400_000, 1_200, 0, ""));
        harness.apply(&row("send", 1_200, 0, 5_000_000, ""));
        let after_traffic = harness.apply(&row("poll", 0, 0, 6_000_000, ""));
        assert_eq!(
            int(&after_traffic, "changes"),
            0,
            "no traffic shape that does not carry a report may notify the listener"
        );

        // The positive half is taken from the Go output rather than hand-tuned.
        // `changes` is the cumulative notification count that every row echoes, so
        // the rule is about its increments, not about its value: a row may print a
        // nonzero count without having notified anything, and the campaign does
        // exactly that on the `msend`/`pdone`/`pfin` rows that follow a report.
        let stimulus = stimulus_rows(include_str!("operations.tsv"));
        let (_, body) = golden_rows();
        assert_eq!(
            stimulus.len(),
            body.len(),
            "the two tables are one campaign"
        );
        let mut notifications = 0usize;
        let mut previous_case: &str = "";
        let mut previous_changes = 0i64;
        for (item, row) in stimulus.iter().zip(body.iter()) {
            let changes: i64 = row["changes"].parse().expect("changes column");
            if item.case != previous_case {
                previous_case = item.case.as_str();
                previous_changes = 0;
            }
            if changes > previous_changes {
                notifications += 1;
                assert_eq!(
                    item.op,
                    "twcc",
                    "row {} raised the listener notification count from {} to {}, but only \
                     `HandleTWCCFeedback` reaches `OnCongestionStateChange`",
                    item.id(),
                    previous_changes,
                    changes
                );
                let reports: i64 = row["twcc_reports"].parse().expect("twcc_reports column");
                assert!(
                    changes <= reports,
                    "row {} counts {} notifications against {} reports processed",
                    item.id(),
                    changes,
                    reports
                );
            }
            previous_changes = changes;
        }
        assert!(
            notifications > 0,
            "the Go output never notifies the listener, so these columns are dead weight"
        );
        assert!(
            body.iter()
                .any(|row| row["committed"].parse::<i64>().expect("committed") != 0),
            "the Go listener never received a capacity either, so `committed` is dead weight"
        );
        let replay = replay_whole_campaign();
        for column in ["changes", "committed"] {
            assert!(
                !replay
                    .diffs
                    .iter()
                    .any(|diff| diff.contains(&format!(" {column}:"))),
                "the Rust replay disagrees with the Go listener projection in {column}: the Go \
                 output raises the notification count {} times over {} replayed rows",
                notifications,
                stimulus.len()
            );
        }
    }

    #[test]
    fn the_recv_arm_echoes_the_tracker_slot_it_actually_found() {
        let mut harness = Harness::plain();
        harness.apply(&row("send", 1_200, 0, 0, ""));
        let absent = harness.apply(&row("recv", 40_000, 5_000, 0, ""));
        assert_eq!(
            int(&absent, "recv_found"),
            0,
            "a slot holding another sequence is not this packet"
        );
        assert_eq!(
            int(&absent, "recv_st_us"),
            0,
            "Go returns the zero packetInfo"
        );
        assert_eq!(int(&absent, "recv_rd_us"), 0);
        assert_eq!(int(&absent, "trk_last_recv_sn"), -1);

        let found = harness.apply(&row("recv", 32768, 5_000, 0, ""));
        assert_eq!(int(&found, "recv_found"), 1);
        assert_eq!(int(&found, "recv_st_us"), 0);
        assert_eq!(
            int(&found, "recv_rd_us"),
            0,
            "the first receive sets the base"
        );
        harness.apply(&row("send", 1_200, 0, 100_000, ""));
        let second = harness.apply(&row("recv", 32769, 150_000, 150_000, ""));
        assert_eq!(int(&second, "recv_st_us"), 100_000);
        assert_eq!(int(&second, "recv_sd_us"), 100_000);
        assert_eq!(
            int(&second, "recv_rd_us"),
            145_000,
            "the receive time is relative to the first *received* clock (5_000), not the row clock"
        );
        assert_eq!(int(&second, "trk_last_recv_sn"), 32769);

        // A zero receive digit is the lost packet: the slot still exists, but
        // nothing is recorded and the base is untouched.
        let lost = harness.apply(&row("recv", 32768, 0, 200_000, ""));
        assert_eq!(int(&lost, "recv_found"), 1);
        assert_eq!(int(&lost, "recv_st_us"), 0);
        assert_eq!(int(&lost, "recv_rd_us"), 0);
    }

    #[test]
    fn the_traffic_arm_separates_the_zero_config_from_the_case_config() {
        // The `zero_wl=1` arm is the object upstream's `&trafficStats{}` literal
        // is: the *same* counters with every weighted-loss leaf defaulted to zero.
        // Without this control both arms would echo one number, and the campaign's
        // `g_wl`/`pg_wl`/`lg_wl_raw` columns would be unexplained.
        let mut harness = Harness::with_config("wl_pen=0.5");
        let configured = harness.apply(&row4(
            "traffic",
            0,
            100_000,
            50_000,
            100_000,
            0,
            "aby=8000;apk=4;lpk=1",
        ));
        let zeroed = harness.apply(&row4(
            "traffic",
            0,
            100_000,
            50_000,
            100_000,
            0,
            "aby=8000;apk=4;lpk=1;zero_wl=1",
        ));
        assert_eq!(int(&configured, "l_traffic"), 1);
        assert_eq!(int(&configured, "lt_duration_us"), 100_000);
        assert_eq!(int(&configured, "lt_send_delta_us"), 50_000);
        assert_eq!(int(&configured, "lt_recv_delta_us"), 100_000);
        assert_eq!(int(&configured, "lt_acked_bytes"), 8_000);
        // The loss itself is the same answer, because this span clears both the
        // case's validity window and its decayed rate threshold; what the zero
        // config removes is the penalty that the case's factor adds back.
        assert!(float(&configured, "lt_weighted_loss") > 0.0);
        assert_eq!(
            float(&configured, "lt_weighted_loss"),
            float(&zeroed, "lt_weighted_loss"),
            "the zero config does not change the loss, only what it costs"
        );
        assert!(int(&configured, "lt_loss_penalty") > 0);
        assert_eq!(
            int(&zeroed, "lt_loss_penalty"),
            0,
            "a zero penalty factor is exactly zero simulated queuing"
        );
        assert!(
            float(&configured, "lt_ctr") < float(&zeroed, "lt_ctr"),
            "the penalty must feed back into the captured ratio"
        );

        // The validity window is the leaf that does change the loss: a 50 ms span
        // is too short for the case config and long enough for the zero config.
        let too_short = harness.apply(&row4(
            "traffic",
            0,
            50_000,
            50_000,
            50_000,
            0,
            "aby=8000;apk=4;lpk=1",
        ));
        assert_eq!(float(&too_short, "lt_weighted_loss"), 0.0);
        let short_zeroed = harness.apply(&row4(
            "traffic",
            0,
            50_000,
            50_000,
            50_000,
            0,
            "aby=8000;apk=4;lpk=1;zero_wl=1",
        ));
        assert!(float(&short_zeroed, "lt_weighted_loss") > 0.0);

        let idle = harness.apply(&row("poll", 0, 0, 0, ""));
        assert_eq!(int(&idle, "l_traffic"), 0);
        for column in NO_TRAFFIC_INT {
            assert_eq!(
                int(&idle, column),
                -2,
                "{column} must carry Go's unarmed sentinel"
            );
        }
        assert_eq!(float(&idle, "lt_weighted_loss"), -2.0);
    }

    #[test]
    fn the_standalone_group_and_leaf_trend_are_the_harness_objects_not_the_detectors() {
        // `gadd`/`lginh`/`tadd` drive objects the oracle owns, which is how the
        // campaign can reach a group with an inherited queue and a trend with an
        // arbitrary sample sequence. If they were aliases of detector state the
        // two column families would move together.
        let mut harness = Harness::plain();
        let armed = harness.apply(&row4("gadd", 5, 1_000, 2_000, 100, 0, "size=1000;rd=100"));
        assert_eq!(int(&armed, "l_group"), 1);
        assert_eq!(int(&armed, "lg_err"), 0);
        assert_eq!(int(&armed, "lg_pri_pkts"), 1);
        assert_eq!(int(&armed, "lg_recv_delta_us"), 100);
        assert_eq!(int(&armed, "lg_send_delta_us"), 100);
        assert_eq!(int(&armed, "lg_lost_pkts"), 0);
        assert_eq!(
            int(&armed, "g_exists"),
            0,
            "the detector never made a group here"
        );

        // A reported loss only counts while the packet has no receive time, and
        // a retransmission lands in its own class.
        harness.apply(&row4("gadd", 6, 3_000, 0, 0, 0, "size=1000;lost=1"));
        let grown = harness.apply(&row4(
            "gadd",
            7,
            4_000,
            6_000,
            0,
            0,
            "size=1000;rd=300;rtx=1",
        ));
        assert_eq!(int(&grown, "lg_pri_pkts"), 1);
        assert_eq!(int(&grown, "lg_rtx_pkts"), 1);
        assert_eq!(int(&grown, "lg_lost_pkts"), 1);
        assert_eq!(int(&grown, "lg_lost_pri_pkts"), 1);
        assert_eq!(int(&grown, "lg_lost_rtx_pkts"), 0);
        assert_eq!(
            int(&grown, "lg_min_send_us"),
            900,
            "the window starts at the send minus its own delta"
        );
        assert_eq!(int(&grown, "lg_max_send_us"), 4_000);
        assert_eq!(int(&grown, "lg_recv_delta_us"), 400);

        // A group that reached its minimum packet count is finalized, and the
        // next newer packet is refused; `lginh` rebuilds it empty with the
        // inherited delay the oracle asked for.
        let mut sealed = Harness::with_config("grp_min=1");
        let first = sealed.apply(&row4("gadd", 1, 1_000, 1_100, 100, 0, "size=1000;rd=100"));
        assert_eq!(int(&first, "lg_finalized"), 1);
        assert_eq!(int(&first, "lg_err"), 0);
        let refused = sealed.apply(&row4("gadd", 2, 2_000, 2_100, 100, 0, "size=1000"));
        assert_eq!(int(&refused, "lg_err"), 1, "the group is finalized");
        assert_eq!(int(&refused, "lg_finalized"), 1);
        assert_eq!(
            int(&refused, "lg_pri_pkts"),
            1,
            "the refusal changed nothing"
        );
        let rebuilt = sealed.apply(&row("lginh", 250, 0, 7_000, ""));
        assert_eq!(int(&rebuilt, "lg_err"), -2, "lginh is not an add");
        assert_eq!(int(&rebuilt, "lg_finalized"), 0);
        assert_eq!(int(&rebuilt, "lg_pri_pkts"), 0);
        assert_eq!(int(&rebuilt, "lg_propagated_us"), 250);
        let accepted = sealed.apply(&row4("gadd", 4, 8_000, 8_100, 100, 0, "size=1000;rd=100"));
        assert_eq!(int(&accepted, "lg_err"), 0);

        // The leaf trend: every offered value counts, the collapse window can
        // refuse one, and the echoed list is the recorded one.
        let mut trend = Harness::with_config("ctr_req=3;ctr_collapse=1000000");
        let first = trend.apply(&row("tadd", 0, 0, 0, "1.0"));
        assert_eq!(int(&first, "l_trend"), 1);
        assert_eq!(int(&first, "lr_num_samples"), 1);
        assert_eq!(int(&first, "lr_len"), 1);
        assert_eq!(list_of(&first, "lr_values"), vec![1.0]);
        assert_eq!(float(&first, "lr_lowest"), 1.0);
        assert_eq!(float(&first, "lr_highest"), 1.0);
        let collapsed = trend.apply(&row("tadd", 0, 0, 500_000, "1.0"));
        assert_eq!(int(&collapsed, "lr_num_samples"), 2);
        assert_eq!(
            int(&collapsed, "lr_len"),
            1,
            "the collapse window refused it"
        );
        let spread = trend.apply(&row("tadd", 0, 0, 2_000_001, "-2.5"));
        assert_eq!(int(&spread, "lr_len"), 2);
        assert_eq!(list_of(&spread, "lr_values"), vec![1.0, -2.5]);
        assert_eq!(float(&spread, "lr_lowest"), -2.5);
        assert_eq!(
            int(&spread, "lr_direction"),
            2,
            "two samples need the max-wait span before the downward trend counts"
        );
        assert_eq!(int(&spread, "lr_span_us"), 2_000_001);
        assert_eq!(
            int(&spread, "lr_has_enough"),
            1,
            "Go compares the offered count (3) against RequiredSamples (3), not the recorded list"
        );
        assert_eq!(int(&spread, "lr_num_samples"), 3);
        let unarmed = Harness::plain().apply(&row("poll", 0, 0, 0, ""));
        assert_eq!(int(&unarmed, "l_trend"), 0);
        for column in NO_LEAF_TREND_INT {
            assert_eq!(
                int(&unarmed, column),
                -2,
                "{column} must carry Go's sentinel"
            );
        }
        assert_eq!(list_of(&unarmed, "lr_values"), Vec::<f64>::new());
    }

    #[test]
    fn gsel_selects_a_group_the_newest_is_not() {
        // The campaign's own `symcls` case with its own config, replayed: five
        // packets, one report that finalizes the older group, then both
        // selections at the same clock. The expectations are the digits the
        // pinned Go printed, so this is a differential, not a restatement.
        let mut harness = Harness::with_config("sn_start=1;grp_min=2;grp_win=5000000");
        harness.apply(&row4("msend", 0, 5, 100_000, 1_200, 0, ""));
        let reported = harness.apply(&row(
            "twcc",
            0,
            0,
            500_000,
            "base=2;ref=1000;fb=0;states=1:0,1:100000,3,0",
        ));
        // A row that carries no selection echoes the newest group only.
        assert_eq!(int(&reported, "num_groups"), 2);
        assert_eq!(int(&reported, "g_min_sn"), 5);
        assert_eq!(int(&reported, "g_max_sn"), 5);
        assert_eq!(int(&reported, "g_is_finalized"), 0);
        let older = harness.apply(&row("gsel", 0, 0, 500_000, ""));
        assert_eq!(
            int(&older, "g_min_sn"),
            2,
            "index 0 is the group of sn 2 and 3"
        );
        assert_eq!(int(&older, "g_max_sn"), 3);
        assert_eq!(int(&older, "g_min_send_us"), 100_000);
        assert_eq!(
            int(&older, "g_is_finalized"),
            1,
            "the selected group is finalized, which the newest one is not"
        );
        let newest = harness.apply(&row("gsel", 1, 0, 500_000, ""));
        assert_eq!(int(&newest, "g_min_sn"), 5);
        assert_eq!(int(&newest, "g_is_finalized"), 0);
        // Out of range is Go's `-1` branch, and the selector is per case.
        let beyond = harness.apply(&row("gsel", 9, 0, 500_000, ""));
        assert_eq!(int(&beyond, "g_exists"), 0);
        assert_eq!(int(&beyond, "g_min_sn"), -1);
        let fresh = Harness::plain().apply(&row("poll", 0, 0, 0, ""));
        assert_eq!(int(&fresh, "g_exists"), 0);
    }

    #[test]
    fn the_probe_cluster_echo_carries_declared_microseconds_through_nanosecond_stamps() {
        // The same case read at two declared scales must differ by exactly that
        // scale: this is what proves `pdone` converts microseconds into the
        // nanosecond stamps Go carries instead of passing the digit through.
        let mut harness = Harness::plain();
        let started = harness.apply(&row("pstart", 7, 100_000, 0, "bytes=20000;dur_us=1000000"));
        assert_eq!(int(&started, "pci_duration_us"), 1_000_000);
        assert_eq!(int(&started, "pci_goal_desired_bytes"), 20_000);
        assert_eq!(int(&started, "pci_desired_bps"), 100_000);
        assert_eq!(int(&started, "pci_id"), 7);
        assert_eq!(int(&started, "pg_exists"), 1);
        assert_eq!(
            int(&started, "pg_done_us"),
            0,
            "an unfinished probe keeps Go's zero"
        );
        assert_eq!(int(&started, "pci_result_completed"), 0);

        let wide = harness.apply(&row("pdone", 7, 12_000, 0, "start_us=0;end_us=600000"));
        assert_eq!(int(&wide, "pci_result_start_us"), 0);
        assert_eq!(int(&wide, "pci_result_end_us"), 600_000);
        assert_eq!(int(&wide, "pci_result_dur_us"), 600_000);
        assert_eq!(int(&wide, "pci_result_bytes"), 12_000);
        assert_eq!(int(&wide, "pci_result_completed"), 1);
        assert_eq!(
            int(&wide, "pci_result_packets"),
            0,
            "an absent leaf stays 0"
        );

        let mut narrow_harness = Harness::plain();
        narrow_harness.apply(&row("pstart", 7, 100_000, 0, "bytes=20000;dur_us=1000000"));
        let narrow = narrow_harness.apply(&row("pdone", 7, 12_000, 0, "start_us=0;end_us=600"));
        assert_eq!(
            int(&narrow, "pci_result_dur_us"),
            600,
            "a 1000x smaller declared digit must move the echo by exactly 1000x"
        );

        let mut uncompleted = Harness::plain();
        uncompleted.apply(&row("pstart", 7, 100_000, 0, "bytes=20000;dur_us=1000000"));
        let uncompleted = uncompleted.apply(&row(
            "pdone",
            7,
            12_000,
            0,
            "start_us=0;end_us=600000;completed=0;packets=3;primary=100;rtx=200",
        ));
        assert_eq!(int(&uncompleted, "pci_result_completed"), 0);
        assert_eq!(int(&uncompleted, "pci_result_packets"), 3);
        assert_eq!(
            int(&uncompleted, "pci_result_bytes"),
            12_300,
            "the byte total is the three classes, as upstream computes it"
        );

        let mut idle = Harness::plain();
        let none = idle.apply(&row("poll", 0, 0, 0, ""));
        assert_eq!(int(&none, "pg_exists"), 0);
        for column in NO_PROBE_GROUP {
            assert_eq!(
                int(&none, column),
                -1,
                "{column} must carry Go's no-probe sentinel"
            );
        }
        assert_eq!(float(&none, "pg_wl"), 0.0);
        assert_eq!(int(&none, "pg_abr"), 0);
    }

    #[test]
    fn the_settle_wait_is_the_clamped_five_rtts_and_the_sequence_shortcut_skips_it() {
        let mut harness = Harness::plain();
        harness.apply(&row("pstart", 7, 100_000, 0, "bytes=20000;dur_us=1000000"));
        harness.apply(&row("pdone", 7, 12_000, 0, "start_us=0;end_us=0"));
        // Five RTTs of 20 ms is 100 ms, which the 250 ms floor raises: settling at
        // the nominal wait would pass if the floor drifted into the RTT product.
        assert_eq!(
            int(
                &harness.apply(&row("settle", 0, 0, 100_000, "rtt_us=20000")),
                "settle_ok"
            ),
            0,
            "the configured floor, not the nominal five RTTs, is the wait"
        );
        assert_eq!(
            int(
                &harness.apply(&row("settle", 0, 0, 249_000, "rtt_us=20000")),
                "settle_ok"
            ),
            0
        );
        assert_eq!(
            int(
                &harness.apply(&row("settle", 0, 0, 250_000, "rtt_us=20000")),
                "settle_ok"
            ),
            1,
            "the comparison is exclusive, so the floor itself finalizes"
        );

        // A five-second RTT product is the other clamp: 25 s, capped at 5 s.
        let mut capped = Harness::plain();
        capped.apply(&row("pstart", 7, 100_000, 0, "bytes=20000;dur_us=1000000"));
        capped.apply(&row("pdone", 7, 12_000, 0, "start_us=0;end_us=0"));
        assert_eq!(
            int(
                &capped.apply(&row("settle", 0, 0, 4_999_000, "rtt_us=1000000")),
                "settle_ok"
            ),
            0,
            "the cap, not the raw five-second product, is the wait"
        );
        assert_eq!(
            int(
                &capped.apply(&row("settle", 0, 0, 5_000_000, "rtt_us=1000000")),
                "settle_ok"
            ),
            1
        );

        // The sequence shortcut, both ways round: reaching the tracker's probe
        // high sequence finalizes with no wait, and a cluster the tracker never
        // stamped cannot take the shortcut.
        let mut probe = Harness::plain();
        probe.apply(&row("pstart", 7, 100_000, 0, "bytes=20000;dur_us=1000000"));
        probe.apply(&row("msend", 0, 1, 0, "cluster=7;probe=1"));
        probe.apply(&row("pgadd", 32768, 100, 0, "size=1000;cluster=7;probe=1"));
        probe.apply(&row("pdone", 7, 12_000, 0, "start_us=0;end_us=600000"));
        let shortcut = probe.apply(&row("settle", 0, 0, 1_000, "rtt_us=20000"));
        assert_eq!(int(&shortcut, "settle_ok"), 1);
        assert_eq!(
            int(&shortcut, "pg_max_sn"),
            32768,
            "the shortcut is only real because the group reached the tracker's own high mark"
        );

        let mut unstamped = Harness::plain();
        unstamped.apply(&row("pstart", 7, 100_000, 0, "bytes=20000;dur_us=1000000"));
        unstamped.apply(&row("msend", 0, 1, 0, "probe=1"));
        unstamped.apply(&row("pgadd", 32768, 100, 0, "size=1000;cluster=7;probe=1"));
        unstamped.apply(&row("pdone", 7, 12_000, 0, "start_us=0;end_us=600000"));
        assert_eq!(
            int(
                &unstamped.apply(&row("settle", 0, 0, 1_000, "rtt_us=20000")),
                "settle_ok"
            ),
            0,
            "a probe sequence the tracker never stamped cannot open the shortcut"
        );
    }

    #[test]
    fn tseed_is_the_only_op_the_campaign_may_not_use_and_an_unknown_op_is_fatal() {
        assert_eq!(
            REPLAYABLE_OPS.iter().filter(|op| **op == "tseed").count(),
            0,
            "this port has no TrendDetector.Seed, so a replayed tseed would be an invention"
        );
        for op in ["tseed", "bogus", ""] {
            let mut harness = Harness::plain();
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                harness.apply(&row(op, 0, 0, 0, ""))
            }))
            .is_err();
            assert!(panicked, "{op} reached apply without a hard error");
        }
        // Every op the campaign actually uses is in the vocabulary, and the
        // vocabulary is exactly the campaign's op set, so the coverage invariant
        // above can only fail on a campaign edit.
        let mut used: Vec<String> = stimulus_rows(include_str!("operations.tsv"))
            .iter()
            .map(|row| row.op.clone())
            .collect();
        used.sort_unstable();
        used.dedup();
        for op in &used {
            assert!(
                REPLAYABLE_OPS.contains(&op.as_str()),
                "the campaign uses {op}, which the replay does not rebuild"
            );
        }
        let mut vocabulary: Vec<String> =
            REPLAYABLE_OPS.iter().map(|op| (*op).to_string()).collect();
        vocabulary.sort_unstable();
        assert_eq!(
            used,
            vocabulary,
            "the replay rebuilds {} ops that the campaign never runs",
            vocabulary.len() - used.len()
        );
    }

    #[test]
    fn the_stimulus_reader_pads_short_rows_and_never_truncates_a_wide_one() {
        let rows = data_rows("a\tb\n\nc\td\te\tf\tg\th\ti\tj\tk\tl\tm\n");
        assert_eq!(rows.len(), 2, "blank lines are not rows");
        assert_eq!(
            rows[0].len(),
            9,
            "a leaf-free stimulus row is padded, not rejected"
        );
        assert_eq!(rows[0][8], "");
        assert_eq!(rows[1].len(), 11, "a golden output row keeps every column");
        assert_eq!(rows[1][10], "m");
    }

    #[test]
    fn the_wire_sequence_and_the_tracker_counter_stay_one_send_apart_from_go() {
        // The documented representation difference, asserted rather than assumed:
        // Go's counter is the value the next send hands out, this port's is the
        // last sequence it reconstructed. The digits below are the campaign's own
        // `sn_start=1` cases, which is why the harness is configured that way.
        let mut harness = Harness::with_config("sn_start=1");
        let before = harness.apply(&row("send", 1_200, 0, 0, ""));
        assert_eq!(int(&before, "sn"), 1, "the first send hands out sn_start");
        assert_eq!(
            int(&before, "trk_sn"),
            2,
            "Go's counter would read next, not last"
        );
        assert_eq!(int(&before, "trk_base_send_us"), 0);
        let after = harness.apply(&row("send", 1_200, 0, 1_000, ""));
        assert_eq!(int(&after, "sn"), 2);
        assert_eq!(int(&after, "trk_sn"), 3);
        let mut idle = Harness::with_config("sn_start=1");
        let untouched = idle.apply(&row("poll", 0, 0, 0, ""));
        assert_eq!(
            int(&untouched, "trk_sn"),
            1,
            "with no send at all Go's counter is still the configured start"
        );
        // The other half of the same rule is the default the campaign's
        // `trk_none__dflt_sn_start` case replays: an unspecified start is Go's
        // pinned 1<<15, so an untouched tracker echoes 32768, not 0.
        let defaulted = Harness::plain().apply(&row("poll", 0, 0, 0, ""));
        assert_eq!(int(&defaulted, "trk_sn"), 1 << 15);
        assert_eq!(
            int(&untouched, "sn"),
            -1,
            "a row that sent nothing prints the sentinel"
        );
        assert_eq!(int(&untouched, "trk_base_send_us"), NO_BASE_US);
        assert_eq!(int(&untouched, "pg_err"), NOT_ATTEMPTED);
        assert_eq!(int(&untouched, "fin_signal"), -1);
        assert_eq!(int(&untouched, "settle_ok"), -1);
    }

    #[test]
    fn every_non_finite_loss_row_prints_zero_conversion_in_the_go_output() {
        // The claim `traffic_echo` rests on: on the oracle host the `int64()` of
        // the NaN that a non-finite weighted loss produces is 0, so every rate
        // that depends on it prints 0. This reads the pinned Go digits rather
        // than assuming them, and a campaign row that prints anything else fails
        // here before it can be explained away.
        let (header, body) = golden_rows();
        assert_eq!(header.len(), body[0].len());
        let loss_columns = [
            ("g_wl", "g_abr", "g_ctr"),
            ("pg_wl", "pg_abr", "pg_ctr"),
            ("lg_wl", "lg_abr", "lg_ctr"),
            ("lg_wl_raw", "lg_abr_raw", "lg_ctr_raw"),
        ];
        let substituted = loss_columns
            .iter()
            .map(|(wl, _, _)| *wl)
            .collect::<Vec<_>>();
        let mut seen = 0;
        let mut families = vec![0; loss_columns.len()];
        for row in &body {
            for (index, (wl, abr, ctr)) in loss_columns.into_iter().enumerate() {
                let value: f64 = row[wl].parse().unwrap();
                if value.is_finite() {
                    continue;
                }
                seen += 1;
                families[index] += 1;
                assert_eq!(
                    row[abr], "0",
                    "{}: {wl}={} is non-finite but Go printed {abr}={}",
                    row["case"], row[wl], row[abr]
                );
                assert_eq!(
                    row[ctr], "0",
                    "{}: {wl}={} is non-finite but Go printed {ctr}={}",
                    row["case"], row[wl], row[ctr]
                );
            }
            // The model only substitutes for these four families, so no other
            // float column may be non-finite without this test noticing.
            for column in FLOAT_COLUMNS {
                if substituted.contains(&column) {
                    continue;
                }
                let value: f64 = row[column].parse().unwrap();
                assert!(
                    value.is_finite(),
                    "{}: column {column}={} is non-finite, which traffic_echo does not model",
                    row["case"],
                    row[column]
                );
            }
        }
        // Three of the four families really do reach a non-finite loss in this
        // campaign (measured: g_wl 6, pg_wl 43, lg_wl_raw 16 rows); the merged
        // standalone-group echo `lg_wl` never does, so the claim is carried by the
        // other three and the per-family floors pin that instead of a bare total
        // that a cross-row leak could inflate.
        for (index, (wl, _, _)) in loss_columns.iter().enumerate() {
            let floor = match *wl {
                "g_wl" => 5,
                "pg_wl" => 40,
                "lg_wl_raw" => 10,
                _ => 0,
            };
            assert!(
                families[index] >= floor,
                "{wl} has {} non-finite rows, below the {floor} the campaign must keep exercising",
                families[index]
            );
        }
        assert!(
            seen > 60,
            "the campaign must actually exercise the non-finite conversion, saw {seen} rows"
        );
    }

    #[test]
    fn the_echo_model_is_production_on_finite_rows_and_only_a_substitution_elsewhere() {
        // The other half of the same claim: wherever the loss is finite, the echo
        // *is* production's answer, so the model cannot be masking a difference in
        // the rate arithmetic. Where it is not, production refuses the call and the
        // model alone answers, which is what the campaign test above ties to Go.
        let mut harness = Harness::with_config("wl_pen=0.5;wl_mindur=0;wl_pps=0");
        let mut finite = 0;
        for (min_send, max_send, send_delta, recv_delta) in [
            (1_000, 2_000, 500, 600),
            (0, 100_000, 200_000, 100_000),
            (5_000_000, 10_000_000, 4_000_000, 5_000_000),
            (0, 100_000, 0, 0),
        ] {
            let observed = harness.apply(&row4(
                "traffic",
                min_send,
                max_send,
                send_delta,
                recv_delta,
                0,
                "aby=8000;apk=4;lpk=1",
            ));
            let mut stats = TrafficStats::new(harness.detector.config().weighted_loss);
            stats.merge(&TrafficData {
                min_send_time: min_send,
                max_send_time: max_send,
                send_delta,
                recv_delta,
                acked_packets: 4,
                acked_bytes: 8_000,
                lost_packets: 1,
                lost_bytes: 1_000,
            });
            let (wl, abr, ctr, penalty) = traffic_echo(&stats);
            assert!(
                wl.is_finite(),
                "{min_send}..{max_send} must stay finite here"
            );
            finite += 1;
            assert_eq!(stats.weighted_loss(), wl);
            assert_eq!(stats.acknowledged_bitrate(), abr);
            assert_eq!(stats.captured_traffic_ratio(), ctr);
            assert_eq!(stats.loss_penalty(), penalty);
            assert!(floats_agree(float(&observed, "lt_weighted_loss"), wl));
            assert_eq!(int(&observed, "lt_abr"), abr);
            assert!(floats_agree(float(&observed, "lt_ctr"), ctr));
            assert_eq!(int(&observed, "lt_loss_penalty"), penalty);
        }
        assert_eq!(finite, 4);

        // The `zero_wl=1` shape. A zero-config aggregate only goes non-finite when
        // its duration is zero, so the bitrate never reaches the conversion and the
        // refusal is confined to the two answers that do.
        let mut idle = TrafficStats::new(zero_weighted_loss());
        idle.merge(&TrafficData {
            min_send_time: 0,
            max_send_time: 0,
            send_delta: 100_000,
            recv_delta: 100_000,
            acked_packets: 0,
            acked_bytes: 0,
            lost_packets: 0,
            lost_bytes: 0,
        });
        assert!(idle.weighted_loss().is_nan());
        for (what, refusal) in [("loss_penalty", 0), ("captured_traffic_ratio", 1)] {
            let stats_ref = &idle;
            let panicked =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match refusal {
                    0 => stats_ref.loss_penalty(),
                    _ => stats_ref.captured_traffic_ratio() as i64,
                }))
                .is_err();
            assert!(
                panicked,
                "production must refuse {what} on a non-finite loss"
            );
        }
        assert_eq!(
            idle.acknowledged_bitrate(),
            0,
            "the zero-duration guard answers first"
        );
        let (wl, abr, ctr, penalty) = traffic_echo(&idle);
        assert!(wl.is_nan());
        assert_eq!(
            penalty, 0,
            "the substituted conversion is the reference host's 0"
        );
        assert_eq!(abr, 0);
        assert!(
            floats_agree(ctr, 1.0),
            "the substituted penalty leaves the ratio as the raw send/recv span quotient, got {ctr}"
        );

        // The bare aggregate, which is what a group with no acknowledgements
        // reports: same NaN loss, but the ratio's own guard answers before the
        // conversion is ever reached.
        let empty = TrafficStats::new(zero_weighted_loss());
        assert!(empty.weighted_loss().is_nan());
        assert_eq!(empty.captured_traffic_ratio(), 0.0);
        assert_eq!(empty.acknowledged_bitrate(), 0);
        let (wl, abr, ctr, penalty) = traffic_echo(&empty);
        assert!(wl.is_nan());
        assert_eq!((abr, ctr, penalty), (0, 0.0, 0));
    }
}
