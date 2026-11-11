//! Replays the complete original Go ONE/RID phase logs at the encrypted media boundary.
use super::*;
use serde::Deserialize;
use std::{env, fs, path::PathBuf};
use str0m::{
    media::{Frequency, MediaTime, SenderFeedback},
    rtp::rtcp::SenderInfo,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    codec: String,
    mode: String,
    phase: usize,
    source_frame: u16,
    source_sn: u16,
    tid: u8,
    writes: usize,
    marker: Option<bool>,
    out_frame: Option<u16>,
    out_sn_offset: Option<u16>,
    payload_hex: Option<String>,
    padding_sn_offset: Option<u16>,
    internal_sid: Option<u8>,
    key: Option<bool>,
    out_ts_offset: Option<u32>,
    outer_spatial: Option<u8>,
    rid: Option<String>,
    source_ssrc: Option<u32>,
    source_ts: Option<u32>,
}

pub(super) struct Campaign {
    rows: Vec<Row>,
    pub rid: bool,
}

impl Campaign {
    pub(super) fn phases(&self) -> Vec<Phase> {
        self.rows
            .iter()
            .map(|row| {
                let (template, first, last, attached) = if self.rid {
                    let templates = [0, 0, 4, 4, 1, 1, 3, 0, 2, 4, 1];
                    let template = templates[row.phase];
                    (template, true, true, template == 0)
                } else {
                    [
                        (0, true, false, true),
                        (0, false, true, false),
                        (4, true, false, false),
                        (1, true, false, false),
                        (1, false, true, false),
                        (4, false, true, false),
                    ][row.phase]
                };
                assert_eq!(row.tid, u8::from(template == 4), "source TID");
                let rid = row.rid.as_deref().map(|value| match value {
                    "q" => "q",
                    "f" => "f",
                    _ => panic!("unexpected Go source RID {value}"),
                });
                if let Some(rid) = rid {
                    let high = rid == "f";
                    assert_eq!(row.internal_sid, Some(0));
                    assert_eq!(row.outer_spatial, Some(if high { 2 } else { 0 }));
                    assert_eq!(row.source_ssrc, Some(if high { 202 } else { 101 }));
                    assert_eq!(row.key, Some(template == 0));
                } else {
                    assert_eq!(row.internal_sid, None);
                    assert_eq!(row.outer_spatial, None);
                    assert_eq!(row.source_ssrc, None);
                    assert_eq!(row.key, None);
                }
                assert_eq!(rid.is_some(), self.rid);
                assert_eq!(row.source_ts.is_some(), self.rid);
                Phase {
                    rid,
                    sn: row.source_sn,
                    frame: row.source_frame,
                    timestamp: row.source_ts.unwrap_or(u32::from(row.source_frame) * 3000),
                    template,
                    first,
                    last,
                    attached,
                }
            })
            .collect()
    }

    pub(super) fn assert_replay(
        &self,
        phases: &[Phase],
        packets: &[str0m::rtp::RtpPacket],
        indices: &[usize],
    ) {
        assert_eq!(phases.len(), self.rows.len());
        assert_eq!(indices.len(), self.rows.len() + 1);
        assert_eq!(indices[0], 0, "no warmup packet may reach the subscriber");
        let first = packets
            .first()
            .expect("first Go phase has actual physical media");
        let mut descriptor_parser = crate::av1::PacketReceiver::default();
        for (i, row) in self.rows.iter().enumerate() {
            let input = phases[i];
            let output = &packets[indices[i]..indices[i + 1]];
            let media: Vec<_> = output
                .iter()
                .filter(|packet| !packet.header.has_padding)
                .collect();
            let padding: Vec<_> = output
                .iter()
                .filter(|packet| packet.header.has_padding)
                .collect();
            assert_eq!(media.len(), row.writes, "Go phase {i}");
            assert_eq!(media.len(), usize::from(row.out_frame.is_some()));
            assert_eq!(media.len(), usize::from(row.out_sn_offset.is_some()));
            assert_eq!(media.len(), usize::from(row.payload_hex.is_some()));
            assert_eq!(padding.len(), usize::from(row.padding_sn_offset.is_some()));
            assert_eq!(row.out_ts_offset.is_some(), self.rid && !media.is_empty());
            assert_eq!(row.marker.is_some(), !self.rid && !media.is_empty());
            if let Some(packet) = media.first() {
                assert_eq!(packet.header.ssrc, first.header.ssrc);
                assert_eq!(
                    packet
                        .header
                        .sequence_number
                        .wrapping_sub(first.header.sequence_number),
                    row.out_sn_offset.unwrap(),
                    "Go SN at phase {i}",
                );
                if let Some(ts) = row.out_ts_offset {
                    assert_eq!(
                        packet.header.timestamp.wrapping_sub(first.header.timestamp),
                        ts,
                        "Go TS at phase {i}"
                    );
                }
                assert_eq!(packet.header.marker, row.marker.unwrap_or(input.last));
                assert_eq!(
                    packet
                        .payload
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>(),
                    row.payload_hex.as_deref().unwrap(),
                    "Go payload bytes at phase {i}"
                );
                let dd = descriptor_parser
                    .parse(
                        &packet.header.ext_vals,
                        *packet.seq_no,
                        packet.timestamp,
                        false,
                    )
                    .expect("physical output has a parseable DD");
                assert_eq!(
                    dd.frame as u16,
                    row.out_frame.unwrap(),
                    "Go output DD frame at phase {i}"
                );
                assert_eq!(dd.spatial(), 0);
                assert_eq!(dd.temporal(), row.tid);
                assert_eq!(dd.active, 1, "Go admits only the T0 decode target");
                assert_eq!(dd.attached(), input.attached);
                let raw = packet
                    .header
                    .ext_vals
                    .user_values
                    .get::<crate::av1::RawDescriptor>()
                    .unwrap();
                assert_eq!(
                    raw.0[0] >> 6,
                    u8::from(input.first) * 2 + u8::from(input.last)
                );
                assert_eq!(raw.0[0] & 63, input.structure() + input.template);
            }
            if let Some(packet) = padding.first() {
                assert_eq!(packet.header.ssrc, first.header.ssrc);
                assert_eq!(
                    packet
                        .header
                        .sequence_number
                        .wrapping_sub(first.header.sequence_number),
                    row.padding_sn_offset.unwrap()
                );
                assert!(packet.payload.is_empty());
            }
            println!(
                "RUST_DD_ONE_RID_MATCH codec={} mode={} rid={} phase={i} media={} padding={}",
                row.codec,
                row.mode,
                self.rid,
                media.len(),
                padding.len()
            );
        }
    }
}

// The fixed Go observer injects same-NTP reports into public HandleRTCPSenderReportData.
// This test drives the equivalent Rust actor report consumer with the two actual
// received source SSRCs. The sibling SwitchRid test continues to prove real SRTCP.
pub(super) fn inject_reports(engine: &mut MediaEngine, tid: TransportId, mid: Mid, now: Instant) {
    let mut reports = Vec::new();
    let mut direct = engine.transports.get_mut(&tid).unwrap().rtc.direct_api();
    for (rid, ts) in [("q", 30000), ("f", 90000)] {
        let rid = Rid::from(rid);
        let ssrc = direct.stream_rx_by_mid(mid, Some(rid)).unwrap().ssrc();
        reports.push(SenderFeedback {
            mid,
            rid: Some(rid),
            received_at: now,
            sender_info: SenderInfo {
                ssrc,
                ntp_time: std::time::UNIX_EPOCH + Duration::from_secs(1_730_000_000),
                rtp_time: MediaTime::new(ts, Frequency::NINETY_KHZ),
                sender_packet_count: 40,
                sender_octet_count: 40,
            },
        });
    }
    assert_ne!(reports[0].sender_info.ssrc, reports[1].sender_info.ssrc);
    let up = engine.ups.get_mut(engine.up_by_mid[&(tid, mid)]).unwrap();
    assert!(up.one_dd.get_report(0).is_none() && up.one_dd.get_report(2).is_none());
    for (layer, report) in [0, 2].into_iter().zip(reports) {
        up.one_dd.report(layer, report.sender_info);
    }
}

fn read_log(variable: &str, prefix: &str, count: usize) -> Vec<Row> {
    let path =
        PathBuf::from(env::var_os(variable).unwrap_or_else(|| panic!("{variable} is required")));
    let text = fs::read_to_string(path).unwrap();
    assert!(
        text.lines().any(|line| line == "PASS"),
        "original Go test did not pass"
    );
    let rows: Vec<Row> = text
        .lines()
        .filter_map(|line| line.strip_prefix(prefix))
        .map(|json| serde_json::from_str(json).expect("exact original Go row schema"))
        .collect();
    assert_eq!(rows.len(), count);
    rows
}

#[test]
#[ignore = "requires both fixed original Go physical logs"]
fn all_68_fixed_go_one_rid_rows_match_encrypted_rust_media() {
    for (rid, variable, prefix, count) in [
        (
            false,
            "GO_DD_ONE_PHYSICAL_LOG",
            "SOURCE_DD_ONE_PHYSICAL ",
            6,
        ),
        (
            true,
            "GO_DD_RID_PHYSICAL_LOG",
            "SOURCE_DD_RID_PHYSICAL ",
            11,
        ),
    ] {
        let rows = read_log(variable, prefix, 4 * count);
        let mut remaining = rows.into_iter();
        for (mime, codec) in [("video/VP9", Codec::Vp9), ("video/AV1", Codec::Av1)] {
            for (mode_name, mode) in [
                (
                    "ONE_SPATIAL_LAYER_PER_STREAM",
                    VideoLayerMode::OneSpatialLayerPerStream,
                ),
                (
                    "ONE_SPATIAL_LAYER_PER_STREAM_INCOMPLETE_RTCP_SR",
                    VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr,
                ),
            ] {
                let rows: Vec<_> = remaining.by_ref().take(count).collect();
                assert_eq!(rows.len(), count);
                for (phase, row) in rows.iter().enumerate() {
                    assert_eq!(
                        (row.codec.as_str(), row.mode.as_str(), row.phase),
                        (mime, mode_name, phase)
                    );
                }
                case(codec, mode, Scenario::Literal(&Campaign { rows, rid }));
            }
        }
        assert!(remaining.next().is_none());
    }
}
