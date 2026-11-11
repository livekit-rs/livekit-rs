//! Authenticated VP9 dependency descriptor publication through the production engine and rate observer.
use super::vp9_dd_original_packets::SourceDd;
use super::*;
use rtc_core::media_api::{MediaCommand, PublishedTrackSpec};
use rtc_core::model::{TrackKind, TrackSource};
use str0m::format::Codec;
use str0m::net::{Protocol, Receive};
use str0m::rtp::RtpWrite;
use str0m::{Candidate, Input, Output};

#[derive(Clone, Copy, Debug, PartialEq)]
enum ReportProfile {
    Spatial,
    OneEncoding,
    AssembledVp9,
    UnassembledVp9,
    MissingDd,
    PauseResume,
    MixedRepair,
    Temporal { send_enhancement: bool },
}

fn report_after(
    mut engine: MediaEngine,
    source: TrackSource,
    start: Instant,
    profile: ReportProfile,
) -> [Option<Duration>; 3] {
    let unassembled = profile == ReportProfile::UnassembledVp9;
    let assembled = unassembled || profile == ReportProfile::AssembledVp9;
    let missing_dd = profile == ReportProfile::MissingDd;
    let two_fragments = assembled || missing_dd;
    let one = assembled || profile == ReportProfile::OneEncoding;
    let pause_resume = profile == ReportProfile::PauseResume;
    let mixed = profile == ReportProfile::MixedRepair;
    let temporal = match profile {
        ReportProfile::Temporal { send_enhancement } => Some(send_enhancement),
        ReportProfile::MixedRepair => Some(true),
        _ => None,
    };
    engine.cfg.enabled_codecs = vec![CodecSpec {
        mime: "video/VP9".into(),
        fmtp_line: String::new(),
    }];
    let queue = RoutedPacketQueue::default();
    engine.out = Box::new(queue.clone());
    let server = "127.0.0.1:9721".parse().unwrap();
    let remote = "127.0.0.1:9722".parse().unwrap();
    let tid = transport(&mut engine, PcRole::Publisher, start);
    let transport = engine.transports.get_mut(&tid).unwrap();
    let candidate = transport
        .rtc
        .add_local_candidate(Candidate::host(server, "udp").unwrap())
        .unwrap()
        .clone();
    transport.candidates.push(candidate);
    let sid = TrackSid::from("TR_configured_bitrate");
    engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: tid,
            track: PublishedTrackSpec {
                sid: sid.clone(),
                kind: TrackKind::Video,
                source,
                cid: "bitrate-window".into(),
                layers: [
                    rtc_core::model::VideoQuality::Low,
                    rtc_core::model::VideoQuality::Medium,
                ]
                .into_iter()
                .enumerate()
                .map(|(i, quality)| rtc_core::model::VideoLayer {
                    quality,
                    width: 320 * (i as u32 + 1),
                    height: 180 * (i as u32 + 1),
                    bitrate: 10000,
                    ssrc: 0,
                    spatial_layer: i as i32,
                    rid: String::new(),
                    repair_ssrc: 0,
                })
                .collect(),
                codecs: vec![rtc_core::model::TrackCodec {
                    mime_type: "video/VP9".into(),
                    layer_mode: if one {
                        rtc_core::model::VideoLayerMode::OneSpatialLayerPerStream
                    } else {
                        rtc_core::model::VideoLayerMode::MultipleSpatialLayersPerStream
                    },
                    ..Default::default()
                }],
                stereo: false,
                disable_dtx: false,
                disable_red: false,
                encrypted: false,
                muted: false,
                backup_codec_policy: Default::default(),
            },
        },
        start,
    );
    let builder = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .clear_codecs()
        .set_rtp_mode(true)
        .enable_vp9(true);
    let mut peer = if assembled {
        builder
    } else {
        builder.set_extension(
            13,
            str0m::rtp::Extension::with_serializer(crate::av1::URI, SourceDd),
        )
    }
    .build(start);
    peer.add_local_candidate(Candidate::host(remote, "udp").unwrap());
    let mid = published_mid(&offer(&mut engine, &mut peer, tid, start, |change| {
        change.add_media(
            MediaKind::Video,
            Direction::SendOnly,
            None,
            Some("bitrate-window".into()),
            None,
        );
    }));
    engine.bind_track(tid, &mid.to_string(), sid.clone(), start);
    let ui = engine.up_by_sid[&sid];
    assert_eq!(engine.ups.get(ui).unwrap().spec.source, source);
    let mut first_frame = None;
    let mut first_bitrate = [None; 3];
    let vp9 = peer
        .codec_config()
        .params()
        .iter()
        .find(|p| p.spec().codec == Codec::Vp9)
        .unwrap();
    let pt = vp9.pt();
    let rtx = vp9.resend();
    let mut wire_packets = Vec::new();
    let mut withheld = None;
    let mut repaired = None;
    let mut observed_padding = false;
    let mut deliver = |engine: &mut MediaEngine, now, contents: &[u8]| {
        // The network observer reads clear RTP header fields and the actual
        // authenticated datagram length. Neither the engine payload counter
        // nor its decoded header length determines the expected result.
        let mut duplicate = None;
        if contents.len() >= 12 && contents[0] >> 6 == 2 {
            let observed_pt = contents[1] & 0x7f;
            let seq = u16::from_be_bytes([contents[2], contents[3]]);
            if observed_pt == *pt {
                let padding = contents[0] & 0x20 != 0;
                if mixed && seq == 2 {
                    assert!(withheld.replace(contents.to_vec()).is_none());
                    return;
                }
                if padding {
                    assert!(mixed);
                    observed_padding = true;
                } else {
                    wire_packets.push((now, seq, contents.len()));
                }
            } else if mixed && rtx.is_some_and(|pt| *pt == observed_pt) {
                let timestamp = u32::from_be_bytes(contents[4..8].try_into().unwrap());
                if timestamp == 6000 {
                    assert!(repaired.replace(contents.len()).is_none());
                    // The original had not reached the receiver. Its distinct
                    // SRTP primary SSRC/sequence must still be deduplicated at
                    // the logical RTP boundary after authenticated RTX fills it.
                    duplicate = withheld.clone();
                }
            }
        }
        engine.handle_datagram(crate::net::Datagram {
            transport: tid,
            proto: Proto::Udp,
            source: remote,
            destination: server,
            data: contents,
            received_at: now,
        });
        if let Some(bytes) = duplicate {
            engine.handle_datagram(crate::net::Datagram {
                transport: tid,
                proto: Proto::Udp,
                source: remote,
                destination: server,
                data: &bytes,
                received_at: now,
            });
        }
    };
    let mut seq = 0u16;
    let mut release_at = start;
    let mut epochs = Vec::new();
    let mut resumed_reports = Vec::new();
    let mut fresh_seq = None;
    let mut unassembled_bitrate = None;
    for tick in 0..1800u64 {
        let now = start + Duration::from_millis(tick);
        release_at = now;
        if pause_resume && matches!(tick, 320 | 680) {
            engine.handle_command(
                MediaCommand::SetPublisherMuted {
                    track_sid: sid.clone(),
                    muted: tick == 320,
                },
                now,
            );
        }
        peer.handle_input(Input::Timeout(now)).unwrap();
        if peer.is_connected()
            && tick.is_multiple_of(15)
            && if mixed {
                seq < 4
            } else {
                (!(one || two_fragments) || seq == 0)
                    && temporal.is_none_or(|send_enhancement| seq < 1 + u16::from(send_enhancement))
            }
        {
            seq += 1;
            if two_fragments {
                let mut api = peer.direct_api();
                let writer = api.stream_tx_by_mid(mid, None).unwrap();
                let mut start_payload = vec![0x08, 0x82];
                start_payload.resize(90, 0);
                let mut end_payload = vec![0x04];
                end_payload.resize(90, 0);
                writer.write_rtp(RtpWrite::new(pt, 1.into(), 3000, now, start_payload));
                if !unassembled {
                    writer.write_rtp(
                        RtpWrite::new(pt, 2.into(), 3000, now, end_payload).marker(true),
                    );
                }
            } else if mixed && seq == 4 {
                peer.direct_api()
                    .stream_tx_by_mid(mid, None)
                    .unwrap()
                    .write_rtp(RtpWrite::padding_only(
                        pt,
                        u64::from(seq).into(),
                        u32::from(seq) * 3000,
                        now,
                    ));
            } else {
                // Fixed Go DD serialization: two S0T0/S1T0 targets with different DTIs.
                let [hi, lo] = seq.to_be_bytes();
                if pause_resume && tick == 990 {
                    fresh_seq = Some(seq);
                }
                let attached = seq == 1 || fresh_seq == Some(seq);
                let high = !attached && seq.is_multiple_of(2);
                let mut raw = vec![0xc0 | u8::from(high), hi, lo];
                if attached {
                    raw.extend(if temporal.is_some() {
                        // Original Go serializer: two S0T0 decode targets, one isolated S0T1.
                        [0x80, 0x02, 0x7a, 0x02, 0x00].as_slice()
                    } else {
                        [0x80, 0x01, 0xba, 0x20].as_slice()
                    });
                }
                let mut ext = str0m::rtp::ExtensionValues::default();
                ext.user_values.set(crate::av1::RawDescriptor(raw.into()));
                let mut payload = vec![0x9c, 0x81, seq as u8, 0x00, 0x34, 0x00];
                payload.resize(
                    if temporal.is_some() && seq == 2 {
                        140
                    } else {
                        90
                    },
                    0,
                );
                peer.direct_api()
                    .stream_tx_by_mid(mid, None)
                    .unwrap()
                    .write_rtp(
                        RtpWrite::new(
                            pt,
                            u64::from(seq).into(),
                            u32::from(seq) * 3000,
                            now,
                            payload,
                        )
                        .marker(true)
                        .nackable(mixed)
                        .ext_vals(ext),
                    );
            }
        }
        loop {
            match peer.poll_output().unwrap() {
                Output::Transmit(packet) => deliver(&mut engine, now, &packet.contents),
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        let outbound = { queue.0.lock().unwrap().drain(..).collect::<Vec<_>>() };
        for (destination, data) in outbound {
            assert_eq!(destination, remote);
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server, remote, &data).unwrap(),
            ))
            .unwrap();
            loop {
                match peer.poll_output().unwrap() {
                    Output::Transmit(packet) => deliver(&mut engine, now, &packet.contents),
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
        let up = engine.ups.get(ui).unwrap();
        if unassembled && up.bitrates()[0] > 0 {
            unassembled_bitrate = Some(up.bitrates()[0]);
            break;
        }
        if pause_resume && matches!(tick, 319 | 670 | 680 | 989) {
            let observed = up.bitrate_observation();
            epochs.push((tick, observed.bitrates, observed.generation));
        }
        if pause_resume && (1100..=1150).contains(&tick) {
            resumed_reports.push((now, up.bitrates()));
            if tick == 1150 {
                break;
            }
        }
        if let Some(received) = up.stats[0].last_seen {
            let first = *first_frame.get_or_insert(received);
            for (layer, bps) in up.bitrates().into_iter().enumerate() {
                if bps > 0 && first_bitrate[layer].is_none() {
                    first_bitrate[layer] = Some(now.duration_since(first));
                    eprintln!(
                        "actual source {source:?} layer={layer} first_report={:?} bps={bps}",
                        first_bitrate[layer]
                    );
                }
            }
            if !pause_resume
                && first_bitrate[0].is_some()
                && (one || two_fragments || temporal.is_some() || first_bitrate[1].is_some())
            {
                break;
            }
        }
    }
    if unassembled {
        let up = engine.ups.get(ui).unwrap();
        assert!(peer.is_connected() && !up.started);
        assert!(!up.dd_source.engaged() && !up.one_dd.engaged(0));
        assert!(first_frame.is_none());
        assert_eq!(first_bitrate, [None; 3]);
        assert_eq!(up.stats[0].last_seen, None);
        assert_eq!(up.bitrates(), [7_296, 0, 0]);
        let &[(received, sequence, size)] = wire_packets.as_slice() else {
            panic!("expected exactly one authenticated original fragment")
        };
        let auth_tag = engine.transports[&tid]
            .rtc
            .receive_srtp_auth_tag_len()
            .unwrap();
        assert_eq!(sequence, 1);
        assert_eq!(size - auth_tag, 114);
        let accepted_packets = engine
            .transports
            .get_mut(&tid)
            .unwrap()
            .rtc
            .direct_api()
            .stream_rx_by_mid(mid, None)
            .unwrap()
            .media_packets_received();
        assert_eq!(accepted_packets, 1);
        let elapsed = release_at.duration_since(received);
        assert_eq!(elapsed, Duration::from_millis(125));
        eprintln!(
            "INCOMPLETE_RUST accepted_packets={accepted_packets} logical_rtp_bytes=114 complete_frames=0 reported_bps={} elapsed_ms={}",
            unassembled_bitrate.unwrap(),
            elapsed.as_millis()
        );
        engine.handle_command(
            MediaCommand::UnpublishTrack {
                track_sid: sid.clone(),
            },
            release_at,
        );
        engine.handle_command(MediaCommand::CloseTransport(tid), release_at);
        assert!(!engine.transports.contains_key(&tid) && !engine.up_by_sid.contains_key(&sid));
        return first_bitrate;
    }
    assert!(
        peer.is_connected() && first_frame.is_some(),
        "real ICE/DTLS/SRTP did not produce a parsed VP9 DD packet"
    );
    let up = engine.ups.get(ui).unwrap();
    if two_fragments {
        assert_eq!(up.svc, missing_dd);
        assert_eq!(up.dd_source.engaged(), missing_dd);
        assert!(!up.one_dd.engaged(0) && !up.av1_packets.is_active());
        assert_eq!(
            wire_packets
                .iter()
                .map(|(_, seq, _)| *seq)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    } else if one {
        assert!(
            !up.svc && up.one_dd.engaged(0),
            "physical independent encoding"
        );
        assert_eq!(wire_packets.len(), 1);
    } else {
        assert!(up.svc, "actual source entered aggregate DD path");
    }
    let measured = engine.ups.get(ui).unwrap().bitrates();
    if mixed {
        assert!(rtx.is_some() && withheld.is_some() && observed_padding);
        let elapsed = first_bitrate[0].expect("aggregate DD must report actual repaired media");
        let auth_tag = engine.transports[&tid]
            .rtc
            .receive_srtp_auth_tag_len()
            .expect("real publisher SRTP profile");
        assert_eq!(
            wire_packets
                .iter()
                .map(|(_, seq, _)| *seq)
                .collect::<Vec<_>>(),
            [1, 3]
        );
        let base = 2 * wire_packets
            .iter()
            .map(|(_, _, size)| size - auth_tag)
            .sum::<usize>();
        let rtx = repaired.expect("actual network loss should elicit authenticated RTX") - auth_tag;
        let to_bps = |bytes: usize| (bytes as f64 * 8.0 / elapsed.as_secs_f64()) as u64;
        let actual_base = engine
            .ups
            .get(ui)
            .unwrap()
            .bitrate_observation_for(0)
            .bitrates[0];
        eprintln!(
            "actual padded/RTX SRTP: base={base} repair_outer={rtx} repair_logical={} rates={actual_base}/{}",
            rtx - 2,
            measured[0]
        );
        assert_eq!(actual_base, to_bps(base));
        assert_eq!(measured, [to_bps(rtx - 2), 0, 0]);
    }
    let expected = first_bitrate[0].filter(|_| !pause_resume && !mixed).map(|elapsed| {
        let auth_tag_len = engine.transports[&tid]
            .rtc
            .receive_srtp_auth_tag_len()
            .expect("authenticated SRTP protection profile");
        let first = first_frame.unwrap();
        let mut rtp_bytes = [0usize; 3];
        for &(sent, seq, size) in &wire_packets {
            if sent >= first && sent < first + elapsed {
                let packet_size = size - auth_tag_len;
                assert!(packet_size > 90, "RTP header is present");
                match temporal {
                    Some(true) if seq == 2 => rtp_bytes[0] += packet_size,
                    Some(false) if seq == 1 => rtp_bytes[0] += 2 * packet_size,
                    Some(_) => {}
                    None if one || two_fragments => rtp_bytes[0] += packet_size,
                    None => {
                        if !seq.is_multiple_of(2) {
                            rtp_bytes[0] += packet_size;
                        }
                        rtp_bytes[1] += packet_size;
                    }
                }
            }
        }
        eprintln!("source={source:?} profile={profile:?} expected_rtp_bytes={rtp_bytes:?} measured_bps={measured:?}");
        rtp_bytes.map(|size| (size as f64 * 8.0 / elapsed.as_secs_f64()) as u64)
    });
    engine.handle_command(
        MediaCommand::UnpublishTrack {
            track_sid: sid.clone(),
        },
        release_at,
    );
    engine.handle_command(MediaCommand::CloseTransport(tid), release_at);
    assert!(!engine.transports.contains_key(&tid));
    assert!(!engine.up_by_sid.contains_key(&sid));
    if temporal.is_some() {
        assert!(
            first_bitrate[0].is_some(),
            "authenticated temporal source must report"
        );
    }
    if let Some(expected) = expected {
        assert_eq!(
            measured, expected,
            "video tracker must charge complete logical RTP"
        );
    }
    if pause_resume {
        let transmitted_while_muted = wire_packets.iter().any(|(sent, _, _)| {
            let tick = sent.duration_since(start).as_millis();
            (320..680).contains(&tick)
        });
        assert!(transmitted_while_muted, "muted phase received actual SRTP");
        eprintln!("encrypted DD mute/resume epochs (ms, bps, generation): {epochs:?}");
        assert_eq!(epochs.len(), 4);
        assert!(epochs[0].1[0] > 0 && epochs[0].1[1] > 0, "original epoch");
        assert_eq!(epochs[1].1, [0; 3], "mute drains despite incoming RTP");
        assert_eq!(epochs[2].1, [0; 3], "unmute clears the measured sample");
        assert_ne!(
            epochs[2].2, epochs[0].2,
            "unmute retires the old cost generation"
        );
        assert_eq!(
            epochs[3].1, [0; 3],
            "bare continuations cannot start the new clock"
        );
        let fresh = fresh_seq.expect("fresh attached packet was written");
        let arrival = wire_packets
            .iter()
            .find(|(_, seq, _)| *seq == fresh)
            .expect("fresh attached packet was delivered over SRTP")
            .0;
        let due = arrival + Duration::from_millis(125);
        assert!(resumed_reports.iter().any(|(at, _)| *at < due));
        assert!(
            resumed_reports
                .iter()
                .filter(|(at, _)| *at < due)
                .all(|(_, rates)| *rates == [0; 3])
        );
        let (reported, rates) = resumed_reports
            .into_iter()
            .find(|(_, rates)| rates[0] > 0 && rates[1] > 0)
            .expect("fresh attached DD restarts both layers");
        assert_eq!(reported, due);
        eprintln!(
            "fresh DD arrival {:?}, first rates {rates:?} after {:?}",
            arrival.duration_since(start),
            reported.duration_since(arrival)
        );
    }
    first_bitrate
}

#[test]
fn actual_dd_bitrate_windows() {
    for (source, expected) in [(TrackSource::Camera, 125), (TrackSource::ScreenShare, 450)] {
        let now = Instant::now();
        let mut engine = engine(now);
        engine.cfg.video.bitrate_report.video = [
            Some(Duration::from_millis(125)),
            Some(Duration::from_millis(900)),
            None,
        ];
        engine.cfg.video.bitrate_report.screenshare = [
            Some(Duration::from_millis(450)),
            Some(Duration::from_millis(900)),
            None,
        ];
        let [low, high, absent] = report_after(engine, source, now, ReportProfile::Spatial);
        let low = low.expect("authenticated DD low report");
        assert_eq!(high, Some(low));
        assert_eq!(low, Duration::from_millis(expected), "{source:?}");
        assert_eq!(absent, None);
    }
}

#[test]
fn cleared_aggregate_dd_report_map_keeps_real_video_without_reporting() {
    for intervals in [[None; 3], [None, Some(Duration::from_millis(50)), None]] {
        let now = Instant::now();
        let mut engine = engine(now);
        engine.cfg.video.bitrate_report.screenshare = intervals;
        assert_eq!(
            report_after(
                engine,
                TrackSource::ScreenShare,
                now,
                ReportProfile::Spatial
            ),
            [None; 3]
        );
    }
}

#[test]
fn authenticated_dd_mute_drains_and_resume_waits_for_fresh_target_epoch() {
    let now = Instant::now();
    let mut engine = engine(now);
    engine.cfg.video.bitrate_report.video = [
        Some(Duration::from_millis(125)),
        Some(Duration::from_millis(900)),
        None,
    ];
    report_after(engine, TrackSource::Camera, now, ReportProfile::PauseResume);
}

fn assert_actual_temporal_cost(send_enhancement: bool) {
    let now = Instant::now();
    let mut engine = engine(now);
    engine.cfg.video.bitrate_report.video = [Some(Duration::from_millis(125)), None, None];
    assert_eq!(
        report_after(
            engine,
            TrackSource::Camera,
            now,
            ReportProfile::Temporal { send_enhancement },
        ),
        [Some(Duration::from_millis(125)), None, None],
    );
}

#[test]
fn actual_dd_temporal_targets_keep_separate_complete_wire_costs() {
    assert_actual_temporal_cost(true);
}

#[test]
fn actual_dd_duplicate_base_targets_count_both_in_one_temporal_cell() {
    assert_actual_temporal_cost(false);
}

#[test]
fn actual_dd_mixed_padding_and_rtx_charge_only_logical_packets() {
    let now = Instant::now();
    let mut engine = engine(now);
    engine.cfg.video.bitrate_report.video = [Some(Duration::from_millis(500)), None, None];
    assert_eq!(
        report_after(engine, TrackSource::Camera, now, ReportProfile::MixedRepair),
        [Some(Duration::from_millis(500)), None, None],
    );
}

#[test]
fn actual_independent_dd_encoding_charges_complete_logical_rtp() {
    let now = Instant::now();
    let mut engine = engine(now);
    engine.cfg.video.bitrate_report.video = [Some(Duration::from_millis(125)), None, None];
    assert_eq!(
        report_after(engine, TrackSource::Camera, now, ReportProfile::OneEncoding),
        [Some(Duration::from_millis(125)), None, None],
    );
}

#[test]
fn actual_frame_assembled_vp9_charges_both_logical_packets() {
    let now = Instant::now();
    let mut engine = engine(now);
    engine.cfg.video.bitrate_report.video = [Some(Duration::from_millis(125)), None, None];
    assert_eq!(
        report_after(
            engine,
            TrackSource::Camera,
            now,
            ReportProfile::AssembledVp9
        ),
        [Some(Duration::from_millis(125)), None, None],
    );
}

#[test]
fn actual_missing_dd_vp9_charges_fallback_packets_once() {
    let now = Instant::now();
    let mut engine = engine(now);
    engine.cfg.video.bitrate_report.video = [Some(Duration::from_millis(125)), None, None];
    assert_eq!(
        report_after(engine, TrackSource::Camera, now, ReportProfile::MissingDd),
        [Some(Duration::from_millis(125)), None, None],
    );
}

#[test]
fn actual_vp9_incomplete_picture_counts_at_packet_arrival() {
    let now = Instant::now();
    let mut engine = engine(now);
    engine.cfg.video.bitrate_report.video = [Some(Duration::from_millis(125)), None, None];
    assert_eq!(
        report_after(
            engine,
            TrackSource::Camera,
            now,
            ReportProfile::UnassembledVp9
        ),
        [None; 3],
    );
}
