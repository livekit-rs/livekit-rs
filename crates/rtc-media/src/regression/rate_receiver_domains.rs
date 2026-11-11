//! Actual negotiated three-RID SRTP receiver domains; source payload PID is
//! intentionally constant/TID3 with DD so only the original DD consumer can
//! measure its T0 cadence. This is protocol/producer proof, not pixel decoding.
use super::*;
use rtc_core::media_api::{MediaCommand, PublishedTrackSpec};
use rtc_core::model::{TrackKind, TrackSource, VideoLayer, VideoQuality};
use str0m::format::Codec;
use str0m::media::{Rid, Simulcast, SimulcastLayer};
use str0m::net::{Protocol, Receive};
use str0m::rtp::{ExtensionSerializer, ExtensionValues, RtpWrite};
use str0m::{Candidate, Input, Output};
#[derive(Debug)]
struct SourceDd;
impl ExtensionSerializer for SourceDd {
    fn write_to(&self, out: &mut [u8], values: &ExtensionValues) -> usize {
        let Some(raw) = values.user_values.get::<crate::av1::RawDescriptor>() else {
            return 0;
        };
        if raw.0.len() > out.len() {
            return 0;
        }
        out[..raw.0.len()].copy_from_slice(&raw.0);
        raw.0.len()
    }
    fn parse_value(&self, input: &[u8], values: &mut ExtensionValues) -> bool {
        ExtensionSerializer::parse_value(&crate::av1::Serializer, input, values)
    }
    fn is_video(&self) -> bool {
        true
    }
    fn is_audio(&self) -> bool {
        false
    }
    fn requires_two_byte_form(&self, _: &ExtensionValues) -> bool {
        true
    }
}
fn source_dd(frame: u16, attached: bool) -> ExtensionValues {
    let mut bits = Vec::new();
    let mut write = |value: u32, n: usize| {
        for shift in (0..n).rev() {
            bits.push(((value >> shift) & 1) as u8)
        }
    };
    write(3, 2);
    write(u32::from(!attached), 6);
    write(u32::from(frame), 16);
    if attached {
        write(1, 1);
        write(0, 4);
        write(0, 6);
        write(0, 5);
        write(0, 2);
        write(3, 2);
        write(2, 2);
        write(3, 2);
        write(0, 1);
        write(1, 1);
        write(0, 4);
        write(0, 1);
        write(0, 1);
        write(0, 1);
    }
    let mut raw = vec![0; (bits.len() + 7) / 8];
    for (i, bit) in bits.into_iter().enumerate() {
        raw[i / 8] |= bit << (7 - i % 8)
    }
    let mut values = ExtensionValues::default();
    values
        .user_values
        .set(crate::av1::RawDescriptor(raw.into()));
    values
}
#[test]
fn actual_three_rid_srtp_sources_keep_payload_and_dd_rate_domains_separate() {
    for dd in [false, true] {
        case(dd, false)
    }
}
#[test]
#[ignore = "requires actual original Buffer.Write+ReceiverBase getter receipt"]
fn actual_three_rid_srtp_rates_match_original_buffer_receiver_consumer() {
    for dd in [false, true] {
        case(dd, true)
    }
}
fn case(with_dd: bool, require_original: bool) {
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.rtc.congestion_control.enabled = false;
    engine.cfg.enabled_codecs = vec![CodecSpec {
        mime: "video/VP9".into(),
        fmtp_line: String::new(),
    }];
    let packets = RoutedPacketQueue::default();
    engine.out = Box::new(packets.clone());
    let server = "127.0.0.1:9741".parse().unwrap();
    let remote = "127.0.0.1:9742".parse().unwrap();
    let tid = transport(&mut engine, PcRole::Publisher, start);
    let native = engine.transports.get_mut(&tid).unwrap();
    let candidate = native
        .rtc
        .add_local_candidate(Candidate::host(server, "udp").unwrap())
        .unwrap()
        .clone();
    native.candidates.push(candidate);
    native.spec.primary = false;
    let sid = TrackSid::from("TR_rate_rids");
    let rids = ["q", "h", "f"];
    engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: tid,
            track: PublishedTrackSpec {
                sid: sid.clone(),
                kind: TrackKind::Video,
                source: TrackSource::Camera,
                cid: "rate-domains".into(),
                layers: rids
                    .iter()
                    .enumerate()
                    .map(|(i, rid)| VideoLayer {
                        quality: [VideoQuality::Low, VideoQuality::Medium, VideoQuality::High][i],
                        width: 320 * (i as u32 + 1),
                        height: 180 * (i as u32 + 1),
                        bitrate: 10000,
                        ssrc: 0,
                        spatial_layer: i as i32,
                        rid: (*rid).into(),
                        repair_ssrc: 0,
                    })
                    .collect(),
                codecs: vec![],
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
    let mut peer = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .clear_codecs()
        .enable_vp9(true)
        .set_rtp_mode(true)
        .set_extension(
            13,
            str0m::rtp::Extension::with_serializer(crate::av1::URI, SourceDd),
        )
        .build(start);
    peer.add_local_candidate(Candidate::host(remote, "udp").unwrap());
    let mid = published_mid(&offer(&mut engine, &mut peer, tid, start, |change| {
        let mut simulcast = Simulcast::new();
        for rid in rids {
            simulcast.add_send_layer(SimulcastLayer::new(rid))
        }
        change.add_media(
            MediaKind::Video,
            Direction::SendOnly,
            None,
            Some("rate-domains".into()),
            Some(simulcast),
        );
    }));
    engine.bind_track(tid, &mid.to_string(), sid.clone(), start);
    let ui = engine.up_by_mid[&(tid, mid)];
    for (i, rid) in rids.iter().enumerate() {
        assert_eq!(
            engine
                .ups
                .get(ui)
                .unwrap()
                .spatial_for_rid(Some(Rid::from(*rid))),
            i as i32,
            "actual negotiated RID must bind its logical source layer"
        )
    }
    let pt = peer
        .codec_config()
        .params()
        .iter()
        .find(|p| p.spec().codec == Codec::Vp9)
        .unwrap()
        .pt();
    let mut sent = [0usize; 3];
    let mut next = [0u64; 3];
    let steps = [18000u32, 6000, 3000];
    let intervals = [200u64, 67, 34];
    let mut finished = None;
    for tick in 0..15000u64 {
        let now = start + Duration::from_millis(tick);
        peer.handle_input(Input::Timeout(now)).unwrap();
        if peer.is_connected() {
            for slot in 0..3 {
                if sent[slot] < 64 && tick >= next[slot] {
                    let frame = sent[slot] as u16;
                    let payload = if with_dd {
                        vec![0xec, 0, 0x60, 0, 0x84, slot as u8]
                    } else {
                        vec![0xac, frame as u8, 0, frame as u8, 0x80, slot as u8]
                    };
                    let values = if with_dd {
                        source_dd(100 + frame, frame == 0)
                    } else {
                        Default::default()
                    };
                    peer.direct_api()
                        .stream_tx_by_mid(mid, Some(Rid::from(rids[slot])))
                        .expect("actual negotiated native send stream per RID")
                        .write_rtp(
                            RtpWrite::new(
                                pt,
                                (1000 + u64::from(frame)).into(),
                                3000 + u32::from(frame) * steps[slot],
                                now,
                                payload,
                            )
                            .marker(true)
                            .ext_vals(values),
                        );
                    sent[slot] += 1;
                    next[slot] = tick + intervals[slot];
                }
            }
        }
        loop {
            match peer.poll_output().unwrap() {
                Output::Transmit(packet) => engine.handle_datagram(crate::net::Datagram {
                    transport: tid,
                    proto: Proto::Udp,
                    source: remote,
                    destination: server,
                    data: &packet.contents,
                    received_at: now,
                }),
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        let outbound = { packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() };
        for (destination, data) in outbound {
            assert_eq!(destination, remote);
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server, remote, &data).unwrap(),
            ))
            .unwrap();
            loop {
                match peer.poll_output().unwrap() {
                    Output::Transmit(packet) => engine.handle_datagram(crate::net::Datagram {
                        transport: tid,
                        proto: Proto::Udp,
                        source: remote,
                        destination: server,
                        data: &packet.contents,
                        received_at: now,
                    }),
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
        if sent == [64; 3] {
            let done = *finished.get_or_insert(tick);
            if tick >= done + 100 {
                break;
            }
        }
    }
    assert_eq!(
        sent, [64; 3],
        "all three actual SRTP encodings delivered complete source cadences"
    );
    let up = engine.ups.get(ui).unwrap();
    let identities = up.source_rates.test_source_ids();
    assert!(identities.iter().all(Option::is_some));
    assert_ne!(identities[0], identities[1]);
    assert_ne!(identities[1], identities[2]);
    assert_ne!(identities[0], identities[2]);
    let observed = std::array::from_fn::<_, 3, _>(|slot| up.temporal_rates(slot));
    if !with_dd {
        assert_eq!(
            observed,
            [
                [5.0, 0.0, 0.0, 0.0],
                [15.0, 0.0, 0.0, 0.0],
                [30.0, 0.0, 0.0, 0.0]
            ]
        )
    } else {
        assert!(
            observed[0][0] > 0.0
                && observed[0][0] < observed[1][0]
                && observed[1][0] < observed[2][0]
        );
        for (slot, rate) in observed.iter().enumerate() {
            assert_eq!(
                &rate[1..],
                &[0.0; 3],
                "valid DD T0 overrides contradictory constant payload PID/TID3 in RID{}",
                rids[slot]
            );
            assert!(up.source_rates.test_using_dd(slot));
        }
    }
    if require_original {
        let input = std::fs::read_to_string(
            std::env::var("GO_RECEIVER_RATE_RESULTS")
                .expect("actual original Buffer.Write/ReceiverBase output"),
        )
        .unwrap();
        let mut matched = 0;
        for line in input.lines() {
            let Some(row) = line.strip_prefix("SOURCE_RECEIVER_RATE ") else {
                continue;
            };
            let row: serde_json::Value = serde_json::from_str(row).unwrap();
            if row["dd"].as_bool() != Some(with_dd) {
                continue;
            }
            let slot = row["layer"].as_u64().unwrap() as usize;
            let rates = row["rates"].as_array().unwrap();
            assert_eq!(rates.len(), 4);
            let expected = std::array::from_fn::<_, 4, _>(|i| rates[i].as_f64().unwrap() as f32);
            assert_eq!(
                observed[slot], expected,
                "actual encrypted RID{} must consume original production receiver rates",
                rids[slot]
            );
            matched += 1;
        }
        assert_eq!(matched, 3);
    }
    engine.handle_command(
        MediaCommand::UnpublishTrack {
            track_sid: sid.clone(),
        },
        start + Duration::from_secs(16),
    );
    assert!(!engine.up_by_sid.contains_key(&sid));
    engine.handle_command(
        MediaCommand::CloseTransport(tid),
        start + Duration::from_secs(16),
    );
    assert!(engine.transports.is_empty());
    let _ = peer.close();
}
