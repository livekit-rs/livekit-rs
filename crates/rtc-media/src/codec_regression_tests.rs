use super::*;
use rtc_core::{
    media_api::{DownTrackSpec, MediaCommand, PublishedTrackSpec},
    model::{BackupCodecPolicy, TrackCodec, TrackKind, TrackSettings, TrackSource},
};
use str0m::format::Codec;
use str0m::media::{Frequency, MediaTime};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output};

#[test]
fn actual_dual_codec_srtp_regression_uses_live_room_threshold_and_preserves_subscription() {
    for (policy, threshold, count, later_count, regresses) in [
        (BackupCodecPolicy::PreferRegression, 0, 100, 100, true),
        (BackupCodecPolicy::PreferRegression, -1, 0, 0, false),
        (BackupCodecPolicy::PreferRegression, 5, 5, 5, false),
        // Membership changed after SDP but before backup's actual first RTP.
        (BackupCodecPolicy::PreferRegression, 5, 5, 4, true),
        (BackupCodecPolicy::Regression, -1, 100, 100, true),
        (BackupCodecPolicy::Simulcast, 0, 1, 1, false),
        (BackupCodecPolicy::from_i32(99), 0, 1, 1, false),
        (BackupCodecPolicy::from_i32(-1), 0, 1, 1, false),
    ] {
        actual_dual_codec(
            policy,
            threshold,
            count,
            later_count,
            regresses,
            false,
            false,
            false,
            false,
        );
    }
}

#[test]
fn actual_simulcast_backup_only_subscriber_receives_its_negotiated_codec_without_global_regression()
{
    actual_dual_codec(
        BackupCodecPolicy::Simulcast,
        5,
        100,
        100,
        false,
        true,
        false,
        false,
        false,
    );
}

#[test]
fn actual_in_order_mid_session_payload_change_invalidates_primary_and_routes_real_backup() {
    for policy in [
        BackupCodecPolicy::PreferRegression,
        BackupCodecPolicy::from_i32(99),
    ] {
        actual_dual_codec(policy, 5, 100, 100, false, false, true, false, false);
    }
}

#[test]
fn actual_delayed_srtcp_rr_reaches_only_live_bound_subscriber_and_retires_with_track() {
    actual_dual_codec(
        BackupCodecPolicy::Regression,
        0,
        1,
        1,
        true,
        false,
        false,
        true,
        false,
    );
}

fn actual_dual_codec(
    policy: BackupCodecPolicy,
    threshold: i64,
    count: usize,
    later_count: usize,
    regresses: bool,
    backup_only: bool,
    invalid_pt: bool,
    rtt_probe: bool,
    timing_probe: bool,
) {
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.rtc.congestion_control.enabled = false;
    let forward_stats = timing_probe.then(|| {
        rtc_core::forward_stats::ForwardStats::new(100_000_000, 100_000_000, 1_000_000_000, start)
            .unwrap()
    });
    engine.cfg.rtc.forward_stats = forward_stats.clone();
    engine.cfg.video.codec_regression_threshold = threshold;
    engine.set_room_participant_count(count);
    for mime in ["video/vp9", "video/vp8", "video/rtx"] {
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: mime.into(),
            fmtp_line: String::new(),
        });
    }
    let packets = RoutedPacketQueue::default();
    engine.out = Box::new(packets.clone());
    let server_addr = "127.0.0.1:9601".parse().unwrap();
    let pub_addr = "127.0.0.1:9602".parse().unwrap();
    let sub_addr = "127.0.0.1:9603".parse().unwrap();
    let pub_tid = transport(&mut engine, PcRole::Publisher, start);
    {
        let t = engine.transports.get_mut(&pub_tid).unwrap();
        let candidate = t
            .rtc
            .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
            .unwrap()
            .clone();
        t.candidates.push(candidate);
        t.spec.primary = false;
    }
    let sid = TrackSid::from("TR_codec_regression");
    engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: pub_tid,
            track: PublishedTrackSpec {
                backup_codec_policy: policy,
                sid: sid.clone(),
                kind: TrackKind::Video,
                source: TrackSource::Camera,
                cid: "primary".into(),
                layers: vec![],
                codecs: [("video/vp9", "primary"), ("video/vp8", "backup")]
                    .into_iter()
                    .map(|(mime, cid)| TrackCodec {
                        mime_type: mime.into(),
                        cid: cid.into(),
                        ..Default::default()
                    })
                    .collect(),
                stereo: false,
                disable_dtx: false,
                disable_red: false,
                encrypted: false,
                muted: false,
            },
        },
        start,
    );
    let mut publisher = client(start);
    publisher.add_local_candidate(Candidate::host(pub_addr, "udp").unwrap());
    let mut change = publisher.sdp_api();
    let primary = change.add_media(
        MediaKind::Video,
        Direction::SendOnly,
        None,
        Some("primary".into()),
        None,
    );
    let backup = change.add_media(
        MediaKind::Video,
        Direction::SendOnly,
        None,
        Some("backup".into()),
        None,
    );
    let (offered, pending) = change.apply().unwrap();
    let offered = crate::sdp_munge::map_sections(&offered.to_sdp_string(), |mid, section| {
        Some(crate::sdp_munge::prioritize_publisher_codec(
            section,
            if mid == Some(primary.to_string().as_str()) {
                "video/vp9"
            } else {
                "video/vp8"
            },
        ))
    });
    engine.on_remote_description(pub_tid, SdpType::Offer, offered, 1, start);
    let mut events = vec![];
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|event| match event {
            MediaEvent::LocalDescription {
                kind: SdpType::Answer,
                sdp,
                ..
            } => Some(sdp),
            _ => None,
        })
        .unwrap();
    publisher
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
        .unwrap();
    engine.bind_track(pub_tid, &primary.to_string(), sid.clone(), start);
    if !backup_only {
        engine.bind_track(pub_tid, &backup.to_string(), sid.clone(), start);
    }
    let primary_ui = engine.up_by_mid[&(pub_tid, primary)];
    assert_eq!(engine.ups.get(primary_ui).unwrap().mime, "video/vp9");
    let sub_tid = TransportId::next();
    engine.create_transport(
        TransportSpec {
            ice_preference: Default::default(),
            one_shot: false,
            id: sub_tid,
            participant: "PA_test".into(),
            role: PcRole::Subscriber,
            ice_lite: true,
            server_data_channels: true,
            enable_data_tracks: true,
            primary: true,
            client_supports_red: false,
            codec_switch_requires_renegotiation: false,
            disable_playout_delay: false,
            allow_pause: false,
        },
        IceCredentials::random(),
        vec![crate::net::LocalCandidate {
            proto: Proto::Udp,
            addr: server_addr,
            base: server_addr,
        }],
        start,
    );
    let mut subscriber = if rtt_probe {
        // Original GetRttMs accepts only the most recently emitted SR. With
        // identical1s native SR/RR schedules, a50ms delayed RR can always refer
        // to the just-superseded SR. Use actual200ms peer reports so a valid
        // current-marker observation exists; keep production latest-SR fencing.
        Rtc::builder()
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .set_rtcp_report_interval_video(Duration::from_millis(200))
            .set_rtcp_report_interval_audio(Duration::from_millis(200))
            .build(start)
    } else if backup_only {
        Rtc::builder()
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .clear_codecs()
            .enable_vp8(true)
            .build(start)
    } else {
        client(start)
    };
    subscriber.add_local_candidate(Candidate::host(sub_addr, "udp").unwrap());
    answer_server_offer(&mut engine, &mut subscriber, sub_tid, start);
    engine.handle_command(
        MediaCommand::Subscribe(DownTrackSpec {
            subscriber: "PA_test".into(),
            publisher: "PA_test".into(),
            track_sid: sid.clone(),
            stream_id: "PA_test|TR_codec_regression".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        }),
        start,
    );
    let negotiated = start + Duration::from_millis(160);
    engine.handle_timeout(negotiated);
    answer_server_offer(&mut engine, &mut subscriber, sub_tid, negotiated);
    let di = engine.down_by_key[&("PA_test".into(), sid.clone())];
    if backup_only {
        engine.bind_track(pub_tid, &backup.to_string(), sid.clone(), negotiated);
    }
    let backup_ui = engine.up_by_mid[&(pub_tid, backup)];
    assert_ne!(
        primary_ui, backup_ui,
        "two actual receiver m-lines remain independently bound"
    );
    assert_eq!(engine.ups.get(backup_ui).unwrap().mime, "video/vp8");
    if backup_only {
        assert_eq!(engine.downs.get(di).unwrap().up, backup_ui);
        assert!(
            engine
                .codec_quality_snapshot(&sid)
                .unwrap()
                .iter()
                .any(|codec| {
                    codec.codec == "vp8" && codec.qualities.iter().any(|quality| quality.enabled)
                })
        );
    }
    let original_mid = engine.downs.get(di).unwrap().mid;
    let mut primary_before = 0;
    let mut backup_after = 0;
    let mut primary_after = 0;
    let mut backup_delta_leak = 0;
    let mut saw_regressed_quality = false;
    let mut delayed_reports = std::collections::VecDeque::<(Instant, Vec<u8>)>::new();
    let mut last_report = None;
    let mut observed_rr = 0usize;
    let mut delayed_rr_datagrams = 0usize;
    for tick in 0..5000u64 {
        let now = negotiated + Duration::from_millis(tick);
        if tick == 1500 {
            engine.set_room_participant_count(later_count);
        }
        // A later room-count change alone must not re-run a receiver transition.
        if tick == 3300 {
            engine.set_room_participant_count(0);
        }
        while delayed_reports.front().is_some_and(|(at, _)| *at <= now) {
            let (_, bytes) = delayed_reports.pop_front().unwrap();
            engine.handle_datagram(crate::net::Datagram {
                transport: sub_tid,
                proto: Proto::Udp,
                source: sub_addr,
                destination: server_addr,
                data: &bytes,
                received_at: now,
            });
        }
        publisher.handle_input(Input::Timeout(now)).unwrap();
        subscriber.handle_input(Input::Timeout(now)).unwrap();
        if publisher.is_connected() && tick.is_multiple_of(30) {
            for (mid, mut codec) in [(primary, Codec::Vp9), (backup, Codec::Vp8)] {
                // Same accepted MID and primary SSRC, only the in-order PT
                // changes. A new association/receiver cannot substitute for it.
                if invalid_pt && mid == primary && tick >= 3500 {
                    codec = Codec::Vp8;
                }
                if mid == backup && tick < 2000 {
                    continue;
                }
                let writer = publisher.writer(mid).unwrap();
                let pt = writer
                    .payload_params()
                    .find(|p| p.spec().codec == codec)
                    .unwrap()
                    .pt();
                let mut frame = vec![0; 64];
                if codec == Codec::Vp9 {
                    frame[0] = 0x82;
                    frame[63] = 9;
                } else {
                    frame[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
                    frame[63] = 8;
                    if tick < 2300 {
                        frame[0] |= 1;
                    }
                }
                writer
                    .write(
                        pt,
                        now,
                        MediaTime::new(tick * 90, Frequency::NINETY_KHZ),
                        frame,
                    )
                    .unwrap();
            }
        }
        for (peer, tid, address, receiving) in [
            (&mut publisher, pub_tid, pub_addr, false),
            (&mut subscriber, sub_tid, sub_addr, true),
        ] {
            loop {
                match peer.poll_output().unwrap() {
                    Output::Transmit(packet) => {
                        // Delay the actual encrypted downlink receiver reports,
                        // not TWCC/ICE or a synthetic stats observation.
                        if rtt_probe && receiving && packet.contents.get(1) == Some(&201) {
                            let bytes = packet.contents.to_vec();
                            delayed_rr_datagrams += 1;
                            last_report = Some(bytes.clone());
                            delayed_reports.push_back((now + Duration::from_millis(50), bytes));
                        } else {
                            engine.handle_datagram(crate::net::Datagram {
                                transport: tid,
                                proto: Proto::Udp,
                                source: address,
                                destination: server_addr,
                                data: &packet.contents,
                                received_at: now,
                            });
                        }
                    }
                    Output::Event(Event::MediaData(data)) if receiving => {
                        let codec = data.params.spec().codec;
                        if (800..1800).contains(&tick) && codec == Codec::Vp9 {
                            primary_before += 1;
                        }
                        if (2100..2250).contains(&tick) && codec == Codec::Vp8 {
                            backup_delta_leak += 1;
                        }
                        if tick > if invalid_pt { 3800 } else { 2600 } {
                            if codec == Codec::Vp8 {
                                backup_after += 1;
                            }
                            if codec == Codec::Vp9 {
                                primary_after += 1;
                            }
                        }
                    }
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
        engine.handle_timeout(now);
        let mut events = vec![];
        engine.drain_events(&mut events);
        for event in events {
            if let MediaEvent::SubscriberRtt {
                transport,
                ref track,
                rtt_ms,
                received_at,
            } = event
            {
                if rtt_probe {
                    assert_eq!(transport, sub_tid);
                    assert_eq!(track, &sid);
                    assert!(
                        (50..=51).contains(&rtt_ms),
                        "actual 50ms encrypted RR delay: {rtt_ms}"
                    );
                    assert_eq!(
                        received_at, now,
                        "immediate RR consumer retains receipt time"
                    );
                    observed_rr += 1;
                }
            }
            if let MediaEvent::SubscribedQuality { codecs, .. } = event {
                if codecs.len() == 2
                    && codecs[0].codec == "vp9"
                    && codecs[0].qualities.iter().all(|q| !q.enabled)
                    && codecs[1].codec == "vp8"
                    && codecs[1].qualities.iter().any(|q| q.enabled)
                {
                    saw_regressed_quality = true;
                }
            }
        }
        for (destination, bytes) in packets.0.lock().unwrap().drain(..) {
            let peer = if destination == pub_addr {
                &mut publisher
            } else {
                assert_eq!(destination, sub_addr);
                &mut subscriber
            };
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server_addr, destination, &bytes).unwrap(),
            ))
            .unwrap();
        }
    }
    if backup_only {
        assert_eq!(primary_before, 0, "unsupported primary must not leak");
    } else {
        assert!(
            primary_before > 10,
            "actual primary SRTP must forward before backup arrives"
        );
    }
    assert_eq!(
        backup_delta_leak, 0,
        "receiver migration must wait for backup keyframe"
    );
    if regresses || backup_only || invalid_pt {
        assert!(
            backup_after > 10 && primary_after == 0,
            "actual backup must replace primary: {backup_after}/{primary_after}"
        );
        if regresses {
            assert!(
                saw_regressed_quality,
                "publisher dynacast must stop primary and carry demand to backup"
            );
            assert_eq!(engine.up_by_sid[&sid], backup_ui);
        } else if invalid_pt {
            assert_eq!(engine.up_by_sid[&sid], backup_ui);
            assert!(
                engine.codec_receivers[&sid]
                    .receivers
                    .iter()
                    .any(|receiver| receiver.up == primary_ui && receiver.invalid)
            );
            assert!(
                !engine.codec_receivers[&sid].target_received,
                "Invalid routing must not invent normal target-received eligibility after a blocked first target"
            );
        } else {
            assert_eq!(
                engine.up_by_sid[&sid], primary_ui,
                "backup-only subscriber cannot globally regress SIMULCAST"
            );
            assert_eq!(engine.downs.get(di).unwrap().up, backup_ui);
        }
    } else {
        assert!(
            primary_after > 10 && backup_after == 0,
            "threshold/SIMULCAST must preserve primary: {backup_after}/{primary_after}"
        );
    }
    assert_eq!(
        engine.downs.get(di).unwrap().mid,
        original_mid,
        "codec regression preserves negotiated receiving m-line"
    );
    assert_eq!(
        engine.stats().subscribed_tracks,
        1,
        "no new subscription lifecycle on codec switch"
    );
    if rtt_probe {
        assert!(
            delayed_rr_datagrams > 5,
            "real native peer must actually emit encrypted receiver-report datagrams"
        );
        assert!(
            observed_rr > 0,
            "actual authenticated RR must reach bound subscriber consumer"
        );
    }
    if let Some(stats) = &forward_stats {
        stats.tick(negotiated + Duration::from_secs(6));
        assert!(
            stats.snapshot().histogram_count > 10,
            "real source SRTP must reach successful subscriber admission, not fabricated frame metadata"
        );
    }
    engine.handle_command(
        MediaCommand::UnpublishTrack {
            track_sid: sid.clone(),
        },
        negotiated + Duration::from_secs(6),
    );
    if rtt_probe {
        engine.drain_events(&mut vec![]);
        let bytes = last_report.expect("real encrypted RR was captured");
        engine.handle_datagram(crate::net::Datagram {
            transport: sub_tid,
            proto: Proto::Udp,
            source: sub_addr,
            destination: server_addr,
            data: &bytes,
            received_at: negotiated + Duration::from_secs(7),
        });
        let mut retired_events = Vec::new();
        engine.drain_events(&mut retired_events);
        assert!(
            !retired_events
                .iter()
                .any(|e| matches!(e, MediaEvent::SubscriberRtt { .. })),
            "retired downtrack report must not be delivered to another owner generation"
        );
    }

    assert!(!engine.up_by_sid.contains_key(&sid));
    assert!(!engine.codec_receivers.contains_key(&sid));
    assert!(engine.up_by_mid.keys().all(|(tid, _)| *tid != pub_tid));
    assert_eq!(engine.stats().uptracks, 0);
    assert_eq!(engine.stats().downtracks, 0);
    assert_eq!(engine.stats().subscribed_tracks, 0);
}

#[test]
fn actual_srtp_broadcast_admission_reports_original_packet_cohort_samples() {
    actual_dual_codec(
        BackupCodecPolicy::Regression,
        0,
        1,
        1,
        true,
        false,
        false,
        false,
        true,
    );
}
