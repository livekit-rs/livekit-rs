//! Public str0m observation contract used by application-side congestion control.
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use str0m::bwe::Bitrate;
use str0m::media::{Direction, Frequency, MediaKind, MediaTime};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output, Rtc};

fn peer(now: Instant, bwe: bool) -> Rtc {
    let config = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap());
    let config = if bwe {
        config.enable_bwe(Some(Bitrate::bps(400_000)))
    } else {
        config
    };
    config.build(now)
}

fn transfer(from: &mut Rtc, to: &mut Rtc, now: Instant) -> usize {
    transfer_with_twcc_replay(from, to, now, false).0
}

fn transfer_with_twcc_replay(
    from: &mut Rtc,
    to: &mut Rtc,
    now: Instant,
    replay: bool,
) -> (usize, usize) {
    let mut frames = 0;
    let mut replayed = 0;
    loop {
        match from.poll_output().unwrap() {
            Output::Transmit(packet) => {
                to.handle_input(Input::Receive(
                    now,
                    Receive::new(
                        Protocol::Udp,
                        packet.source,
                        packet.destination,
                        &packet.contents,
                    )
                    .unwrap(),
                ))
                .unwrap();
                // TWCC is a standalone SRTCP RTPFB packet (FMT15/PT205).
                // Replay the actual authenticated packet, never a fabricated ACK.
                if replay
                    && packet.contents.len() >= 2
                    && packet.contents[0] & 0x1f == 15
                    && packet.contents[1] == 205
                {
                    let before = to.bwe().snapshot().unwrap();
                    to.handle_input(Input::Receive(
                        now,
                        Receive::new(
                            Protocol::Udp,
                            packet.source,
                            packet.destination,
                            &packet.contents,
                        )
                        .unwrap(),
                    ))
                    .unwrap();
                    let after = to.bwe().snapshot().unwrap();
                    assert_eq!(
                        before.acknowledged_bitrate.map(|r| r.as_u64()),
                        after.acknowledged_bitrate.map(|r| r.as_u64()),
                        "duplicate TWCC must not add ACK samples"
                    );
                    assert_eq!(before.last_twcc_feedback_at, after.last_twcc_feedback_at);
                    replayed += 1;
                }
            }
            Output::Event(Event::MediaData(data))
                if data.params.spec().codec == str0m::format::Codec::Vp8 =>
            {
                frames += 1;
            }
            Output::Timeout(_) => break,
            _ => {}
        }
    }
    (frames, replayed)
}

#[test]
fn bootstrap_timeouts_and_repeated_observation_are_not_twcc_feedback() {
    let now = Instant::now();
    let mut rtc = peer(now, true);
    let initial = rtc.bwe().snapshot().unwrap();
    assert!(initial.last_twcc_feedback_at.is_none());
    assert!(initial.last_successful_probe.is_none());
    assert!(initial.latest_feedback.is_none());
    assert!(initial.latest_scheduled_probe.is_none());
    rtc.bwe().set_desired_bitrate(Bitrate::bps(2_000_000));
    for i in 0..100 {
        rtc.handle_input(Input::Timeout(now + Duration::from_millis(i * 20)))
            .unwrap();
        while !matches!(rtc.poll_output().unwrap(), Output::Timeout(_)) {}
        assert!(
            rtc.bwe()
                .snapshot()
                .unwrap()
                .last_twcc_feedback_at
                .is_none()
        );
    }
    assert!(peer(now, false).bwe().snapshot().is_none());
}

#[test]
fn real_twcc_advances_feedback_time_and_silence_does_not_refresh_it() {
    let start = Instant::now();
    let mut sender = peer(start, true);
    let mut receiver = peer(start, false);
    let sender_addr: SocketAddr = "127.0.0.1:9701".parse().unwrap();
    let receiver_addr: SocketAddr = "127.0.0.1:9702".parse().unwrap();
    sender.add_local_candidate(Candidate::host(sender_addr, "udp").unwrap());
    receiver.add_local_candidate(Candidate::host(receiver_addr, "udp").unwrap());
    let mut change = sender.sdp_api();
    let mid = change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    let (offer, pending) = change.apply().unwrap();
    let answer = receiver.sdp_api().accept_offer(offer).unwrap();
    sender.sdp_api().accept_answer(pending, answer).unwrap();
    sender.bwe().set_desired_bitrate(Bitrate::bps(1_000_000));
    let mut probe_starts = std::collections::HashMap::new();
    let mut replayed_twcc = 0;
    for tick in 0..1000u64 {
        let now = start + Duration::from_millis(tick * 5);
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        if sender.is_connected() && tick % 6 == 0 {
            let writer = sender.writer(mid).unwrap();
            let pt = writer
                .payload_params()
                .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
                .unwrap()
                .pt();
            let mut frame = vec![0; 1400];
            frame[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
            writer
                .write(
                    pt,
                    now,
                    MediaTime::new(tick * 450, Frequency::NINETY_KHZ),
                    frame,
                )
                .unwrap();
        }
        transfer(&mut sender, &mut receiver, now);
        replayed_twcc += transfer_with_twcc_replay(&mut receiver, &mut sender, now, true).1;
        if let Some(probe) = sender.bwe().snapshot().unwrap().last_successful_probe {
            let measured_start = probe.started_at.expect("measurement has a send interval");
            assert!(measured_start <= probe.at);
            let previous = probe_starts
                .entry(probe.cluster_id)
                .or_insert(measured_start);
            assert!(
                measured_start <= *previous,
                "late feedback cannot renew old proof"
            );
            *previous = measured_start;
        }
    }
    assert!(
        replayed_twcc > 0,
        "actual authenticated TWCC replay path must execute"
    );
    let observed = sender.bwe().snapshot().unwrap();
    let feedback_at = observed.last_twcc_feedback_at.expect("real receiver TWCC");
    assert!(feedback_at > start);
    let quality = observed.latest_feedback.expect("real matched TWCC quality");
    assert_eq!(quality.at, feedback_at);
    assert!(quality.received_packets > 0);
    assert_eq!(quality.lost_packets, 0, "lossless in-memory link");
    assert_eq!(quality.loss_fraction(), Some(0.0));
    assert!(quality.max_rtt.is_some());
    assert!(observed.acknowledged_bitrate.is_some());
    assert!(observed.latest_scheduled_probe.is_some());
    let probe = observed.last_successful_probe.expect("measured probe");
    assert!(
        probe
            .started_at
            .is_some_and(|at| at >= start && at <= probe.at)
    );
    // Drive only the sender for longer than the application's stale-feedback
    // interval. Local periodic estimates/stats must retain the old receipt time.
    for tick in 0..300u64 {
        let now = start + Duration::from_secs(6) + Duration::from_millis(tick * 50);
        sender.handle_input(Input::Timeout(now)).unwrap();
        while !matches!(sender.poll_output().unwrap(), Output::Timeout(_)) {}
        assert_eq!(
            sender.bwe().snapshot().unwrap().last_twcc_feedback_at,
            Some(feedback_at)
        );
        assert_eq!(
            sender.bwe().snapshot().unwrap().latest_feedback,
            Some(quality)
        );
    }
    sender.bwe().reset(Bitrate::bps(100_000));
    let reset = sender.bwe().snapshot().unwrap();
    assert!(reset.last_twcc_feedback_at.is_none());
    assert!(reset.last_successful_probe.is_none());
    assert!(reset.latest_feedback.is_none());
}

#[test]
fn feedback_loss_fraction_distinguishes_empty_partial_and_all_lost_batches() {
    let mut sample = str0m::bwe::BweFeedbackObservation {
        at: Instant::now(),
        received_packets: 0,
        lost_packets: 0,
        max_rtt: None,
        min_propagation_rtt: None,
    };
    assert_eq!(sample.loss_fraction(), None);
    sample.received_packets = 8;
    sample.lost_packets = 2;
    assert_eq!(sample.loss_fraction(), Some(0.2));
    sample.received_packets = 0;
    assert_eq!(sample.loss_fraction(), Some(1.0));
    assert!(
        sample.max_rtt.is_none(),
        "all-lost feedback has no acknowledged RTT"
    );
}

/// All RTP is transmitted through real ICE/DTLS/SRTP and acknowledged by TWCC.
/// Before video starts, only tiny valid Opus packets and native padding/probes
/// are sent. Those Opus bytes cannot consume the ALR budget at its 40kbps floor;
/// observing an ALR exit here proves actual padding is accounted. The production
/// robust estimator must also gain actual ACK samples before any video is sent.
#[test]
fn native_padding_can_end_audio_alr_and_populate_robust_ack_before_video() {
    let start = Instant::now();
    let mut sender = peer(start, true);
    let mut receiver = peer(start, false);
    let sender_addr: SocketAddr = "127.0.0.1:9801".parse().unwrap();
    let receiver_addr: SocketAddr = "127.0.0.1:9802".parse().unwrap();
    sender.add_local_candidate(Candidate::host(sender_addr, "udp").unwrap());
    receiver.add_local_candidate(Candidate::host(receiver_addr, "udp").unwrap());
    let mut change = sender.sdp_api();
    let audio = change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    let video = change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    let (offer, pending) = change.apply().unwrap();
    let answer = receiver.sdp_api().accept_offer(offer).unwrap();
    sender.sdp_api().accept_answer(pending, answer).unwrap();
    sender.bwe().set_desired_bitrate(Bitrate::bps(80_000));
    let mut saw_audio_alr = false;
    let mut baseline_ack = None;
    let mut padding_ack_peak = 0;
    let mut padding_exited_alr = false;
    let mut padding_measured_probe = false;
    let mut video_received = 0;
    for tick in 0..15000u64 {
        let now = start + Duration::from_millis(tick);
        if tick == 3000 {
            baseline_ack = sender
                .bwe()
                .snapshot()
                .unwrap()
                .acknowledged_bitrate
                .map(|rate| rate.as_u64());
            sender.bwe().set_desired_bitrate(Bitrate::bps(2_000_000));
        }
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        if sender.is_connected() && tick % 20 == 0 {
            let writer = sender.writer(audio).unwrap();
            let pt = writer
                .payload_params()
                .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                .unwrap()
                .pt();
            writer
                .write(
                    pt,
                    now,
                    MediaTime::new(tick * 48, Frequency::FORTY_EIGHT_KHZ),
                    vec![0xf8, 0xff, 0xfe],
                )
                .unwrap();
        }
        if sender.is_connected() && tick >= 9000 && tick % 30 == 0 {
            let writer = sender.writer(video).unwrap();
            let pt = writer
                .payload_params()
                .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
                .unwrap()
                .pt();
            let mut frame = vec![0; 3000];
            frame[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
            writer
                .write(
                    pt,
                    now,
                    MediaTime::new(tick * 90, Frequency::NINETY_KHZ),
                    frame,
                )
                .unwrap();
        }
        transfer(&mut sender, &mut receiver, now);
        let frames = transfer(&mut receiver, &mut sender, now);
        if tick >= 9000 {
            video_received += frames;
        }
        let observation = sender.bwe().snapshot().unwrap();
        if tick < 3000 {
            saw_audio_alr |= observation.application_limited;
        }
        if (3000..9000).contains(&tick) {
            padding_exited_alr |= !observation.application_limited;
            padding_measured_probe |= observation.last_successful_probe.is_some();
            padding_ack_peak = padding_ack_peak.max(
                observation
                    .acknowledged_bitrate
                    .map_or(0, |rate| rate.as_u64()),
            );
        }
    }
    assert!(
        saw_audio_alr,
        "the actual audio-only connection must enter ALR"
    );
    assert!(
        padding_exited_alr
            && padding_measured_probe
            && padding_ack_peak > baseline_ack.expect("real audio-phase ACK samples"),
        "real padding-only discovery must end ALR and produce higher measured robust ACK throughput before video is sent"
    );
    assert!(
        video_received > 50,
        "audio-to-video RTP must flow after discovery"
    );
    let observation = sender.bwe().snapshot().unwrap();
    assert_eq!(
        observation.ack_fast_rate_changes, 0,
        "robust throughput has no classic variance hook"
    );
    assert!(observation.last_twcc_feedback_at.is_some());
    assert!(observation.last_successful_probe.is_some());
}

/// Reproduce a pacing stall with real negotiated video/RTX and encrypted RTP.
/// This exercises Session's cleanup of regular-stream padding, not only the
/// fallback probe SSRC or a synthetic empty pacer queue.
#[test]
fn delayed_probe_discards_regular_rtx_padding_and_preserves_media_recovery() {
    let start = Instant::now();
    let mut sender = peer(start, true);
    let mut receiver = peer(start, false);
    let sender_addr: SocketAddr = "127.0.0.1:9751".parse().unwrap();
    let receiver_addr: SocketAddr = "127.0.0.1:9752".parse().unwrap();
    sender.add_local_candidate(Candidate::host(sender_addr, "udp").unwrap());
    receiver.add_local_candidate(Candidate::host(receiver_addr, "udp").unwrap());
    let mut change = sender.sdp_api();
    let mid = change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    let (offer, pending) = change.apply().unwrap();
    let answer = receiver.sdp_api().accept_offer(offer).unwrap();
    sender.sdp_api().accept_answer(pending, answer).unwrap();
    let write_video = |rtc: &mut Rtc, now: Instant, clock: u64| {
        let writer = rtc.writer(mid).unwrap();
        let pt = writer
            .payload_params()
            .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
            .unwrap()
            .pt();
        let mut frame = vec![0; 1400];
        frame[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
        writer
            .write(pt, now, MediaTime::new(clock, Frequency::NINETY_KHZ), frame)
            .unwrap();
    };
    // Establish a genuine VP8/RTX send stream with cached retransmittable media.
    for tick in 0..600u64 {
        let now = start + Duration::from_millis(tick * 5);
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        if sender.is_connected() && tick % 6 == 0 {
            write_video(&mut sender, now, tick * 450);
        }
        transfer(&mut sender, &mut receiver, now);
        transfer(&mut receiver, &mut sender, now);
    }
    assert!(sender.is_connected() && receiver.is_connected());
    let probe_start = start + Duration::from_secs(3);
    sender.bwe().reset(Bitrate::bps(1_000_000));
    sender.bwe().set_desired_bitrate(Bitrate::bps(2_000_000));
    let mut first_rtx_at = None;
    // Stop draining immediately after one actual regular-stream RTX padding
    // packet. No media is written during this phase, so remaining queued data
    // on this stream is generated padding rather than a media frame.
    'find_first: for tick in 0..2000u64 {
        let now = probe_start + Duration::from_micros(tick * 100);
        sender.handle_input(Input::Timeout(now)).unwrap();
        loop {
            match sender.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    let regular_rtx = packet.contents.len() >= 12
                        && packet.contents[0] >> 6 == 2
                        && packet.contents[1] & 0x7f == 97
                        && packet.contents[8..12] != [0, 0, 0, 0];
                    receiver
                        .handle_input(Input::Receive(
                            now,
                            Receive::new(
                                Protocol::Udp,
                                packet.source,
                                packet.destination,
                                &packet.contents,
                            )
                            .unwrap(),
                        ))
                        .unwrap();
                    if regular_rtx {
                        assert!(
                            sender
                                .bwe()
                                .snapshot()
                                .unwrap()
                                .latest_scheduled_probe
                                .is_some_and(|p| p.at >= probe_start),
                            "RTX padding must belong to newly scheduled discovery"
                        );
                        first_rtx_at = Some(now);
                        break 'find_first;
                    }
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
    }
    let first_rtx_at = first_rtx_at.expect("real nonzero-SSRC RTX padding must execute");
    let mut padding_pending_at = None;
    for tick in 1..100u64 {
        let now = first_rtx_at + Duration::from_micros(tick * 100);
        // Deliberately do not poll output: leave generated padding queued.
        sender.handle_input(Input::Timeout(now)).unwrap();
        let queued = sender
            .direct_api()
            .stream_tx_by_mid(mid, None)
            .unwrap()
            .queue_info()
            .map_or(0, |q| q.packet_count());
        if queued > 0 {
            padding_pending_at = Some(now);
            break;
        }
    }
    let pending_at = padding_pending_at.expect("regular RTX padding must remain queued");
    let resumed = pending_at + Duration::from_millis(50);
    sender.handle_input(Input::Timeout(resumed)).unwrap();
    // Session clears expired padding before sampling the regular send queue.
    // A fresh replacement may generate its own padding after this snapshot;
    // that is authorized new work rather than replay of the expired train.
    let queued_after_cancel = sender
        .direct_api()
        .stream_tx_by_mid(mid, None)
        .unwrap()
        .queue_info()
        .expect("regular stream retains its public pacer snapshot")
        .packet_count();
    assert_eq!(
        queued_after_cancel, 0,
        "expired regular RTX padding survived cancellation"
    );

    let mut video_frames = 0;
    for tick in 0..1200u64 {
        let now = resumed + Duration::from_millis(tick * 5);
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        if tick % 6 == 0 {
            write_video(&mut sender, now, 360_000 + tick * 450);
        }
        transfer(&mut sender, &mut receiver, now);
        video_frames += transfer(&mut receiver, &mut sender, now);
    }
    assert!(
        video_frames > 50,
        "real video must continue after cancelling old padding"
    );
    let observation = sender.bwe().snapshot().unwrap();
    assert!(
        observation
            .last_twcc_feedback_at
            .is_some_and(|at| at > resumed)
    );
    assert!(
        observation
            .last_successful_probe
            .is_some_and(|p| p.at > resumed),
        "a subsequent genuine probe must still complete with real TWCC feedback"
    );
}
