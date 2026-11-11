//! Sustained validation through actual ICE/DTLS/SRTP/TWCC packet exchange.
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use str0m::bwe::{Bitrate, BweValidationStatus};
use str0m::media::{Direction, Frequency, MediaKind, MediaTime};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output, Rtc};

fn peer(now: Instant, bwe: bool) -> Rtc {
    let cfg = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap());
    if bwe {
        cfg.enable_bwe(Some(Bitrate::kbps(400))).build(now)
    } else {
        cfg.build(now)
    }
}
#[derive(Default)]
struct ObservedTransfer {
    rtp: usize,
    protected_bytes: usize,
    max_protected_packet: usize,
    video_frames: usize,
    can_probe: bool,
}
fn transfer(from: &mut Rtc, to: &mut Rtc, now: Instant, discard_rtp: bool) -> usize {
    transfer_observed(from, to, now, discard_rtp).rtp
}
fn transfer_observed(
    from: &mut Rtc,
    to: &mut Rtc,
    now: Instant,
    discard_rtp: bool,
) -> ObservedTransfer {
    transfer_observed_recording(from, to, now, discard_rtp, None)
}
#[derive(Clone, Copy)]
struct ActualDeliveredRtp {
    sent_at: Instant,
    received_at: Instant,
    protected_bytes: usize,
}
fn transfer_observed_recording(
    from: &mut Rtc,
    to: &mut Rtc,
    now: Instant,
    discard_rtp: bool,
    mut records: Option<&mut Vec<ActualDeliveredRtp>>,
) -> ObservedTransfer {
    let mut observed = ObservedTransfer::default();
    let mut due_now_rounds = 0;
    for _ in 0..4096 {
        match from.poll_output().unwrap() {
            Output::Transmit(p) => {
                let is_rtp = p.contents.len() > 1
                    && p.contents[0] & 0xc0 == 0x80
                    && !(192..=223).contains(&p.contents[1]);
                if is_rtp {
                    observed.rtp += 1;
                    observed.protected_bytes += p.contents.len();
                    observed.max_protected_packet =
                        observed.max_protected_packet.max(p.contents.len());
                    if discard_rtp {
                        continue;
                    }
                }
                to.handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, p.source, p.destination, &p.contents).unwrap(),
                ))
                .unwrap();
                if is_rtp && let Some(records) = records.as_mut() {
                    records.push(ActualDeliveredRtp {
                        sent_at: now,
                        received_at: now,
                        protected_bytes: p.contents.len(),
                    });
                }
            }
            Output::Event(Event::EgressBitrateEstimate(str0m::bwe::BweKind::Twcc {
                can_probe,
                ..
            })) => {
                observed.can_probe |= can_probe;
            }
            Output::Event(Event::MediaData(data))
                if data.params.spec().codec == str0m::format::Codec::Vp8 =>
            {
                observed.video_frames += 1;
            }
            Output::Event(_) => (),
            Output::Timeout(deadline) if deadline <= now && due_now_rounds < 15 => {
                // Match production Transport::drive: drain bounded already-due
                // work at the same real clock, without inventing finer timers.
                due_now_rounds += 1;
                from.handle_input(Input::Timeout(now)).unwrap();
            }
            Output::Timeout(_) => return observed,
        }
    }
    panic!("native output failed to settle within bounded work");
}
fn pair(start: Instant) -> (Rtc, Rtc) {
    let (sender, receiver, _) = pair_with_audio(start, false);
    (sender, receiver)
}
fn pair_with_audio(start: Instant, audio: bool) -> (Rtc, Rtc, Option<str0m::media::Mid>) {
    let (sender, receiver, audio_mid, _) = pair_with_policies(start, audio, true);
    (sender, receiver, audio_mid)
}
fn pair_with_policies(
    start: Instant,
    audio: bool,
    automatic: bool,
) -> (Rtc, Rtc, Option<str0m::media::Mid>, str0m::media::Mid) {
    let mut sender = peer(start, true);
    sender.bwe().set_automatic_probing(automatic);
    let mut receiver = peer(start, false);
    let a: SocketAddr = "127.0.0.1:9901".parse().unwrap();
    let b: SocketAddr = "127.0.0.1:9902".parse().unwrap();
    sender.add_local_candidate(Candidate::host(a, "udp").unwrap());
    receiver.add_local_candidate(Candidate::host(b, "udp").unwrap());
    let mut change = sender.sdp_api();
    let video_mid = change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    let audio_mid =
        audio.then(|| change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None));
    let (offer, pending) = change.apply().unwrap();
    let answer = receiver.sdp_api().accept_offer(offer).unwrap();
    sender.sdp_api().accept_answer(pending, answer).unwrap();
    for tick in 0..1000u64 {
        let now = start + Duration::from_millis(tick);
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        transfer(&mut sender, &mut receiver, now, false);
        transfer(&mut receiver, &mut sender, now, false);
    }
    assert!(sender.is_connected() && receiver.is_connected());
    (sender, receiver, audio_mid, video_mid)
}
#[test]
fn actual_native_padding_proves_one_and_two_megabit_validation_and_then_resumes_probing() {
    let start = Instant::now();
    let (mut sender, mut receiver) = pair(start);
    let mut tick = 1000u64;
    for target in [Bitrate::mbps(1), Bitrate::mbps(2)] {
        let requested = start + Duration::from_millis(tick);
        sender.bwe().reset(Bitrate::kbps(32));
        sender.bwe().set_desired_bitrate(target);
        let id = sender
            .bwe()
            .start_validation(target, requested)
            .expect("ready transport starts independent validation after reset");
        let mut transmitted = 0;
        loop {
            let now = start + Duration::from_millis(tick);
            tick += 1;
            sender.handle_input(Input::Timeout(now)).unwrap();
            receiver.handle_input(Input::Timeout(now)).unwrap();
            transmitted += transfer(&mut sender, &mut receiver, now, false);
            transfer(&mut receiver, &mut sender, now, false);
            let v = sender.bwe().snapshot().unwrap().last_validation.unwrap();
            assert_eq!(v.id, id);
            assert_eq!(v.requested_at, requested);
            if now.duration_since(requested) < Duration::from_secs(1) {
                assert_ne!(v.status, BweValidationStatus::Passed);
            }
            if matches!(
                v.status,
                BweValidationStatus::Passed | BweValidationStatus::Failed
            ) {
                assert_eq!(
                    v.status,
                    BweValidationStatus::Passed,
                    "actual validation: {v:?}"
                );
                assert!(
                    v.send_span >= Duration::from_secs(1) && v.recv_span >= Duration::from_secs(1)
                );
                assert!(v.measured_bitrate.unwrap() >= target);
                assert!(v.received_packets > 50);
                assert_eq!(v.lost_packets, 0);
                assert!(transmitted > 50);
                break;
            }
            assert!(now.duration_since(requested) <= Duration::from_secs(4));
        }
        // Retaining the terminal result must not cancel later ordinary clusters.
        let mut saw_ordinary = false;
        sender.bwe().set_desired_bitrate(Bitrate::mbps(3));
        for _ in 0..250 {
            let now = start + Duration::from_millis(tick);
            tick += 1;
            sender.handle_input(Input::Timeout(now)).unwrap();
            receiver.handle_input(Input::Timeout(now)).unwrap();
            transfer(&mut sender, &mut receiver, now, false);
            transfer(&mut receiver, &mut sender, now, false);
            saw_ordinary |= sender
                .bwe()
                .snapshot()
                .unwrap()
                .latest_scheduled_probe
                .is_some();
        }
        assert!(saw_ordinary);
    }
}
#[test]
fn missing_validation_feedback_expires_and_reset_cancels_old_train_identity() {
    let start = Instant::now();
    let (mut sender, mut receiver) = pair(start);
    sender.bwe().reset(Bitrate::kbps(32));
    let requested = start + Duration::from_secs(1);
    let target = Bitrate::mbps(1);
    let id = sender.bwe().start_validation(target, requested).unwrap();
    assert!(
        sender
            .bwe()
            .start_validation(target, requested + Duration::from_secs(1))
            .is_none()
    );
    for tick in 1000..4100u64 {
        let now = start + Duration::from_millis(tick);
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        let sent = transfer(&mut sender, &mut receiver, now, true);
        if (Duration::from_secs(2)..Duration::from_secs(3)).contains(&now.duration_since(requested))
        {
            assert_eq!(
                sent, 0,
                "validation must stop sending at its two-second wall deadline"
            );
        }
        transfer(&mut receiver, &mut sender, now, false);
    }
    let v = sender.bwe().snapshot().unwrap().last_validation.unwrap();
    assert_eq!(v.id, id);
    assert_eq!(v.requested_at, requested);
    assert_eq!(v.status, BweValidationStatus::Failed);
    let restart = start + Duration::from_millis(4100);
    sender.bwe().reset(Bitrate::kbps(32));
    assert!(sender.bwe().snapshot().unwrap().last_validation.is_none());
    let id = sender.bwe().start_validation(target, restart).unwrap();
    sender.bwe().cancel_validation(restart);
    let v = sender.bwe().snapshot().unwrap().last_validation.unwrap();
    assert_eq!(v.id, id);
    assert_eq!(v.status, BweValidationStatus::Cancelled);
}

#[test]
fn same_instant_reset_never_reuses_old_actual_twcc_cluster_identity() {
    let start = Instant::now();
    let (mut sender, mut receiver) = pair(start);
    let now = start + Duration::from_secs(1);
    sender.bwe().reset(Bitrate::kbps(32));
    let first_id = sender
        .bwe()
        .start_validation(Bitrate::mbps(1), now)
        .unwrap();
    sender.handle_input(Input::Timeout(now)).unwrap();
    // These actual protected packets are in the TWCC register before reset.
    assert!(transfer(&mut sender, &mut receiver, now, false) > 0);
    sender.bwe().reset(Bitrate::kbps(32));
    let second_id = sender
        .bwe()
        .start_validation(Bitrate::mbps(1), now)
        .unwrap();
    assert!(
        second_id > first_id,
        "IDs remain monotonic within a transport across BWE reset"
    );
    // Deliver the real feedback for old packets without sending any new train.
    let feedback_at = now + Duration::from_millis(100);
    receiver.handle_input(Input::Timeout(feedback_at)).unwrap();
    transfer(&mut receiver, &mut sender, feedback_at, false);
    let observation = sender.bwe().snapshot().unwrap().last_validation.unwrap();
    assert_eq!(observation.id, second_id);
    assert_eq!(observation.received_packets, 0);
    assert_eq!(observation.lost_packets, 0);
    assert_eq!(observation.status, BweValidationStatus::Pending);
}

#[test]
fn actual_validation_with_two_to_ten_millisecond_wakes_and_mixed_opus_reaches_target() {
    let profiles: &[(&[u64], bool)] = &[
        (&[2, 7, 3, 10, 4, 9, 6, 5], true),
        (&[5], false),
        (&[10], false),
        (&[5], true),
        (&[10], true),
    ];
    for &(steps, mixed_audio) in profiles {
        for target in [Bitrate::kbps(300), Bitrate::kbps(600), Bitrate::mbps(2)] {
            let start = Instant::now();
            let (mut sender, mut receiver, audio_mid) = pair_with_audio(start, mixed_audio);
            let requested = start + Duration::from_secs(1);
            sender.bwe().reset(Bitrate::kbps(32));
            let id = sender.bwe().start_validation(target, requested).unwrap();
            let mut elapsed = Duration::ZERO;
            let mut next_audio = Duration::ZERO;
            let mut audio_clock = 0;
            let mut sent = 0;
            for tick in 0..1000 {
                let now = requested + elapsed;
                if let Some(audio_mid) = audio_mid
                    && elapsed >= next_audio
                {
                    let writer = sender.writer(audio_mid).unwrap();
                    let pt = writer
                        .payload_params()
                        .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                        .unwrap()
                        .pt();
                    // A real Opus silence frame shares the same paced transport.
                    writer
                        .write(
                            pt,
                            now,
                            MediaTime::new(audio_clock, Frequency::FORTY_EIGHT_KHZ),
                            vec![0xf8, 0xff, 0xfe],
                        )
                        .unwrap();
                    audio_clock += 960;
                    next_audio = elapsed + Duration::from_millis(20);
                }
                sender.handle_input(Input::Timeout(now)).unwrap();
                receiver.handle_input(Input::Timeout(now)).unwrap();
                sent += transfer(&mut sender, &mut receiver, now, false);
                transfer(&mut receiver, &mut sender, now, false);
                let v = sender.bwe().snapshot().unwrap().last_validation.unwrap();
                assert_eq!(v.id, id);
                if matches!(
                    v.status,
                    BweValidationStatus::Passed | BweValidationStatus::Failed
                ) {
                    assert_eq!(
                        v.status,
                        BweValidationStatus::Passed,
                        "jittered actual validation {target:?}, steps={steps:?}, audio={mixed_audio}: {v:?}"
                    );
                    assert!(v.send_span >= Duration::from_secs(1));
                    assert!(v.recv_span >= Duration::from_secs(1));
                    assert!(v.measured_bitrate.unwrap() >= target);
                    assert_eq!(v.lost_packets, 0);
                    assert!(v.at.duration_since(requested) <= Duration::from_secs(3));
                    assert!(sent > 50 && v.received_packets > 50);
                    break;
                }
                assert!(
                    elapsed <= Duration::from_secs(3),
                    "fixed validation deadline must terminate"
                );
                elapsed += Duration::from_millis(steps[tick % steps.len()]);
            }
        }
    }
}

#[test]
fn actual_periodic_policy_cancels_queued_padding_preserves_validation_and_reset_policy() {
    use str0m::bwe::ProbeKind;
    let start = Instant::now();
    let (mut sender, mut receiver) = pair(start);
    sender.bwe().set_desired_bitrate(Bitrate::kbps(200));
    let mut tick = 1000u64;
    loop {
        let now = start + Duration::from_millis(tick);
        tick += 1;
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        let snapshot = sender.bwe().snapshot().unwrap();
        if snapshot
            .latest_scheduled_probe
            .is_some_and(|p| p.kind == ProbeKind::PeriodicAlr)
        {
            // handle_timeout has selected the actual periodic padding source,
            // but no packet from this newly scheduled train has been drained.
            sender.bwe().set_periodic_alr_probing(false);
            let after = sender.bwe().snapshot().unwrap();
            assert_eq!(snapshot.raw_estimate, after.raw_estimate);
            assert_eq!(snapshot.last_twcc_feedback_at, after.last_twcc_feedback_at);
            assert_eq!(
                transfer(&mut sender, &mut receiver, now, false),
                0,
                "cancelled periodic train must not leak its generated padding"
            );
            break;
        }
        transfer(&mut sender, &mut receiver, now, false);
        transfer(&mut receiver, &mut sender, now, false);
        assert!(
            tick < 21000,
            "default policy must still permit periodic ALR probing"
        );
    }
    // A reset preserves the policy but still permits initial and sustained
    // recovery. Disabling periodic exploration is not disabling all BWE.
    sender.bwe().reset(Bitrate::kbps(32));
    let requested = start + Duration::from_millis(tick);
    let id = sender
        .bwe()
        .start_validation(Bitrate::mbps(1), requested)
        .unwrap();
    for _ in 0..3100 {
        let now = start + Duration::from_millis(tick);
        tick += 1;
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        transfer(&mut sender, &mut receiver, now, false);
        transfer(&mut receiver, &mut sender, now, false);
        let v = sender.bwe().snapshot().unwrap().last_validation.unwrap();
        assert_eq!(v.id, id);
        if v.status == BweValidationStatus::Passed {
            break;
        }
        assert_ne!(v.status, BweValidationStatus::Failed);
        assert!(now.duration_since(requested) <= Duration::from_secs(3));
    }
    assert_eq!(
        sender
            .bwe()
            .snapshot()
            .unwrap()
            .last_validation
            .unwrap()
            .status,
        BweValidationStatus::Passed
    );
    let disabled_until = tick + 7000;
    while tick < disabled_until {
        let now = start + Duration::from_millis(tick);
        tick += 1;
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        transfer(&mut sender, &mut receiver, now, false);
        transfer(&mut receiver, &mut sender, now, false);
        assert!(
            sender
                .bwe()
                .snapshot()
                .unwrap()
                .latest_scheduled_probe
                .is_none_or(|p| p.kind != ProbeKind::PeriodicAlr)
        );
    }
    let resumed = start + Duration::from_millis(tick - 1);
    sender.bwe().set_periodic_alr_probing(true);
    let mut observed = None;
    for _ in 0..10000 {
        let now = start + Duration::from_millis(tick);
        tick += 1;
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        transfer(&mut sender, &mut receiver, now, false);
        transfer(&mut receiver, &mut sender, now, false);
        if let Some(p) = sender.bwe().snapshot().unwrap().latest_scheduled_probe
            && p.kind == ProbeKind::PeriodicAlr
        {
            assert!(p.at.duration_since(resumed) >= Duration::from_secs(5));
            observed = Some(p);
            break;
        }
    }
    assert!(
        observed.is_some(),
        "periodic probing resumes after its original interval"
    );
}

#[test]
fn managed_discovery_preserves_actual_validation_and_paced_video_supply_after_reset() {
    for target in [Bitrate::mbps(1), Bitrate::mbps(2)] {
        let start = Instant::now();
        let (mut sender, mut receiver, _, video_mid) = pair_with_policies(start, false, false);
        assert!(
            sender
                .bwe()
                .snapshot()
                .unwrap()
                .latest_scheduled_probe
                .is_none()
        );
        sender.bwe().reset(Bitrate::kbps(32));
        sender.bwe().set_desired_bitrate(target);
        let mut tick = 1000u64;
        let requested = start + Duration::from_millis(tick);
        // Switch while validation is active; it must survive cancellation of
        // ordinary discovery, including repeated idempotent false calls.
        sender.bwe().set_automatic_probing(true);
        let id = sender.bwe().start_validation(target, requested).unwrap();
        sender.bwe().set_automatic_probing(false);
        let mut physical_capability_seen = false;
        loop {
            let now = start + Duration::from_millis(tick);
            tick += 1;
            sender.bwe().set_automatic_probing(false);
            sender.handle_input(Input::Timeout(now)).unwrap();
            receiver.handle_input(Input::Timeout(now)).unwrap();
            physical_capability_seen |=
                transfer_observed(&mut sender, &mut receiver, now, false).can_probe;
            transfer(&mut receiver, &mut sender, now, false);
            let snapshot = sender.bwe().snapshot().unwrap();
            assert!(
                snapshot.latest_scheduled_probe.is_none(),
                "managed mode must schedule no ordinary clusters"
            );
            let v = snapshot.last_validation.unwrap();
            assert_eq!(v.id, id);
            assert_eq!(v.requested_at, requested);
            assert_ne!(
                v.status,
                BweValidationStatus::Failed,
                "actual managed validation: {v:?}"
            );
            if v.status == BweValidationStatus::Passed {
                assert!(
                    v.send_span >= Duration::from_secs(1) && v.recv_span >= Duration::from_secs(1)
                );
                assert!(v.measured_bitrate.unwrap() >= target);
                assert_eq!(v.lost_packets, 0);
                eprintln!(
                    "actual managed proof goal={target:?}, measured={:?}, raw={:?}, configuredNormalPacing={:?}",
                    v.measured_bitrate,
                    snapshot.raw_estimate,
                    sender.bwe().configured_pacing_rate()
                );
                break;
            }
            assert!(now.duration_since(requested) <= Duration::from_secs(3));
        }
        assert!(
            physical_capability_seen,
            "automatic policy must not falsify can_probe capability"
        );
        let frame_size: usize = (target.as_u64() / 400).try_into().unwrap(); //20ms payload at the actual goal.
        let mut submitted = 0;
        let mut received = 0;
        let mut actual_protected = 0;
        let mut max_pending = 0;
        for step in 0..5000 {
            let now = start + Duration::from_millis(tick);
            tick += 1;
            if step < 4000 && step % 20 == 0 {
                let writer = sender.writer(video_mid).unwrap();
                let pt = writer
                    .payload_params()
                    .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
                    .unwrap()
                    .pt();
                let mut frame = vec![0; frame_size];
                frame[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
                writer
                    .write(
                        pt,
                        now,
                        MediaTime::new(tick * 90, Frequency::NINETY_KHZ),
                        frame,
                    )
                    .unwrap();
                submitted += 1;
            }
            sender.handle_input(Input::Timeout(now)).unwrap();
            receiver.handle_input(Input::Timeout(now)).unwrap();
            actual_protected +=
                transfer_observed(&mut sender, &mut receiver, now, false).protected_bytes;
            received += transfer_observed(&mut receiver, &mut sender, now, false).video_frames;
            max_pending = max_pending.max(submitted - received);
            assert!(
                sender
                    .bwe()
                    .snapshot()
                    .unwrap()
                    .latest_scheduled_probe
                    .is_none()
            );
        }
        eprintln!(
            "actual managed pacedVP8 goal={target:?}, submitted={submitted}, assembled={received}, protectedBytes={actual_protected}, maxPendingFrames={max_pending}, raw={:?}, configuredNormalPacing={:?}",
            sender.bwe().snapshot().unwrap().raw_estimate,
            sender.bwe().configured_pacing_rate()
        );
        assert_eq!(
            received, submitted,
            "actual encrypted paced-video frame assembly must drain, not just unpaced audio"
        );
        assert_eq!(submitted, 200);
        assert!(actual_protected >= frame_size * submitted);
        assert!(
            max_pending <= 10,
            "this lossless local profile must bound queued video at200ms"
        );
    }
}

/// Real encrypted datagrams held until their actual receive clock is due.
/// Control traffic remains immediate; neither TWCC nor estimator state is forged.
fn transfer_with_rtp_queue(
    from: &mut Rtc,
    to: &mut Rtc,
    now: Instant,
    delay: Duration,
    queue: &mut Vec<(Instant, SocketAddr, SocketAddr, Vec<u8>)>,
) -> usize {
    let mut rounds = 0;
    let mut emitted = 0;
    for _ in 0..4096 {
        match from.poll_output().unwrap() {
            Output::Transmit(p) => {
                let rtp = p.contents.len() > 1
                    && p.contents[0] & 0xc0 == 0x80
                    && !(192..=223).contains(&p.contents[1]);
                if rtp {
                    emitted += 1;
                    queue.push((now + delay, p.source, p.destination, p.contents.to_vec()));
                } else {
                    to.handle_input(Input::Receive(
                        now,
                        Receive::new(Protocol::Udp, p.source, p.destination, &p.contents).unwrap(),
                    ))
                    .unwrap();
                }
            }
            Output::Event(_) => (),
            Output::Timeout(at) if at <= now && rounds < 15 => {
                rounds += 1;
                from.handle_input(Input::Timeout(now)).unwrap();
            }
            Output::Timeout(_) => break,
        }
    }
    assert!(queue.len() < 8192, "bounded real-datagram queue");
    let mut i = 0;
    while i < queue.len() {
        if queue[i].0 <= now {
            let (_, source, destination, bytes) = queue.remove(i);
            to.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, source, destination, &bytes).unwrap(),
            ))
            .unwrap();
        } else {
            i += 1;
        }
    }
    emitted
}

fn write_paced_vp8(sender: &mut Rtc, mid: str0m::media::Mid, now: Instant, tick: u64, size: usize) {
    let writer = sender.writer(mid).unwrap();
    let pt = writer
        .payload_params()
        .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
        .unwrap()
        .pt();
    let mut frame = vec![0; size];
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

#[test]
fn actual_overuse_then_rtp_silence_reset_and_fresh_validation_restores_paced_video() {
    use str0m::bwe::BweEstimateCause;
    let start = Instant::now();
    let target = Bitrate::mbps(2);
    let (mut sender, mut receiver, _, mid) = pair_with_policies(start, false, false);
    sender.bwe().reset(target);
    sender.bwe().set_desired_bitrate(target);
    let mut tick = 1000;
    let mut queue = Vec::new();
    let mut overuse = None;
    let mut first_overuse = None;
    let mut initial_sent = 0;
    let mut initial_received = 0;
    // Increasing actual receive delay creates a real delay trend, not loss or
    // a fabricated snapshot. Detection may precede the RTT-gated rate cut;
    // require both within the same bounded drive, without changing its limit.
    for step in 0..3000u64 {
        let now = start + Duration::from_millis(tick);
        tick += 1;
        if step % 20 == 0 {
            write_paced_vp8(&mut sender, mid, now, tick, 5000);
            initial_sent += 1;
        }
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        transfer_with_rtp_queue(
            &mut sender,
            &mut receiver,
            now,
            Duration::from_millis(20 + step / 5),
            &mut queue,
        );
        initial_received += transfer_observed(&mut receiver, &mut sender, now, false).video_frames;
        let snapshot = sender.bwe().snapshot().unwrap();
        if snapshot.cause == BweEstimateCause::DelayOveruse {
            let rate = snapshot
                .raw_estimate
                .expect("real Overuse has a native rate");
            first_overuse.get_or_insert((step, rate));
            if rate < target {
                eprintln!(
                    "actual delayed VP8 firstOveruse={first_overuse:?}, decreasedAtMs={step}, rate={rate:?}"
                );
                overuse = Some(rate);
                break;
            }
        }
    }
    assert!(
        overuse.is_some(),
        "actual TWCC delay detector must signal Overuse"
    );
    assert!(
        overuse.unwrap() < target,
        "actual congestion must reduce native estimate"
    );
    // Drain every pre-congestion frame before renegotiating. Inactivating a
    // half-sent frame would instead test receiver gap/jitter-buffer recovery.
    for _ in 0..8000 {
        let now = start + Duration::from_millis(tick);
        tick += 1;
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        transfer_with_rtp_queue(&mut sender, &mut receiver, now, Duration::ZERO, &mut queue);
        initial_received += transfer_observed(&mut receiver, &mut sender, now, false).video_frames;
        if initial_received == initial_sent && queue.is_empty() {
            break;
        }
    }
    assert_eq!(
        initial_received, initial_sent,
        "all initial actual frames must drain before inactivity"
    );
    // An active sending m-line intentionally maintains 50kbps keepalive
    // padding even with no frames. Negotiate actual inactivity to exercise
    // genuine RTP silence; merely stopping frame writes cannot create it.
    let mut change = sender.sdp_api();
    change.set_direction(mid, Direction::Inactive);
    let (offer, pending) = change.apply().unwrap();
    let answer = receiver.sdp_api().accept_offer(offer).unwrap();
    sender.sdp_api().accept_answer(pending, answer).unwrap();
    let mut silent_ticks = 0;
    for _ in 0..8000 {
        let now = start + Duration::from_millis(tick);
        tick += 1;
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        let emitted =
            transfer_with_rtp_queue(&mut sender, &mut receiver, now, Duration::ZERO, &mut queue);
        transfer(&mut receiver, &mut sender, now, false);
        silent_ticks = if emitted == 0 && queue.is_empty() {
            silent_ticks + 1
        } else {
            0
        };
        if silent_ticks >= 1000 {
            break;
        }
    }
    assert_eq!(
        silent_ticks, 1000,
        "real media queue must drain then remain RTP-silent"
    );
    let mut change = sender.sdp_api();
    change.set_direction(mid, Direction::SendOnly);
    let (offer, pending) = change.apply().unwrap();
    let answer = receiver.sdp_api().accept_offer(offer).unwrap();
    sender.sdp_api().accept_answer(pending, answer).unwrap();
    let requested = start + Duration::from_millis(tick);
    sender.bwe().reset(Bitrate::kbps(32));
    sender.bwe().set_desired_bitrate(target);
    let id = sender.bwe().start_validation(target, requested).unwrap();
    loop {
        let now = start + Duration::from_millis(tick);
        tick += 1;
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        transfer(&mut sender, &mut receiver, now, false);
        transfer(&mut receiver, &mut sender, now, false);
        let snapshot = sender.bwe().snapshot().unwrap();
        assert!(
            snapshot.latest_scheduled_probe.is_none(),
            "autooff must survive actual-congestion reset"
        );
        let proof = snapshot.last_validation.unwrap();
        assert_eq!(proof.id, id);
        assert_eq!(proof.requested_at, requested);
        assert_ne!(
            proof.status,
            BweValidationStatus::Failed,
            "fresh actual proof: {proof:?}"
        );
        if proof.status == BweValidationStatus::Passed {
            assert!(proof.send_span >= Duration::from_secs(1));
            assert!(proof.recv_span >= Duration::from_secs(1));
            assert!(proof.measured_bitrate.unwrap() >= target);
            assert_eq!(proof.lost_packets, 0);
            eprintln!(
                "actual recovery proof={proof:?}, raw={:?}, pacing={:?}",
                snapshot.raw_estimate,
                sender.bwe().configured_pacing_rate()
            );
            break;
        }
        assert!(now.duration_since(requested) <= Duration::from_secs(3));
    }
    let (mut sent, mut received, mut max_pending, mut protected) = (0, 0, 0, 0);
    for step in 0..5000 {
        let now = start + Duration::from_millis(tick);
        tick += 1;
        if step < 4000 && step % 20 == 0 {
            write_paced_vp8(&mut sender, mid, now, tick, 5000);
            sent += 1;
        }
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        protected += transfer_observed(&mut sender, &mut receiver, now, false).protected_bytes;
        received += transfer_observed(&mut receiver, &mut sender, now, false).video_frames;
        max_pending = max_pending.max(sent - received);
    }
    assert_eq!(sent, 200);
    assert_eq!(received, sent);
    assert!(protected >= sent * 5000);
    eprintln!(
        "actual recovered sent={sent}, received={received}, maxPending={max_pending}, protected={protected}, finalNative={:?}",
        sender.bwe().snapshot()
    );
    assert!(
        max_pending <= 10,
        "recovered real paced VP8 pending={max_pending}"
    );
}

#[test]
fn actual_duplex_twcc_metadata_preserves_receive_clock_across_193ms_gap_and_validation() {
    let start = Instant::now();
    let (mut sender, mut receiver, audio_mid, video_mid) = pair_with_policies(start, true, false);
    sender.bwe().set_incoming_twcc_observations(true);
    receiver.bwe().set_incoming_twcc_observations(true);
    let mut change = receiver.sdp_api();
    let reverse = change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    let (offer, pending) = change.apply().unwrap();
    let answer = sender.sdp_api().accept_offer(offer).unwrap();
    receiver.sdp_api().accept_answer(pending, answer).unwrap();
    sender.bwe().reset(Bitrate::mbps(2));
    let traffic_start = start + Duration::from_millis(1000);
    let requested = traffic_start + Duration::from_millis(500);
    let mut id = None;
    let mut queue = Vec::new();
    let mut passed = false;
    let mut metadata_seen = false;
    let mut last_counter = None;
    let mut reports = 0;
    for step in 0..3000u64 {
        let now = traffic_start + Duration::from_millis(step);
        if step == 500 {
            // Observe the real read gap first. A new explicit validation
            // starts after that diagnostic disturbance; no failure bypass.
            sender.bwe().reset(Bitrate::kbps(32));
            id = Some(
                sender
                    .bwe()
                    .start_validation(Bitrate::mbps(1), requested)
                    .unwrap(),
            );
        }
        if step % 20 == 0 {
            write_paced_vp8(&mut sender, video_mid, now, 1000 + step, 1000);
            for (rtc, mid) in [(&mut sender, audio_mid.unwrap()), (&mut receiver, reverse)] {
                let writer = rtc.writer(mid).unwrap();
                let pt = writer
                    .payload_params()
                    .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                    .unwrap()
                    .pt();
                writer
                    .write(
                        pt,
                        now,
                        MediaTime::new(step * 48, Frequency::FORTY_EIGHT_KHZ),
                        vec![0xf8, 0xff, 0xfe],
                    )
                    .unwrap();
            }
        }
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        // Anchor actual receipts during the first200ms, then hold real
        // encrypted RTP over a193ms socket-read gap. Initial silence has
        // no prior receipt and cannot measure an interarrival gap.
        // No future timestamp or ACK is fabricated.
        transfer_with_rtp_queue(
            &mut sender,
            &mut receiver,
            now,
            Duration::from_millis(if (200..393).contains(&step) {
                393 - step
            } else {
                0
            }),
            &mut queue,
        );
        transfer(&mut receiver, &mut sender, now, false);
        if let Some(o) = receiver.bwe().incoming_twcc_snapshot() {
            metadata_seen = true;
            assert!(
                o.max_encoding_error <= Duration::from_micros(125),
                "actual encoded TWCC: {o:?}"
            );
            assert_eq!(
                o.missing_packets, 0,
                "ordered delivered RTP must not invent loss"
            );
            assert_eq!(o.received_packets, usize::from(o.status_count));
            assert!(o.built_at.is_some() && o.emitted_at.is_some());
            assert!(o.emitted_at.unwrap() >= o.built_at.unwrap());
            assert!(o.sender_ssrc != 0 && o.media_ssrc != 0);
            if last_counter != Some(o.feedback_count) {
                reports += 1;
                last_counter = Some(o.feedback_count);
            }
            let again = receiver.bwe().incoming_twcc_snapshot().unwrap();
            assert_eq!(
                o.emitted_at, again.emitted_at,
                "read-only polling cannot invent emission"
            );
        }
        let Some(id) = id else {
            continue;
        };
        let proof = sender.bwe().snapshot().unwrap().last_validation.unwrap();
        assert_eq!(proof.id, id);
        assert_ne!(
            proof.status,
            BweValidationStatus::Failed,
            "actual duplex proof {proof:?}"
        );
        if proof.status == BweValidationStatus::Passed {
            assert!(
                proof.send_span >= Duration::from_secs(1)
                    && proof.recv_span >= Duration::from_secs(1)
            );
            assert!(proof.measured_bitrate.unwrap() >= Bitrate::mbps(1));
            assert_eq!(proof.lost_packets, 0);
            passed = true;
            break;
        }
    }
    eprintln!(
        "actual duplex passed={passed}, metadataSeen={metadata_seen}, reports={reports}, native={:?}, incoming={:?}, reverseIncoming={:?}",
        sender.bwe().snapshot(),
        receiver.bwe().incoming_twcc_snapshot(),
        sender.bwe().incoming_twcc_snapshot()
    );
    assert!(
        passed,
        "actual fresh sustained proof must pass within original deadline"
    );
    assert!(
        metadata_seen,
        "actual incoming TWCC metadata must be collected"
    );
    assert!(
        reports >= 2,
        "actual incoming feedback must progress, reports={reports}"
    );
    assert!(
        receiver
            .bwe()
            .incoming_twcc_snapshot()
            .unwrap()
            .max_receiver_gap
            >= Duration::from_millis(193)
    );
    assert!(
        sender.bwe().incoming_twcc_snapshot().is_some(),
        "reverse real audio also produces feedback metadata"
    );
}

#[test]
fn actual_validation_after_one_fifty_millisecond_driver_gap_can_prove_healthy_delivery() {
    let start = Instant::now();
    let (mut sender, mut receiver, _, _) = pair_with_policies(start, false, false);
    let target = Bitrate::bps(215_010);
    sender.bwe().reset(Bitrate::kbps(32));
    let requested = start + Duration::from_millis(1000);
    let id = sender.bwe().start_validation(target, requested).unwrap();
    let mut elapsed_ms = 0;
    let mut total_bytes = 0usize;
    let mut max_packet = 0usize;
    let mut completed = false;
    let mut actual_packets = Vec::new();
    while elapsed_ms <= 3000 {
        let now = requested + Duration::from_millis(elapsed_ms);
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        let sent = transfer_observed_recording(
            &mut sender,
            &mut receiver,
            now,
            false,
            Some(&mut actual_packets),
        );
        transfer(&mut receiver, &mut sender, now, false);
        total_bytes += sent.protected_bytes;
        max_packet = max_packet.max(sent.max_protected_packet);
        // Preserve bounded credit: a delayed wake cannot replay the missing50ms
        // of service as an unlimited burst. Every counted byte is real SRTP.
        if sent.rtp > 0 {
            let wake_bound = target.as_u64() / 800 + sent.max_protected_packet as u64;
            assert!(
                sent.protected_bytes as u64 <= wake_bound,
                "actual wake burst exceeds10ms credit plus one actual packet"
            );
        }
        let cumulative_bound = target.as_u64() * (elapsed_ms + 10) / 8000 + max_packet as u64;
        assert!(total_bytes as u64 <= cumulative_bound);
        let proof = sender.bwe().snapshot().unwrap().last_validation.unwrap();
        assert_eq!(proof.id, id);
        assert_eq!(
            proof.requested_at, requested,
            "feedback may not renew the fixed request"
        );
        assert_eq!(
            proof.lost_packets, 0,
            "this profile delivers every actual datagram"
        );
        if matches!(
            proof.status,
            BweValidationStatus::Passed | BweValidationStatus::Failed
        ) {
            eprintln!(
                "actual one50msgap elapsedMs={elapsed_ms}, totalProtected={total_bytes}, maxPacket={max_packet}, proof={proof:?}"
            );
            // Independent test-only replay over complete, contiguous suffixes
            // of actual encrypted packets, not the estimator's claimed rate.
            // Every packet and every gap inside each interval remains counted.
            let train: Vec<_> = actual_packets
                .iter()
                .filter(|p| p.sent_at < requested + Duration::from_secs(2))
                .collect();
            let mut best = None;
            let mut best_rate = f64::NEG_INFINITY;
            if let Some(last) = train.last() {
                for (index, first) in train.iter().enumerate() {
                    if first.sent_at < requested + Duration::from_millis(250) {
                        continue;
                    }
                    let send_span = last.sent_at.duration_since(first.sent_at);
                    let recv_span = last.received_at.duration_since(first.received_at);
                    if send_span < Duration::from_secs(1) || recv_span < Duration::from_secs(1) {
                        continue;
                    }
                    let bytes: usize = train[index..].iter().map(|p| p.protected_bytes).sum();
                    let send_bytes = bytes - last.protected_bytes;
                    let recv_bytes = bytes - first.protected_bytes;
                    let rate = (send_bytes as f64 * 8.0 / send_span.as_secs_f64())
                        .min(recv_bytes as f64 * 8.0 / recv_span.as_secs_f64());
                    if rate > best_rate {
                        best_rate = rate;
                        best = Some((
                            rate,
                            index,
                            train.len() - index,
                            first.sent_at.duration_since(requested),
                            last.sent_at.duration_since(requested),
                            send_span,
                            recv_span,
                            send_bytes,
                            recv_bytes,
                        ));
                    }
                }
            }
            eprintln!(
                "actual one50msgap independentCompleteSuffix(rate,index,count,first,last,sendSpan,recvSpan,sendBytes,recvBytes)={best:?}; actualPacketCount={}",
                train.len()
            );
            assert_eq!(
                proof.status,
                BweValidationStatus::Passed,
                "a subsequent healthy raw delivery interval must meet the unchanged proof standard"
            );
            assert_eq!(proof.measurement_segments, 2);
            assert!(proof.cancelled_local_credit > Duration::ZERO);
            assert!(proof.request_totals.measured_bitrate.unwrap() < target);

            assert!(proof.send_span >= Duration::from_secs(1));
            assert!(proof.recv_span >= Duration::from_secs(1));
            assert!(proof.measured_bitrate.unwrap() >= target);
            assert!(now.duration_since(requested) <= Duration::from_secs(3));
            completed = true;
            break;
        }
        // No handle/input/poll calls occur inside this actual logical-clock
        // interval: last wake200ms, next wake250ms, then original1ms cadence.
        elapsed_ms += if elapsed_ms == 200 { 50 } else { 1 };
    }
    assert!(completed, "fixed3s proof deadline cannot be extended");
}

#[test]
fn actual_encrypted_twcc_distinguishes_report_batch_wait_from_return_path_queue() {
    for (return_ms, expected_prop_ms) in [(10u64, 20u64), (160, 170)] {
        let start = Instant::now();
        let (mut sender, mut receiver, audio_mid, _) = pair_with_policies(start, true, false);
        let mid = audio_mid.unwrap();
        let origin = start + Duration::from_secs(1);
        let mut forward = Vec::new();
        let mut reverse = Vec::new();
        let mut emitted = 0;
        let mut reports = 0;
        let mut last_report = None;
        let mut minimum = None;
        let mut saw_report_wait = false;
        // Both peers process real clocks and encrypted datagrams continuously.
        // TWCC's actual50ms reporting cadence is unmodified. Only the return
        // network queue differs; no feedback payload or record is fabricated.
        for ms in 0..800u64 {
            let now = origin + Duration::from_millis(ms);
            sender.handle_input(Input::Timeout(now)).unwrap();
            if ms % 20 == 0 {
                let writer = sender.writer(mid).unwrap();
                let pt = writer
                    .payload_params()
                    .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                    .unwrap()
                    .pt();
                writer
                    .write(
                        pt,
                        now,
                        MediaTime::new(ms * 48, Frequency::FORTY_EIGHT_KHZ),
                        vec![0xf8, 0xff, 0xfe],
                    )
                    .unwrap();
            }
            emitted += transfer_with_rtp_queue(
                &mut sender,
                &mut receiver,
                now,
                Duration::from_millis(10),
                &mut forward,
            );
            receiver.handle_input(Input::Timeout(now)).unwrap();
            let mut rounds = 0;
            for _ in 0..4096 {
                match receiver.poll_output().unwrap() {
                    Output::Transmit(p) => reverse.push((
                        now + Duration::from_millis(return_ms),
                        p.source,
                        p.destination,
                        p.contents.to_vec(),
                    )),
                    Output::Event(_) => (),
                    Output::Timeout(at) if at <= now && rounds < 15 => {
                        rounds += 1;
                        receiver.handle_input(Input::Timeout(now)).unwrap();
                    }
                    Output::Timeout(_) => break,
                }
            }
            assert!(reverse.len() < 8192);
            let mut index = 0;
            while index < reverse.len() {
                if reverse[index].0 <= now {
                    let (_, source, destination, bytes) = reverse.remove(index);
                    sender
                        .handle_input(Input::Receive(
                            now,
                            Receive::new(Protocol::Udp, source, destination, &bytes).unwrap(),
                        ))
                        .unwrap();
                } else {
                    index += 1;
                }
            }
            if let Some(batch) = sender.bwe().snapshot().unwrap().latest_feedback
                && Some(batch.at) != last_report
            {
                // Observe every receipt identity, including warm-up. Crossing
                // the sampling threshold does not make a cached batch fresh.
                last_report = Some(batch.at);
                if ms <= 200 + return_ms {
                    continue;
                }
                reports += 1;
                assert_eq!(batch.at, now);
                assert!(batch.received_packets > 0);
                assert_eq!(batch.lost_packets, 0);
                let propagation = batch
                    .min_propagation_rtt
                    .expect("actual matched valid timing");
                let maximum = batch.max_rtt.unwrap();
                let expected = Duration::from_millis(expected_prop_ms);
                // A final packet can wait at most one20ms media interval
                // before report creation. That unknown tail is NOT removed.
                assert!(propagation + Duration::from_micros(250) >= expected);
                assert!(
                    propagation
                        <= expected + Duration::from_millis(20) + Duration::from_micros(250),
                    "actual={batch:?}"
                );
                minimum = Some(minimum.map_or(propagation, |old: Duration| old.min(propagation)));
                saw_report_wait |= maximum >= propagation + Duration::from_millis(20);
            }
        }
        assert!(emitted >= 40 && reports >= 3);
        let minimum = minimum.unwrap();
        eprintln!(
            "actual encrypted report returnMs={return_ms} reports={reports} minimumPropagation={minimum:?} observedBatchWait={saw_report_wait}"
        );
        assert!(
            minimum.abs_diff(Duration::from_millis(expected_prop_ms)) <= Duration::from_micros(250)
        );
        assert!(
            saw_report_wait,
            "actual feedback RTT contains receiver batch waiting"
        );
    }
}

// Explicit dual-rate experiment. These helpers do not change the original
// single-rate fixtures, clocks, thresholds or packet accounting above.
fn collect_validation_return_packets(
    receiver: &mut Rtc,
    now: Instant,
    queue: &mut Vec<(SocketAddr, SocketAddr, Vec<u8>)>,
) -> (usize, usize) {
    let mut rounds = 0;
    let (mut audio, mut video) = (0, 0);
    for _ in 0..4096 {
        match receiver.poll_output().unwrap() {
            Output::Transmit(packet) => {
                queue.push((packet.source, packet.destination, packet.contents.to_vec()))
            }
            Output::Event(Event::MediaData(data)) => match data.params.spec().codec {
                str0m::format::Codec::Opus => audio += 1,
                str0m::format::Codec::Vp8 => video += 1,
                _ => (),
            },
            Output::Event(_) => (),
            Output::Timeout(deadline) if deadline <= now && rounds < 15 => {
                rounds += 1;
                receiver.handle_input(Input::Timeout(now)).unwrap();
            }
            Output::Timeout(_) => return (audio, video),
        }
    }
    panic!("receiver output exceeded existing bounded drain");
}

fn assert_actual_dual_padding_envelope(records: &[ActualDeliveredRtp], pacing: Bitrate) {
    // Every actual wake and every10/100ms trailing window, including all
    // packets at an endpoint; no whole-epoch averaging hides a short burst.
    for end in 0..records.len() {
        for window in [
            Duration::ZERO,
            Duration::from_millis(10),
            Duration::from_millis(100),
        ] {
            let now = records[end].sent_at;
            let start = now - window;
            let packets: Vec<_> = records[..=end]
                .iter()
                .filter(|p| p.sent_at >= start)
                .collect();
            let bytes: u128 = packets.iter().map(|p| p.protected_bytes as u128).sum();
            let max_packet = packets.iter().map(|p| p.protected_bytes).max().unwrap() as u128;
            assert!(
                bytes * 8 * 1_000_000_000
                    <= pacing.as_u64() as u128
                        * (window.as_nanos() + Duration::from_millis(10).as_nanos())
                        + max_packet * 8 * 1_000_000_000,
                "actual dual padding exceeds Q envelope window={window:?} bytes={bytes}"
            );
        }
    }
}

#[test]
fn actual_dual_rate_whole_proof_survives_repeated_twenty_ms_local_service_gaps() {
    use str0m::bwe::BweValidationProofScope;
    for (dual, mixed) in [(false, false), (true, false), (true, true)] {
        let start = Instant::now();
        let (mut sender, mut receiver, audio_mid, video_mid) =
            pair_with_policies(start, mixed, false);
        let requested = start + Duration::from_secs(1);
        let required = Bitrate::mbps(2);
        let pacing = if dual {
            Bitrate::bps(2_400_000)
        } else {
            required
        };
        sender.bwe().reset(Bitrate::kbps(32));
        sender.bwe().set_desired_bitrate(required);
        let id = if dual {
            sender
                .bwe()
                .start_validation_with_pacing(required, pacing, requested)
        } else {
            sender.bwe().start_validation(required, requested)
        }
        .unwrap();
        let mut feedback = Vec::new();
        let mut records = Vec::new();
        let mut largest_segment = Duration::ZERO;
        let mut last_service = None;
        let mut largest_service_interval = Duration::ZERO;
        let mut finished = false;
        let (mut received_audio, mut received_video) = (0, 0);
        for ms in 0..=3001u64 {
            let now = requested + Duration::from_millis(ms);
            // Receiver always gets its actual1ms wake. Its real RTCP output
            // waits until the sender actually wakes; no old receive time is
            // assigned and no sender Input/poll occurs during the service gap.
            receiver.handle_input(Input::Timeout(now)).unwrap();
            let (audio, video) =
                collect_validation_return_packets(&mut receiver, now, &mut feedback);
            received_audio += audio;
            received_video += video;
            assert!(feedback.len() <= 8192);
            // Last service at20ms, next service at40ms, repeated every100ms.
            // The receiver still advances on each of the intervening ticks.
            if (21..40).contains(&(ms % 100)) {
                continue;
            }
            if let Some(previous) = last_service {
                largest_service_interval =
                    largest_service_interval.max(now.duration_since(previous));
            }
            last_service = Some(now);
            if mixed && ms % 20 == 0 {
                write_paced_vp8(&mut sender, video_mid, now, ms, 2000);
                let writer = sender.writer(audio_mid.unwrap()).unwrap();
                let pt = writer
                    .payload_params()
                    .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                    .unwrap()
                    .pt();
                writer
                    .write(
                        pt,
                        now,
                        MediaTime::new(ms * 48, Frequency::FORTY_EIGHT_KHZ),
                        vec![0xf8, 0xff, 0xfe],
                    )
                    .unwrap();
            }
            sender.handle_input(Input::Timeout(now)).unwrap();
            for (source, destination, bytes) in feedback.drain(..) {
                sender
                    .handle_input(Input::Receive(
                        now,
                        Receive::new(Protocol::Udp, source, destination, &bytes).unwrap(),
                    ))
                    .unwrap();
            }
            transfer_observed_recording(&mut sender, &mut receiver, now, false, Some(&mut records));
            let snapshot = sender.bwe().snapshot().unwrap();
            let proof = snapshot.last_validation.unwrap();
            assert_eq!(proof.id, id);
            assert_eq!(proof.requested_at, requested);
            assert_eq!(proof.target_bitrate, required);
            assert_eq!(proof.pacing_bitrate, pacing);
            assert_eq!(proof.lost_packets, 0, "healthy actual datagrams: {proof:?}");
            largest_segment = largest_segment.max(proof.send_span).max(proof.recv_span);
            assert!(
                largest_segment < Duration::from_secs(1),
                "a segment must not silently supply this whole-proof case"
            );
            if !matches!(
                proof.status,
                BweValidationStatus::Passed | BweValidationStatus::Failed
            ) {
                assert!(now.duration_since(requested) < Duration::from_secs(3));
                continue;
            }
            assert_eq!(largest_service_interval, Duration::from_millis(20));
            assert!(
                proof.measurement_segments >= 10,
                "actual positive credit discard must be exercised: {proof:?}"
            );
            assert!(proof.cancelled_local_credit > Duration::ZERO);
            assert!(proof.request_totals.send_span >= Duration::from_secs(1));
            assert!(proof.request_totals.recv_span >= Duration::from_secs(1));
            assert!(now.duration_since(requested) <= Duration::from_secs(3));
            if dual {
                assert_eq!(
                    proof.status,
                    BweValidationStatus::Passed,
                    "dual actual delivery: {proof:?}"
                );
                let selected = proof
                    .selected_proof
                    .expect("Passed must identify its actual packet cohort");
                assert!(matches!(
                    selected.scope,
                    BweValidationProofScope::WholeRequest
                ));
                assert!(selected.send_span >= Duration::from_secs(1));
                assert!(selected.recv_span >= Duration::from_secs(1));
                assert!(selected.measured_bitrate >= required);
                assert_eq!(selected.send_span, proof.request_totals.send_span);
                assert_eq!(selected.recv_span, proof.request_totals.recv_span);
                assert_eq!(selected.send_bytes, proof.request_totals.send_bytes);
                assert_eq!(selected.recv_bytes, proof.request_totals.recv_bytes);
                assert!(selected.measured_payload_bitrate.as_u64() > 0);
                assert!(selected.measured_payload_bitrate <= selected.measured_bitrate);
                if !mixed {
                    // This ordered, zero-loss padding-only path has no
                    // pre-activation media or unpaced RTP. Actual ACKs cover
                    // a prefix of the independently recorded encrypted sends.
                    let n = usize::try_from(selected.received_packets).unwrap();
                    assert!(n > 1 && n <= records.len());
                    let cohort = &records[..n];
                    let first = cohort.first().unwrap();
                    let last = cohort.last().unwrap();
                    assert_eq!(first.sent_at, selected.started_at);
                    let send_span = last.sent_at.duration_since(first.sent_at);
                    let first_receive = cohort.iter().map(|p| p.received_at).min().unwrap();
                    let last_receive = cohort.iter().map(|p| p.received_at).max().unwrap();
                    let recv_span = last_receive.duration_since(first_receive);
                    assert_eq!(send_span, selected.send_span);
                    // Real TWCC250us delta quantization can shift each
                    // endpoint by125us; no arbitrary rate tolerance is used.
                    let quantum = Duration::from_micros(250);
                    assert!(selected.recv_span.abs_diff(recv_span) <= quantum);
                    let sum: u64 = cohort.iter().map(|p| p.protected_bytes as u64).sum();
                    let send_bytes = sum - last.protected_bytes as u64;
                    assert_eq!(selected.send_bytes, send_bytes);
                    // Multiple packets may have the same real receive clock.
                    // Their first endpoint size is genuinely ambiguous in the
                    // test metadata; keep the exact min/max byte bounds.
                    let first_sizes: Vec<_> = cohort
                        .iter()
                        .filter(|p| p.received_at == first_receive)
                        .map(|p| p.protected_bytes as u64)
                        .collect();
                    let recv_min_bytes = sum - first_sizes.iter().max().unwrap();
                    let recv_max_bytes = sum - first_sizes.iter().min().unwrap();
                    assert!((recv_min_bytes..=recv_max_bytes).contains(&selected.recv_bytes));
                    let send_rate = send_bytes as f64 * 8.0 / send_span.as_secs_f64();
                    let min_rate = send_rate
                        .min(recv_min_bytes as f64 * 8.0 / (recv_span + quantum).as_secs_f64());
                    let max_rate = send_rate.min(
                        recv_max_bytes as f64 * 8.0
                            / recv_span.saturating_sub(quantum).as_secs_f64(),
                    );
                    assert!(
                        min_rate >= required.as_f64(),
                        "independent actual endpoint proof is below P"
                    );
                    let measured = selected.measured_bitrate.as_u64();
                    assert!(
                        (min_rate.floor() as u64..=max_rate.ceil() as u64).contains(&measured),
                        "selected rate must agree with actual byte/time bounds, not P or Q"
                    );
                    assert_actual_dual_padding_envelope(&records, pacing);
                }
            } else {
                assert_eq!(
                    proof.status,
                    BweValidationStatus::Failed,
                    "legacy single remains segment-only"
                );
                assert!(proof.selected_proof.is_none());
                assert!(
                    proof.request_totals.measured_bitrate.unwrap() < required,
                    "actual single-rate supply control, not a presumed duty-cycle formula: {proof:?}"
                );
            }
            if mixed {
                assert!(
                    received_audio > 0 && received_video > 0,
                    "real Opus and VP8 must both traverse the mixed train"
                );
                let bytes: u128 = records.iter().map(|p| p.protected_bytes as u128).sum();
                let max_packet = records.iter().map(|p| p.protected_bytes).max().unwrap() as u128;
                let span = records
                    .last()
                    .unwrap()
                    .sent_at
                    .duration_since(records.first().unwrap().sent_at);
                // This no-backlog20ms audio profile checks combined actual
                // protected bytes. It does not assert this envelope for
                // arbitrary priority/unpaced audio bursts; source tests
                // separately preserve that low-latency business exception.
                assert!(
                    bytes * 8 * 1_000_000_000
                        <= pacing.as_u64() as u128
                            * (span.as_nanos() + Duration::from_millis(10).as_nanos())
                            + max_packet * 8 * 1_000_000_000,
                    "actual mixed bytes exceed Q ledger plus original10ms credit and one packet"
                );
            }
            eprintln!(
                "actual repeated20ms service dual={dual} mixed={mixed} audio={received_audio} video={received_video} records={} maxPacket={} maxService={largest_service_interval:?} largestSegment={largest_segment:?} proof={proof:?} raw={:?} ack={:?} pacing={:?}",
                records.len(),
                records.iter().map(|p| p.protected_bytes).max().unwrap_or(0),
                snapshot.raw_estimate,
                snapshot.acknowledged_bitrate,
                sender.bwe().configured_pacing_rate()
            );
            finished = true;
            break;
        }
        assert!(
            finished,
            "original fixed3s deadline must terminate both controls"
        );
    }
}
