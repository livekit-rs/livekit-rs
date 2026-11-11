//! Independent actual ICE/DTLS/SRTP/TWCC negative controls for local-credit segments.
//! Network disturbances never notify the estimator or manufacture feedback.
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use str0m::bwe::{Bitrate, BweValidationStatus};
use str0m::media::{Direction, MediaKind};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Input, Output, Rtc};

struct Datagram {
    source: SocketAddr,
    destination: SocketAddr,
    bytes: Vec<u8>,
    release_at: Instant,
}
struct Network {
    queue: VecDeque<Datagram>,
    held: usize,
    dropped: usize,
    delivered: usize,
    previous_receipt: Option<Instant>,
    largest_gap: Duration,
    // Exact fixed-point bits*nanoseconds; floor by integer arithmetic, no tolerance.
    tokens: u128,
    last_refill: Instant,
    force_one_loss: bool,
}
impl Network {
    fn new(now: Instant) -> Self {
        Self {
            queue: VecDeque::new(),
            held: 0,
            dropped: 0,
            delivered: 0,
            previous_receipt: None,
            largest_gap: Duration::ZERO,
            tokens: 4500 * 8 * 1_000_000_000,
            last_refill: now,
            force_one_loss: false,
        }
    }
    fn service(
        &mut self,
        from: &mut Rtc,
        to: &mut Rtc,
        now: Instant,
        elapsed: Duration,
        low: bool,
    ) {
        let mut due_rounds = 0;
        for _ in 0..4096 {
            match from.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    let is_rtp = packet.contents.len() >= 2
                        && packet.contents[0] & 0xc0 == 0x80
                        && !(192..=223).contains(&packet.contents[1]);
                    if is_rtp && low {
                        let refill =
                            now.saturating_duration_since(self.last_refill).as_nanos() * 120_000;
                        self.tokens = (self.tokens + refill).min(4500 * 8 * 1_000_000_000);
                        self.last_refill = now;
                        let cost = packet.contents.len() as u128 * 8 * 1_000_000_000;
                        // A known interior packet is genuinely lost, not a forged
                        // lost status. Subsequent real RTP produces actual TWCC.
                        if (!self.force_one_loss && elapsed >= Duration::from_millis(200))
                            || cost > self.tokens
                        {
                            self.force_one_loss |= elapsed >= Duration::from_millis(200);
                            self.dropped += 1;
                            continue;
                        }
                        self.tokens -= cost;
                    }
                    let hold = is_rtp
                        && !low
                        && (Duration::from_millis(200)..Duration::from_millis(250))
                            .contains(&elapsed);
                    if hold {
                        self.held += 1;
                    }
                    let release_at = if hold {
                        now + (Duration::from_millis(250) - elapsed)
                    } else {
                        now
                    };
                    self.queue.push_back(Datagram {
                        source: packet.source,
                        destination: packet.destination,
                        bytes: packet.contents.to_vec(),
                        release_at,
                    });
                }
                Output::Event(_) => (),
                Output::Timeout(deadline) if deadline <= now && due_rounds < 15 => {
                    due_rounds += 1;
                    from.handle_input(Input::Timeout(now)).unwrap();
                }
                Output::Timeout(_) => break,
            }
        }
        while self.queue.front().is_some_and(|p| p.release_at <= now) {
            let packet = self.queue.pop_front().unwrap();
            let is_rtp = packet.bytes.len() >= 2
                && packet.bytes[0] & 0xc0 == 0x80
                && !(192..=223).contains(&packet.bytes[1]);
            to.handle_input(Input::Receive(
                now,
                Receive::new(
                    Protocol::Udp,
                    packet.source,
                    packet.destination,
                    &packet.bytes,
                )
                .unwrap(),
            ))
            .unwrap();
            if is_rtp {
                if let Some(last) = self.previous_receipt {
                    self.largest_gap = self.largest_gap.max(now.saturating_duration_since(last));
                }
                self.previous_receipt = Some(now);
                self.delivered += 1;
            }
        }
    }
}
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
fn pair(start: Instant) -> (Rtc, Rtc) {
    let (mut sender, mut receiver) = (peer(start, true), peer(start, false));
    sender.bwe().set_automatic_probing(false);
    sender.add_local_candidate(
        Candidate::host("127.0.0.1:9941".parse::<SocketAddr>().unwrap(), "udp").unwrap(),
    );
    receiver.add_local_candidate(
        Candidate::host("127.0.0.1:9942".parse::<SocketAddr>().unwrap(), "udp").unwrap(),
    );
    let mut change = sender.sdp_api();
    change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    let (offer, pending) = change.apply().unwrap();
    let answer = receiver.sdp_api().accept_offer(offer).unwrap();
    sender.sdp_api().accept_answer(pending, answer).unwrap();
    let (mut forward, mut reverse) = (Network::new(start), Network::new(start));
    for tick in 0..1000 {
        let now = start + Duration::from_millis(tick);
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        // Keep the setup outside the experimental hold interval.
        forward.service(&mut sender, &mut receiver, now, Duration::ZERO, false);
        reverse.service(&mut receiver, &mut sender, now, Duration::ZERO, false);
    }
    assert!(sender.is_connected() && receiver.is_connected());
    (sender, receiver)
}
fn run(low: bool) {
    let start = Instant::now();
    let (mut sender, mut receiver) = pair(start);
    let requested = start + Duration::from_secs(1);
    let target = Bitrate::bps(215_010);
    sender.bwe().reset(Bitrate::kbps(32));
    sender.bwe().set_desired_bitrate(target);
    let id = sender.bwe().start_validation(target, requested).unwrap();
    let (mut forward, mut reverse) = (Network::new(requested), Network::new(requested));
    let mut matched_loss_seen = false;
    for tick in 0..=3001 {
        let elapsed = Duration::from_millis(tick);
        let now = requested + elapsed;
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        forward.service(&mut sender, &mut receiver, now, elapsed, low);
        reverse.service(&mut receiver, &mut sender, now, Duration::ZERO, false);
        let proof = sender.bwe().snapshot().unwrap().last_validation.unwrap();
        assert_eq!(proof.id, id);
        assert_eq!(proof.requested_at, requested);
        assert_eq!(
            proof.measurement_segments, 1,
            "network events cannot create a local-credit boundary: {proof:?}"
        );
        assert_eq!(proof.cancelled_local_credit, Duration::ZERO);
        assert!(proof.last_credit_discard_at.is_none());
        if low {
            matched_loss_seen |= proof.lost_packets > 0;
            assert_ne!(
                proof.status,
                BweValidationStatus::Passed,
                "actual constrained/lost delivery is not proof"
            );
            if matched_loss_seen {
                assert_eq!(proof.status, BweValidationStatus::Failed);
            }
        }
    }
    if low {
        assert!(forward.force_one_loss && forward.dropped > 0);
        assert!(
            matched_loss_seen,
            "actual RTP loss must be reported by real matched TWCC"
        );
    } else {
        assert!(forward.held > 0 && forward.delivered > 0);
        assert!(
            forward.largest_gap >= Duration::from_millis(50),
            "actual network receive gap was not exercised: {:?}",
            forward.largest_gap
        );
    }
}
#[test]
fn real_receive_gap_does_not_create_local_credit_segment() {
    run(false);
}
#[test]
fn real_low_capacity_and_matched_loss_cannot_be_segmented_into_pass() {
    run(true);
}

#[test]
fn real_low_capacity_rejects_explicit_dual_rate_whole_proof() {
    let start = Instant::now();
    let (mut sender, mut receiver) = pair(start);
    let requested = start + Duration::from_secs(1);
    let required = Bitrate::mbps(2);
    sender.bwe().reset(Bitrate::kbps(32));
    sender.bwe().set_desired_bitrate(required);
    let id = sender
        .bwe()
        .start_validation_with_pacing(required, Bitrate::bps(2_400_000), requested)
        .unwrap();
    let (mut forward, mut reverse) = (Network::new(requested), Network::new(requested));
    let mut matched_loss_seen = false;
    for tick in 0..=3001 {
        let elapsed = Duration::from_millis(tick);
        let now = requested + elapsed;
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        // Existing real120kbps/4500B integer bucket. No capacity value is
        // supplied to native BWE; it sees only real encrypted RTP and TWCC.
        forward.service(&mut sender, &mut receiver, now, elapsed, true);
        reverse.service(&mut receiver, &mut sender, now, Duration::ZERO, false);
        let proof = sender.bwe().snapshot().unwrap().last_validation.unwrap();
        assert_eq!(proof.id, id);
        assert_eq!(proof.requested_at, requested);
        assert_eq!(proof.target_bitrate, required);
        assert_eq!(proof.pacing_bitrate, Bitrate::bps(2_400_000));
        assert_ne!(proof.status, BweValidationStatus::Passed);
        assert!(proof.selected_proof.is_none());
        matched_loss_seen |= proof.lost_packets > 0;
        if matched_loss_seen {
            assert_eq!(proof.status, BweValidationStatus::Failed);
        }
    }
    assert!(forward.dropped > 0 && forward.delivered > 0);
    assert!(
        matched_loss_seen,
        "real low capacity must yield actual matched loss, never a manufactured status"
    );
}

// New dual-rate near-boundary fixture only; the existing120kbps helper and
// legacy assertions above retain their original byte units and behaviour.
fn service_dual_near_capacity(network: &mut Network, from: &mut Rtc, to: &mut Rtc, now: Instant) {
    let mut rounds = 0;
    for _ in 0..4096 {
        match from.poll_output().unwrap() {
            Output::Transmit(packet) => {
                let rtp = packet.contents.len() >= 2
                    && packet.contents[0] & 0xc0 == 0x80
                    && !(192..=223).contains(&packet.contents[1]);
                if rtp {
                    // ActualIPv4+UDP28bytes, as in the original browser proxy.
                    // Exact fixed point; the packet either fits or is lost.
                    let refill = now
                        .saturating_duration_since(network.last_refill)
                        .as_nanos()
                        * 1_200_000;
                    network.tokens = (network.tokens + refill).min(4500 * 8 * 1_000_000_000);
                    network.last_refill = now;
                    let cost = (packet.contents.len() as u128 + 28) * 8 * 1_000_000_000;
                    if cost > network.tokens {
                        network.dropped += 1;
                        continue;
                    }
                    network.tokens -= cost;
                }
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
                if rtp {
                    network.delivered += 1;
                }
            }
            Output::Event(_) => (),
            Output::Timeout(deadline) if deadline <= now && rounds < 15 => {
                rounds += 1;
                from.handle_input(Input::Timeout(now)).unwrap();
            }
            Output::Timeout(_) => return,
        }
    }
    panic!("near-capacity fixture exceeded existing bounded drain");
}

#[test]
fn real_one_point_two_mbps_path_cannot_prove_dual_two_mbps_request() {
    let start = Instant::now();
    let (mut sender, mut receiver) = pair(start);
    let requested = start + Duration::from_secs(1);
    let required = Bitrate::mbps(2);
    sender.bwe().reset(Bitrate::kbps(32));
    sender.bwe().set_desired_bitrate(required);
    let id = sender
        .bwe()
        .start_validation_with_pacing(required, Bitrate::bps(2_400_000), requested)
        .unwrap();
    let (mut forward, mut reverse) = (Network::new(requested), Network::new(requested));
    let mut actual_matched_loss = false;
    for tick in 0..=3001 {
        let now = requested + Duration::from_millis(tick);
        sender.handle_input(Input::Timeout(now)).unwrap();
        receiver.handle_input(Input::Timeout(now)).unwrap();
        service_dual_near_capacity(&mut forward, &mut sender, &mut receiver, now);
        reverse.service(&mut receiver, &mut sender, now, Duration::ZERO, false);
        let proof = sender.bwe().snapshot().unwrap().last_validation.unwrap();
        assert_eq!(proof.id, id);
        assert_eq!(proof.requested_at, requested);
        assert_eq!(proof.target_bitrate, required);
        assert_eq!(proof.pacing_bitrate, Bitrate::bps(2_400_000));
        assert_ne!(proof.status, BweValidationStatus::Passed);
        assert!(proof.selected_proof.is_none());
        actual_matched_loss |= proof.lost_packets > 0;
        if actual_matched_loss {
            assert_eq!(proof.status, BweValidationStatus::Failed);
        }
    }
    assert!(forward.delivered > 0 && forward.dropped > 0);
    assert!(
        actual_matched_loss,
        "1.2Mbps real link must expose actualTWCC loss"
    );
}
