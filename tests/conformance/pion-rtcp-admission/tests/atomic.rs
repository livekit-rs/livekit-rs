//! Genuine ICE/DTLS/SRTCP negative input. No feedback events, packet-parser
//! results, keys, or successful worker replies are injected into the receiver.
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use str0m::bwe::Bitrate;
use str0m::media::{Direction, Frequency, MediaKind, MediaTime};
use str0m::net::{Protocol, Receive};
use str0m::rtp::RawPacket;
use str0m::{Candidate, Event, Input, Output, Rtc};

static AUTHENTICATED_CALLS: AtomicUsize = AtomicUsize::new(0);
fn source_gate(raw: &[u8]) -> bool {
    AUTHENTICATED_CALLS.fetch_add(1, Ordering::SeqCst);
    rtc_media::pion_gcc::admit_compound(raw)
}
#[derive(Default)]
struct Seen {
    native_rtcp: usize,
    remb: usize,
    observed_rtcp: usize,
    last_rtp: Option<(u32, u16)>,
}
impl Seen {
    fn delivered(&self) -> usize {
        self.native_rtcp + self.remb + self.observed_rtcp
    }
}
fn peer(now: Instant, mode: usize) -> Rtc {
    let mut config = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .enable_raw_packets(true);
    if mode == 2 {
        config = config.enable_bwe(Some(Bitrate::bps(1_000_000)));
    }
    let mut rtc = config.build(now);
    rtc.direct_api().set_remote_feedback_meter(true);
    if mode == 1 {
        rtc.direct_api().set_send_side_feedback(true);
    }
    rtc
}
fn pump(a: &mut Rtc, b: &mut Rtc, now: Instant, seen: &mut Seen) {
    let mut due = 0;
    for _ in 0..4096 {
        match a.poll_output().unwrap() {
            Output::Transmit(p) => b
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, p.source, p.destination, &p.contents).unwrap(),
                ))
                .unwrap(),
            Output::Event(Event::RawPacket(p)) => match *p {
                RawPacket::RtcpRx(_) => seen.native_rtcp += 1,
                RawPacket::RtpTx(h, _) => seen.last_rtp = Some((*h.ssrc, h.sequence_number)),
                _ => {}
            },
            Output::Event(Event::EgressBitrateEstimate(
                str0m::bwe::BweKind::ReceiverEstimate { .. },
            )) => seen.remb += 1,
            Output::Event(Event::SendSideFeedback(str0m::SendSideFeedback::Report { .. })) => {
                seen.observed_rtcp += 1
            }
            Output::Timeout(at) if at <= now && due < 15 => {
                due += 1;
                a.handle_input(Input::Timeout(now)).unwrap();
            }
            Output::Timeout(_) => return,
            Output::Event(_) => {}
        }
    }
    panic!("output did not settle")
}
fn tick(a: &mut Rtc, b: &mut Rtc, now: Instant, sa: &mut Seen, sb: &mut Seen) {
    a.handle_input(Input::Timeout(now)).unwrap();
    b.handle_input(Input::Timeout(now)).unwrap();
    pump(a, b, now, sa);
    pump(b, a, now, sb);
}
fn write(rtc: &mut Rtc, mid: str0m::media::Mid, now: Instant, index: u64) {
    let writer = rtc.writer(mid).unwrap();
    let pt = writer
        .payload_params()
        .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
        .unwrap()
        .pt();
    let mut frame = vec![0; 500];
    frame[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
    writer
        .write(
            pt,
            now,
            MediaTime::new(index * 90, Frequency::NINETY_KHZ),
            frame,
        )
        .unwrap();
}
fn pair(start: Instant, mode: usize, gate: bool) -> (Rtc, Rtc, str0m::media::Mid, Seen, Seen) {
    let (mut a, mut b) = (peer(start, mode), peer(start, 0));
    if gate {
        a.direct_api().set_rtcp_admission(Some(source_gate));
    }
    let aa: SocketAddr = "127.0.0.1:9901".parse().unwrap();
    let bb: SocketAddr = "127.0.0.1:9902".parse().unwrap();
    a.add_local_candidate(Candidate::host(aa, "udp").unwrap());
    b.add_local_candidate(Candidate::host(bb, "udp").unwrap());
    let mut change = a.sdp_api();
    let mid = change.add_media(MediaKind::Video, Direction::SendRecv, None, None, None);
    let (offer, pending) = change.apply().unwrap();
    let answer = b.sdp_api().accept_offer(offer).unwrap();
    a.sdp_api().accept_answer(pending, answer).unwrap();
    let (mut sa, mut sb) = (Seen::default(), Seen::default());
    for ms in 0..1200 {
        let now = start + Duration::from_millis(ms);
        if ms >= 1000 && ms % 20 == 0 {
            write(&mut a, mid, now, ms);
        }
        tick(&mut a, &mut b, now, &mut sa, &mut sb);
    }
    assert!(a.is_connected() && b.is_connected());
    assert!(sa.last_rtp.is_some());
    (a, b, mid, sa, sb)
}
fn inject(
    a: &mut Rtc,
    b: &mut Rtc,
    plain: &[u8],
    now: Instant,
    corrupt_auth: bool,
    seen: &mut Seen,
) {
    let mut protected = b
        ._protect_rtcp(plain)
        .expect("real completed DTLS installs peer SRTP send context");
    assert!(protected.len() > plain.len());
    if corrupt_auth {
        *protected.last_mut().unwrap() ^= 1;
    }
    a.handle_input(Input::Receive(
        now,
        Receive::new(
            Protocol::Udp,
            "127.0.0.1:9902".parse().unwrap(),
            "127.0.0.1:9901".parse().unwrap(),
            &protected,
        )
        .unwrap(),
    ))
    .unwrap();
    pump(a, b, now, seen);
}
fn compound(ssrc: u32, sequence: u16) -> Vec<u8> {
    let mut remb = vec![
        0x8f, 206, 0, 5, 0, 0, 0, 1, 0, 0, 0, 0, b'R', b'E', b'M', b'B', 1, 0x0b, 0xd0, 0x90,
    ];
    remb.extend_from_slice(&ssrc.to_be_bytes());
    let mut nack = vec![0x81, 205, 0, 3, 0, 0, 0, 1];
    nack.extend_from_slice(&ssrc.to_be_bytes());
    nack.extend_from_slice(&sequence.to_be_bytes());
    nack.extend_from_slice(&[0, 0]);
    let twcc = [
        0x8f, 205, 0, 5, 0, 0, 0, 1, 0, 0, 0, 2, 0, 1, 0, 1, 0, 0, 1, 0, 0x20, 1, 0, 0,
    ];
    [remb, nack, twcc.to_vec()].concat()
}
#[test]
fn real_authenticated_invalid_tail_cannot_leak_native_feedback_in_any_mode() {
    for mode in 0..3 {
        let start = Instant::now();
        let (mut a, mut b, mid, mut sa, mut sb) = pair(start, mode, true);
        let (ssrc, seq) = sa.last_rtp.unwrap();
        let valid = compound(ssrc, seq);
        assert!(source_gate(&valid));
        // A valid source report is delivered, proving the gate is not a stub.
        let at = start + Duration::from_millis(1200);
        let before = sa.delivered();
        inject(&mut a, &mut b, &valid, at, false, &mut sa);
        assert!(sa.native_rtcp > 0 && sa.delivered() > before);
        let stats = a.direct_api().remote_feedback_stats();
        let mut invalid = valid.clone();
        invalid.push(0);
        for offset in 1..=2 {
            let delivered = sa.delivered();
            let calls = AUTHENTICATED_CALLS.load(Ordering::SeqCst);
            inject(
                &mut a,
                &mut b,
                &invalid,
                at + Duration::from_millis(offset),
                false,
                &mut sa,
            );
            assert_eq!(
                AUTHENTICATED_CALLS.load(Ordering::SeqCst),
                calls + 1,
                "actual auth precedes policy"
            );
            assert_eq!(
                sa.delivered(),
                delivered,
                "no REMB/NACK/TWCC/raw/observer prefix in mode {mode}"
            );
            let after = a.direct_api().remote_feedback_stats();
            assert_eq!(
                after.iter().map(|v| v.repeated_nacks).sum::<u32>(),
                stats.iter().map(|v| v.repeated_nacks).sum::<u32>(),
                "no rejected-prefix repeated NACK mutation"
            );
        }
        let calls = AUTHENTICATED_CALLS.load(Ordering::SeqCst);
        let delivered = sa.delivered();
        inject(
            &mut a,
            &mut b,
            &valid,
            at + Duration::from_millis(3),
            true,
            &mut sa,
        );
        assert_eq!(
            AUTHENTICATED_CALLS.load(Ordering::SeqCst),
            calls,
            "invalid authentication never calls policy"
        );
        assert_eq!(sa.delivered(), delivered);
        // Rejection does not close media or disable subsequent valid feedback.
        let before = sa.delivered();
        inject(
            &mut a,
            &mut b,
            &valid,
            at + Duration::from_millis(4),
            false,
            &mut sa,
        );
        assert!(sa.delivered() > before);
        for ms in 1220..1320 {
            let now = start + Duration::from_millis(ms);
            if ms % 20 == 0 {
                write(&mut a, mid, now, ms);
            }
            tick(&mut a, &mut b, now, &mut sa, &mut sb);
        }
        assert!(a.is_connected() && b.is_connected());
    }
    // Default native API behavior remains unchanged when assembly has not
    // installed a policy. This is a real encrypted mechanism-negative control.
    let start = Instant::now();
    let (mut a, mut b, _, mut seen, _) = pair(start, 0, false);
    let (ssrc, seq) = seen.last_rtp.unwrap();
    let mut invalid = compound(ssrc, seq);
    invalid.push(0);
    let before = seen.delivered();
    inject(
        &mut a,
        &mut b,
        &invalid,
        start + Duration::from_millis(1200),
        false,
        &mut seen,
    );
    assert!(
        seen.delivered() > before,
        "no-hook control actually exposes the old valid-prefix behavior"
    );
}

// Source-only reachability regression: actual encrypted RTP/RTCP, no observer
// queue/event injection. These two schedules intentionally have different scope.
#[derive(Default)]
struct ObserverCounts {
    sent: usize,
    explicit_reports: usize,
    discontinuities: usize,
}
const OBSERVER_RR: [u8; 8] = [0x80, 201, 0, 1, 0, 0, 0, 77];
fn observer_drain(a: &mut Rtc, b: &mut Rtc, now: Instant, seen: &mut ObserverCounts) {
    let mut due = 0;
    for _ in 0..16384 {
        match a.poll_output().unwrap() {
            Output::Transmit(p) => b
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, p.source, p.destination, &p.contents).unwrap(),
                ))
                .unwrap(),
            Output::Event(Event::SendSideFeedback(str0m::SendSideFeedback::Sent(_))) => {
                seen.sent += 1
            }
            Output::Event(Event::SendSideFeedback(str0m::SendSideFeedback::Report {
                packet,
                ..
            })) => {
                if packet == OBSERVER_RR {
                    seen.explicit_reports += 1;
                }
            }
            Output::Event(Event::SendSideFeedback(str0m::SendSideFeedback::Discontinuity)) => {
                seen.discontinuities += 1
            }
            Output::Timeout(at) if at <= now && due < 16 => {
                due += 1;
                a.handle_input(Input::Timeout(now)).unwrap();
            }
            Output::Timeout(_) => return,
            Output::Event(_) => {}
        }
    }
    panic!("bounded actual native output drain did not settle");
}
fn observer_peer_drain(b: &mut Rtc, a: &mut Rtc, now: Instant, seen: &mut ObserverCounts) {
    let mut due = 0;
    for _ in 0..16384 {
        match b.poll_output().unwrap() {
            Output::Transmit(packet) => {
                a.handle_input(Input::Receive(
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
                observer_drain(a, b, now, seen);
            }
            Output::Timeout(at) if at <= now && due < 16 => {
                due += 1;
                b.handle_input(Input::Timeout(now)).unwrap();
            }
            Output::Timeout(_) => return,
            Output::Event(_) => {}
        }
    }
    panic!("actual peer output did not settle");
}
fn observer_input(a: &mut Rtc, b: &mut Rtc, now: Instant) {
    let protected = b
        ._protect_rtcp(&OBSERVER_RR)
        .expect("real DTLS-SRTP send context required");
    assert!(protected.len() > OBSERVER_RR.len());
    a.handle_input(Input::Receive(
        now,
        Receive::new(
            Protocol::Udp,
            "127.0.0.1:9902".parse().unwrap(),
            "127.0.0.1:9901".parse().unwrap(),
            &protected,
        )
        .unwrap(),
    ))
    .unwrap();
}
#[test]
fn ordinary_production_one_input_then_full_drain_keeps_real_observer_live_beyond_512_events() {
    let start = Instant::now();
    let (mut a, mut b, mid, _, _) = pair(start, 1, false);
    // Avoid touching the existing test's global callback counter. This is the
    // same real application typed policy, without a measurement side effect.
    a.direct_api()
        .set_rtcp_admission(Some(rtc_media::pion_gcc::admit_compound));
    let mut seen = ObserverCounts::default();
    for index in 0..1025u64 {
        let now = start + Duration::from_millis(1300 + index * 20);
        write(&mut a, mid, now, index * 20 + 1300);
        a.handle_input(Input::Timeout(now)).unwrap();
        // Product Transport.drive drains each mutation before another input.
        observer_drain(&mut a, &mut b, now, &mut seen);
        observer_input(&mut a, &mut b, now);
        observer_drain(&mut a, &mut b, now, &mut seen);
        // Complete real consent/receiver feedback in the other direction,
        // still draining A after each individual actual input.
        observer_peer_drain(&mut b, &mut a, now, &mut seen);
    }
    assert!(
        seen.sent >= 1025,
        "must observe actual protected RTP, not only parsed RTCP"
    );
    assert_eq!(
        seen.explicit_reports, 1025,
        "every actual authenticated report must survive normal draining"
    );
    assert_eq!(seen.discontinuities, 0);
    let now = start + Duration::from_millis(22000);
    observer_input(&mut a, &mut b, now);
    observer_drain(&mut a, &mut b, now, &mut seen);
    assert_eq!(
        seen.explicit_reports, 1026,
        "real subsequent feedback remains enabled"
    );
    assert!(a.is_connected() && b.is_connected());
}
#[test]
fn deliberate_native_input_batch_without_product_drain_exposes_sticky_terminal_discontinuity() {
    let start = Instant::now();
    let (mut a, mut b, _, _, _) = pair(start, 1, false);
    a.direct_api()
        .set_rtcp_admission(Some(rtc_media::pion_gcc::admit_compound));
    let now = start + Duration::from_millis(1300);
    // This is an intentionally different public-native caller schedule, NOT
    // MediaEngine.handle_datagram: no poll_output between authenticated inputs.
    for _ in 0..513 {
        observer_input(&mut a, &mut b, now);
    }
    let mut seen = ObserverCounts::default();
    observer_drain(&mut a, &mut b, now, &mut seen);
    assert_eq!(seen.discontinuities, 1);
    assert_eq!(
        seen.explicit_reports, 0,
        "overflow explicitly discards incomplete pending history"
    );
    observer_input(&mut a, &mut b, now + Duration::from_millis(1));
    observer_drain(&mut a, &mut b, now + Duration::from_millis(1), &mut seen);
    assert_eq!(
        seen.explicit_reports, 0,
        "native overflow is terminal for this observer lifetime"
    );
    assert_eq!(
        seen.discontinuities, 1,
        "must not manufacture repeated reset callbacks"
    );
    assert!(
        a.is_connected() && b.is_connected(),
        "this control exposes observer loss, not a simulated PC disconnect"
    );
}
