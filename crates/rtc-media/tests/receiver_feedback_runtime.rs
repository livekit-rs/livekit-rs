//! Real encrypted receiver feedback enters the isolated runtime owner. NOT_RUN.
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use str0m::bwe::Bitrate;
use str0m::media::{Direction, Frequency, MediaKind, MediaTime};
use str0m::net::{Protocol, Receive};
use str0m::rtp::{RawPacket, RtpHeader};
use str0m::{Candidate, Event, Input, Output, Rtc};

fn peer(now: Instant) -> Rtc {
    Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .enable_raw_packets(true)
        .build(now)
}
#[derive(Debug)]
struct WireHeader {
    padding: bool,
    ssrc: u32,
    extension_ids: Vec<u8>,
    sequence: u16,
    header_bytes: usize,
}
// Independently inspect only the clear RTP header in the actual SRTP datagram.
// Payload is encrypted and is neither parsed nor used to invent receive proof.
fn wire_header(bytes: &[u8]) -> WireHeader {
    assert!(bytes.len() >= 12 && bytes[0] >> 6 == 2);
    let mut ids = vec![];
    let offset = 12 + (bytes[0] as usize & 15) * 4;
    let mut header_bytes = offset;
    if bytes[0] & 0x10 != 0 {
        assert!(offset + 4 <= bytes.len());
        let profile = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
        let size = u16::from_be_bytes([bytes[offset + 2], bytes[offset + 3]]) as usize * 4;
        let ext = &bytes[offset + 4..offset + 4 + size];
        header_bytes = offset + 4 + size;
        let mut at = 0;
        while at < ext.len() {
            if ext[at] == 0 {
                at += 1;
                continue;
            }
            if profile == 0xbede {
                let id = ext[at] >> 4;
                assert_ne!(id, 15, "no reserved one-byte extension");
                let len = (ext[at] & 15) as usize + 1;
                assert!(at + 1 + len <= ext.len());
                ids.push(id);
                at += 1 + len;
            } else {
                assert_eq!(profile & 0xfff0, 0x1000);
                assert!(at + 2 <= ext.len());
                let len = ext[at + 1] as usize;
                assert!(at + 2 + len <= ext.len());
                ids.push(ext[at]);
                at += 2 + len;
            }
        }
    }
    WireHeader {
        padding: bytes[0] & 0x20 != 0,
        ssrc: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        extension_ids: ids,
        sequence: u16::from_be_bytes([bytes[2], bytes[3]]),
        header_bytes,
    }
}
#[derive(Clone, Copy, Debug)]
struct PlainComponent {
    ssrc: u32,
    sequence: u16,
    header_bytes: usize,
    payload_bytes: usize,
}
#[derive(Default)]
struct Observed {
    headers: Vec<RtpHeader>,
    plain_components: Vec<PlainComponent>,
    wire_headers: Vec<WireHeader>,
    incoming_headers: Vec<RtpHeader>,
    padding: Vec<(Instant, usize)>,
    video_frames: usize,
    incoming_twcc_headers: usize,
    outgoing_twcc_feedback: usize,
    remb: Vec<(Vec<u32>, u64)>,
    twcc_estimates: Vec<u64>,
    send_side: Vec<str0m::SendSideFeedback>,
}
fn pump(from: &mut Rtc, to: &mut Rtc, now: Instant, discard_rtp: bool, seen: &mut Observed) {
    let mut due = 0;
    for _ in 0..4096 {
        match from.poll_output().unwrap() {
            Output::Transmit(p) => {
                let rtp = p.contents.len() > 1
                    && p.contents[0] & 0xc0 == 0x80
                    && !(192..=223).contains(&p.contents[1]);
                if rtp {
                    seen.wire_headers.push(wire_header(&p.contents));
                }
                if rtp && p.contents[0] & 0x20 != 0 {
                    seen.padding.push((now, p.contents.len()));
                }
                if rtp && discard_rtp {
                    continue;
                }
                to.handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, p.source, p.destination, &p.contents).unwrap(),
                ))
                .unwrap();
            }
            Output::Event(Event::SendSideFeedback(event)) => seen.send_side.push(event),
            Output::Event(Event::RawPacket(p)) => match *p {
                RawPacket::RtpTx(header, body) => {
                    let padding = if header.has_padding {
                        usize::from(body.last().copied().unwrap_or(0))
                    } else {
                        0
                    };
                    seen.plain_components.push(PlainComponent {
                        ssrc: *header.ssrc,
                        sequence: header.sequence_number,
                        header_bytes: header.header_len,
                        payload_bytes: body.len() - header.header_len - padding,
                    });
                    seen.headers.push(header);
                }
                RawPacket::RtpRx(header, _) => {
                    seen.incoming_twcc_headers +=
                        usize::from(header.ext_vals.transport_cc.is_some());
                    seen.incoming_headers.push(header);
                }
                RawPacket::RtcpTx(str0m::rtp::rtcp::Rtcp::Twcc(_)) => {
                    seen.outgoing_twcc_feedback += 1
                }
                _ => (),
            },
            Output::Event(Event::EgressBitrateEstimate(str0m::bwe::BweKind::Twcc {
                estimate,
                ..
            })) => seen.twcc_estimates.push(estimate.as_u64()),
            Output::Event(Event::EgressBitrateEstimate(
                str0m::bwe::BweKind::ReceiverEstimate { ssrcs, estimate },
            )) => {
                seen.remb.push((ssrcs, estimate.as_u64()));
            }
            Output::Event(Event::MediaData(data))
                if data.params.spec().codec == str0m::format::Codec::Vp8 =>
            {
                seen.video_frames += 1
            }
            Output::Event(_) => (),
            Output::Timeout(at) if at <= now && due < 15 => {
                due += 1;
                from.handle_input(Input::Timeout(now)).unwrap();
            }
            Output::Timeout(_) => return,
        }
    }
    panic!("output work did not settle");
}
fn pair(start: Instant) -> (Rtc, Rtc, str0m::media::Mid) {
    pair_with(start, peer(start), peer(start))
}
fn pair_with(start: Instant, mut a: Rtc, mut b: Rtc) -> (Rtc, Rtc, str0m::media::Mid) {
    let aa: SocketAddr = "127.0.0.1:9901".parse().unwrap();
    let ba: SocketAddr = "127.0.0.1:9902".parse().unwrap();
    a.add_local_candidate(Candidate::host(aa, "udp").unwrap());
    b.add_local_candidate(Candidate::host(ba, "udp").unwrap());
    let mut change = a.sdp_api();
    let mid = change.add_media(MediaKind::Video, Direction::SendRecv, None, None, None);
    let (offer, pending) = change.apply().unwrap();
    let answer = b.sdp_api().accept_offer(offer).unwrap();
    a.sdp_api().accept_answer(pending, answer).unwrap();
    for i in 0..1000 {
        let now = start + Duration::from_millis(i);
        a.handle_input(Input::Timeout(now)).unwrap();
        b.handle_input(Input::Timeout(now)).unwrap();
        pump(&mut a, &mut b, now, false, &mut Observed::default());
        pump(&mut b, &mut a, now, false, &mut Observed::default());
    }
    assert!(a.is_connected() && b.is_connected());
    (a, b, mid)
}
fn write_frame(rtc: &mut Rtc, mid: str0m::media::Mid, now: Instant, index: u64) {
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
fn tick(a: &mut Rtc, b: &mut Rtc, now: Instant, drop: bool, as_: &mut Observed, bs: &mut Observed) {
    a.handle_input(Input::Timeout(now)).unwrap();
    b.handle_input(Input::Timeout(now)).unwrap();
    pump(a, b, now, drop, as_);
    pump(b, a, now, false, bs);
}

// Load the real runtime owner, not a second implementation of its policy.
mod remote_bwe {
    pub use rtc_media::remote_bwe::*;
}
// Exact production owner has additional hooks exercised by library tests.
#[allow(dead_code)]
#[path = "../src/feedback.rs"]
mod feedback;
mod pion_gcc {
    pub use rtc_media::pion_gcc::*;
}

#[test]
fn real_srtcp_remb_drives_remote_policy_while_media_is_abs_without_outgoing_twcc() {
    use rtc_core::config::CongestionControl;
    let start = Instant::now();
    let (mut a, mut b, mid) = pair(start);
    a.direct_api().set_send_transport_cc(false);
    a.direct_api().set_remote_feedback_meter(true);
    let mut owner = feedback::Feedback::new(&CongestionControl::default(), start);
    owner.update_tracks(vec![mid], 1_000_000);
    assert_eq!(owner.capacity(), 100_000_000); // source optimistic policy, not measurement.
    let (mut seen, mut reverse) = (Observed::default(), Observed::default());
    let mut reports = 0;
    for ms in 1000..4000 {
        let now = start + Duration::from_millis(ms);
        if ms % 20 == 0 {
            write_frame(&mut a, mid, now, ms);
        }
        if ms >= 1200 && ms % 100 == 0 {
            let value = 800_000u64
                .saturating_sub(((ms - 1200) / 100) * 20_000)
                .max(100_000);
            // Real receiver API serializes/encrypts RTCP. No Egress event is fabricated.
            b.direct_api()
                .stream_rx_by_mid(mid, None)
                .expect("actual received RTP source")
                .request_remb(Bitrate::bps(value));
        }
        tick(&mut a, &mut b, now, false, &mut seen, &mut reverse);
        for (ssrcs, estimate) in seen.remb.drain(..) {
            let stats = a.direct_api().remote_feedback_stats();
            if owner.accept_remb(&ssrcs, &stats) {
                assert!(!stats.is_empty());
                owner.estimate(estimate, &stats, &mut a, now);
                reports += 1;
            }
        }
    }
    assert!(reports >= 12, "real reports={reports}");
    assert!(
        owner.capacity() < 1_000_000,
        "actual REMB trend must reach source capacity policy"
    );
    assert!(
        reverse.video_frames >= 100,
        "pass-through video remains real media"
    );
    assert!(
        seen.headers
            .iter()
            .all(|h| h.ext_vals.transport_cc.is_none())
    );
    assert!(
        seen.headers
            .iter()
            .any(|h| h.ext_vals.abs_send_time.is_some())
    );
    assert!(
        a.bwe().snapshot().is_none(),
        "default policy must not enable GCC"
    );
    assert!(
        seen.wire_headers
            .iter()
            .all(|h| !h.padding && !h.extension_ids.contains(&3))
    );
    assert!(
        seen.wire_headers
            .iter()
            .any(|h| h.extension_ids.contains(&2) && h.ssrc != 0)
    );
    assert_eq!(a.direct_api().receiver_estimate_drops(), 0);
    let totals = a.direct_api().remote_feedback_stats();
    assert!(
        totals
            .iter()
            .any(|s| s.packets_primary >= 100 && s.bytes_primary > 0)
    );
    // Exercise the actual runtime prober, not only the padding executor. All
    // estimates still arrive through real encrypted RTCP from this receiver.
    let mut started = false;
    for ms in 4000..14000 {
        let now = start + Duration::from_millis(ms);
        if ms % 20 == 0 {
            write_frame(&mut a, mid, now, ms);
        }
        if ms % 100 == 0 {
            b.direct_api()
                .stream_rx_by_mid(mid, None)
                .unwrap()
                .request_remb(Bitrate::bps(800_000));
        }
        if !started {
            started = owner.maybe_start(Some(1_200_000), true, false, &mut a, now);
        }
        owner.service(&mut a, true, now);
        tick(&mut a, &mut b, now, false, &mut seen, &mut reverse);
        for (ssrcs, estimate) in seen.remb.drain(..) {
            let stats = a.direct_api().remote_feedback_stats();
            if owner.accept_remb(&ssrcs, &stats) {
                owner.estimate(estimate, &stats, &mut a, now);
            }
        }
    }
    assert!(
        started,
        "source regulator eventually authorizes actual exploration"
    );
    assert!(
        !seen.padding.is_empty(),
        "source uniform scheduler emits actual SRTP padding"
    );
    assert!(
        a.bwe().snapshot().is_none(),
        "runtime exploration never enables native GCC"
    );
}

#[test]
fn remote_burst_uses_real_budget_without_timer_per_packet_and_preserves_duplex_media() {
    let start = Instant::now();
    let (mut a, mut b, mid) = pair(start);
    a.direct_api().set_send_transport_cc(false);
    a.direct_api().set_remote_feedback_meter(true);
    a.direct_api().enable_twcc_feedback();
    let (mut seen, mut reverse) = (Observed::default(), Observed::default());
    for ms in 1000..1100 {
        let now = start + Duration::from_millis(ms);
        if ms % 20 == 0 {
            write_frame(&mut a, mid, now, ms);
            write_frame(&mut b, mid, now, ms);
        }
        tick(&mut a, &mut b, now, false, &mut seen, &mut reverse);
    }
    let now = start + Duration::from_millis(1100);
    let before = seen.padding.len();
    let id = a
        .direct_api()
        .request_padding_burst(4096, Duration::from_millis(100), now)
        .unwrap();
    tick(&mut a, &mut b, now, false, &mut seen, &mut reverse);
    let emitted = &seen.padding[before..];
    assert!(!emitted.is_empty() && emitted.len() <= 16);
    assert!(
        emitted.iter().all(|(at, _)| *at == now),
        "same-wake authorized burst"
    );
    assert!(emitted.iter().map(|(_, bytes)| *bytes).sum::<usize>() <= 4096);
    let o = a.direct_api().padding_status().unwrap();
    assert_eq!(o.id, id);
    assert_eq!(o.mode, str0m::PaddingMode::Burst);
    assert_eq!(o.status, str0m::PaddingStatus::Completed);
    assert!(!a.direct_api().cancel_padding(id));
    let new_now = now + Duration::from_millis(1);
    let new = a
        .direct_api()
        .request_padding_burst(1024, Duration::from_millis(100), new_now)
        .unwrap();
    assert!(new > id);
    assert!(!a.direct_api().cancel_padding(id));
    assert!(a.direct_api().cancel_padding(new));
    let before = seen.padding.len();
    for ms in 1101..1500 {
        let now = start + Duration::from_millis(ms);
        if ms % 20 == 0 {
            write_frame(&mut a, mid, now, ms);
            write_frame(&mut b, mid, now, ms);
        }
        tick(&mut a, &mut b, now, false, &mut seen, &mut reverse);
    }
    assert_eq!(
        seen.padding.len(),
        before,
        "cancelled generation emits no padding"
    );
    assert!(
        seen.video_frames >= 20 && reverse.video_frames >= 20,
        "both actual encrypted video streams continue"
    );
    assert!(
        seen.incoming_twcc_headers > 0 && seen.outgoing_twcc_feedback > 0,
        "publisher feedback stays independent"
    );
    assert!(
        seen.headers
            .iter()
            .all(|header| header.ext_vals.transport_cc.is_none())
    );
    assert!(a.bwe().snapshot().is_none());
}

#[test]
fn interceptor_actual_twcc_drives_remote_owner_without_native_media_pacer() {
    use rtc_core::config::CongestionControl;
    let start = Instant::now();
    let mut a = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .enable_raw_packets(true)
        .build(start);
    a.direct_api().set_send_side_feedback(true);
    a.direct_api().set_remote_feedback_meter(true);
    a.direct_api().set_send_transport_cc(true);
    a.bwe().set_automatic_probing(false);
    let mut b = peer(start);
    b.direct_api().enable_twcc_feedback();
    let (mut a, mut b, mid) = pair_with(start, a, b);
    assert!(
        a.bwe().snapshot().is_none(),
        "native estimator is not enabled for Pion interceptor"
    );
    assert_eq!(a.bwe().configured_pacing_rate(), None);
    let config = CongestionControl {
        use_send_side_bwe_interceptor: true,
        ..Default::default()
    };
    let mut owner = feedback::Feedback::new(&config, start);
    owner.update_tracks(vec![mid], 200_000);
    let (mut seen, mut reverse) = (Observed::default(), Observed::default());
    // A syntactically shaped but unauthenticated RTCP report from the actual
    // peer tuple must never become an application-estimator observation.
    let clear = [
        0x8f, 205, 0, 5, 0, 0, 0, 1, 0, 0, 0, 2, 0, 1, 0, 1, 0, 0, 1, 0, 0x20, 1, 0, 0,
    ];
    let rejected = start + Duration::from_millis(1000);
    a.handle_input(Input::Receive(
        rejected,
        Receive::new(
            Protocol::Udp,
            "127.0.0.1:9902".parse().unwrap(),
            "127.0.0.1:9901".parse().unwrap(),
            &clear,
        )
        .unwrap(),
    ))
    .unwrap();
    pump(&mut a, &mut b, rejected, false, &mut seen);
    assert!(
        !seen
            .send_side
            .iter()
            .any(|e| matches!(e, str0m::SendSideFeedback::Report { .. })),
        "raw unprotected bytes are not source feedback"
    );
    let mut actual_estimates = 0;
    let mut actual_callbacks = 0;
    for ms in 1000..3500 {
        let now = start + Duration::from_millis(ms);
        if ms % 20 == 0 {
            write_frame(&mut a, mid, now, ms);
        }
        tick(&mut a, &mut b, now, false, &mut seen, &mut reverse);
        for event in seen.send_side.drain(..) {
            if let str0m::SendSideFeedback::Sent(packet) = &event {
                let plain = seen
                    .plain_components
                    .iter()
                    .find(|p| p.ssrc == packet.ssrc && p.sequence == packet.sequence)
                    .expect("actual emitted plaintext RTP metadata");
                let wire = seen
                    .wire_headers
                    .iter()
                    .find(|p| p.ssrc == packet.ssrc && p.sequence == packet.sequence)
                    .expect("actual emitted SRTP header");
                assert_eq!(packet.header_bytes, plain.header_bytes);
                assert_eq!(packet.header_bytes, wire.header_bytes);
                assert_eq!(packet.payload_bytes, plain.payload_bytes);
            }
            let (receipt, changes) = owner.interceptor_input(event).unwrap();
            for estimate in changes {
                let stats = a.direct_api().remote_feedback_stats();
                if owner.interceptor_estimate_changed(estimate) {
                    owner.estimate(estimate, &stats, &mut a, receipt);
                    actual_callbacks += 1;
                }
                actual_estimates += 1;
            }
        }
        assert!(
            seen.twcc_estimates.is_empty(),
            "native estimate cannot act as Pion callback"
        );
        assert_eq!(a.bwe().configured_pacing_rate(), None);
    }
    assert!(actual_estimates > 0, "real TWCC estimator events");
    assert!(
        actual_callbacks > 0,
        "actual changed TWCC target reaches Remote policy callback"
    );
    assert!(a.bwe().snapshot().is_none());
    let actual = owner.interceptor_stats().unwrap();
    assert!(
        actual.sent_packets > 100 && actual.feedback_packets > 0 && actual.matched_received > 0
    );
    assert_eq!(actual.callbacks, actual_estimates);
    assert!(reverse.outgoing_twcc_feedback > 0);
    assert!(
        reverse.video_frames >= 100,
        "actual pass-through VP8 remains supplied"
    );
    assert!(
        seen.headers
            .iter()
            .any(|h| h.ext_vals.transport_cc.is_some())
    );
    assert!(
        a.bwe()
            .start_validation(Bitrate::bps(1_000_000), start + Duration::from_millis(3500))
            .is_none()
    );
    assert!(
        a.bwe()
            .start_validation_with_pacing(
                Bitrate::bps(1_000_000),
                Bitrate::bps(1_200_000),
                start + Duration::from_millis(3500)
            )
            .is_none()
    );
    let now = start + Duration::from_millis(3500);
    a.direct_api()
        .request_padding_burst(4096, Duration::from_millis(100), now)
        .unwrap();
    let before = seen.padding.len();
    tick(&mut a, &mut b, now, false, &mut seen, &mut reverse);
    assert!(
        seen.padding.len() > before,
        "independent remote execution remains available"
    );
    assert_eq!(a.bwe().configured_pacing_rate(), None);
}

#[test]
fn real_negotiated_audio_omits_source_feedback_extensions_in_offer_and_answer_without_affecting_video()
 {
    use str0m::rtp::Extension;
    for native in [false, true] {
        for restricted_offerer in [false, true] {
            let start = Instant::now();
            let mut cfg = Rtc::builder()
                .set_crypto_provider(rtc_crypto::provider().into())
                .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
                .enable_raw_packets(true);
            if !native {
                cfg.extension_map().set_media_applicability(
                    Extension::TransportSequenceNumber,
                    false,
                    true,
                );
                cfg.extension_map().set_media_applicability(
                    Extension::AbsoluteSendTime,
                    false,
                    true,
                );
            }
            let mut restricted = cfg.build(start);
            restricted.direct_api().set_send_side_feedback(true);
            let other = peer(start);
            let (mut a, mut b) = if restricted_offerer {
                (restricted, other)
            } else {
                (other, restricted)
            };
            a.add_local_candidate(
                Candidate::host("127.0.0.1:9901".parse().unwrap(), "udp").unwrap(),
            );
            b.add_local_candidate(
                Candidate::host("127.0.0.1:9902".parse().unwrap(), "udp").unwrap(),
            );
            let mut change = a.sdp_api();
            let audio = change.add_media(MediaKind::Audio, Direction::SendRecv, None, None, None);
            let video = change.add_media(MediaKind::Video, Direction::SendRecv, None, None, None);
            let (offer, pending) = change.apply().unwrap();
            let answer = b.sdp_api().accept_offer(offer).unwrap();
            a.sdp_api().accept_answer(pending, answer).unwrap();
            let (mut left, mut right) = (Observed::default(), Observed::default());
            for ms in 0..1000 {
                let now = start + Duration::from_millis(ms);
                tick(&mut a, &mut b, now, false, &mut left, &mut right);
            }
            assert!(a.is_connected() && b.is_connected());
            for ms in 1000..1500 {
                let now = start + Duration::from_millis(ms);
                let sender = if restricted_offerer { &mut a } else { &mut b };
                if ms % 20 == 0 {
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
                            MediaTime::new(ms * 48, Frequency::FORTY_EIGHT_KHZ),
                            vec![0xf8, 0xff, 0xfe],
                        )
                        .unwrap();
                    write_frame(sender, video, now, ms);
                }
                tick(&mut a, &mut b, now, false, &mut left, &mut right);
            }
            let seen = if restricted_offerer { &left } else { &right };
            let mut audio_packets = 0;
            let mut video_packets = 0;
            for event in &seen.send_side {
                if let str0m::SendSideFeedback::Sent(packet) = event {
                    let audio = packet.payload_bytes == 3;
                    let header = seen
                        .headers
                        .iter()
                        .find(|h| *h.ssrc == packet.ssrc && h.sequence_number == packet.sequence)
                        .unwrap();
                    let wire = seen
                        .wire_headers
                        .iter()
                        .find(|h| h.ssrc == packet.ssrc && h.sequence == packet.sequence)
                        .unwrap();
                    let plain = seen
                        .plain_components
                        .iter()
                        .find(|p| p.ssrc == packet.ssrc && p.sequence == packet.sequence)
                        .unwrap();
                    assert_eq!(packet.header_bytes, wire.header_bytes);
                    assert_eq!(packet.payload_bytes, plain.payload_bytes);
                    if audio {
                        audio_packets += 1;
                        assert_eq!(packet.twcc_negotiated, native);
                        assert_eq!(packet.transport_sequence.is_some(), native);
                        assert_eq!(header.ext_vals.transport_cc.is_some(), native);
                        if !native {
                            assert!(header.ext_vals.abs_send_time.is_none());
                        }
                    } else {
                        video_packets += 1;
                        assert!(packet.twcc_negotiated && packet.transport_sequence.is_some());
                        assert!(header.ext_vals.transport_cc.is_some());
                    }
                }
            }
            assert!(
                audio_packets >= 20 && video_packets >= 20,
                "actual encrypted audio/video: {audio_packets}/{video_packets}"
            );
        }
    }
}
