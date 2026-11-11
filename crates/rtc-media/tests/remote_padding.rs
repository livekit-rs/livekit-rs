//! Real encrypted execution of independent padding; never a capacity proof.
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use str0m::bwe::Bitrate;
use str0m::media::{Direction, Frequency, MediaKind, MediaTime};
use str0m::net::{Protocol, Receive};
use str0m::rtp::{RawPacket, RtpHeader};
use str0m::{Candidate, Event, Input, Output, PaddingRequestError, PaddingStatus, Rtc};

mod rtp_ {
    pub use str0m::bwe::Bitrate;
}
#[allow(dead_code)]
#[path = "../../../vendor/str0m/src/remote_padding.rs"]
mod exact_executor;

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
}
// Independently inspect only the clear RTP header in the actual SRTP datagram.
// Payload is encrypted and is neither parsed nor used to invent receive proof.
fn wire_header(bytes: &[u8]) -> WireHeader {
    assert!(bytes.len() >= 12 && bytes[0] >> 6 == 2);
    let mut ids = vec![];
    let offset = 12 + (bytes[0] as usize & 15) * 4;
    if bytes[0] & 0x10 != 0 {
        assert!(offset + 4 <= bytes.len());
        let profile = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
        let size = u16::from_be_bytes([bytes[offset + 2], bytes[offset + 3]]) as usize * 4;
        let ext = &bytes[offset + 4..offset + 4 + size];
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
    }
}
#[derive(Default)]
struct Observed {
    headers: Vec<RtpHeader>,
    wire_headers: Vec<WireHeader>,
    incoming_headers: Vec<RtpHeader>,
    padding: Vec<(Instant, usize)>,
    video_frames: usize,
    incoming_twcc_headers: usize,
    outgoing_twcc_feedback: usize,
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
            Output::Event(Event::RawPacket(p)) => match *p {
                RawPacket::RtpTx(header, _) => seen.headers.push(header),
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
    let (mut a, mut b) = (peer(start), peer(start));
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
#[test]
fn actual_remote_padding_is_bounded_rtx_without_native_bwe_or_fake_receipt() {
    let start = Instant::now();
    let (mut a, mut b, mid) = pair(start);
    let now = start + Duration::from_secs(1);
    write_frame(&mut a, mid, now, 1000);
    let (mut seen, mut reverse) = (Observed::default(), Observed::default());
    for i in 0..100 {
        tick(
            &mut a,
            &mut b,
            now + Duration::from_millis(i),
            false,
            &mut seen,
            &mut reverse,
        );
    }
    assert!(reverse.video_frames > 0);
    let rtx = a
        .direct_api()
        .stream_tx_by_mid(mid, None)
        .unwrap()
        .rtx()
        .expect("negotiated RTX source");
    assert!(a.bwe().snapshot().is_none());
    let request_at = now + Duration::from_millis(100);
    let id = a
        .direct_api()
        .request_padding(
            5000,
            Bitrate::bps(100_000),
            Duration::from_millis(500),
            request_at,
        )
        .unwrap();
    assert_eq!(a.direct_api().padding_status().unwrap().sent_packets, 0);
    seen.padding.clear();
    seen.headers.clear();
    seen.wire_headers.clear();
    // Deliberately withhold every emitted RTP datagram: local execution must
    // remain distinct from remote receipt even when the complete request ends.
    for i in 0..600 {
        tick(
            &mut a,
            &mut b,
            request_at + Duration::from_millis(i),
            true,
            &mut seen,
            &mut reverse,
        );
    }
    let status = a.direct_api().padding_status().unwrap();
    assert_eq!(status.id, id);
    assert_eq!(status.first_output_at, seen.padding.first().map(|p| p.0));
    assert_eq!(status.last_output_at, seen.padding.last().map(|p| p.0));
    assert!(matches!(
        status.status,
        PaddingStatus::Completed | PaddingStatus::Expired
    ));
    assert!(status.sent_packets > 0);
    assert_eq!(status.sent_packets as usize, seen.padding.len());
    assert_eq!(
        status.sent_protected_bytes as usize,
        seen.padding.iter().map(|x| x.1).sum::<usize>()
    );
    assert!(status.sent_protected_bytes <= 5000);
    eprintln!(
        "independent padding: actualWire={} wireP={} rawMetadataP={} wireRtxMatch={}",
        seen.wire_headers.len(),
        seen.wire_headers.iter().filter(|h| h.padding).count(),
        seen.headers.iter().filter(|h| h.has_padding).count(),
        seen.wire_headers.iter().filter(|h| h.ssrc == *rtx).count()
    );
    assert_eq!(seen.wire_headers.len(), seen.headers.len());
    assert!(
        seen.wire_headers
            .iter()
            .all(|h| h.padding && h.ssrc == *rtx)
    );
    assert!(seen.headers.iter().all(|h| h.has_padding && h.ssrc == rtx));
    for (i, (at, _)) in seen.padding.iter().enumerate() {
        let bytes: usize = seen.padding[..=i].iter().map(|x| x.1).sum();
        assert!(
            (bytes as u128) * 8 * 1_000_000_000
                <= 100_000u128 * at.duration_since(request_at).as_nanos()
                    + 512 * 8 * 1_000_000_000u128
        );
    }
    assert!(a.bwe().snapshot().is_none());
}
#[test]
fn actual_cancel_identity_and_expiry_preserve_queued_media() {
    let start = Instant::now();
    let (mut a, mut b, mid) = pair(start);
    let now = start + Duration::from_secs(1);
    write_frame(&mut a, mid, now, 1000);
    let (mut seen, mut reverse) = (Observed::default(), Observed::default());
    for i in 0..100 {
        tick(
            &mut a,
            &mut b,
            now + Duration::from_millis(i),
            false,
            &mut seen,
            &mut reverse,
        );
    }
    let at = now + Duration::from_millis(100);
    let first = a
        .direct_api()
        .request_padding(
            100_000,
            Bitrate::bps(100_000),
            Duration::from_millis(100),
            at,
        )
        .unwrap();
    write_frame(&mut a, mid, at, 1100); // Real media remains queued across cancellation.
    assert!(a.direct_api().cancel_padding(first));
    assert!(!a.direct_api().cancel_padding(first));
    assert!(
        a.direct_api()
            .padding_status()
            .unwrap()
            .first_output_at
            .is_none()
    );
    assert!(
        a.direct_api()
            .padding_status()
            .unwrap()
            .last_output_at
            .is_none()
    );
    let before = reverse.video_frames;
    for i in 0..100 {
        tick(
            &mut a,
            &mut b,
            at + Duration::from_millis(i),
            false,
            &mut seen,
            &mut reverse,
        );
    }
    assert!(reverse.video_frames > before);
    let newer = a
        .direct_api()
        .request_padding(
            100_000,
            Bitrate::bps(100_000),
            Duration::from_millis(50),
            at + Duration::from_millis(100),
        )
        .unwrap();
    assert!(newer > first);
    assert!(!a.direct_api().cancel_padding(first));
    for i in 100..300 {
        tick(
            &mut a,
            &mut b,
            at + Duration::from_millis(i),
            false,
            &mut seen,
            &mut reverse,
        );
    }
    assert_eq!(
        a.direct_api().padding_status().unwrap().status,
        PaddingStatus::Expired
    );
    let count = seen.padding.len();
    for i in 300..400 {
        tick(
            &mut a,
            &mut b,
            at + Duration::from_millis(i),
            false,
            &mut seen,
            &mut reverse,
        );
    }
    assert_eq!(seen.padding.len(), count);
}
#[test]
fn outgoing_twcc_gate_keeps_abs_and_actual_incoming_feedback() {
    let start = Instant::now();
    let (mut a, mut b, mid) = pair(start);
    let now = start + Duration::from_secs(1);
    a.direct_api().set_send_transport_cc(false);
    write_frame(&mut a, mid, now, 1000);
    write_frame(&mut b, mid, now, 1000);
    let (mut seen, mut reverse) = (Observed::default(), Observed::default());
    for i in 0..300 {
        tick(
            &mut a,
            &mut b,
            now + Duration::from_millis(i),
            false,
            &mut seen,
            &mut reverse,
        );
    }
    assert!(!seen.headers.is_empty() && !reverse.headers.is_empty());
    assert!(
        seen.headers
            .iter()
            .all(|h| h.ext_vals.transport_cc.is_none() && h.ext_vals.abs_send_time.is_some())
    );
    assert!(
        reverse
            .headers
            .iter()
            .any(|h| h.ext_vals.transport_cc.is_some())
    );
    assert!(
        seen.incoming_twcc_headers > 0,
        "actual incoming RTP retains transport sequence"
    );
    assert!(
        seen.outgoing_twcc_feedback > 0,
        "actual receiver TWCC feedback remains enabled"
    );
    let at = now + Duration::from_millis(300);
    a.direct_api()
        .request_padding(5000, Bitrate::bps(100_000), Duration::from_millis(100), at)
        .unwrap();
    seen.headers.clear();
    seen.wire_headers.clear();
    for i in 0..200 {
        tick(
            &mut a,
            &mut b,
            at + Duration::from_millis(i),
            false,
            &mut seen,
            &mut reverse,
        );
    }
    assert!(!seen.headers.is_empty());
    let map = a.media(mid).unwrap().remote_extmap();
    let abs = map.id_of(str0m::rtp::Extension::AbsoluteSendTime).unwrap();
    let cc = map
        .id_of(str0m::rtp::Extension::TransportSequenceNumber)
        .unwrap();
    eprintln!(
        "gated padding: wire={} wireP={} metadataP={} actualAbs={} actualTwcc={}",
        seen.wire_headers.len(),
        seen.wire_headers.iter().filter(|h| h.padding).count(),
        seen.headers.iter().filter(|h| h.has_padding).count(),
        seen.wire_headers
            .iter()
            .filter(|h| h.extension_ids.contains(&abs))
            .count(),
        seen.wire_headers
            .iter()
            .filter(|h| h.extension_ids.contains(&cc))
            .count()
    );
    assert_eq!(seen.wire_headers.len(), seen.headers.len());
    assert!(
        seen.wire_headers
            .iter()
            .all(|h| h.padding && h.extension_ids.contains(&abs) && !h.extension_ids.contains(&cc))
    );
    assert!(seen.headers.iter().all(|h| h.has_padding
        && h.ext_vals.transport_cc.is_none()
        && h.ext_vals.abs_send_time.is_some()));
    a.direct_api().set_send_transport_cc(true);
    seen.headers.clear();
    seen.wire_headers.clear();
    write_frame(&mut a, mid, at + Duration::from_millis(200), 1500);
    for i in 200..300 {
        tick(
            &mut a,
            &mut b,
            at + Duration::from_millis(i),
            false,
            &mut seen,
            &mut reverse,
        );
    }
    assert!(
        seen.headers
            .iter()
            .any(|h| h.ext_vals.transport_cc.is_some())
    );
}
#[test]
fn padding_rejects_unready_and_native_bwe_without_enabling_or_mutating_it() {
    let now = Instant::now();
    let mut rtc = peer(now);
    assert_eq!(
        rtc.direct_api()
            .request_padding(5000, Bitrate::bps(100_000), Duration::from_secs(1), now),
        Err(PaddingRequestError::NotReady)
    );
    let mut native = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .enable_bwe(Some(Bitrate::bps(400_000)))
        .build(now);
    assert_eq!(
        native.direct_api().request_padding(
            5000,
            Bitrate::bps(100_000),
            Duration::from_secs(1),
            now
        ),
        Err(PaddingRequestError::NativeBweEnabled)
    );
    assert!(
        rtc.direct_api().padding_status().is_none()
            && native.direct_api().padding_status().is_none()
    );
}

#[test]
fn actual_missing_rtx_source_is_explicitly_rejected() {
    let start = Instant::now();
    let (mut sender, _receiver, mid) = pair(start);
    if let Some(stream) = sender.direct_api().stream_tx_by_mid(mid, None) {
        let ssrc = stream.ssrc();
        assert!(sender.direct_api().remove_stream_tx(ssrc));
    }
    assert_eq!(
        sender.direct_api().request_padding(
            5000,
            Bitrate::bps(100_000),
            Duration::from_secs(1),
            start + Duration::from_secs(1)
        ),
        Err(PaddingRequestError::NoSource)
    );
    assert!(sender.direct_api().padding_status().is_none());
}

#[test]
fn connected_invalid_and_busy_requests_do_not_replace_live_identity() {
    let start = Instant::now();
    let (mut sender, mut receiver, mid) = pair(start);
    let now = start + Duration::from_secs(1);
    write_frame(&mut sender, mid, now, 1000);
    for i in 0..100 {
        tick(
            &mut sender,
            &mut receiver,
            now + Duration::from_millis(i),
            false,
            &mut Observed::default(),
            &mut Observed::default(),
        );
    }
    let at = now + Duration::from_millis(100);
    let id = sender
        .direct_api()
        .request_padding(5000, Bitrate::bps(100_000), Duration::from_secs(1), at)
        .unwrap();
    assert_eq!(
        sender
            .direct_api()
            .request_padding(511, Bitrate::bps(100_000), Duration::from_secs(1), at),
        Err(PaddingRequestError::InvalidLimits)
    );
    assert_eq!(
        sender.direct_api().request_padding(
            5000,
            Bitrate::bps(100_000),
            Duration::from_secs(1),
            at
        ),
        Err(PaddingRequestError::Busy)
    );
    let observation = sender.direct_api().padding_status().unwrap();
    assert_eq!(observation.id, id);
    assert_eq!(observation.sent_packets, 0);
    assert!(sender.direct_api().cancel_padding(id));
}

#[test]
fn actual_expired_before_first_poll_never_emits_authorized_padding() {
    let start = Instant::now();
    let (mut sender, mut receiver, mid) = pair(start);
    let now = start + Duration::from_secs(1);
    write_frame(&mut sender, mid, now, 1000);
    for i in 0..100 {
        tick(
            &mut sender,
            &mut receiver,
            now + Duration::from_millis(i),
            false,
            &mut Observed::default(),
            &mut Observed::default(),
        );
    }
    let at = now + Duration::from_millis(100);
    sender
        .direct_api()
        .request_padding(5000, Bitrate::bps(100_000), Duration::from_millis(20), at)
        .unwrap();
    let mut actual = Observed::default();
    tick(
        &mut sender,
        &mut receiver,
        at + Duration::from_millis(100),
        false,
        &mut actual,
        &mut Observed::default(),
    );
    assert_eq!(
        sender.direct_api().padding_status().unwrap().status,
        PaddingStatus::Expired
    );
    assert_eq!(
        sender.direct_api().padding_status().unwrap().sent_packets,
        0
    );
    assert!(actual.padding.is_empty());
    assert!(
        sender
            .direct_api()
            .padding_status()
            .unwrap()
            .first_output_at
            .is_none()
    );
    assert!(
        sender
            .direct_api()
            .padding_status()
            .unwrap()
            .last_output_at
            .is_none()
    );
}

#[test]
fn actual_long_scheduler_gaps_never_release_padding_catchup_bursts() {
    let start = Instant::now();
    let (mut sender, mut receiver, mid) = pair(start);
    let now = start + Duration::from_secs(1);
    write_frame(&mut sender, mid, now, 1000);
    for i in 0..100 {
        tick(
            &mut sender,
            &mut receiver,
            now + Duration::from_millis(i),
            false,
            &mut Observed::default(),
            &mut Observed::default(),
        );
    }
    let at = now + Duration::from_millis(100);
    sender
        .direct_api()
        .request_padding(
            100_000,
            Bitrate::bps(1_000_000),
            Duration::from_millis(450),
            at,
        )
        .unwrap();
    let mut observed = Observed::default();
    for i in 0..5 {
        let before = observed.padding.len();
        tick(
            &mut sender,
            &mut receiver,
            at + Duration::from_millis(i * 100),
            false,
            &mut observed,
            &mut Observed::default(),
        );
        assert_eq!(
            observed.padding.len() - before,
            1,
            "one actual packet at each wake; no accrued send credit"
        );
    }
    tick(
        &mut sender,
        &mut receiver,
        at + Duration::from_millis(500),
        false,
        &mut observed,
        &mut Observed::default(),
    );
    assert_eq!(
        sender.direct_api().padding_status().unwrap().status,
        PaddingStatus::Expired
    );
    assert_eq!(observed.padding.len(), 5);
    assert_eq!(
        sender.direct_api().padding_status().unwrap().sent_packets,
        5
    );
}

#[test]
fn actual_inherited_transport_sequence_cannot_leak_through_disabled_send_gate() {
    use str0m::rtp::{ExtensionValues, RtpWrite};
    let start = Instant::now();
    let (mut sender, mut receiver, mid) = pair(start);
    let now = start + Duration::from_secs(1);
    let pt = sender
        .writer(mid)
        .unwrap()
        .payload_params()
        .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
        .unwrap()
        .pt();
    sender.direct_api().set_send_transport_cc(false);
    let ext = ExtensionValues {
        transport_cc: Some(54_321),
        ..Default::default()
    };
    let payload = vec![0x10, 0x00, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1];
    sender
        .direct_api()
        .stream_tx_by_mid(mid, None)
        .unwrap()
        .write_rtp(
            RtpWrite::new(pt, 100.into(), 90_000, now, payload)
                .marker(true)
                .nackable(true)
                .ext_vals(ext),
        );
    let (mut outgoing, mut incoming) = (Observed::default(), Observed::default());
    for i in 0..100 {
        tick(
            &mut sender,
            &mut receiver,
            now + Duration::from_millis(i),
            false,
            &mut outgoing,
            &mut incoming,
        );
    }
    assert!(!outgoing.headers.is_empty() && !incoming.incoming_headers.is_empty());
    assert!(
        outgoing
            .headers
            .iter()
            .all(|h| h.ext_vals.transport_cc.is_none() && h.ext_vals.abs_send_time.is_some())
    );
    assert!(
        incoming
            .incoming_headers
            .iter()
            .all(|h| h.ext_vals.transport_cc.is_none() && h.ext_vals.abs_send_time.is_some()),
        "real decrypted receive header cannot retain inherited SN"
    );
}
