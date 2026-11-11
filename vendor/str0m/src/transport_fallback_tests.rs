use super::*;
use crate::streams::RtpWrite;

#[test]
fn actual_authenticated_rr_loss_survives_missing_lsr_and_rtt_no_change_but_not_auth_unknown_or_rtx()
{
    let start = Instant::now();
    let crypto = crate::crypto::test_default_provider();
    let profile = SrtpProfile::AES128_CM_SHA1_80;
    let material = vec![0x41; profile.keying_material_len()];
    let key = KeyingMaterial::new(&material);
    let mut peer = SrtpContext::new(crypto, profile, &key, true);
    let mut session = Session::new(&crate::RtcConfig::default());
    session.receiver_report_rtt_enabled = true;
    session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
    let params = session.codec_config.params().to_vec();
    let pt = params
        .iter()
        .find(|p| p.spec().codec == crate::format::Codec::Vp8)
        .unwrap()
        .pt();
    let codec = session.codec_config.clone();
    let media = Media::from_direct_api(
        "v".into(),
        0,
        crate::media::MediaKind::Video,
        ExtensionMap::empty(),
    );
    let stream =
        session
            .streams
            .declare_stream_tx(101.into(), Some(201.into()), MidRid("v".into(), None));
    stream.write_rtp(RtpWrite::new(pt, 100.into(), 3000, start, vec![0x10, 0, 1]));
    stream.handle_timeout(start, || (&media, &codec));
    let mut wire = Vec::new();
    let packet = stream
        .poll_packet(start, &ExtensionMap::empty(), None, &params, &mut wire)
        .expect("real primary RTP write/poll");
    let mut primary = SrtpContext::new(crypto, profile, &key, false);
    assert!(primary.protect_rtp(&wire, &packet.header, 100).len() > wire.len());
    let source = stream.receiver_report_source_id();
    let send = |session: &mut Session,
                peer: &mut SrtpContext,
                ssrc: u32,
                loss: u8,
                bad_auth: bool,
                at: Instant| {
        let mut bytes = vec![0u8; 32];
        bytes[..4].copy_from_slice(&[0x81, 201, 0, 7]);
        bytes[4..8].copy_from_slice(&999u32.to_be_bytes());
        bytes[8..12].copy_from_slice(&ssrc.to_be_bytes());
        bytes[12] = loss;
        bytes[16..20].copy_from_slice(&100u32.to_be_bytes());
        // No LSR/DLSR: this is valid loss feedback, never a fabricated RTT.
        let mut encrypted = peer.protect_rtcp(&bytes);
        if bad_auth {
            *encrypted.last_mut().unwrap() ^= 1;
        }
        session.handle_rtcp_receive(at, &encrypted);
    };
    for (ssrc, loss, bad) in [(101, 250, true), (999, 250, false), (201, 250, false)] {
        send(&mut session, &mut peer, ssrc, loss, bad, start);
        assert!(session.receiver_report_loss.is_empty());
        assert!(session.receiver_report_rtt.is_empty());
    }
    for (offset, loss) in [(1, 70), (2, 90), (3, 0)] {
        let at = start + Duration::from_millis(offset);
        send(&mut session, &mut peer, 101, loss, false, at);
        let sample = session
            .receiver_report_loss
            .pop_front()
            .expect("actual authenticated primary RR");
        assert_eq!(sample.source_id, source);
        assert_eq!(sample.ssrc, 101);
        assert_eq!(sample.fraction_lost, loss);
        assert_eq!(sample.received_at, at);
        assert!(
            session.receiver_report_rtt.is_empty(),
            "no-LSR/unchanged RTT cannot erase independently emitted loss"
        );
        assert!(session.receiver_report_loss.is_empty());
    }
    // Actual encrypted RR message with two blocks for the SAME live primary.
    // Source DownTrack filters then MediaLossProxy takes their maximum before
    // its first/strict-one-second callback; do not emit the first low block.
    let mut report = vec![0u8; 56];
    report[..4].copy_from_slice(&[0x82, 201, 0, 13]);
    report[4..8].copy_from_slice(&999u32.to_be_bytes());
    for (offset, loss) in [(8, 5), (32, 90)] {
        report[offset..offset + 4].copy_from_slice(&101u32.to_be_bytes());
        report[offset + 4] = loss;
        report[offset + 8..offset + 12].copy_from_slice(&100u32.to_be_bytes());
    }
    session.handle_rtcp_receive(
        start + Duration::from_millis(4),
        &peer.protect_rtcp(&report),
    );
    assert_eq!(
        session
            .receiver_report_loss
            .pop_front()
            .unwrap()
            .fraction_lost,
        90
    );
    assert!(
        session.receiver_report_loss.is_empty(),
        "one genuine RR/primary produces exactly one max callback observation"
    );
    let changed = session
        .streams
        .stream_tx(&101.into())
        .unwrap()
        .reset_receiver_report_source();
    assert_ne!(changed, source);
    send(
        &mut session,
        &mut peer,
        101,
        99,
        false,
        start + Duration::from_secs(1),
    );
    assert_eq!(
        session.receiver_report_loss.pop_front().unwrap().source_id,
        changed
    );
}
