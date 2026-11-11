//! Go v1.13.10 never requests an upstream keyframe merely because an upstream
//! frame arrived with missing packets. Upstream PLIs come only from the buffer's
//! key-frame seeder, the down track's layer-lock requester and subscriber PLI/FIR
//! forwarding (`buffer_base.go:672`, `downtrack.go:978`, `downtrack.go:2079`).
use super::*;
use rtc_core::media_api::{DownTrackSpec, MediaCommand};
use rtc_core::model::TrackSettings;
use str0m::format::{Codec, CodecExtra, Vp8CodecExtra};
use str0m::media::{Frequency, MediaData, MediaTime};

#[test]
fn non_contiguous_upstream_frame_does_not_request_a_publisher_keyframe() {
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.enabled_codecs.push(CodecSpec {
        mime: "video/VP8".into(),
        fmtp_line: String::new(),
    });
    let publisher_tid = transport(&mut engine, PcRole::Publisher, start);
    engine
        .transports
        .get_mut(&publisher_tid)
        .unwrap()
        .spec
        .primary = false;
    let mut publisher = client(start);
    let events = offer(&mut engine, &mut publisher, publisher_tid, start, |api| {
        api.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    });
    let mid = published_mid(&events);
    let sid = TrackSid::from("TR_gap_pli");
    engine.bind_track(publisher_tid, &mid.to_string(), sid.clone(), start);
    engine.handle_command(
        MediaCommand::Subscribe(DownTrackSpec {
            subscriber: "PA_test".into(),
            publisher: "PA_pub".into(),
            track_sid: sid.clone(),
            stream_id: "PA_pub|TR_gap_pli".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        }),
        start,
    );
    let subscriber_tid = transport(&mut engine, PcRole::Subscriber, start);
    let negotiated = start + Duration::from_millis(200);
    engine.handle_timeout(negotiated);
    let mut subscriber = client(start);
    answer_server_offer(&mut engine, &mut subscriber, subscriber_tid, negotiated);
    let params = engine.transports[&publisher_tid]
        .rtc
        .codec_config()
        .params()
        .iter()
        .find(|p| p.spec().codec == Codec::Vp8)
        .copied()
        .unwrap();
    let frame = |seq: u64, keyframe: bool, contiguous: bool, now: Instant| MediaData {
        mid,
        pt: params.pt(),
        rid: None,
        params,
        time: MediaTime::new(seq * 3000, Frequency::NINETY_KHZ),
        network_time: now,
        seq_range: seq.into()..=seq.into(),
        contiguous,
        data: if keyframe {
            vec![0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1].into()
        } else {
            vec![0x11, 0, 0, 0, 0, 0, 0, 0, 0, 0].into()
        },
        ext_vals: Default::default(),
        codec_extra: CodecExtra::Vp8(Vp8CodecExtra {
            discardable: false,
            sync: false,
            layer_index: 0,
            picture_id: None,
            tl0_picture_id: None,
            is_keyframe: keyframe,
        }),
        last_sender_info: None,
        audio_start_of_talk_spurt: false,
    };
    let ui = engine.up_by_sid[&sid];
    // Lock the down track on a keyframe first; layer acquisition may ask for one.
    let t0 = negotiated + Duration::from_millis(10);
    engine.on_media_data(publisher_tid, frame(1, true, true, t0), t0);
    let t1 = t0 + Duration::from_millis(33);
    engine.on_media_data(publisher_tid, frame(2, false, true, t1), t1);
    engine.ups.get_mut(ui).unwrap().stats[0].clear_last_pli();
    // A later inter frame that str0m reports with missing packets.
    let t2 = t1 + Duration::from_secs(3);
    engine.on_media_data(publisher_tid, frame(5, false, false, t2), t2);
    assert_eq!(
        engine.ups.get(ui).unwrap().stats[0].last_pli(),
        None,
        "an upstream sequence gap alone must not trigger a publisher PLI"
    );
}
