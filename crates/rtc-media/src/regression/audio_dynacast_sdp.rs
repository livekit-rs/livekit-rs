//! Audio multi-codec subscription driven through the real SDP offer/answer
//! path, not by hand-built objects.
//!
//! The sibling unit file calls `notify_audio_codec_negotiated` directly, so it
//! stays green even when no negotiation path invokes it. These vectors feed
//! genuine str0m offers and answers into `MediaEngine::on_remote_description`,
//! the entry the signal connection uses, and assert the announcement leaves the
//! engine with the committed mime set. Upstream reaches the same shape from the
//! wire: a multi-codec audio publication builds `dynacastManagerAudio`
//! (mediatrack.go:161-167) and every answered subscription reports its
//! negotiated mime (subscribedtrack.go:456-482 → participant.go:2955-2982).

use super::*;
use rtc_core::media_api::{DownTrackSpec, MediaCommand, PcRole, PublishedTrackSpec};
use rtc_core::model::{
    BackupCodecPolicy, CodecSpec, TrackCodec, TrackKind, TrackSettings, TrackSource,
};
use str0m::Rtc;
use str0m::change::SdpAnswer;
use str0m::media::{Direction, MediaKind};

const OPUS: &str = "audio/opus";
const RED: &str = "audio/red";
const SDP_SID: &str = "TR_audio_sdp";
const PUB: &str = "PA_sdp_pub";
// The fixture registers every transport under one participant, so the down
// track's subscriber must be that id for `sending_transport` to find the PC.
const PARTICIPANT: &str = "PA_test";

/// A client that offers and receives `audio/opus` plus `audio/red` with the
/// LiveKit payload types the server registers (codecs.rs:59-64: PT 111 opus,
/// PT 63 red with `apt=111`), so both codecs survive the intersection.
fn dual_codec_client(now: Instant) -> Rtc {
    Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .clear_codecs()
        .enable_opus(true, true)
        .build(now)
}

fn sdp_engine(now: Instant) -> MediaEngine {
    let mut engine = engine(now);
    for mime in [OPUS, RED] {
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: mime.into(),
            fmtp_line: String::new(),
        });
    }
    engine
}

/// Two declared codecs — opus first, RED second under its own CID — with
/// `disable_red` set so the RED codec is reachable only through an explicit
/// codec CID, exactly like a publisher whose TrackInfo lists both mimes.
fn dual_codec_audio_spec() -> PublishedTrackSpec {
    PublishedTrackSpec {
        backup_codec_policy: BackupCodecPolicy::Simulcast,
        sid: TrackSid::from(SDP_SID),
        kind: TrackKind::Audio,
        source: TrackSource::Microphone,
        cid: "audio-cid".into(),
        layers: vec![],
        codecs: [("audio-cid", OPUS), ("red-cid", RED)]
            .into_iter()
            .map(|(cid, mime)| TrackCodec {
                mime_type: mime.into(),
                cid: cid.into(),
                ..Default::default()
            })
            .collect(),
        stereo: false,
        disable_dtx: false,
        disable_red: true,
        encrypted: false,
        muted: false,
    }
}

/// Drain queued events, returning the audio-codec announcements and panicking
/// on any other event so a stray queue item cannot hide a missing one.
fn take_announcements(engine: &mut MediaEngine) -> Vec<(ParticipantSid, TrackSid, Vec<String>)> {
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    events
        .into_iter()
        .filter_map(|event| match event {
            MediaEvent::SubscribedAudioCodec {
                publisher,
                track_sid,
                codecs,
            } => Some((
                publisher,
                track_sid,
                codecs.into_iter().map(|c| c.codec).collect(),
            )),
            MediaEvent::LocalDescription { .. } => None,
            other => panic!("unexpected queued event {other:?}"),
        })
        .collect()
}

fn local_descriptions(events: &[MediaEvent]) -> Vec<(SdpType, String, u32)> {
    events
        .iter()
        .filter_map(|event| match event {
            MediaEvent::LocalDescription { kind, sdp, id, .. } => Some((*kind, sdp.clone(), *id)),
            _ => None,
        })
        .collect()
}

fn local_descriptions_of(engine: &mut MediaEngine) -> Vec<(SdpType, String, u32)> {
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    local_descriptions(&events)
}

/// Run the registration the room performs after AddTrack: re-bind through the
/// real `RemoteMediaAdded` rows so each negotiated publisher mime gets its
/// receiver row, like Go's `OnSetupReceiver` path (mediatrack.go:167-175).
fn register_remote_media(engine: &mut MediaEngine, tid: TransportId, now: Instant) {
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    for event in events {
        if let MediaEvent::RemoteMediaAdded(remote) = &event
            && remote.transport == tid
        {
            engine.bind_track(tid, &remote.mid, TrackSid::from(SDP_SID), now);
        }
    }
}

#[test]
fn sdp_negotiated_codec_switch_to_red_announces_the_audio_red_mime() {
    // A publisher whose TrackInfo declares opus first answers with opus as its
    // receiver mime. When the room switches the media options to the declared
    // RED codec (`UpdatePublishedTrack`, room/publish.rs:535 — the same command
    // upstream's `UpdateTrack` feeds into the receiver selection), the next
    // negotiated subscription must announce `audio/red`. This leg proves the
    // announced mime follows the live receiver row, not a fixture constant.
    let start = Instant::now();
    let mut engine = sdp_engine(start);
    let tid = transport(&mut engine, PcRole::Publisher, start);
    engine.transports.get_mut(&tid).unwrap().spec.primary = false;
    engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: tid,
            track: dual_codec_audio_spec(),
        },
        start,
    );
    publish_over_sdp(&mut engine, tid, start);
    let ui = *engine.up_by_sid.get(&TrackSid::from(SDP_SID)).unwrap();
    assert_eq!(engine.ups.get(ui).unwrap().mime, OPUS);

    // The room records the negotiated codec list and switches the primary to
    // RED: codecs[0] becomes audio/red under the published CID.
    let mut switched = dual_codec_audio_spec();
    switched.codecs = [("audio-cid", RED), ("red-cid", OPUS)]
        .into_iter()
        .map(|(cid, mime)| TrackCodec {
            mime_type: mime.into(),
            cid: cid.into(),
            ..Default::default()
        })
        .collect();
    engine.handle_command(MediaCommand::UpdatePublishedTrack { track: switched }, start);
    // Re-bind so the receiver row moves to the new primary mime, exactly what
    // the renegotiated publication does through RemoteMediaAdded -> bind_track.
    let pub_mid = engine.ups.get(ui).unwrap().mid.clone();
    engine.bind_track(tid, &pub_mid.to_string(), TrackSid::from(SDP_SID), start);
    register_remote_media(&mut engine, tid, start);
    let ui = *engine.up_by_sid.get(&TrackSid::from(SDP_SID)).unwrap();
    assert_eq!(
        engine.ups.get(ui).unwrap().mime,
        RED,
        "the red-first codec list must move the receiver row to audio/red"
    );
    assert_eq!(engine.audio_dynacast.len(), 1);

    // Subscribe on a second PC and answer; the announcement must carry the
    // publisher's receiver mime (audio/red), which only the wiring under test
    // can supply.
    let sub_tid = transport(&mut engine, PcRole::Subscriber, start);
    let mut subscriber = dual_codec_client(start);
    engine.negotiate(sub_tid, true, start);
    let (_offer_sdp, id) = local_descriptions_of(&mut engine)
        .into_iter()
        .find_map(|(kind, sdp, id)| (kind == SdpType::Offer).then_some((sdp, id)))
        .expect("subscriber transport emits a server offer");
    let answer = subscriber
        .sdp_api()
        .accept_offer(str0m::change::SdpOffer::from_sdp_string(&_offer_sdp).unwrap())
        .unwrap();
    engine.on_remote_description(sub_tid, SdpType::Answer, answer.to_sdp_string(), id, start);
    engine.run(start);
    engine.events.clear();
    engine.subscribe(
        DownTrackSpec {
            subscriber: ParticipantSid::from(PARTICIPANT),
            publisher: ParticipantSid::from(PUB),
            track_sid: TrackSid::from(SDP_SID),
            stream_id: "audio-subs".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        start,
    );
    let negotiation_at = start + Duration::from_millis(160);
    engine.handle_timeout(negotiation_at);
    let (offer_sdp, id) = local_descriptions_of(&mut engine)
        .into_iter()
        .find_map(|(kind, sdp, id)| (kind == SdpType::Offer).then_some((sdp, id)))
        .expect("subscription triggers a renegotiated offer");
    let answer = subscriber
        .sdp_api()
        .accept_offer(str0m::change::SdpOffer::from_sdp_string(&offer_sdp).unwrap())
        .unwrap()
        .to_sdp_string();
    engine.on_remote_description(sub_tid, SdpType::Answer, answer, id, negotiation_at);
    assert_eq!(
        take_announcements(&mut engine),
        vec![(
            ParticipantSid::from(PUB),
            TrackSid::from(SDP_SID),
            vec![RED.to_string()]
        )],
        "the committed mime set follows the audio/red receiver row"
    );
}

/// One real publisher exchange over `tid`: offer with the audio section, answer,
/// bind, then register what the SDP actually negotiated.
fn publish_over_sdp(engine: &mut MediaEngine, tid: TransportId, now: Instant) {
    engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: tid,
            track: dual_codec_audio_spec(),
        },
        now,
    );
    let mut publisher = dual_codec_client(now);
    let mut change = publisher.sdp_api();
    let mid = change.add_media(
        MediaKind::Audio,
        Direction::SendOnly,
        None,
        Some("audio-cid".into()),
        None,
    );
    let (offered, pending) = change.apply().unwrap();
    engine.on_remote_description(tid, SdpType::Offer, offered.to_sdp_string(), 1, now);
    let events_all = {
        let mut events = Vec::new();
        engine.drain_events(&mut events);
        events
    };
    let answer = local_descriptions(&events_all)
        .into_iter()
        .find_map(|(kind, sdp, _)| (kind == SdpType::Answer).then_some(sdp))
        .expect("server answers the publisher offer");
    publisher
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
        .unwrap();
    engine.bind_track(tid, &mid.to_string(), TrackSid::from(SDP_SID), now);
    // The registration the room performs after AddTrack: re-bind through the
    // RemoteMediaAdded rows so each negotiated publisher mime gets its receiver
    // row, like Go's `OnSetupReceiver` path (mediatrack.go:167-175).
    register_remote_media(engine, tid, now);
}

#[test]
fn sdp_negotiated_single_pc_audio_subscription_announces_the_committed_mime() {
    let start = Instant::now();
    let mut engine = sdp_engine(start);
    let tid = transport(&mut engine, PcRole::Single, start);
    engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: tid,
            track: dual_codec_audio_spec(),
        },
        start,
    );
    let mut client = dual_codec_client(start);
    client.add_local_candidate(str0m::Candidate::host("127.0.0.1:9741".parse().unwrap(), "udp").unwrap());
    // Publication first: the client publishes on the audio m-line.
    let mut change = client.sdp_api();
    let mid = change.add_media(
        MediaKind::Audio,
        Direction::SendOnly,
        None,
        Some("audio-cid".into()),
        None,
    );
    let (offered, pending) = change.apply().unwrap();
    engine.on_remote_description(tid, SdpType::Offer, offered.to_sdp_string(), 1, start);
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    let answer = local_descriptions(&events)
        .into_iter()
        .find_map(|(kind, sdp, _)| (kind == SdpType::Answer).then_some(sdp))
        .expect("server answers the publication offer");
    client
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
        .unwrap();
    engine.bind_track(tid, &mid.to_string(), TrackSid::from(SDP_SID), start);
    register_remote_media(&mut engine, tid, start);

    // The room records the negotiation in TrackInfo and re-binds the receiver
    // with the declared codec set, the same two commands the AddTrack flow
    // issues (room/publish.rs sends UpdatePublishedTrack after merging the
    // negotiated codec mids). Without this the up track keeps the minimal
    // single-codec spec, has no manager, and nothing can be announced.
    engine.handle_command(
        MediaCommand::UpdatePublishedTrack {
            track: dual_codec_audio_spec(),
        },
        start,
    );
    engine.bind_track(tid, &mid.to_string(), TrackSid::from(SDP_SID), start);
    register_remote_media(&mut engine, tid, start);
    assert_eq!(
        engine.audio_dynacast.len(),
        1,
        "a two-codec audio publication must build the manager"
    );

    // Subscription on the same PC. The engine asks the client for one more send
    // audio section — the `MediaSectionsRequirement` the room turns into an
    // AddTrack response — and the client honours it with a recvonly m-line.
    engine.subscribe(
        DownTrackSpec {
            subscriber: ParticipantSid::from(PARTICIPANT),
            publisher: ParticipantSid::from(PUB),
            track_sid: TrackSid::from(SDP_SID),
            stream_id: "audio-subs".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        start,
    );
    engine.run(start);
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    let requirement = events
        .iter()
        .find_map(|event| match event {
            MediaEvent::MediaSectionsRequirement { audios, videos, .. } => Some((*audios, *videos)),
            _ => None,
        })
        .expect("the single-PC subscription requests a send section");
    assert_eq!(requirement, (1, 0));

    let mut change = client.sdp_api();
    change.add_media(MediaKind::Audio, Direction::RecvOnly, None, None, None);
    let (offered, pending) = change.apply().unwrap();
    let offered = offered.to_sdp_string();
    let sections = crate::sdp_munge::split_sections(&offered).1;
    let sub_section = sections
        .iter()
        .copied()
        .filter(|s| crate::sdp_munge::section_direction(s) == Some("recvonly"))
        .next()
        .expect("client offers a recvonly audio section");
    assert!(
        sub_section.contains("a=rtpmap:63 red/"),
        "the client offer must carry opus and red: {sub_section}"
    );
    engine.on_remote_description(tid, SdpType::Offer, offered, 2, start);
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    let answer = local_descriptions(&events)
        .into_iter()
        .find_map(|(kind, sdp, _)| (kind == SdpType::Answer).then_some(sdp.clone()))
        .expect("server answers the subscription renegotiation");
    let sid_probe = TrackSid::from(SDP_SID);
    let ptr_before = engine.audio_dynacast.get(&sid_probe).map(|m| m as *const _ as usize);
    let di0 = engine.downs.iter().next().unwrap().0;
    let upi = engine.downs.get(di0).unwrap().up;
    let up_sid = engine.ups.get(upi).unwrap().sid.clone();
    eprintln!("DBG down.up={upi} up.sid={up_sid:?} sid_probe={sid_probe:?} equal={}", up_sid==sid_probe);
    eprintln!("DBG map keys={:?} ptr_after={:?}", engine.audio_dynacast.keys().map(|k| format!("{k:?}")).collect::<Vec<_>>(), engine.audio_dynacast.get(&sid_probe).map(|m| m as *const _ as usize));
    eprintln!("DBG ptr_before={ptr_before:?}");
    let out = { let m = engine.audio_dynacast.get_mut(&sid_probe).unwrap(); m.notify_subscription(ParticipantSid::from("ZZ_probe"), OPUS, true) };
    eprintln!("DBG direct manager output={out:?}");
    eprintln!("DBG mime probe: norm(opus)={:?}", crate::dynacast::normalize_audio_mime(OPUS));
    let out2 = { let mut m = crate::dynacast::DynacastAudio::default(); m.notify_subscription(ParticipantSid::from("ZZ_probe"), "audio/opus", true) };
    eprintln!("DBG fresh manager output={out2:?}");
    let mref = engine.audio_dynacast.get(&sid_probe).unwrap();
    eprintln!("DBG fixture mgr demand_len={:?} committed_len={:?}", mref.demand.len(), mref.committed.len());
    let out3 = { let m = engine.audio_dynacast.get_mut(&sid_probe).unwrap(); m.notify_subscription(ParticipantSid::from("ZZ_probe"), OPUS, true) };
    let mref = engine.audio_dynacast.get(&sid_probe).unwrap();
    eprintln!("DBG out3={out3:?} after demand_len={} committed_len={}", mref.demand.len(), mref.committed.len());
    let section = crate::sdp_munge::split_sections(&answer)
        .1
        .into_iter()
        .filter(|s| crate::sdp_munge::section_direction(s) == Some("sendonly"))
        .next()
        .expect("server answers the subscription m-line sendonly");
    assert!(
        section.contains("a=rtpmap:63 red/"),
        "RED stays negotiated on the subscription m-line: {section}"
    );
    client
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
        .unwrap();

    // The load-bearing chain: the real offer path bound the down track, took
    // the receiver mime of the bound up track, committed it and queued the
    // announcement addressed to the publisher participant.
    assert_eq!(
        take_announcements(&mut engine),
        vec![(
            ParticipantSid::from(PUB),
            TrackSid::from(SDP_SID),
            vec![OPUS.to_string()]
        )],
        "one announcement per negotiated sub, carrying the full receiver mime"
    );
}

#[test]
fn sdp_negotiated_dual_pc_answer_announces_and_a_repeat_subscriber_stays_silent() {
    let start = Instant::now();
    let mut engine = sdp_engine(start);
    let pub_tid = transport(&mut engine, PcRole::Publisher, start);
    engine.transports.get_mut(&pub_tid).unwrap().spec.primary = false;
    publish_over_sdp(&mut engine, pub_tid, start);
    // Same room-side registration as the single-PC vector: merge the declared
    // codec set into the live publication and re-bind the receiver.
    engine.handle_command(
        MediaCommand::UpdatePublishedTrack {
            track: dual_codec_audio_spec(),
        },
        start,
    );
    let ui0 = *engine.up_by_sid.get(&TrackSid::from(SDP_SID)).unwrap();
    let pub_mid = engine.ups.get(ui0).unwrap().mid.clone();
    engine.bind_track(pub_tid, &pub_mid.to_string(), TrackSid::from(SDP_SID), start);
    register_remote_media(&mut engine, pub_tid, start);
    assert_eq!(
        engine.audio_dynacast.len(),
        1,
        "a two-codec audio publication must build the manager"
    );
    let ui = *engine.up_by_sid.get(&TrackSid::from(SDP_SID)).unwrap();
    assert_eq!(engine.ups.get(ui).unwrap().mime, OPUS);

    let sub_tid = transport(&mut engine, PcRole::Subscriber, start);
    let mut subscriber = dual_codec_client(start);
    // Establish the subscriber PC with the empty server offer, then subscribe.
    engine.negotiate(sub_tid, true, start);
    let (offer_sdp, id) = local_descriptions_of(&mut engine)
        .into_iter()
        .find_map(|(kind, sdp, id)| (kind == SdpType::Offer).then_some((sdp, id)))
        .expect("subscriber transport emits a server offer");
    let answer = subscriber
        .sdp_api()
        .accept_offer(str0m::change::SdpOffer::from_sdp_string(&offer_sdp).unwrap())
        .unwrap();
    engine.on_remote_description(sub_tid, SdpType::Answer, answer.to_sdp_string(), id, start);
    engine.run(start);
    engine.events.clear();

    engine.subscribe(
        DownTrackSpec {
            subscriber: ParticipantSid::from("PA_sdp_sub"),
            publisher: ParticipantSid::from(PUB),
            track_sid: TrackSid::from(SDP_SID),
            stream_id: "audio-subs".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        start,
    );
    let negotiation_at = start + Duration::from_millis(160);
    engine.handle_timeout(negotiation_at);
    let (offer_sdp, id) = local_descriptions_of(&mut engine)
        .into_iter()
        .find_map(|(kind, sdp, id)| (kind == SdpType::Offer).then_some((sdp, id)))
        .expect("the subscription triggers a renegotiated offer");
    assert!(
        offer_sdp.contains("a=rtpmap:63 red/"),
        "the server offer must present opus and red: {offer_sdp}"
    );
    let answer = subscriber
        .sdp_api()
        .accept_offer(str0m::change::SdpOffer::from_sdp_string(&offer_sdp).unwrap())
        .unwrap()
        .to_sdp_string();
    let section = crate::sdp_munge::split_sections(&answer)
        .1
        .into_iter()
        .find(|s| crate::sdp_munge::section_kind(s) == Some("audio"))
        .expect("answered audio section");
    assert!(
        section.contains("a=rtpmap:111 opus/48000"),
        "the subscriber answered opus: {section}"
    );
    engine.on_remote_description(sub_tid, SdpType::Answer, answer, id, negotiation_at);

    // Chain leg 1: the real answer path produced the announcement.
    assert_eq!(
        take_announcements(&mut engine),
        vec![(
            ParticipantSid::from(PUB),
            TrackSid::from(SDP_SID),
            vec![OPUS.to_string()]
        )],
        "the real answer path must notify through the audio dynacast manager"
    );

    // Chain leg 2: a second subscriber of the same mime changes nothing,
    // because the committed map is unchanged (dynacastmanageraudio.go:144-158).
    engine.subscribe(
        DownTrackSpec {
            subscriber: ParticipantSid::from("PA_sdp_sub2"),
            publisher: ParticipantSid::from(PUB),
            track_sid: TrackSid::from(SDP_SID),
            stream_id: "audio-subs2".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        start,
    );
    let second_at = start + Duration::from_millis(400);
    engine.handle_timeout(second_at);
    let (offer_sdp, id) = local_descriptions_of(&mut engine)
        .into_iter()
        .find_map(|(kind, sdp, id)| (kind == SdpType::Offer).then_some((sdp, id)))
        .expect("the second subscription renegotiates");
    let answer = subscriber
        .sdp_api()
        .accept_offer(str0m::change::SdpOffer::from_sdp_string(&offer_sdp).unwrap())
        .unwrap()
        .to_sdp_string();
    engine.on_remote_description(sub_tid, SdpType::Answer, answer, id, second_at);
    assert!(
        take_announcements(&mut engine).is_empty(),
        "an unchanged committed map announces nothing, even over real SDP"
    );
}
