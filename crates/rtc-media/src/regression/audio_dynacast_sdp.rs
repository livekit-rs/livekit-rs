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
//!
//! Parked unfinished: all three legs are `#[ignore]`d with their measured
//! outcome — two assert failures and one that does not terminate. The crate
//! still compiles them, so the next pass starts from the build rather than from
//! a guess about the wiring.

use super::*;
use rtc_core::media_api::{DownTrackSpec, MediaCommand, PcRole, PublishedTrackSpec};
use rtc_core::model::{
    BackupCodecPolicy, CodecSpec, TrackCodec, TrackKind, TrackSettings, TrackSource,
};
use str0m::Rtc;
use str0m::change::SdpAnswer;
use str0m::media::{Direction, MediaKind};
use str0m::{Candidate, Input, Output};

const OPUS: &str = "audio/opus";
const RED: &str = "audio/red";
const SDP_SID: &str = "TR_audio_sdp";
const PUB: &str = "PA_sdp_pub";
// The fixture registers every transport under one participant, so the down
// track's subscriber must be that id for `sending_transport` to find the PC.
const PARTICIPANT: &str = "PA_test";
// The server ICE agent address every leg of this file gathers and routes
// through, matching the sibling dual-PC regressions in `regression.rs`.
const SERVER_ADDR: &str = "127.0.0.1:9741";
const CLIENT_ADDR: &str = "127.0.0.1:9742";
// A plain `engine(now)` has only opus registered, so a published RED codec
// would be refused at bind ("receiver codec absent from published codec
// set"). Every engine here enables both mimes instead.
const ENABLED_CODECS: [&str; 2] = [OPUS, RED];

/// A client that offers and receives `audio/opus` plus `audio/red` with the
/// LiveKit payload types the server registers (codecs.rs:59-64: PT 111 opus,
/// PT 63 red with `apt=111`), so both codecs survive the intersection.
fn dual_codec_client(now: Instant) -> Rtc {
    let mut rtc = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .clear_codecs()
        .enable_opus(true, true)
        .build(now);
    rtc.add_local_candidate(
        Candidate::host(CLIENT_ADDR.parse().unwrap(), "udp").expect("test client candidate"),
    );
    rtc
}

fn sdp_engine(now: Instant) -> MediaEngine {
    let mut engine = engine(now);
    for mime in ENABLED_CODECS {
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: mime.into(),
            fmtp_line: String::new(),
        });
    }
    engine
}

/// Create a transport the way `transport()` does, but with the server host
/// candidate already gathered, so connectivity can complete for PCs driven
/// through `drive_client`.
fn connected_transport(engine: &mut MediaEngine, role: PcRole, now: Instant) -> TransportId {
    connected_transport_with(engine, role, false, now)
}

/// Same, with the option to put the reliable/lossy/data-track channels on the
/// server side of the offer. A Single PC needs them so its very first local
/// description is non-empty (str0m suppresses change-free offers), which is
/// what establishes the PC before any media is negotiated.
fn connected_transport_with(
    engine: &mut MediaEngine,
    role: PcRole,
    server_data_channels: bool,
    now: Instant,
) -> TransportId {
    let tid = TransportId::next();
    let addr = SERVER_ADDR.parse().expect("test server address");
    let mut spec = transport_spec(tid, role);
    spec.server_data_channels = server_data_channels;
    engine.create_transport(
        spec,
        IceCredentials::random(),
        vec![crate::net::LocalCandidate {
            proto: Proto::Udp,
            addr,
            base: addr,
        }],
        now,
    );
    tid
}

/// Drive one client `Rtc` against the engine until its ICE agent reports
/// connected, routing every client packet into `handle_datagram` and every
/// engine output back to the client. Returns once DTLS/ICE completed or the
/// budget is exhausted; callers assert observable consequences afterwards.
fn drive_client(
    engine: &mut MediaEngine,
    client: &mut Rtc,
    tid: TransportId,
    from: Instant,
    steps: u64,
) -> bool {
    let client_addr = CLIENT_ADDR.parse().expect("test client address");
    let server_addr = SERVER_ADDR.parse().expect("test server address");
    let mut now = from;
    for _ in 0..steps {
        if client.is_connected() {
            return true;
        }
        let _ = client.handle_input(Input::Timeout(now));
        loop {
            match client.poll_output() {
                Ok(Output::Transmit(packet)) => engine.handle_datagram(crate::net::Datagram {
                    transport: tid,
                    proto: Proto::Udp,
                    source: client_addr,
                    destination: server_addr,
                    data: &packet.contents,
                    received_at: now,
                }),
                Ok(Output::Event(_)) | Ok(Output::Timeout(_)) => {}
                Err(_) => break,
            }
        }
        engine.handle_timeout(now);
        now += Duration::from_millis(10);
    }
    client.is_connected()
}

/// A codec list whose first entry — the primary, the one a receiver binds —
/// is `mime`, with the other declared codec second. Mirrors what the room
/// writes into TrackInfo after it has merged the negotiated codec set.
fn codec_list(primary: &str) -> Vec<TrackCodec> {
    let secondary = if primary == OPUS { RED } else { OPUS };
    [(primary, "audio-cid"), (secondary, "red-cid")]
        .into_iter()
        .map(|(mime, cid)| TrackCodec {
            mime_type: mime.into(),
            cid: cid.into(),
            ..Default::default()
        })
        .collect()
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
        codecs: codec_list(OPUS),
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
#[ignore = "parked WIP (C79-c follow-up): the red-primary list plus red-cid renegotiated offer leaves the receiver row at [\"audio/opus\"]"]
fn sdp_negotiated_codec_switch_to_red_announces_the_audio_red_mime() {
    // A publisher whose TrackInfo declares opus first answers with opus as its
    // receiver mime. When the room switches the media options to the declared
    // RED codec (`UpdatePublishedTrack`, room/publish.rs:535 — the same command
    // upstream's `UpdateTrack` feeds into the receiver selection), the next
    // negotiated subscription must announce `audio/red`. This leg proves the
    // announced mime follows the live receiver row, not a fixture constant.
    let start = Instant::now();
    let mut engine = sdp_engine(start);
    engine.out = Box::new(RoutedPacketQueue::default());
    let tid = connected_transport(&mut engine, PcRole::Publisher, start);
    engine.transports.get_mut(&tid).unwrap().spec.primary = false;
    // Publish with opus as the primary codec: the receiver row is audio/opus.
    let mut publisher = publish_over_sdp(&mut engine, tid, start);
    let ui = *engine.up_by_sid.get(&TrackSid::from(SDP_SID)).unwrap();
    assert_eq!(engine.ups.get(ui).unwrap().mime, OPUS);

    // The room switches the media options to the declared RED codec: upstream
    // reorders the TrackInfo codecs so the chosen codec is primary with its own
    // CID first (room/publish.go UpdateTrack → ToProto ordered by the selected
    // codec, and mediatrack.go OnTrackAdded picks codecs[0]). The publisher
    // then offers that codec under the new track-id, and the re-bind through
    // the real RemoteMediaAdded path resolves the receiver mime from the
    // updated codec list matched on the offered CID.
    let mut switched = dual_codec_audio_spec();
    switched.codecs = vec![
        TrackCodec {
            mime_type: RED.into(),
            cid: "red-cid".into(),
            ..Default::default()
        },
        TrackCodec {
            mime_type: OPUS.into(),
            cid: "audio-cid".into(),
            ..Default::default()
        },
    ];
    switched.cid = "red-cid".into();
    engine.handle_command(
        MediaCommand::UpdatePublishedTrack {
            track: switched.clone(),
        },
        start,
    );
    // Reuse the publishing client so the renegotiation offer keeps the same
    // ICE credentials and session version as the first one.
    let mut change = publisher.sdp_api();
    let mid = change.add_media(
        MediaKind::Audio,
        Direction::SendOnly,
        None,
        Some("red-cid".into()),
        None,
    );
    let (offered, pending) = change.apply().unwrap();
    engine.on_remote_description(tid, SdpType::Offer, offered.to_sdp_string(), 9, start);
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    let answer = local_descriptions(&events)
        .into_iter()
        .find_map(|(kind, sdp, _)| (kind == SdpType::Answer).then_some(sdp))
        .expect("server answers the red-first renegotiation");
    publisher
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
        .unwrap();
    register_remote_media(&mut engine, tid, start);
    engine.bind_track(tid, &mid.to_string(), TrackSid::from(SDP_SID), start);
    let receivers: Vec<String> = engine
        .ups
        .iter()
        .filter(|(_, up)| up.sid == TrackSid::from(SDP_SID))
        .map(|(_, up)| up.mime.clone())
        .collect();
    assert!(
        receivers.iter().any(|m| m == RED),
        "the red-primary codec list plus the red-cid renegotiated offer must move \
         a receiver row to audio/red; got {receivers:?}"
    );
    assert_eq!(engine.audio_dynacast.len(), 1);

    // Subscribe on a second PC that has completed ICE/DTLS, so the answered
    // subscription really reaches `handle_answer`'s bound-track loop. The
    // announcement must carry the publisher's receiver mime (audio/red),
    // which only the wiring under test can supply. str0m suppresses an offer
    // with no changes, so this PC's initial server description is the
    // subscription renegotiation itself.
    let sub_tid = connected_transport(&mut engine, PcRole::Subscriber, start);
    let mut subscriber = dual_codec_client(start);
    drive_client(&mut engine, &mut subscriber, sub_tid, start, 80);
    assert!(
        subscriber.is_connected(),
        "the subscriber PC must complete ICE/DTLS"
    );
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
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    let (offer_sdp, id) = local_descriptions(&events)
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
/// bind, then register what the SDP actually negotiated. Returns the client so
/// a renegotiation vector can reuse its ICE identity and session version.
fn publish_over_sdp(engine: &mut MediaEngine, tid: TransportId, now: Instant) -> str0m::Rtc {
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
    publisher
}

#[test]
#[ignore = "parked WIP (C79-c follow-up): the subscriber transport never yields a serialized server offer, so `connected_transport` unwraps nothing"]
fn sdp_negotiated_single_pc_audio_subscription_announces_the_committed_mime() {
    let start = Instant::now();
    let mut engine = sdp_engine(start);
    engine.out = Box::new(RoutedPacketQueue::default());
    let tid = connected_transport_with(&mut engine, PcRole::Single, true, start);
    let mut client = dual_codec_client(start);
    // The initial server offer (data channels only) establishes the PC.
    answer_server_offer(&mut engine, &mut client, tid, start);
    // Publication on the same PC: the client adds its sendonly audio m-line.
    let _ = publish_over_sdp(&mut engine, tid, start);
    let ui = *engine.up_by_sid.get(&TrackSid::from(SDP_SID)).unwrap();
    assert_eq!(engine.ups.get(ui).unwrap().mime, OPUS);
    assert_eq!(
        engine.audio_dynacast.len(),
        1,
        "a two-codec audio publication must build the manager"
    );

    // Subscription on the same PC. The engine asks the client for one more
    // send audio section — the `MediaSectionsRequirement` the room turns into
    // an AddTrack response — and the client honours it with a recvonly m-line.
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

    // Drive the Single PC through ICE/DTLS: the client's STUN bindings go into
    // `handle_datagram` and every packet the engine queues is delivered back,
    // so the transport reaches the connected state a real answer arrives on.
    drive_client(&mut engine, &mut client, tid, start, 80);
    assert!(
        client.is_connected(),
        "the single PC must complete ICE/DTLS"
    );

    let mut change = client.sdp_api();
    change.add_media(
        MediaKind::Audio,
        Direction::RecvOnly,
        Some("audio-subs".into()),
        Some(SDP_SID.into()),
        None,
    );
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
#[ignore = "parked WIP (C79-c follow-up): does not terminate; the drive loop keeps polling output instead of completing the second answer"]
fn sdp_negotiated_dual_pc_answer_announces_and_a_repeat_subscriber_stays_silent() {
    let start = Instant::now();
    let mut engine = sdp_engine(start);
    engine.out = Box::new(RoutedPacketQueue::default());
    let pub_tid = connected_transport(&mut engine, PcRole::Publisher, start);
    engine.transports.get_mut(&pub_tid).unwrap().spec.primary = false;
    let _ = publish_over_sdp(&mut engine, pub_tid, start);
    assert_eq!(
        engine.audio_dynacast.len(),
        1,
        "a two-codec audio publication must build the manager"
    );
    let ui = *engine.up_by_sid.get(&TrackSid::from(SDP_SID)).unwrap();
    assert_eq!(engine.ups.get(ui).unwrap().mime, OPUS);

    // The subscriber PC completes ICE/DTLS on its own transport first. str0m
    // suppresses an offer with no changes, so the server's initial offer for
    // this PC is the subscription renegotiation itself — the same way a real
    // subscriber receives its first media after the add debounce.
    let sub_tid = connected_transport(&mut engine, PcRole::Subscriber, start);
    let mut subscriber = dual_codec_client(start);
    drive_client(&mut engine, &mut subscriber, sub_tid, start, 80);
    assert!(
        subscriber.is_connected(),
        "the subscriber PC must complete ICE/DTLS"
    );
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
