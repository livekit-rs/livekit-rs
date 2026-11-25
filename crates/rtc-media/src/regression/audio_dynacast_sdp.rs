//! Audio multi-codec subscription driven through the real SDP offer/answer
//! path, not by hand-built objects.
//!
//! The sibling unit file calls `notify_audio_codec_negotiated` directly, so it
//! stays green even when no negotiation path invokes it. These vectors reach the
//! two production call sites instead: one subscribes on the publishing PC, where
//! the client re-offers and the offer path binds
//! ([`MediaEngine::on_remote_description`] → `signaling.rs:838`), and one
//! subscribes on a second PC that answers the offer the engine wrote
//! (`signaling.rs:1006`). Each leg asserts the announcement leaves the engine
//! with the mime its own negotiation selected, so a hard-coded mime fails the
//! RED-first pair (measured: `M-C84-2`, see the evidence slot).
//!
//! Ground truth (pinned livekit v1.13.10 e5655ac):
//! - `participant.go:3170-3247`: the publisher's audio codec list is the
//!   AddTrack request's `SimulcastCodecs` in the client's own order, deduped by
//!   mime, each entry keeping its `Cid`. The order is client data, so "RED
//!   first" is a real publication shape rather than a server-side preference.
//! - `mediatrack.go:161-167`: the audio dynacast manager exists only when that
//!   list has more than one codec, and `mediatrack.go:167-175` feeds every
//!   receiver the negotiation sets up into `AddCodec(mime)`.
//! - `mediatrackreceiver.go:182-228`: `SetupReceiver` keys receivers by
//!   `receiver.Mime()` — the mime of the stream that arrived — and writes that
//!   mime back into the TrackInfo codec entry at the receiver's priority index.
//!   No TrackInfo update in v1.13.10 replaces or reorders the codec list:
//!   `UpdateCodecInfo` (`:803`) patches `Cid` and layers on mime matches,
//!   `UpdateCodecSdpCid` (`:835`) patches `SdpCid`, `UpdateAudioTrack` (`:919`)
//!   patches the stereo/DTX features, and `RoomService` exposes no `UpdateTrack`
//!   RPC at all (`roomservice.go:73-396`).
//! - `subscribedtrack.go:456-482` → `mediatrack.go:189-195`: the demand comes
//!   from each subscriber's own negotiated codec
//!   (`OnCodecNegotiated` → `NotifySubscription(sub, mime, true)`), so the
//!   announced mime follows the negotiation and never a constant.
//!
//! The payload types are the ones `codecs.rs:59-64` registers: PT 111 opus and
//! PT 63 red with `apt=111`.

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
// The fixture registers every transport under one participant, so both sides of
// the exchange carry that id: `transport_spec` (regression.rs:116-135) fixes it
// and `bind_track` takes the publisher from the transport spec. Publisher-only
// delivery of the announcement is the room router's contract, proven in
// crates/rtc-core/src/room/tests/dynacast_signal_routing.rs.
const PARTICIPANT: &str = "PA_test";
// A plain `engine(now)` has only opus registered, so a published RED codec
// would be refused at bind ("receiver codec absent from published codec
// set"). Every engine here enables both mimes instead.
const ENABLED_CODECS: [&str; 2] = [OPUS, RED];

/// A client offering `audio/opus` (PT 111) and `audio/red` (PT 63), so both
/// survive the intersection with the server's codec config.
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
    for mime in ENABLED_CODECS {
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: mime.into(),
            fmtp_line: String::new(),
        });
    }
    engine
}

/// The codec list as the pinned Go stores an AddTrack request's
/// `SimulcastCodecs`: the client's order, each mime under its own CID.
fn codec_list(primary: &str) -> Vec<TrackCodec> {
    let secondary = if primary == OPUS { RED } else { OPUS };
    [(primary, "primary-cid"), (secondary, "secondary-cid")]
        .into_iter()
        .map(|(mime, cid)| TrackCodec {
            mime_type: mime.into(),
            cid: cid.into(),
            ..Default::default()
        })
        .collect()
}

/// A two-codec audio publication whose primary codec is `primary`. RED stays
/// enabled because the leg under test is the negotiated codec set, not the
/// encoder fallback policy.
fn audio_spec(primary: &str) -> PublishedTrackSpec {
    PublishedTrackSpec {
        backup_codec_policy: BackupCodecPolicy::Simulcast,
        sid: TrackSid::from(SDP_SID),
        kind: TrackKind::Audio,
        source: TrackSource::Microphone,
        cid: "primary-cid".into(),
        layers: vec![],
        codecs: codec_list(primary),
        stereo: false,
        disable_dtx: false,
        disable_red: false,
        encrypted: false,
        muted: false,
    }
}

/// The audio-codec announcements inside an event list, keeping the publisher,
/// track and committed mimes the production call site queued.
///
/// Both real call sites push the announcement into the same queue that carries
/// their negotiation bookkeeping, so the tolerated variants are listed instead
/// of filtered quietly: anything else panics, and a stray queue item cannot hide
/// a missing announcement.
fn announcements(events: &[MediaEvent]) -> Vec<(ParticipantSid, TrackSid, Vec<String>)> {
    events
        .iter()
        .filter_map(|event| match event {
            MediaEvent::SubscribedAudioCodec {
                publisher,
                track_sid,
                codecs,
            } => Some((
                publisher.clone(),
                track_sid.clone(),
                codecs.iter().map(|c| c.codec.clone()).collect(),
            )),
            MediaEvent::LocalDescription { .. }
            | MediaEvent::MediaSectionsRequirement { .. }
            | MediaEvent::RemoteMediaAdded { .. }
            | MediaEvent::SubscriptionPrepared { .. }
            | MediaEvent::SubscriptionBound { .. }
            | MediaEvent::TransportEstablished { .. } => None,
            other => panic!("unexpected queued event {other:?}"),
        })
        .collect()
}

/// Drain whatever the engine still holds after a step and read its announcements.
fn queued_announcements(engine: &mut MediaEngine) -> Vec<(ParticipantSid, TrackSid, Vec<String>)> {
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    announcements(&events)
}

/// Let `client` add a media section, offer it, and accept the engine's answer.
/// This is the exchange the signal connection performs, and the negotiation
/// itself needs no ICE or DTLS completion — the way the sibling regressions in
/// `regression.rs` drive both publish and subscribe.
fn negotiate(
    engine: &mut MediaEngine,
    client: &mut Rtc,
    tid: TransportId,
    now: Instant,
    edit: impl FnOnce(&mut str0m::change::SdpApi<'_>),
) -> Vec<MediaEvent> {
    let mut change = client.sdp_api();
    edit(&mut change);
    let (offer, pending) = change.apply().unwrap();
    engine.on_remote_description(tid, SdpType::Offer, offer.to_sdp_string(), 1, now);
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|event| match event {
            MediaEvent::LocalDescription {
                kind: SdpType::Answer,
                sdp,
                ..
            } => Some(sdp.clone()),
            _ => None,
        })
        .expect("the engine answers a real client offer");
    client
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
        .unwrap();
    events
}

/// Run the registration the room performs after AddTrack: re-bind through the
/// real `RemoteMediaAdded` rows so each negotiated publisher mime gets its
/// receiver row, like Go's `OnSetupReceiver` path (mediatrack.go:167-175).
fn register_remote_media(
    engine: &mut MediaEngine,
    events: &[MediaEvent],
    tid: TransportId,
    now: Instant,
) {
    for event in events {
        if let MediaEvent::RemoteMediaAdded(remote) = event
            && remote.transport == tid
        {
            engine.bind_track(tid, &remote.mid, TrackSid::from(SDP_SID), now);
        }
    }
    let mut follow_up = Vec::new();
    engine.drain_events(&mut follow_up);
}

/// One fixture PC whose publisher negotiated a two-codec audio publication with
/// `primary` first.
struct SdpAudioPublish {
    engine: MediaEngine,
    client: Rtc,
    tid: TransportId,
    start: Instant,
}

impl SdpAudioPublish {
    fn new(primary: &'static str) -> Self {
        let start = Instant::now();
        let mut engine = sdp_engine(start);
        engine.out = Box::new(RoutedPacketQueue::default());
        let tid = transport(&mut engine, PcRole::Single, start);
        engine.handle_command(
            MediaCommand::ExpectTrack {
                transport: tid,
                track: audio_spec(primary),
            },
            start,
        );
        let mut client = dual_codec_client(start);
        let events = negotiate(&mut engine, &mut client, tid, start, |change| {
            change.add_media(
                MediaKind::Audio,
                Direction::SendOnly,
                None,
                Some("primary-cid".into()),
                None,
            );
        });
        register_remote_media(&mut engine, &events, tid, start);
        SdpAudioPublish {
            engine,
            client,
            tid,
            start,
        }
    }

    /// Mimes of the receiver rows bound to the published track, sorted.
    fn receiver_mimes(&self) -> Vec<String> {
        let mut mimes: Vec<String> = self
            .engine
            .ups
            .iter()
            .filter(|(_, up)| up.sid == TrackSid::from(SDP_SID))
            .map(|(_, up)| up.mime.clone())
            .collect();
        mimes.sort();
        mimes
    }

    /// The subscription the room issues for `subscriber` on this publication.
    fn down_spec(&self, subscriber: &str, stream_id: &str) -> DownTrackSpec {
        DownTrackSpec {
            subscriber: subscriber.into(),
            publisher: ParticipantSid::from(PARTICIPANT),
            track_sid: TrackSid::from(SDP_SID),
            stream_id: stream_id.into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        }
    }

    /// Subscribe on the publishing PC: the client re-offers and the engine binds
    /// the down track in the offer path (`signaling.rs:838`).
    fn subscribe_over_client_offer(
        &mut self,
        stream_id: &str,
    ) -> Vec<(ParticipantSid, TrackSid, Vec<String>)> {
        self.engine.events.clear();
        self.engine
            .subscribe(self.down_spec(PARTICIPANT, stream_id), self.start);
        self.engine.run(self.start);
        let events = negotiate(
            &mut self.engine,
            &mut self.client,
            self.tid,
            self.start,
            |change| {
                change.add_media(
                    MediaKind::Audio,
                    Direction::RecvOnly,
                    Some(stream_id.into()),
                    Some(SDP_SID.into()),
                    None,
                );
            },
        );
        // The call site queues into the same drain `negotiate` returned, so the
        // announcement has to be read there; a second drain alone would find
        // nothing and report a pass as an absence.
        let mut out = announcements(&events);
        out.extend(queued_announcements(&mut self.engine));
        out
    }

    /// Subscribe on a second PC owned by `subscriber`: the engine writes the
    /// offer, so the down track binds in the answer path (`signaling.rs:1006`).
    fn subscribe_over_server_offer(
        &mut self,
        subscriber: &str,
    ) -> Vec<(ParticipantSid, TrackSid, Vec<String>)> {
        let start = self.start;
        let tid = TransportId::next();
        let mut spec = transport_spec(tid, PcRole::Subscriber);
        spec.participant = subscriber.into();
        // The publishing PC keeps the primary slot, as in a two-PC SDK client.
        spec.primary = false;
        self.engine
            .create_transport(spec, IceCredentials::random(), vec![], start);
        self.engine.events.clear();
        let stream_id = format!("{subscriber}|{SDP_SID}");
        self.engine
            .subscribe(self.down_spec(subscriber, &stream_id), start);
        // `negotiate` only arms the debounce timer; the offer is written when it
        // fires, so the clock has to reach it before the client can answer.
        self.engine
            .handle_timeout(start + Duration::from_millis(200));
        let mut client = dual_codec_client(start);
        answer_server_offer(&mut self.engine, &mut client, tid, start);
        queued_announcements(&mut self.engine)
    }
}

#[test]
fn an_opus_first_negotiation_binds_the_audio_opus_receiver() {
    let published = SdpAudioPublish::new(OPUS);
    assert_eq!(
        published.receiver_mimes(),
        vec![OPUS.to_string()],
        "the receiver row takes the mime the publisher negotiation arrived on"
    );
    assert_eq!(
        published.engine.audio_dynacast.len(),
        1,
        "a two-codec audio publication builds the manager (mediatrack.go:161-167)"
    );
}

#[test]
fn a_red_first_negotiation_binds_the_audio_red_receiver() {
    // The only difference from the previous leg is the client's own codec order,
    // so the mime here cannot be a fixture constant.
    let published = SdpAudioPublish::new(RED);
    assert_eq!(
        published.receiver_mimes(),
        vec![RED.to_string()],
        "RED declared first must bind the audio/red receiver"
    );
    assert_eq!(published.engine.audio_dynacast.len(), 1);
}

#[test]
fn the_negotiated_subscription_announces_the_mime_of_its_own_receiver_row() {
    // The real offer path reaches the production call site
    // (`signaling.rs:838`, the sibling of the answer path at `:1006`) and each
    // leg announces what its own publication negotiated.
    let mut opus_first = SdpAudioPublish::new(OPUS);
    assert_eq!(
        opus_first.subscribe_over_client_offer("PA_test|TR_audio_sdp"),
        vec![(
            ParticipantSid::from(PARTICIPANT),
            TrackSid::from(SDP_SID),
            vec![OPUS.to_string()]
        )],
        "an opus-first publication announces audio/opus"
    );

    let mut red_first = SdpAudioPublish::new(RED);
    assert_eq!(
        red_first.subscribe_over_client_offer("PA_test|TR_audio_sdp"),
        vec![(
            ParticipantSid::from(PARTICIPANT),
            TrackSid::from(SDP_SID),
            vec![RED.to_string()]
        )],
        "the same subscription shape on a RED-first publication announces audio/red"
    );
}

#[test]
fn a_second_subscription_of_the_same_committed_mime_announces_nothing() {
    // Go commits the demand per subscriber (dynacastmanageraudio.go:144-158), so
    // re-negotiating a mime that is already committed leaves the payload map
    // unchanged and writes no message. One fixture PC has one participant, so
    // the repeat is the same subscriber re-negotiating.
    let mut published = SdpAudioPublish::new(RED);
    assert_eq!(
        published.subscribe_over_client_offer("PA_test|TR_audio_sdp"),
        vec![(
            ParticipantSid::from(PARTICIPANT),
            TrackSid::from(SDP_SID),
            vec![RED.to_string()]
        )],
        "the first subscription must announce, or the empty second one proves nothing"
    );
    assert!(
        published
            .subscribe_over_client_offer("PA_test|TR_audio_sdp")
            .is_empty(),
        "an unchanged committed mime set announces nothing, even over real SDP"
    );
}

#[test]
fn the_server_offer_answer_path_announces_the_mime_its_receiver_row_negotiated() {
    // The second production call site. A subscriber PC answers the offer the
    // engine wrote, so the demand comes from `signaling.rs:1006` instead of the
    // offer path, and each publication still announces its own negotiated mime.
    let mut opus_first = SdpAudioPublish::new(OPUS);
    assert_eq!(
        opus_first.subscribe_over_server_offer("PA_sub_opus"),
        vec![(
            ParticipantSid::from(PARTICIPANT),
            TrackSid::from(SDP_SID),
            vec![OPUS.to_string()]
        )],
        "an opus-first publication announces audio/opus to its answering subscriber"
    );

    let mut red_first = SdpAudioPublish::new(RED);
    assert_eq!(
        red_first.subscribe_over_server_offer("PA_sub_red"),
        vec![(
            ParticipantSid::from(PARTICIPANT),
            TrackSid::from(SDP_SID),
            vec![RED.to_string()]
        )],
        "a RED-first publication announces audio/red over the same answer path"
    );
}
