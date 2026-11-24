//! Audio dynacast codec demand: what a negotiated subscriber tells the publisher.
//!
//! Go builds the manager only for an audio track that declares more than one
//! codec (mediatrack.go:161-167), reports each negotiated down track as an
//! enablement for that one subscriber
//! (subscribedtrack.go:456-482 -> mediatrack.go:189-195 ->
//! dynacastmanageraudio.go:94-103), and hands the publisher the whole committed
//! map with `mime.String()` payloads (dynacastmanageraudio.go:178-198).

use super::*;
use rtc_core::media_api::{DownTrackSpec, PublishedTrackSpec};
use rtc_core::model::{BackupCodecPolicy, TrackCodec, TrackKind, TrackSettings, TrackSource};

const SID: &str = "TR_audio_dynacast";
const OPUS: &str = "audio/opus";
const RED: &str = "audio/red";

fn published(codecs: &[&str], kind: TrackKind) -> PublishedTrackSpec {
    PublishedTrackSpec {
        backup_codec_policy: BackupCodecPolicy::Simulcast,
        sid: TrackSid::from(SID),
        kind,
        source: TrackSource::Microphone,
        cid: "audio".into(),
        layers: vec![],
        codecs: codecs
            .iter()
            .map(|mime| TrackCodec {
                mime_type: (*mime).to_string(),
                ..Default::default()
            })
            .collect(),
        stereo: false,
        disable_dtx: false,
        disable_red: false,
        encrypted: false,
        muted: false,
    }
}

struct Fixture {
    engine: MediaEngine,
    /// `ui` of the primary receiver, i.e. the first declared codec.
    ui: usize,
    downs: Vec<usize>,
}

/// Publish one audio track with `publisher_codecs`, then bind one down track
/// per `subscriber_mimes` exactly as negotiation would leave them.
fn fixture(publisher_codecs: &[&str], subscriber_mimes: &[&str]) -> Fixture {
    let now = Instant::now();
    let mut engine = engine(now);
    let spec = published(publisher_codecs, TrackKind::Audio);
    let sid = TrackSid::from(SID);
    engine.published.insert(sid.clone(), spec.clone());
    let mut receivers = Vec::new();
    for (index, mime) in publisher_codecs.iter().enumerate() {
        let ui = engine.ups.insert(crate::uptrack::UpTrack::new(
            spec.clone(),
            "PA_pub".into(),
            TransportId::next(),
            Mid::from(index.to_string().as_str()),
            TrackKind::Audio,
            (*mime).to_string(),
            vec![],
            &engine.audio_params,
            Duration::ZERO,
            now,
        ));
        engine
            .codec_receivers
            .entry(sid.clone())
            .or_default()
            .bind(mime, ui);
        receivers.push(ui);
    }
    engine.up_by_sid.insert(sid.clone(), receivers[0]);
    for &ui in &receivers {
        engine.ensure_audio_dynacast(ui);
    }
    let downs = subscriber_mimes
        .iter()
        .enumerate()
        .map(|(index, mime)| {
            let up = *receivers
                .iter()
                .find(|r| engine.ups.get(**r).is_some_and(|u| &u.mime == mime))
                .unwrap_or(&receivers[0]);
            engine.downs.insert(crate::downtrack::DownTrack::new(
                DownTrackSpec {
                    subscriber: format!("PA_sub_{index}").into(),
                    publisher: "PA_pub".into(),
                    track_sid: sid.clone(),
                    stream_id: "audio".into(),
                    settings: TrackSettings::default(),
                    adaptive_stream: false,
                },
                TrackKind::Audio,
                up,
                false,
                mime,
                now,
            ))
        })
        .collect();
    engine.events.clear();
    Fixture {
        engine,
        ui: receivers[0],
        downs,
    }
}

/// Drain and check every queued event, returning the committed payload of each.
fn drain(engine: &mut MediaEngine) -> Vec<Vec<(String, bool)>> {
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    events
        .into_iter()
        .map(|event| {
            let MediaEvent::SubscribedAudioCodec {
                publisher,
                track_sid,
                codecs,
            } = event
            else {
                panic!("unexpected event {event:?}");
            };
            assert_eq!(publisher, ParticipantSid::from("PA_pub"));
            assert_eq!(track_sid, TrackSid::from(SID));
            codecs.into_iter().map(|c| (c.codec, c.enabled)).collect()
        })
        .collect()
}

#[test]
fn a_single_codec_audio_track_has_no_manager() {
    let mut f = fixture(&[OPUS], &[OPUS]);
    assert!(
        f.engine.audio_dynacast.is_empty(),
        "Go builds the manager only when len(ti.Codecs) > 1"
    );
    f.engine.notify_audio_codec_negotiated(f.downs[0]);
    assert!(
        drain(&mut f.engine).is_empty(),
        "without a manager there is no listener to notify"
    );
}

#[test]
fn a_negotiated_audio_down_track_reports_the_full_mime() {
    let mut f = fixture(&[OPUS, RED], &[OPUS]);
    assert_eq!(f.engine.audio_dynacast.len(), 1);
    f.engine.notify_audio_codec_negotiated(f.downs[0]);
    assert_eq!(
        drain(&mut f.engine),
        vec![vec![(OPUS.to_string(), true)]],
        "`mime.String()` keeps the `audio/` prefix; the session encoder trims it"
    );
}

#[test]
fn a_second_subscriber_of_the_same_codec_announces_nothing() {
    let mut f = fixture(&[OPUS, RED], &[OPUS, OPUS]);
    f.engine.notify_audio_codec_negotiated(f.downs[0]);
    assert_eq!(drain(&mut f.engine).len(), 1);
    f.engine.notify_audio_codec_negotiated(f.downs[1]);
    assert!(
        drain(&mut f.engine).is_empty(),
        "an unchanged committed map enqueues nothing"
    );
}

#[test]
fn a_newly_negotiated_codec_widens_the_announcement() {
    let mut f = fixture(&[OPUS, RED], &[OPUS, RED]);
    f.engine.notify_audio_codec_negotiated(f.downs[0]);
    drain(&mut f.engine);
    f.engine.notify_audio_codec_negotiated(f.downs[1]);
    assert_eq!(
        drain(&mut f.engine),
        vec![vec![(OPUS.to_string(), true), (RED.to_string(), true)]],
        "the whole committed map is re-sent, so the publisher starts the new codec"
    );
}

#[test]
fn a_down_track_without_a_bound_receiver_announces_nothing() {
    let mut f = fixture(&[OPUS, RED], &[OPUS]);
    f.engine.codec_receivers.remove(&TrackSid::from(SID));
    f.engine.notify_audio_codec_negotiated(f.downs[0]);
    assert!(
        drain(&mut f.engine).is_empty(),
        "the mime is taken from the publisher's codec receiver group"
    );
}

#[test]
fn a_video_down_track_never_reaches_the_audio_manager() {
    let now = Instant::now();
    let mut f = fixture(&[OPUS, RED], &[OPUS]);
    let di = f.engine.downs.insert(crate::downtrack::DownTrack::new(
        DownTrackSpec {
            subscriber: "PA_sub_video".into(),
            publisher: "PA_pub".into(),
            track_sid: TrackSid::from(SID),
            stream_id: "audio".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        TrackKind::Video,
        f.ui,
        false,
        OPUS,
        now,
    ));
    f.engine.events.clear();
    f.engine.notify_audio_codec_negotiated(di);
    assert!(
        drain(&mut f.engine).is_empty(),
        "video demand goes to the quality manager, never the audio one"
    );
}
