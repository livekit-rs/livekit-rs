//! The dynacast → signal seam: a `MediaEvent` that the engine produced from
//! SDP negotiation becomes exactly one `SignalEvent` addressed to the
//! publisher participant (Go `participant.go:3723` wires the media track's
//! dynacast listener into the publisher participant, and
//! `participant_signal.go:385/389` writes the two messages on that
//! participant's signal connection).
//!
//! These legs own only the routing half — which participant receives which
//! variant, once, with the codec list passed through untouched. The wire
//! payload (the `audio/` trim, lowercase, empty-list suppression) is owned by
//! the lk-compat encoder tests; the producer itself is falsified by removing
//! the `notify_audio_codec_negotiated` call sites in `crates/rtc-media/src/
//! signaling.rs` and re-running the rtc-media lib suite.

use super::*;
use crate::model::{SubscribedAudioCodec, SubscribedCodec, SubscribedQuality, VideoQuality};

fn audio_codecs() -> Vec<SubscribedAudioCodec> {
    vec![
        SubscribedAudioCodec {
            codec: "audio/opus".into(),
            enabled: true,
        },
        SubscribedAudioCodec {
            codec: "audio/red".into(),
            enabled: false,
        },
    ]
}

#[test]
fn an_engine_audio_codec_announcement_signals_the_publisher_once() {
    let (mut room, now) = room();
    let publisher = room.join(params("publisher", 17, false), now).unwrap();
    // A second participant owns the "publisher only" claim: without it the
    // addressing assertion could not fail for any wiring.
    let subscriber = room.join(params("subscriber", 17, false), now).unwrap();
    room.take_effects();

    room.handle_media_event(
        MediaEvent::SubscribedAudioCodec {
            publisher: publisher.sid.clone(),
            track_sid: TrackSid::from("TR_a"),
            codecs: audio_codecs(),
        },
        now,
    );

    let effects = room.take_effects();
    let signals: Vec<&SignalEvent> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Signal { to, event } if to == &publisher.sid => Some(event),
            _ => None,
        })
        .collect();
    assert_eq!(signals.len(), 1, "one announcement, one signal message");
    assert!(
        matches!(
            signals[0],
            SignalEvent::SubscribedAudioCodec { track_sid, codecs }
                if track_sid == &TrackSid::from("TR_a") && *codecs == audio_codecs()
        ),
        "the full-mime demand must reach the publisher unchanged \
         (dynacastmanageraudio.go:178-198 emits mime.String(); the trim \
         happens later in the encoder, participant.go:2969): {signals:?}"
    );
    assert!(
        !effects.iter().any(|e| matches!(
            e,
            Effect::Signal { to, .. } if to == &subscriber.sid
        )),
        "the subscriber that negotiated the codecs is not the address of a \
         demand notification (participant.go:3723 wires the listener on the \
         publisher participant): {effects:?}"
    );
}

#[test]
fn an_engine_quality_announcement_signals_the_publisher_untouched() {
    let (mut room, now) = room();
    let publisher = room.join(params("publisher", 17, false), now).unwrap();
    room.take_effects();

    let codecs = vec![SubscribedCodec {
        codec: "video/VP8".into(),
        qualities: vec![SubscribedQuality {
            quality: VideoQuality::High,
            enabled: true,
        }],
    }];
    room.handle_media_event(
        MediaEvent::SubscribedQuality {
            publisher: publisher.sid.clone(),
            track_sid: TrackSid::from("TR_v"),
            codecs: codecs.clone(),
        },
        now,
    );

    let effects = room.take_effects();
    assert!(
        matches!(
            effects.as_slice(),
            [Effect::Signal {
                to,
                event: SignalEvent::SubscribedQuality {
                    track_sid,
                    codecs: out,
                },
            }] if to == &publisher.sid
                && track_sid == &TrackSid::from("TR_v")
                && out == &codecs
        ),
        "the video demand routes like the audio one, no merge between them \
         (participant.go:2944-2952 vs :2955-2982 are separate messages): {effects:?}"
    );
}

#[test]
fn a_single_audio_codec_demand_still_produces_the_dedicated_signal() {
    // Go's manager commits whatever mimes subscribers negotiated; even a
    // single-entry demand uses the dedicated SubscribedAudioCodecUpdate
    // (participant_signal.go:389-391), never the quality message. The
    // sibling assertions above prove the two variants stay separate.
    let (mut room, now) = room();
    let publisher = room.join(params("publisher", 17, false), now).unwrap();
    room.take_effects();

    room.handle_media_event(
        MediaEvent::SubscribedAudioCodec {
            publisher: publisher.sid.clone(),
            track_sid: TrackSid::from("TR_a"),
            codecs: vec![SubscribedAudioCodec {
                codec: "audio/opus".into(),
                enabled: true,
            }],
        },
        now,
    );

    let effects = room.take_effects();
    assert!(
        matches!(
            effects.as_slice(),
            [Effect::Signal {
                to,
                event: SignalEvent::SubscribedAudioCodec { codecs, .. },
            }] if to == &publisher.sid && codecs.len() == 1
        ),
        "{effects:?}"
    );
}
