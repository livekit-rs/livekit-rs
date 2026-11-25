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

fn one_quality_codec() -> Vec<SubscribedCodec> {
    vec![SubscribedCodec {
        codec: "video/VP8".into(),
        qualities: vec![SubscribedQuality {
            quality: VideoQuality::High,
            enabled: true,
        }],
    }]
}

#[test]
fn the_dynacast_switch_off_stops_both_demand_messages() {
    // Both callbacks return before they build anything when the participant
    // param is set (`participant.go:2906` quality, `:2959` audio). Nothing else
    // on the seam changes, so the same demand must arrive once the switch is
    // back off — otherwise the empty batch below proves nothing.
    let (mut room, now) = room();
    let publisher = room.join(params("publisher", 17, false), now).unwrap();
    room.participants
        .get_mut(&publisher.sid)
        .unwrap()
        .disable_dynacast = true;
    room.take_effects();

    room.handle_media_event(
        MediaEvent::SubscribedQuality {
            publisher: publisher.sid.clone(),
            track_sid: TrackSid::from("TR_v"),
            codecs: one_quality_codec(),
        },
        now,
    );
    room.handle_media_event(
        MediaEvent::SubscribedAudioCodec {
            publisher: publisher.sid.clone(),
            track_sid: TrackSid::from("TR_a"),
            codecs: audio_codecs(),
        },
        now,
    );
    assert!(
        room.take_effects().is_empty(),
        "a publisher with dynacast off receives neither demand message \
         (participant.go:2906, :2959)"
    );

    room.participants
        .get_mut(&publisher.sid)
        .unwrap()
        .disable_dynacast = false;
    room.handle_media_event(
        MediaEvent::SubscribedQuality {
            publisher: publisher.sid.clone(),
            track_sid: TrackSid::from("TR_v"),
            codecs: one_quality_codec(),
        },
        now,
    );
    room.handle_media_event(
        MediaEvent::SubscribedAudioCodec {
            publisher: publisher.sid.clone(),
            track_sid: TrackSid::from("TR_a"),
            codecs: audio_codecs(),
        },
        now,
    );
    assert_eq!(
        room.take_effects().len(),
        2,
        "the switch is the only difference, so both demands do arrive when it is off"
    );
}

#[test]
fn an_empty_demand_writes_no_signal_message() {
    // The empty check is the participant's own second return
    // (`participant.go:2910` quality, `:2963` audio): the manager reported the
    // change, so the seam sees an event, and no message may reach the signal
    // connection for a demand with nothing in it.
    let (mut room, now) = room();
    let publisher = room.join(params("publisher", 17, false), now).unwrap();
    room.take_effects();

    room.handle_media_event(
        MediaEvent::SubscribedQuality {
            publisher: publisher.sid.clone(),
            track_sid: TrackSid::from("TR_v"),
            codecs: Vec::new(),
        },
        now,
    );
    room.handle_media_event(
        MediaEvent::SubscribedAudioCodec {
            publisher: publisher.sid.clone(),
            track_sid: TrackSid::from("TR_a"),
            codecs: Vec::new(),
        },
        now,
    );
    assert!(
        room.take_effects().is_empty(),
        "an empty demand is not a message (participant.go:2910, :2963)"
    );

    room.handle_media_event(
        MediaEvent::SubscribedAudioCodec {
            publisher: publisher.sid.clone(),
            track_sid: TrackSid::from("TR_a"),
            codecs: audio_codecs(),
        },
        now,
    );
    assert_eq!(
        room.take_effects().len(),
        1,
        "the same publisher does receive a demand that has entries, so the \
         empty batch above is the filter and not a dead route"
    );
}

#[test]
fn a_demand_for_a_participant_that_is_gone_is_not_signalled() {
    // The gate reads the publisher participant, so a demand that arrives after
    // that participant left has no address and no switch to consult; Go's
    // callback would not exist for a participant it no longer holds.
    let (mut room, now) = room();
    let publisher = room.join(params("publisher", 17, false), now).unwrap();
    let sid = publisher.sid.clone();
    room.take_effects();

    room.handle_media_event(
        MediaEvent::SubscribedQuality {
            publisher: sid.clone(),
            track_sid: TrackSid::from("TR_v"),
            codecs: one_quality_codec(),
        },
        now,
    );
    assert_eq!(room.take_effects().len(), 1, "live publisher, one message");

    room.remove_participant(&sid, DisconnectReason::ParticipantRemoved, true, now);
    room.take_effects();
    room.handle_media_event(
        MediaEvent::SubscribedQuality {
            publisher: sid,
            track_sid: TrackSid::from("TR_v"),
            codecs: one_quality_codec(),
        },
        now,
    );
    assert!(
        room.take_effects().is_empty(),
        "no participant to address means no demand message"
    );
}
