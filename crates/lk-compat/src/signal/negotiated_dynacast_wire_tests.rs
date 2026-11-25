//! Wire payload of the dynacast signal messages, asserted from the engine's
//! own vocabulary down to protobuf bytes.
//!
//! Ground truth (pinned livekit v1.13.10 e5655ac):
//! - `participant.go:2955-2982`: an empty codec list sends nothing; each mime
//!   is trimmed of the exact `audio/` prefix and lower-cased
//!   (`:2966-2969`) before `SubscribedAudioCodecUpdate` goes on the wire via
//!   `participant_signal.go:389`.
//! - `participant.go:2904-2952`: the same shape for `SubscribedQualityUpdate`
//!   with the `video/` prefix (`:2939-2941`).
//! The values fed here are exactly what `DynacastAudio::payload()` commits
//! (`crates/rtc-media/src/dynacast.rs`, Go `dynacastmanageraudio.go:178-198`):
//! full canonical mimes with the `audio/` prefix, which only this encoder
//! shortens.

use super::*;
use crate::signal::encode::EncodeCtx;
use lk_protocol::livekit::signal_response::Message as Resp;
use prost::Message as _;
use rtc_core::ids::TrackSid;
use rtc_core::model::{SubscribedAudioCodec, SubscribedCodec, SubscribedQuality, VideoQuality};

fn encode_bytes(ev: SignalEvent) -> Vec<u8> {
    let client = ClientMeta::default();
    let mut cache = RequestCache::default();
    let resp = encode_signal(ev, &EncodeCtx { client: &client }, &mut cache)
        .expect("event encodes to a response");
    resp.encode_to_vec()
}

fn audio_update(bytes: &[u8]) -> pb::SubscribedAudioCodecUpdate {
    let resp = pb::SignalResponse::decode(bytes).expect("decodes back to a signal response");
    match resp.message.expect("one message") {
        Resp::SubscribedAudioCodecUpdate(u) => u,
        other => panic!("expected subscribed_audio_codec_update (field 26), got {other:?}"),
    }
}

#[test]
fn the_committed_full_mimes_reach_the_wire_trimmed_and_lower_cased() {
    // Exactly the payload DynacastAudio commits after two subscribers
    // negotiate opus and RED: full canonical mimes (Go mime.String()).
    let bytes = encode_bytes(SignalEvent::SubscribedAudioCodec {
        track_sid: TrackSid::from("TR_a"),
        codecs: vec![
            SubscribedAudioCodec {
                codec: "audio/opus".into(),
                enabled: true,
            },
            SubscribedAudioCodec {
                codec: "audio/red".into(),
                enabled: false,
            },
        ],
    });
    let update = audio_update(&bytes);
    assert_eq!(update.track_sid, "TR_a");
    assert_eq!(
        update.subscribed_audio_codecs,
        vec![
            pb::SubscribedAudioCodec {
                codec: "opus".into(),
                enabled: true,
            },
            pb::SubscribedAudioCodec {
                codec: "red".into(),
                enabled: false,
            },
        ],
        "strings.ToLower(strings.TrimPrefix(codec, \"audio/\")) \
         (participant.go:2966-2969) over the committed order"
    );
}

#[test]
fn the_prefix_trim_is_exact_and_case_sensitive_on_input() {
    // Go trims the literal prefix "audio/" (mime.MimeTypePrefixAudio) with
    // strings.TrimPrefix — no case folding, no generic scheme strip. Anything
    // else keeps its text and is only lower-cased.
    let bytes = encode_bytes(SignalEvent::SubscribedAudioCodec {
        track_sid: TrackSid::from("TR_a"),
        codecs: vec![
            SubscribedAudioCodec {
                codec: "AUDIO/PCMU".into(),
                enabled: true,
            },
            SubscribedAudioCodec {
                codec: "audio/RED".into(),
                enabled: true,
            },
        ],
    });
    let update = audio_update(&bytes);
    assert_eq!(
        update.subscribed_audio_codecs,
        vec![
            pb::SubscribedAudioCodec {
                codec: "audio/pcmu".into(),
                enabled: true,
            },
            pb::SubscribedAudioCodec {
                codec: "red".into(),
                enabled: true,
            },
        ],
        "\"AUDIO/\" is not the exact prefix, so only lowercase applies; \
         \"audio/RED\" loses the prefix then lowercases (participant.go:2966-2969)"
    );
}

#[test]
fn an_empty_demand_from_negotiation_encodes_no_frame_at_all() {
    // Go returns before building the message when the list is empty
    // (participant.go:2963-2965), so the publisher never sees a zero-length
    // update. This is the falsifiable half of "empty list emits nothing":
    // the event exists, the wire stays silent.
    let client = ClientMeta::default();
    let mut cache = RequestCache::default();
    assert!(
        encode_signal(
            SignalEvent::SubscribedAudioCodec {
                track_sid: TrackSid::from("TR_a"),
                codecs: vec![],
            },
            &EncodeCtx { client: &client },
            &mut cache,
        )
        .is_none(),
        "an empty committed list must produce no SignalResponse frame"
    );
}

#[test]
fn the_quality_encoder_normalizes_video_mimes_the_same_way() {
    // The sibling message the negotiation also produces
    // (SignalEvent::SubscribedQuality ← MediaEvent::SubscribedQuality ←
    // forward.rs emit_dynacast_snapshot, whose codec names come from the
    // receiver rows' full mimes). Go: participant.go:2939-2941 trims the
    // exact "video/" prefix and lower-cases; subscribed_qualities (field 2,
    // deprecated) mirrors the first codec's qualities (:2944-2948).
    let bytes = encode_bytes(SignalEvent::SubscribedQuality {
        track_sid: TrackSid::from("TR_v"),
        codecs: vec![SubscribedCodec {
            codec: "video/VP8".into(),
            qualities: vec![
                SubscribedQuality {
                    quality: VideoQuality::Low,
                    enabled: false,
                },
                SubscribedQuality {
                    quality: VideoQuality::High,
                    enabled: true,
                },
            ],
        }],
    });
    let resp = pb::SignalResponse::decode(bytes.as_slice()).expect("decodes");
    #[allow(deprecated)]
    match resp.message.expect("one message") {
        Resp::SubscribedQualityUpdate(u) => {
            assert_eq!(u.track_sid, "TR_v");
            assert_eq!(u.subscribed_codecs.len(), 1);
            assert_eq!(u.subscribed_codecs[0].codec, "vp8", "trim + lowercase");
            assert_eq!(
                u.subscribed_codecs[0].qualities,
                vec![
                    pb::SubscribedQuality {
                        quality: 0,
                        enabled: false,
                    },
                    pb::SubscribedQuality {
                        quality: 2,
                        enabled: true,
                    },
                ]
            );
            assert_eq!(
                u.subscribed_qualities, u.subscribed_codecs[0].qualities,
                "the legacy field carries the first codec's qualities"
            );
        }
        other => panic!("expected subscribed_quality_update (field 14), got {other:?}"),
    }
}
