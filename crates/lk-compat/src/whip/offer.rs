//! Adapter track synthesis AFTER actual native full-offer validation, never a fake SDP validator.
//! Fixed participant.go1059, protocol/sdp.GetMediaStreamTrack/GetSimulcastRids.
use super::fragment::Document;
use lk_protocol::livekit as pb;
use rtc_core::ids::TrackSid;
use rtc_core::session::AddTrack;
pub fn synthesize(offer: &str) -> Vec<AddTrack> {
    let document = Document::from_validated(offer);
    let mut tracks = Vec::new();
    for lines in document.media {
        let kind = lines
            .first()
            .and_then(|line| line.strip_prefix("m="))
            .and_then(|v| v.split(' ').next())
            .unwrap_or_default();
        let audio = kind.eq_ignore_ascii_case("audio");
        if !audio && !kind.eq_ignore_ascii_case("video") {
            continue;
        }
        let attr = |key: &str| {
            let prefix = format!("a={key}:");
            lines.iter().find_map(|line| line.strip_prefix(&prefix))
        };
        let mut cid = attr("msid")
            .and_then(|v| {
                let parts: Vec<_> = v.split(' ').collect();
                if parts.len() == 2 {
                    Some(parts[1].to_owned())
                } else {
                    None
                }
            })
            .unwrap_or_default();
        if cid.is_empty()
            && let Some(value) = attr("ssrc")
        {
            let p: Vec<_> = value.split(' ').collect();
            if p.len() == 3 && p[1].to_ascii_lowercase().starts_with("msid:") {
                cid = p[2].to_owned();
            }
        }
        if cid.is_empty() {
            cid = TrackSid::generate().0;
        }
        let mut request = pb::AddTrackRequest {
            cid,
            name: if audio {
                "synthesized-microphone"
            } else {
                "synthesized-camera"
            }
            .into(),
            source: if audio {
                pb::TrackSource::Microphone as i32
            } else {
                pb::TrackSource::Camera as i32
            },
            r#type: if audio {
                pb::TrackType::Audio as i32
            } else {
                pb::TrackType::Video as i32
            },
            disable_dtx: true,
            stereo: false,
            stream: "camera".into(),
            ..Default::default()
        };
        if !audio {
            let count = attr("simulcast")
                .and_then(|v| {
                    let p: Vec<_> = v.split(' ').collect();
                    if p.len() == 2 && p[0] == "send" {
                        Some(p[1].split(';').count().min(3))
                    } else {
                        None
                    }
                })
                .unwrap_or(1);
            request.layers = (0..count)
                .map(|quality| pb::VideoLayer {
                    quality: quality as i32,
                    ..Default::default()
                })
                .collect();
        }
        tracks.push(crate::signal::params::add_track_from_proto(
            &request,
            tracks.len() as u32 + 1,
        ));
    }
    tracks
}
