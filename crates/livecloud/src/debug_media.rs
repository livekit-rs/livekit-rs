//! LiveKit DebugInfo rendering over same-owner native media observations.
//! No source-address, sequence offset or sender report is guessed by this layer.
use rtc_core::{ids::ParticipantSid, media_debug::*, model::VideoLayerMode};
use serde_json::{Map, Value, json};
use std::borrow::Cow;

fn layer_mode(mode: VideoLayerMode) -> Cow<'static, str> {
    Cow::Borrowed(match mode {
        VideoLayerMode::Unused => "MODE_UNUSED",
        VideoLayerMode::OneSpatialLayerPerStream => "ONE_SPATIAL_LAYER_PER_STREAM",
        VideoLayerMode::MultipleSpatialLayersPerStream => "MULTIPLE_SPATIAL_LAYERS_PER_STREAM",
        VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr => {
            "ONE_SPATIAL_LAYER_PER_STREAM_INCOMPLETE_RTCP_SR"
        }
        VideoLayerMode::Unrecognized(_) => return Cow::Owned(mode.as_i32().to_string()),
    })
}
fn down_track(track: &DownTrackDebug) -> Value {
    let mut munger = json!({"TSOffset":track.timestamp_offset});
    if let Some(packet) = track.last_packet {
        munger["ExtLastSN"] = json!(packet.sequence);
        munger["ExtLastTS"] = json!(packet.timestamp);
        munger["LastMarker"] = json!(packet.marker);
    }
    if let Some(packet) = track.second_last_packet {
        munger["ExtSecondLastSN"] = json!(packet.sequence);
        munger["ExtSecondLastTS"] = json!(packet.timestamp);
        munger["SecondLastMarker"] = json!(packet.marker);
    }
    let mut stats = json!({"LastPli":track.last_pli_unix_ns,"RTPMunger":munger});
    if let Some(report) = track.sender_report {
        stats["NTPTime"] = json!(report.ntp_time);
        stats["RTPTime"] = json!(report.rtp_time);
        stats["PacketCount"] = json!(report.packet_count);
    }
    let bind = match track.bind_state {
        BindDebug::Unbound => 0,
        BindDebug::WaitForReceiverReady => 1,
        BindDebug::Bound => 2,
    };
    let mut output = json!({"SubscriberID":track.subscriber.0,"TrackID":track.track.0,"StreamID":track.stream,"MimeType":track.mime,"BindState":bind,"Muted":track.muted,"PubMuted":track.publisher_muted,"CurrentSpatialLayer":track.current_spatial_layer,"Stats":stats});
    if let Some(ssrc) = track.ssrc {
        output["SSRC"] = json!(ssrc);
    }
    output
}
pub(super) fn enrich_tracks(
    media: &MediaDebugSnapshot,
    publisher: &ParticipantSid,
    tracks: &mut Map<String, Value>,
) {
    for (sid, published) in &media.published {
        if &published.publisher != publisher {
            continue;
        }
        let Some(track) = tracks.get_mut(&sid.0) else {
            continue;
        };
        track["DownTracks"] = Value::Array(published.down_tracks.iter().map(down_track).collect());
        for receiver in &published.receivers {
            let upstream = receiver
                .up_tracks
                .iter()
                .map(|up| {
                    let mut value = json!({"Layer":up.layer,"SSRC":up.ssrc,"RID":up.rid});
                    if !up.msid.is_empty() {
                        value["Msid"] = json!(up.msid);
                    }
                    value
                })
                .collect::<Vec<_>>();
            track[&receiver.mime] = json!({"Mime":receiver.mime,"VideoLayerMode":layer_mode(receiver.video_layer_mode),"UpTracks":upstream});
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn renders_only_owned_observed_media_without_inventing_munger_state() {
        let publisher = ParticipantSid::from("PA_owner");
        let sid = rtc_core::ids::TrackSid::from("TR_live");
        let mut media = MediaDebugSnapshot::default();
        media.published.insert(
            sid.clone(),
            PublishedDebug {
                publisher: publisher.clone(),
                receivers: vec![],
                down_tracks: vec![DownTrackDebug {
                    subscriber: "PA_sub".into(),
                    track: sid.clone(),
                    stream: "stream".into(),
                    ssrc: None,
                    mime: "video/vp8".into(),
                    bind_state: BindDebug::Unbound,
                    muted: false,
                    publisher_muted: false,
                    current_spatial_layer: -1,
                    last_pli_unix_ns: 0,
                    last_packet: None,
                    second_last_packet: None,
                    sender_report: None,
                    timestamp_offset: 0,
                }],
            },
        );
        let mut tracks = Map::new();
        tracks.insert(sid.0.clone(), json!({"ID":sid.0}));
        enrich_tracks(&media, &"PA_other".into(), &mut tracks);
        assert!(tracks[&sid.0].get("DownTracks").is_none());
        enrich_tracks(&media, &publisher, &mut tracks);
        let down = &tracks[&sid.0]["DownTracks"][0];
        assert_eq!(down["BindState"], 0);
        assert!(down.get("SSRC").is_none());
        let stats = &down["Stats"];
        assert!(stats.get("NTPTime").is_none());
        let munger = &stats["RTPMunger"];
        for key in ["SNOffset", "ExtHighestIncomingSN", "ExtLastSN", "ExtLastTS"] {
            assert!(
                munger.get(key).is_none(),
                "unobserved {key} must not be invented"
            );
        }
        // The media snapshot cannot resurrect an already-unpublished core track.
        tracks.clear();
        enrich_tracks(&media, &publisher, &mut tracks);
        assert!(tracks.is_empty());
    }
}
