//! Known-field automatic-recording policy projection. Proto stays at the adapter
//! boundary. Not yet installed in public RoomConfig/presets/JWT/runtime hooks.
use lk_protocol::livekit as pb;
use pb::{auto_track_egress, encoded_file_output, segmented_file_output};
use rtc_core::auto_egress as core;

fn proxy_in(p: &pb::ProxyConfig) -> core::UploadProxy {
    core::UploadProxy {
        url: p.url.clone(),
        username: p.username.clone(),
        password: p.password.clone(),
    }
}
fn proxy_out(p: &core::UploadProxy) -> pb::ProxyConfig {
    pb::ProxyConfig {
        url: p.url.clone(),
        username: p.username.clone(),
        password: p.password.clone(),
    }
}
fn s3_in(v: &pb::S3Upload) -> core::S3Destination {
    core::S3Destination {
        access_key: v.access_key.clone(),
        secret: v.secret.clone(),
        region: v.region.clone(),
        endpoint: v.endpoint.clone(),
        bucket: v.bucket.clone(),
        force_path_style: v.force_path_style,
        metadata: v.metadata.clone().into_iter().collect(),
        tagging: v.tagging.clone(),
        content_disposition: v.content_disposition.clone(),
        proxy: v.proxy.as_ref().map(proxy_in),
        session_token: v.session_token.clone(),
        assume_role_arn: v.assume_role_arn.clone(),
        assume_role_external_id: v.assume_role_external_id.clone(),
    }
}
fn s3_out(v: &core::S3Destination) -> pb::S3Upload {
    pb::S3Upload {
        access_key: v.access_key.clone(),
        secret: v.secret.clone(),
        region: v.region.clone(),
        endpoint: v.endpoint.clone(),
        bucket: v.bucket.clone(),
        force_path_style: v.force_path_style,
        metadata: v.metadata.clone().into_iter().collect(),
        tagging: v.tagging.clone(),
        content_disposition: v.content_disposition.clone(),
        proxy: v.proxy.as_ref().map(proxy_out),
        session_token: v.session_token.clone(),
        assume_role_arn: v.assume_role_arn.clone(),
        assume_role_external_id: v.assume_role_external_id.clone(),
    }
}
fn gcp_in(v: &pb::GcpUpload) -> core::GcpDestination {
    core::GcpDestination {
        credentials: v.credentials.clone(),
        bucket: v.bucket.clone(),
        proxy: v.proxy.as_ref().map(proxy_in),
    }
}
fn gcp_out(v: &core::GcpDestination) -> pb::GcpUpload {
    pb::GcpUpload {
        credentials: v.credentials.clone(),
        bucket: v.bucket.clone(),
        proxy: v.proxy.as_ref().map(proxy_out),
    }
}
fn azure_in(v: &pb::AzureBlobUpload) -> core::AzureDestination {
    core::AzureDestination {
        account_name: v.account_name.clone(),
        account_key: v.account_key.clone(),
        container_name: v.container_name.clone(),
    }
}
fn azure_out(v: &core::AzureDestination) -> pb::AzureBlobUpload {
    pb::AzureBlobUpload {
        account_name: v.account_name.clone(),
        account_key: v.account_key.clone(),
        container_name: v.container_name.clone(),
    }
}
fn ali_in(v: &pb::AliOssUpload) -> core::AliOssDestination {
    core::AliOssDestination {
        access_key: v.access_key.clone(),
        secret: v.secret.clone(),
        region: v.region.clone(),
        endpoint: v.endpoint.clone(),
        bucket: v.bucket.clone(),
    }
}
fn ali_out(v: &core::AliOssDestination) -> pb::AliOssUpload {
    pb::AliOssUpload {
        access_key: v.access_key.clone(),
        secret: v.secret.clone(),
        region: v.region.clone(),
        endpoint: v.endpoint.clone(),
        bucket: v.bucket.clone(),
    }
}
macro_rules! storage_in {
    ($value:expr,$m:ident) => {
        match $value {
            Some($m::Output::S3(v)) => Some(core::UploadDestination::S3(Box::new(s3_in(v)))),
            Some($m::Output::Gcp(v)) => Some(core::UploadDestination::Gcp(gcp_in(v))),
            Some($m::Output::Azure(v)) => Some(core::UploadDestination::Azure(azure_in(v))),
            Some($m::Output::AliOss(v)) => Some(core::UploadDestination::AliOss(ali_in(v))),
            None => None,
        }
    };
}
macro_rules! storage_out {
    ($value:expr,$m:ident) => {
        match $value {
            Some(core::UploadDestination::S3(v)) => Some($m::Output::S3(s3_out(v))),
            Some(core::UploadDestination::Gcp(v)) => Some($m::Output::Gcp(gcp_out(v))),
            Some(core::UploadDestination::Azure(v)) => Some($m::Output::Azure(azure_out(v))),
            Some(core::UploadDestination::AliOss(v)) => Some($m::Output::AliOss(ali_out(v))),
            None => None,
        }
    };
}
pub fn track_in(v: &pb::AutoTrackEgress) -> core::TrackRecordingPolicy {
    core::TrackRecordingPolicy {
        filepath: v.filepath.clone(),
        disable_manifest: v.disable_manifest,
        destination: storage_in!(v.output.as_ref(), auto_track_egress),
    }
}
pub fn track_out(v: &core::TrackRecordingPolicy) -> pb::AutoTrackEgress {
    pb::AutoTrackEgress {
        filepath: v.filepath.clone(),
        disable_manifest: v.disable_manifest,
        output: storage_out!(v.destination.as_ref(), auto_track_egress),
    }
}
fn file_in(v: &pb::EncodedFileOutput) -> core::EncodedFilePolicy {
    core::EncodedFilePolicy {
        file_type: v.file_type,
        filepath: v.filepath.clone(),
        disable_manifest: v.disable_manifest,
        destination: storage_in!(v.output.as_ref(), encoded_file_output),
    }
}
fn file_out(v: &core::EncodedFilePolicy) -> pb::EncodedFileOutput {
    pb::EncodedFileOutput {
        file_type: v.file_type,
        filepath: v.filepath.clone(),
        disable_manifest: v.disable_manifest,
        output: storage_out!(v.destination.as_ref(), encoded_file_output),
    }
}
fn segments_in(v: &pb::SegmentedFileOutput) -> core::SegmentedFilePolicy {
    core::SegmentedFilePolicy {
        protocol: v.protocol,
        filename_prefix: v.filename_prefix.clone(),
        playlist_name: v.playlist_name.clone(),
        segment_duration: v.segment_duration,
        disable_manifest: v.disable_manifest,
        filename_suffix: v.filename_suffix,
        live_playlist_name: v.live_playlist_name.clone(),
        destination: storage_in!(v.output.as_ref(), segmented_file_output),
    }
}
fn segments_out(v: &core::SegmentedFilePolicy) -> pb::SegmentedFileOutput {
    pb::SegmentedFileOutput {
        protocol: v.protocol,
        filename_prefix: v.filename_prefix.clone(),
        playlist_name: v.playlist_name.clone(),
        segment_duration: v.segment_duration,
        disable_manifest: v.disable_manifest,
        filename_suffix: v.filename_suffix,
        live_playlist_name: v.live_playlist_name.clone(),
        output: storage_out!(v.destination.as_ref(), segmented_file_output),
    }
}
fn advanced_in(v: &pb::EncodingOptions) -> core::AdvancedEncoding {
    core::AdvancedEncoding {
        width: v.width,
        height: v.height,
        depth: v.depth,
        framerate: v.framerate,
        audio_codec: v.audio_codec,
        audio_bitrate: v.audio_bitrate,
        audio_frequency: v.audio_frequency,
        video_codec: v.video_codec,
        video_bitrate: v.video_bitrate,
        key_frame_interval: core::FloatBits::from_value(v.key_frame_interval),
        audio_quality: v.audio_quality,
        video_quality: v.video_quality,
    }
}
fn advanced_out(v: &core::AdvancedEncoding) -> pb::EncodingOptions {
    pb::EncodingOptions {
        width: v.width,
        height: v.height,
        depth: v.depth,
        framerate: v.framerate,
        audio_codec: v.audio_codec,
        audio_bitrate: v.audio_bitrate,
        audio_frequency: v.audio_frequency,
        video_codec: v.video_codec,
        video_bitrate: v.video_bitrate,
        key_frame_interval: v.key_frame_interval.value(),
        audio_quality: v.audio_quality,
        video_quality: v.video_quality,
    }
}
pub fn participant_in(v: &pb::AutoParticipantEgress) -> core::ParticipantRecordingPolicy {
    core::ParticipantRecordingPolicy {
        encoding: match &v.options {
            Some(pb::auto_participant_egress::Options::Preset(p)) => {
                Some(core::RecordingEncoding::Preset(*p))
            }
            Some(pb::auto_participant_egress::Options::Advanced(a)) => {
                Some(core::RecordingEncoding::Advanced(advanced_in(a)))
            }
            None => None,
        },
        files: v.file_outputs.iter().map(file_in).collect(),
        segments: v.segment_outputs.iter().map(segments_in).collect(),
    }
}
pub fn participant_out(v: &core::ParticipantRecordingPolicy) -> pb::AutoParticipantEgress {
    pb::AutoParticipantEgress {
        options: match &v.encoding {
            Some(core::RecordingEncoding::Preset(p)) => {
                Some(pb::auto_participant_egress::Options::Preset(*p))
            }
            Some(core::RecordingEncoding::Advanced(a)) => Some(
                pb::auto_participant_egress::Options::Advanced(advanced_out(a)),
            ),
            None => None,
        },
        file_outputs: v.files.iter().map(file_out).collect(),
        segment_outputs: v.segments.iter().map(segments_out).collect(),
    }
}
/// Build only the actual fixed-source auto-track public request. No success or
/// IO entry is created. The caller must use the genuine internal launcher.
pub fn track_request(
    policy: &core::TrackRecordingPolicy,
    room: &str,
    track: &str,
) -> pb::TrackEgressRequest {
    let output = match &policy.destination {
        Some(core::UploadDestination::S3(v)) => Some(pb::direct_file_output::Output::S3(s3_out(v))),
        Some(core::UploadDestination::Gcp(v)) => {
            Some(pb::direct_file_output::Output::Gcp(gcp_out(v)))
        }
        Some(core::UploadDestination::Azure(v)) => {
            Some(pb::direct_file_output::Output::Azure(azure_out(v)))
        }
        // Exact fixed startTrackEgress switch does not copy AliOSS or manifest.
        Some(core::UploadDestination::AliOss(_)) | None => None,
    };
    pb::TrackEgressRequest {
        room_name: room.into(),
        track_id: track.into(),
        output: Some(pb::track_egress_request::Output::File(
            pb::DirectFileOutput {
                filepath: core::auto_track_filepath(&policy.filepath),
                output,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}
pub fn participant_request(
    policy: &core::ParticipantRecordingPolicy,
    room: &str,
    identity: &str,
) -> pb::ParticipantEgressRequest {
    let original = participant_out(policy);
    pb::ParticipantEgressRequest {
        room_name: room.into(),
        identity: identity.into(),
        options: original.options.map(|o| match o {
            pb::auto_participant_egress::Options::Preset(v) => {
                pb::participant_egress_request::Options::Preset(v)
            }
            pb::auto_participant_egress::Options::Advanced(v) => {
                pb::participant_egress_request::Options::Advanced(v)
            }
        }),
        file_outputs: original.file_outputs,
        segment_outputs: original.segment_outputs,
        ..Default::default()
    }
}

fn stream_in(v: &pb::StreamOutput) -> core::StreamRecordingPolicy {
    core::StreamRecordingPolicy {
        protocol: v.protocol,
        urls: v.urls.clone(),
    }
}
fn stream_out(v: &core::StreamRecordingPolicy) -> pb::StreamOutput {
    pb::StreamOutput {
        protocol: v.protocol,
        urls: v.urls.clone(),
    }
}
fn image_in(v: &pb::ImageOutput) -> core::ImageRecordingPolicy {
    use pb::image_output;
    core::ImageRecordingPolicy {
        capture_interval: v.capture_interval,
        width: v.width,
        height: v.height,
        filename_prefix: v.filename_prefix.clone(),
        filename_suffix: v.filename_suffix,
        image_codec: v.image_codec,
        disable_manifest: v.disable_manifest,
        destination: storage_in!(v.output.as_ref(), image_output),
    }
}
fn image_out(v: &core::ImageRecordingPolicy) -> pb::ImageOutput {
    use pb::image_output;
    pb::ImageOutput {
        capture_interval: v.capture_interval,
        width: v.width,
        height: v.height,
        filename_prefix: v.filename_prefix.clone(),
        filename_suffix: v.filename_suffix,
        image_codec: v.image_codec,
        disable_manifest: v.disable_manifest,
        output: storage_out!(v.destination.as_ref(), image_output),
    }
}
pub fn composite_in(v: &pb::RoomCompositeEgressRequest) -> core::RoomCompositeRecordingPolicy {
    core::RoomCompositeRecordingPolicy {
        room_name: v.room_name.clone(),
        layout: v.layout.clone(),
        audio_only: v.audio_only,
        audio_mixing: v.audio_mixing,
        video_only: v.video_only,
        custom_base_url: v.custom_base_url.clone(),
        legacy_output: v.output.as_ref().map(|o| match o {
            pb::room_composite_egress_request::Output::File(x) => {
                core::CompositeLegacyOutput::File(file_in(x))
            }
            pb::room_composite_egress_request::Output::Stream(x) => {
                core::CompositeLegacyOutput::Stream(stream_in(x))
            }
            pb::room_composite_egress_request::Output::Segments(x) => {
                core::CompositeLegacyOutput::Segments(segments_in(x))
            }
        }),
        encoding: v.options.as_ref().map(|o| match o {
            pb::room_composite_egress_request::Options::Preset(p) => {
                core::RecordingEncoding::Preset(*p)
            }
            pb::room_composite_egress_request::Options::Advanced(a) => {
                core::RecordingEncoding::Advanced(advanced_in(a))
            }
        }),
        files: v.file_outputs.iter().map(file_in).collect(),
        streams: v.stream_outputs.iter().map(stream_in).collect(),
        segments: v.segment_outputs.iter().map(segments_in).collect(),
        images: v.image_outputs.iter().map(image_in).collect(),
        webhooks: v
            .webhooks
            .iter()
            .map(|w| core::RecordingWebhook {
                url: w.url.clone(),
                signing_key: w.signing_key.clone(),
                filter: w
                    .filter_params
                    .as_ref()
                    .map(|f| core::RecordingEventFilter {
                        include_events: f.include_events.clone(),
                        exclude_events: f.exclude_events.clone(),
                    }),
            })
            .collect(),
    }
}
pub fn composite_out(v: &core::RoomCompositeRecordingPolicy) -> pb::RoomCompositeEgressRequest {
    pb::RoomCompositeEgressRequest {
        room_name: v.room_name.clone(),
        layout: v.layout.clone(),
        audio_only: v.audio_only,
        audio_mixing: v.audio_mixing,
        video_only: v.video_only,
        custom_base_url: v.custom_base_url.clone(),
        output: v.legacy_output.as_ref().map(|o| match o {
            core::CompositeLegacyOutput::File(x) => {
                pb::room_composite_egress_request::Output::File(file_out(x))
            }
            core::CompositeLegacyOutput::Stream(x) => {
                pb::room_composite_egress_request::Output::Stream(stream_out(x))
            }
            core::CompositeLegacyOutput::Segments(x) => {
                pb::room_composite_egress_request::Output::Segments(segments_out(x))
            }
        }),
        options: v.encoding.as_ref().map(|o| match o {
            core::RecordingEncoding::Preset(p) => {
                pb::room_composite_egress_request::Options::Preset(*p)
            }
            core::RecordingEncoding::Advanced(a) => {
                pb::room_composite_egress_request::Options::Advanced(advanced_out(a))
            }
        }),
        file_outputs: v.files.iter().map(file_out).collect(),
        stream_outputs: v.streams.iter().map(stream_out).collect(),
        segment_outputs: v.segments.iter().map(segments_out).collect(),
        image_outputs: v.images.iter().map(image_out).collect(),
        webhooks: v
            .webhooks
            .iter()
            .map(|w| pb::WebhookConfig {
                url: w.url.clone(),
                signing_key: w.signing_key.clone(),
                filter_params: w.filter.as_ref().map(|f| pb::FilterParams {
                    include_events: f.include_events.clone(),
                    exclude_events: f.exclude_events.clone(),
                }),
            })
            .collect(),
    }
}
pub fn room_egress_in(v: &pb::RoomEgress) -> core::AutomaticRecording {
    core::AutomaticRecording {
        room: v.room.as_ref().map(composite_in),
        participant: v.participant.as_ref().map(participant_in),
        tracks: v.tracks.as_ref().map(track_in),
    }
}
pub fn room_egress_out(v: &core::AutomaticRecording) -> pb::RoomEgress {
    pb::RoomEgress {
        room: v.room.as_ref().map(composite_out),
        participant: v.participant.as_ref().map(participant_out),
        tracks: v.tracks.as_ref().map(track_out),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn known_policy_preserves_present_empty_enums_credentials_and_source_request_quirks() {
        let value = pb::AutoTrackEgress {
            filepath: "dir.with.dot/a.ogg".into(),
            disable_manifest: true,
            output: Some(auto_track_egress::Output::AliOss(pb::AliOssUpload {
                access_key: "synthetic-secret".into(),
                ..Default::default()
            })),
        };
        let domain = track_in(&value);
        assert_eq!(track_out(&domain), value);
        assert!(!format!("{domain:?}").contains("synthetic-secret"));
        let request = track_request(&domain, "room", "track");
        let Some(pb::track_egress_request::Output::File(file)) = request.output else {
            panic!("actual source DirectFile variant missing")
        };
        assert_eq!(file.filepath, "dir-{track_id}.with.dot/a.ogg");
        assert!(!file.disable_manifest);
        assert!(file.output.is_none());
        let participant = pb::AutoParticipantEgress {
            options: Some(pb::auto_participant_egress::Options::Preset(0)),
            file_outputs: vec![pb::EncodedFileOutput {
                file_type: 999,
                filepath: "out".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let domain = participant_in(&participant);
        assert_eq!(participant_out(&domain), participant);
        let request = participant_request(&domain, "room", "identity");
        assert_eq!(request.file_outputs[0].file_type, 999);
        assert!(matches!(
            request.options,
            Some(pb::participant_egress_request::Options::Preset(0))
        ));
        assert!(!request.screen_share);
        assert!(request.stream_outputs.is_empty());
    }
}
