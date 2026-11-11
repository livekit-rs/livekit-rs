//! Isolated source port, protocol/ingress/validation.go. NOT integrated/executed.
use crate::service_bus::rpc::RpcError;
use lk_protocol::livekit as pb;
use std::collections::BTreeMap;
fn invalid(message: &str) -> RpcError {
    RpcError::new("invalid_argument", format!("invalid ingress: {message}"))
}
fn video(message: &str) -> RpcError {
    RpcError::new(
        "invalid_argument",
        format!("invalid video parameters: {message}"),
    )
}
fn audio(message: &str) -> RpcError {
    RpcError::new(
        "invalid_argument",
        format!("invalid audio parameters: {message}"),
    )
}
fn bypass(message: &str) -> RpcError {
    RpcError::new(
        "invalid_argument",
        format!("invalid transcoding bypass configuration: {message}"),
    )
}
pub fn validate(info: &pb::IngressInfo) -> Result<(), RpcError> {
    if info.room_name.is_empty() {
        return Err(invalid("no room name"));
    }
    validate_for_serialization(info)
}
pub fn validate_for_serialization(info: &pb::IngressInfo) -> Result<(), RpcError> {
    if !matches!(info.input_type, 0..=2) {
        return Err(invalid("unsupported input type"));
    }
    match info.input_type {
        0 | 1 => {
            if info.stream_key.is_empty() {
                return Err(invalid("no stream key"));
            }
        }
        2 => {
            if info.url.is_empty() {
                return Err(invalid("no source URL"));
            }
            if info.enabled == Some(false) {
                return Err(invalid("disabled non reusable ingress"));
            }
        }
        _ => unreachable!(),
    }
    if info.participant_identity.is_empty() {
        return Err(invalid("no participant identity"));
    }
    validate_bypass(info)?;
    validate_enable(info)?;
    validate_video(info.video.as_ref())?;
    validate_audio(info.audio.as_ref())
}
fn has_video_encoding(info: &pb::IngressInfo) -> bool {
    info.video
        .as_ref()
        .is_some_and(|v| v.encoding_options.is_some())
}
fn has_audio_encoding(info: &pb::IngressInfo) -> bool {
    info.audio
        .as_ref()
        .is_some_and(|a| a.encoding_options.is_some())
}
fn validate_bypass(info: &pb::IngressInfo) -> Result<(), RpcError> {
    if !info.bypass_transcoding {
        return Ok(());
    }
    if info.input_type != 1 {
        return Err(bypass(
            "bypassing transcoding impossible with selected input type",
        ));
    }
    if has_video_encoding(info) {
        return Err(bypass(
            "video encoding options must be empty if transcoding bypass is selected",
        ));
    }
    if has_audio_encoding(info) {
        return Err(bypass(
            "audio encoding options must be empty if transcoding bypass is selected",
        ));
    }
    Ok(())
}
fn validate_enable(info: &pb::IngressInfo) -> Result<(), RpcError> {
    let enabled = if info.input_type == 1 {
        info.enable_transcoding.unwrap_or(false)
    } else {
        if info.enable_transcoding == Some(false) {
            return Err(bypass(
                "bypassing transcoding impossible with selected input type",
            ));
        }
        true
    };
    if !enabled {
        if has_video_encoding(info) {
            return Err(bypass(
                "video encoding options must be empty if transcoding is disabled",
            ));
        }
        if has_audio_encoding(info) {
            return Err(bypass(
                "audio encoding options must be empty if transcoding is disabled",
            ));
        }
    }
    Ok(())
}
fn validate_video(options: Option<&pb::IngressVideoOptions>) -> Result<(), RpcError> {
    let Some(options) = options else {
        return Ok(());
    };
    if !matches!(options.source, 0 | 1 | 3) {
        return Err(video("invalid track source"));
    }
    match options.encoding_options.as_ref() {
        None => {}
        Some(pb::ingress_video_options::EncodingOptions::Preset(p)) => {
            if pb::IngressVideoEncodingPreset::try_from(*p).is_err() {
                return Err(video("invalid preset"));
            }
        }
        Some(pb::ingress_video_options::EncodingOptions::Options(o)) => {
            // Exactly source <0, including preserving its NaN acceptance.
            if o.frame_rate < 0.0 {
                return Err(video("invalid framerate"));
            }
            if !matches!(o.video_codec, 0 | 1 | 4) {
                return Err(video("video codec unsupported"));
            }
            let mut by_quality = BTreeMap::new();
            for layer in &o.layers {
                if layer.height == 0 || layer.width == 0 {
                    return Err(video("invalid output media dimensions"));
                }
                if layer.width % 2 == 1 {
                    return Err(invalid("layer width must be even"));
                }
                if layer.height % 2 == 1 {
                    return Err(invalid("layer height must be even"));
                }
                if by_quality.insert(layer.quality, layer).is_some() {
                    return Err(video("more than one layer with the same quality level"));
                }
            }
            let (mut old_w, mut old_h) = (0, 0);
            for quality in 0..=2 {
                if let Some(layer) = by_quality.get(&quality) {
                    if layer.height < old_h {
                        return Err(video(
                            "video layers do not have increasing height with increasing quality",
                        ));
                    }
                    if layer.width < old_w {
                        return Err(video(
                            "video layers do not have increasing width with increasing quality",
                        ));
                    }
                    old_w = layer.width;
                    old_h = layer.height;
                }
            }
        }
    }
    Ok(())
}
fn validate_audio(options: Option<&pb::IngressAudioOptions>) -> Result<(), RpcError> {
    let Some(options) = options else {
        return Ok(());
    };
    if !matches!(options.source, 0 | 2 | 4) {
        return Err(audio("invalid track source"));
    }
    match options.encoding_options.as_ref() {
        None => {}
        Some(pb::ingress_audio_options::EncodingOptions::Preset(p)) => {
            if pb::IngressAudioEncodingPreset::try_from(*p).is_err() {
                return Err(audio("invalid preset"));
            }
        }
        Some(pb::ingress_audio_options::EncodingOptions::Options(o)) => {
            if !matches!(o.audio_codec, 0 | 1) {
                return Err(audio("audio codec unsupported"));
            }
            if o.channels > 2 {
                return Err(audio("invalid audio channel count"));
            }
        }
    }
    Ok(())
}
pub fn update_transcoding(info: &mut pb::IngressInfo) {
    if let Some(enabled) = info.enable_transcoding {
        info.bypass_transcoding = !enabled;
        return;
    }
    if info.input_type == 1 {
        info.enable_transcoding = Some(false);
        info.bypass_transcoding = true;
    } else {
        info.enable_transcoding = Some(true);
    }
}
/// Mutate only source-present/nonempty fields; optional false is not absent.
pub fn patch(
    request: &pb::UpdateIngressRequest,
    info: &mut pb::IngressInfo,
) -> Result<(), RpcError> {
    if !request.name.is_empty() {
        info.name = request.name.clone();
    }
    if !request.room_name.is_empty() {
        info.room_name = request.room_name.clone();
    }
    if !request.participant_identity.is_empty() {
        info.participant_identity = request.participant_identity.clone();
    }
    if !request.participant_name.is_empty() {
        info.participant_name = request.participant_name.clone();
    }
    if request.enable_transcoding.is_some() {
        info.enable_transcoding = request.enable_transcoding;
    }
    if !request.participant_metadata.is_empty() {
        info.participant_metadata = request.participant_metadata.clone();
    }
    if request.audio.is_some() {
        info.audio = request.audio.clone();
    }
    if request.video.is_some() {
        info.video = request.video.clone();
    }
    if request.enabled.is_some() {
        info.enabled = request.enabled;
    }
    validate_for_serialization(info)?;
    update_transcoding(info);
    Ok(())
}
