//! Source-bound receiver size observations; never publisher-declared metadata.
//! LiveKit buffer_base.go1091–1204/1478 and receiver_base.go695–728/1203.
/// ReceiverBase uses the published matching codec's declared mode, not whether
/// the actual payload happens to carry multiple DD/VP9 resolutions. In particular
/// the source one-shot synthesized codec declares one spatial layer per stream.
pub(crate) fn multiple_spatial_layers(codecs: &[rtc_core::model::TrackCodec], mime: &str) -> bool {
    codecs
        .iter()
        .find(|codec| codec.mime_type.eq_ignore_ascii_case(mime))
        .is_some_and(|codec| {
            codec.layer_mode == rtc_core::model::VideoLayerMode::MultipleSpatialLayersPerStream
        })
}
#[derive(Default)]
pub(crate) struct VideoDimensions {
    mime: String,
    sizes: [(u32, u32); 3],
}
impl VideoDimensions {
    /// Update from one actual media buffer. SVC replaces all spatial values;
    /// simulcast updates only its actual RID slot. Receiver.VideoSizes stops at
    /// the first incomplete layer, so a high RID arriving first cannot invent base.
    pub fn observe(
        &mut self,
        mime: &str,
        spatial: i32,
        multiple: bool,
        input: &[(u32, u32)],
    ) -> Option<Vec<(u32, u32)>> {
        if input.is_empty() || input.len() > 3 {
            return None;
        }
        if self.mime != mime {
            self.mime = mime.to_owned();
            self.sizes = [(0, 0); 3];
        }
        let mut next = self.sizes;
        if multiple {
            next = [(0, 0); 3];
            next[..input.len()].copy_from_slice(input);
        } else {
            let index = usize::try_from(spatial).ok().filter(|i| *i < 3)?;
            next[index] = input[0];
        }
        if next == self.sizes {
            return None;
        }
        self.sizes = next;
        let prefix: Vec<_> = self
            .sizes
            .iter()
            .copied()
            .take_while(|(w, h)| *w != 0 && *h != 0)
            .collect();
        Some(prefix)
    }
}
/// VP8 frame bytes after the native RTP depacketizer removed the descriptor.
/// Exact ExtractVP8VideoSize start-code/14-bit formula; only genuine keyframes.
pub(crate) fn vp8(keyframe: bool, frame: &[u8]) -> Option<(u32, u32)> {
    if !keyframe || frame.len() < 10 || frame[3..6] != [0x9d, 1, 0x2a] {
        return None;
    }
    let width = u32::from(u16::from_le_bytes([frame[6], frame[7]]) & 0x3fff);
    let height = u32::from(u16::from_le_bytes([frame[8], frame[9]]) & 0x3fff);
    (width != 0 && height != 0).then_some((width, height))
}
pub(crate) fn vp9(keyframe: bool, extra: str0m::format::CodecExtra) -> Option<Vec<(u32, u32)>> {
    if !keyframe {
        return None;
    }
    let str0m::format::CodecExtra::Vp9(extra) = extra else {
        return None;
    };
    let values: Vec<_> = extra
        .layers_widths
        .into_iter()
        .zip(extra.layers_heights)
        .map(|(w, h)| (u32::from(w.unwrap_or(0)), u32::from(h.unwrap_or(0))))
        .collect();
    values
        .iter()
        .any(|(w, h)| *w > 0 && *h > 0)
        .then_some(values)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_av1_resolutions_follow_declared_matching_codec_mode() {
        use rtc_core::model::{TrackCodec, VideoLayerMode};
        let mut codecs = vec![TrackCodec {
            mime_type: "video/AV1".into(),
            layer_mode: VideoLayerMode::OneSpatialLayerPerStream,
            ..Default::default()
        }];
        let resolutions = [(320, 180), (640, 360)];
        let mut one_shot = VideoDimensions::default();
        assert_eq!(
            one_shot.observe(
                "video/AV1",
                0,
                multiple_spatial_layers(&codecs, "video/av1"),
                &resolutions
            ),
            Some(vec![(320, 180)])
        );
        codecs[0].layer_mode = VideoLayerMode::MultipleSpatialLayersPerStream;
        let mut explicit_svc = VideoDimensions::default();
        assert_eq!(
            explicit_svc.observe(
                "video/AV1",
                0,
                multiple_spatial_layers(&codecs, "video/av1"),
                &resolutions
            ),
            Some(resolutions.to_vec())
        );
        assert!(!multiple_spatial_layers(&codecs, "video/VP9"));
        codecs[0].layer_mode = VideoLayerMode::Unused;
        assert!(!multiple_spatial_layers(&codecs, "video/AV1"));
    }

    #[test]
    fn vp8_media_keyframe_only_and_no_declared_size() {
        let mut p = [0u8; 10];
        p[3..6].copy_from_slice(&[0x9d, 1, 0x2a]);
        p[6..8].copy_from_slice(&(640u16 | 0xc000).to_le_bytes());
        p[8..10].copy_from_slice(&(360u16 | 0x8000).to_le_bytes());
        assert_eq!(vp8(true, &p), Some((640, 360)));
        assert_eq!(vp8(false, &p), None);
        assert_eq!(vp8(true, &p[..9]), None);
        p[3] = 0;
        assert_eq!(vp8(true, &p), None);
    }
    #[test]
    fn real_rid_prefix_size_change_and_codec_generation() {
        let mut sizes = VideoDimensions::default();
        assert_eq!(
            sizes.observe("video/VP8", 2, false, &[(1280, 720)]),
            Some(vec![])
        );
        assert_eq!(
            sizes.observe("video/VP8", 0, false, &[(320, 180)]),
            Some(vec![(320, 180)])
        );
        assert_eq!(
            sizes.observe("video/VP8", 1, false, &[(640, 360)]),
            Some(vec![(320, 180), (640, 360), (1280, 720)])
        );
        assert_eq!(sizes.observe("video/VP8", 1, false, &[(640, 360)]), None);
        assert_eq!(
            sizes.observe("video/VP9", 0, true, &[(640, 360), (1280, 720)]),
            Some(vec![(640, 360), (1280, 720)])
        );
        assert_eq!(
            sizes.observe("video/VP9", 0, true, &[(320, 180)]),
            Some(vec![(320, 180)])
        );
    }
    #[test]
    fn vp9_uses_actual_native_ss_dimensions_only_on_a_keyframe() {
        let extra = str0m::format::CodecExtra::Vp9(str0m::format::Vp9CodecExtra {
            layers_widths: [Some(320), Some(640), Some(1280)],
            layers_heights: [Some(180), Some(360), Some(720)],
            ..Default::default()
        });
        assert_eq!(
            vp9(true, extra),
            Some(vec![(320, 180), (640, 360), (1280, 720)])
        );
        assert_eq!(vp9(false, extra), None);
        assert_eq!(vp9(true, str0m::format::CodecExtra::None), None);
    }
}
