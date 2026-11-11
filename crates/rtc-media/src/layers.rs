//! Simulcast rid ↔ VideoQuality ↔ spatial layer mapping (sfu-media.md §1, §2.3).
//!
//! Mirrors `pkg/sfu/buffer/videolayerutils.go`: every mapping goes through the
//! "layer presence" of the track's published qualities, never through the quality
//! enum directly. Single-layer tracks are always spatial 0.

use rtc_core::model::{VideoLayer, VideoQuality};

/// Spatial value meaning "no layer" (upstream `InvalidLayerSpatial`).
pub(crate) const INVALID_SPATIAL: i32 = -1;

/// Highest spatial index the SFU handles.
pub(crate) const MAX_SPATIAL: i32 = 2;

const RIDS_QHF: [&str; 3] = ["q", "h", "f"];
const RIDS_210: [&str; 3] = ["2", "1", "0"];

/// Which of LOW/MEDIUM/HIGH are present in the track's layers. `None` when the
/// track has no layers at all (upstream `lp == nil`).
pub(crate) type Presence = Option<[bool; 3]>;

pub(crate) fn presence(layers: &[VideoLayer]) -> Presence {
    if layers.is_empty() {
        return None;
    }
    let mut lp = [false; 3];
    for l in layers {
        if let Some(i) = l.quality.spatial_index() {
            lp[i] = true;
        }
    }
    Some(lp)
}

fn count(lp: &[bool; 3]) -> usize {
    lp.iter().filter(|b| **b).count()
}

/// `VideoQualityToSpatialLayer`.
pub(crate) fn quality_to_spatial(lp: Presence, q: VideoQuality) -> i32 {
    let Some(lp) = lp else {
        return match q {
            VideoQuality::Low => 0,
            VideoQuality::Medium => 1,
            VideoQuality::High => 2,
            VideoQuality::Off | VideoQuality::Unrecognized(_) => INVALID_SPATIAL,
        };
    };
    let (l, m, h) = (lp[0], lp[1], lp[2]);
    match q {
        VideoQuality::Low => 0,
        VideoQuality::Medium => {
            if (l && m) || (l && h) {
                1
            } else {
                // {M,H} → 0, single layer → 0
                0
            }
        }
        VideoQuality::High => {
            if l && m && h {
                2
            } else if count(&lp) == 2 {
                1
            } else {
                0
            }
        }
        VideoQuality::Off | VideoQuality::Unrecognized(_) => INVALID_SPATIAL,
    }
}

/// `SpatialLayerToVideoQuality`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn spatial_to_quality(lp: Presence, spatial: i32) -> VideoQuality {
    let Some(lp) = lp else {
        return match spatial {
            0 => VideoQuality::Low,
            1 => VideoQuality::Medium,
            2 => VideoQuality::High,
            _ => VideoQuality::Off,
        };
    };
    let (l, m) = (lp[0], lp[1]);
    match spatial {
        0 => {
            if l {
                VideoQuality::Low
            } else if m {
                VideoQuality::Medium
            } else {
                VideoQuality::High
            }
        }
        1 => {
            if l && m {
                VideoQuality::Medium
            } else {
                // {L,H}, {M,H} and invalid combinations → HIGH
                VideoQuality::High
            }
        }
        2 => VideoQuality::High,
        _ => VideoQuality::Off,
    }
}

/// `SpatialLayerToRid` with the rid space from the SDP (normalised, "" = absent).
pub(crate) fn spatial_to_rid(lp: Presence, spatial: i32, rid_space: &[String; 3]) -> String {
    let Some(lp) = lp else {
        return match spatial {
            1 => "h".into(),
            2 => "f".into(),
            _ => "q".into(),
        };
    };
    let n = count(&lp);
    match spatial {
        0 => rid_space[0].clone(),
        1 => {
            if n >= 2 {
                rid_space[1].clone()
            } else {
                rid_space[0].clone()
            }
        }
        2 => {
            if n == 3 {
                rid_space[2].clone()
            } else if n == 2 {
                rid_space[1].clone()
            } else {
                rid_space[0].clone()
            }
        }
        _ => rid_space[0].clone(),
    }
}

/// `VideoQualityToRid`.
pub(crate) fn quality_to_rid(lp: Presence, q: VideoQuality, rid_space: &[String; 3]) -> String {
    spatial_to_rid(lp, quality_to_spatial(lp, q), rid_space)
}

/// `GetSpatialLayerForRid`: empty rid → 0, exact rid match, else 0 (lenient).
pub(crate) fn spatial_for_rid(layers: &[VideoLayer], rid: &str) -> i32 {
    if rid.is_empty() {
        return 0;
    }
    for l in layers {
        if l.rid == rid {
            return l.spatial_layer;
        }
    }
    0
}

/// `GetSpatialLayerForVideoQuality`.
pub(crate) fn spatial_for_quality(layers: &[VideoLayer], q: VideoQuality) -> i32 {
    if q == VideoQuality::Off {
        return INVALID_SPATIAL;
    }
    for l in layers {
        if l.quality == q {
            return l.spatial_layer;
        }
    }
    if layers.is_empty() {
        return 0;
    }
    quality_to_spatial(presence(layers), q)
}

/// `GetVideoQualityForSpatialLayer`.
pub(crate) fn quality_for_spatial(layers: &[VideoLayer], spatial: i32) -> VideoQuality {
    if spatial == INVALID_SPATIAL {
        return VideoQuality::Off;
    }
    for l in layers {
        if l.spatial_layer == spatial {
            return l.quality;
        }
    }
    VideoQuality::Off
}

/// Rids from `a=simulcast` (in offer order, `~` already stripped) into the fixed
/// 3-slot rid space, normalised to the known q/h/f or 2/1/0 order
/// (`NormalizeVideoLayersRid`). Missing slots are "".
pub(crate) fn rid_space_from_sdp(sdp_rids: &[String]) -> [String; 3] {
    let mut out: [String; 3] = Default::default();
    for (i, r) in sdp_rids.iter().take(3).enumerate() {
        out[i] = r.trim_start_matches('~').to_string();
    }
    let known = |set: &[&str; 3]| {
        out.iter()
            .all(|r| r.is_empty() || set.contains(&r.as_str()))
    };
    for set in [&RIDS_QHF, &RIDS_210] {
        if known(set) && out.iter().any(|r| !r.is_empty()) {
            let present: Vec<&str> = set
                .iter()
                .copied()
                .filter(|k| out.iter().any(|r| r == k))
                .collect();
            let mut normalised: [String; 3] = Default::default();
            for (i, k) in present.iter().enumerate() {
                normalised[i] = (*k).to_string();
            }
            return normalised;
        }
    }
    out
}

/// Rewrite every layer's spatial/rid as on first RTP (`mediaTrackReceived`,
/// participant.go:3534-3539). Layers are sorted by quality ascending first.
pub(crate) fn rewrite_layers(layers: &mut [VideoLayer], sdp_rids: &[String]) {
    layers.sort_by_key(|l| l.quality);
    let lp = presence(layers);
    let space = rid_space_from_sdp(sdp_rids);
    for l in layers.iter_mut() {
        l.spatial_layer = quality_to_spatial(lp, l.quality);
        l.rid = quality_to_rid(lp, l.quality, &space);
    }
}

/// Layers for an SDP-only publication without signalled layers
/// (participant.go:1095-1107): one per SDP rid with `Quality = i`, else one dummy
/// zero-valued layer.
pub(crate) fn synthesize_layers(sdp_rids: &[String]) -> Vec<VideoLayer> {
    let blank = |q: VideoQuality| VideoLayer {
        quality: q,
        width: 0,
        height: 0,
        bitrate: 0,
        ssrc: 0,
        spatial_layer: 0,
        rid: String::new(),
        repair_ssrc: 0,
    };
    if sdp_rids.is_empty() {
        return vec![blank(VideoQuality::Low)];
    }
    (0..sdp_rids.len().min(3))
        .map(|i| blank(VideoQuality::from_spatial_index(i)))
        .collect()
}

/// Tolerance applied to the requested height (mediatrackreceiver.go:42).
const DIMENSION_TOLERANCE: f64 = 0.9;

/// `GetQualityForDimension` (§2.3): the smallest layer whose height is ≥ 90 % of the
/// requested height, or the top layer.
pub(crate) fn quality_for_dimension(
    layers: &[VideoLayer],
    width: u32,
    height: u32,
) -> VideoQuality {
    let mut sizes: Vec<u32> = layers.iter().map(|l| l.height).filter(|h| *h > 0).collect();
    sizes.sort_unstable();
    let requested = if sizes.is_empty() {
        // No sizes at all: compare the smaller requested dimension (portrait-safe)
        // against the default ladder.
        let m = if width > 0 && height > 0 {
            width.min(height)
        } else {
            width.max(height)
        };
        sizes = vec![180, 360, m.max(720)];
        m as f64 * DIMENSION_TOLERANCE
    } else {
        let h = if height > 0 { height } else { width };
        h as f64 * DIMENSION_TOLERANCE
    };
    let mut quality = 0usize;
    for (i, s) in sizes.iter().enumerate() {
        quality = i;
        if i + 1 == sizes.len() {
            break;
        }
        if *s as f64 >= requested && *s != sizes[i + 1] {
            break;
        }
    }
    VideoQuality::from_spatial_index(quality)
}

/// Rid string of a spatial layer from the (rewritten) layers.
pub(crate) fn rid_for_spatial(layers: &[VideoLayer], spatial: i32) -> Option<&str> {
    layers
        .iter()
        .find(|l| l.spatial_layer == spatial)
        .map(|l| l.rid.as_str())
        .filter(|r| !r.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use VideoQuality::*;

    fn layer(q: VideoQuality, h: u32) -> VideoLayer {
        VideoLayer {
            quality: q,
            width: h * 16 / 9,
            height: h,
            bitrate: 0,
            ssrc: 0,
            spatial_layer: 0,
            rid: String::new(),
            repair_ssrc: 0,
        }
    }

    fn rids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn three_layers_map_directly() {
        let lp = Some([true, true, true]);
        assert_eq!(quality_to_spatial(lp, Low), 0);
        assert_eq!(quality_to_spatial(lp, Medium), 1);
        assert_eq!(quality_to_spatial(lp, High), 2);
        assert_eq!(spatial_to_quality(lp, 2), High);
        let space = rid_space_from_sdp(&rids(&["q", "h", "f"]));
        assert_eq!(quality_to_rid(lp, High, &space), "f");
        assert_eq!(quality_to_rid(lp, Low, &space), "q");
    }

    #[test]
    fn two_layers_presence_aware() {
        let lm = Some([true, true, false]);
        assert_eq!(quality_to_spatial(lm, High), 1);
        assert_eq!(spatial_to_quality(lm, 1), Medium);
        let lh = Some([true, false, true]);
        assert_eq!(quality_to_spatial(lh, Medium), 1);
        assert_eq!(spatial_to_quality(lh, 1), High);
        let mh = Some([false, true, true]);
        assert_eq!(quality_to_spatial(mh, Low), 0);
        assert_eq!(quality_to_spatial(mh, Medium), 0);
        assert_eq!(quality_to_spatial(mh, High), 1);
        assert_eq!(spatial_to_quality(mh, 0), Medium);
        let space = rid_space_from_sdp(&rids(&["q", "h"]));
        assert_eq!(spatial_to_rid(mh, 2, &space), "h");
    }

    #[test]
    fn single_layer_is_spatial_zero() {
        let h = Some([false, false, true]);
        assert_eq!(quality_to_spatial(h, High), 0);
        assert_eq!(spatial_to_quality(h, 0), High);
        let mut layers = vec![layer(High, 720)];
        rewrite_layers(&mut layers, &[]);
        assert_eq!(layers[0].spatial_layer, 0);
        assert_eq!(layers[0].rid, "");
    }

    #[test]
    fn no_presence_defaults() {
        assert_eq!(quality_to_spatial(None, Medium), 1);
        assert_eq!(spatial_to_rid(None, 2, &Default::default()), "f");
        assert_eq!(quality_to_spatial(None, Off), INVALID_SPATIAL);
    }

    #[test]
    fn rewrite_sets_rids_from_sdp() {
        let mut layers = vec![layer(High, 720), layer(Low, 180), layer(Medium, 360)];
        rewrite_layers(&mut layers, &rids(&["q", "h", "f"]));
        assert_eq!(
            layers.iter().map(|l| l.quality).collect::<Vec<_>>(),
            vec![Low, Medium, High]
        );
        assert_eq!(
            layers.iter().map(|l| l.rid.as_str()).collect::<Vec<_>>(),
            vec!["q", "h", "f"]
        );
        assert_eq!(
            layers.iter().map(|l| l.spatial_layer).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(spatial_for_rid(&layers, "h"), 1);
        assert_eq!(spatial_for_rid(&layers, ""), 0);
        assert_eq!(spatial_for_rid(&layers, "x"), 0);
    }

    #[test]
    fn rid_space_normalises_order() {
        assert_eq!(
            rid_space_from_sdp(&rids(&["f", "q", "h"])),
            ["q".to_string(), "h".into(), "f".into()]
        );
        assert_eq!(
            rid_space_from_sdp(&rids(&["~q", "f"])),
            ["q".to_string(), "f".into(), String::new()]
        );
        assert_eq!(
            rid_space_from_sdp(&rids(&["0", "2"])),
            ["2".to_string(), "0".into(), String::new()]
        );
        assert_eq!(
            rid_space_from_sdp(&rids(&["a", "b"])),
            ["a".to_string(), "b".into(), String::new()]
        );
    }

    #[test]
    fn spatial_for_quality_prefers_exact_layer() {
        let mut layers = vec![layer(Low, 180), layer(High, 720)];
        rewrite_layers(&mut layers, &rids(&["q", "h"]));
        assert_eq!(spatial_for_quality(&layers, High), 1);
        assert_eq!(spatial_for_quality(&layers, Medium), 1);
        assert_eq!(spatial_for_quality(&layers, Off), INVALID_SPATIAL);
        assert_eq!(quality_for_spatial(&layers, 1), High);
        assert_eq!(quality_for_spatial(&layers, -1), Off);
        assert_eq!(rid_for_spatial(&layers, 1), Some("h"));
    }

    #[test]
    fn dimension_to_quality_with_tolerance() {
        let layers = vec![layer(Low, 180), layer(Medium, 360), layer(High, 720)];
        assert_eq!(quality_for_dimension(&layers, 320, 180), Low);
        // 200 * 0.9 = 180 fits the 180p layer
        assert_eq!(quality_for_dimension(&layers, 355, 200), Low);
        assert_eq!(quality_for_dimension(&layers, 400, 210), Medium);
        assert_eq!(quality_for_dimension(&layers, 1280, 720), High);
        assert_eq!(quality_for_dimension(&layers, 1920, 1080), High);
        let two = vec![layer(Low, 180), layer(Medium, 360)];
        assert_eq!(quality_for_dimension(&two, 1920, 1080), Medium);
    }

    #[test]
    fn synthesized_layers() {
        assert_eq!(synthesize_layers(&[]).len(), 1);
        let s = synthesize_layers(&rids(&["q", "h", "f"]));
        assert_eq!(
            s.iter().map(|l| l.quality).collect::<Vec<_>>(),
            vec![Low, Medium, High]
        );
    }
}

#[cfg(test)]
mod open_quality_consumers {
    use super::*;
    fn layer(raw: i32) -> VideoLayer {
        VideoLayer {
            quality: VideoQuality::from_i32(raw),
            width: 1280,
            height: 720,
            bitrate: 100_000,
            ssrc: 1,
            spatial_layer: 0,
            rid: String::new(),
            repair_ssrc: 0,
        }
    }
    #[test]
    fn unknown_published_quality_never_aliases_low_or_a_known_spatial_layer() {
        for raw in [4, 77, i32::MAX] {
            let mut layers = vec![layer(2), layer(raw)];
            rewrite_layers(&mut layers, &["q".into(), "f".into()]);
            assert_eq!(presence(&layers), Some([false, false, true]));
            assert_eq!(layers[0].quality, VideoQuality::High);
            assert_eq!(layers[0].spatial_layer, 0);
            assert_eq!(layers[1].quality.as_i32(), raw);
            assert_eq!(layers[1].spatial_layer, INVALID_SPATIAL);
            assert_eq!(
                spatial_for_quality(&layers, VideoQuality::from_i32(raw)),
                INVALID_SPATIAL
            );
            assert_eq!(quality_for_spatial(&layers, 0), VideoQuality::High);
        }
    }
    #[test]
    fn undeclared_quality_uses_go_default_but_empty_single_layer_stays_zero() {
        let layers = [layer(0), layer(1), layer(2)];
        for raw in [i32::MIN, -1, 4, 77, i32::MAX] {
            let q = VideoQuality::from_i32(raw);
            assert_eq!(quality_to_spatial(None, q), INVALID_SPATIAL);
            assert_eq!(quality_to_spatial(presence(&layers), q), INVALID_SPATIAL);
            assert_eq!(spatial_for_quality(&layers, q), INVALID_SPATIAL);
            assert_eq!(
                spatial_for_quality(&[], q),
                0,
                "Go GetSpatialLayerForVideoQuality empty-layer fallback"
            );
        }
        assert_eq!(spatial_for_quality(&[], VideoQuality::Off), INVALID_SPATIAL);
        // Fixed Go's LayerPresence directly indexes negative quality and can
        // panic there. We retain its identity but safely exclude that value
        // from known presence rather than introducing an array-index panic.
        assert_eq!(presence(&[layer(-1)]), Some([false; 3]));
    }
}
