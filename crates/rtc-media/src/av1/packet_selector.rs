//! Per-original-DD-packet selection and source-preserving output descriptors.
use super::{Forwarder, Picture, decisions::Decision, packet_descriptor, packet_receiver::Parsed};
use std::sync::Arc;
#[derive(Default)]
pub(super) struct FrameNumberWrapper {
    offset: u64,
    last: Option<u64>,
}
impl FrameNumberWrapper {
    fn update(&mut self, new: u64, attached: bool) -> u16 {
        let Some(last) = self.last else {
            self.last = Some(new);
            return new as u16;
        };
        if new <= last {
            return new.wrapping_add(self.offset) as u16;
        }
        if attached {
            let new16 = new.wrapping_add(self.offset) as u16;
            let last16 = last.wrapping_add(self.offset) as u16;
            let diff = new16.wrapping_sub(last16);
            if diff > 0x8000 || (diff == 0x8000 && new16 < last16) {
                self.offset = self.offset.wrapping_add(u64::from(65535 - diff) + 6000);
            }
        }
        self.last = Some(new);
        new.wrapping_add(self.offset) as u16
    }
}
#[derive(Default)]
pub(crate) struct Selector {
    state: Forwarder,
    generation: Option<u64>,
    keyframe: Option<u64>,
    valid: bool,
    wrapper: FrameNumberWrapper,
}
pub(crate) struct Selected {
    pub descriptor: Arc<[u8]>,
    pub spatial: u8,
    pub temporal: u8,
    pub marker: bool,
}
impl Selector {
    pub fn restart(&mut self) {
        self.state = Default::default();
        self.valid = false;
        self.keyframe = None;
        self.generation = None;
    }
    pub fn select(
        &mut self,
        parsed: &Parsed,
        spatial: i32,
        temporal: u8,
        marker: bool,
    ) -> Option<Selected> {
        let dd = &parsed.descriptor;
        if self.generation != Some(parsed.generation) {
            self.restart();
            self.generation = Some(parsed.generation);
        }
        if !self.valid && dd.attached.is_none() {
            return None;
        }
        if self.state.source_decisions.get(dd.frame) == Decision::Dropped {
            return None;
        }
        if parsed.structure_updated {
            self.valid = true;
            self.keyframe = Some(parsed.keyframe);
        }
        if self.keyframe != Some(parsed.keyframe) {
            self.state.record_source_decision(dd.frame, false);
            self.valid = false;
            return None;
        }
        let picture = Picture {
            frames: vec![dd.clone()],
            packet_layers: vec![],
            packet_frames: vec![],
            structure: parsed.structure.clone(),
            active_targets: parsed.active,
            frame_target_masks: vec![parsed.active],
            units: vec![],
            highest: i32::from(dd.dependency.spatial),
            temporal: dd.dependency.temporal,
            keyframe: dd.attached.is_some(),
            resolutions: parsed.structure.resolutions.clone(),
        };
        if !self
            .state
            .observe_integrity(&picture, spatial, temporal, parsed.integrity)
        {
            return None;
        }
        let (selected_spatial, selected_temporal) = *self.state.selected_layers.get(&dd.frame)?;
        let mut active = 0u32;
        for target in 0..parsed.structure.targets {
            let mut layer = (0u8, 0u8);
            for t in &parsed.structure.templates {
                if t.indications.get(target).is_some_and(|dti| *dti != 0) {
                    layer.0 = layer.0.max(t.spatial);
                    layer.1 = layer.1.max(t.temporal);
                }
            }
            if layer.0 <= selected_spatial && layer.1 <= selected_temporal {
                active |= 1u32 << target;
            }
        }
        let frame = self.wrapper.update(parsed.frame, parsed.structure_updated);
        let active = if dd.attached.is_some() {
            dd.active.unwrap_or(parsed.active)
        } else {
            active
        };
        let descriptor =
            packet_descriptor::encode(dd, &parsed.structure, parsed.raw_template, frame, active)?;
        Some(Selected {
            descriptor: descriptor.into(),
            spatial: selected_spatial,
            temporal: selected_temporal,
            marker: marker || (dd.last && dd.dependency.spatial == selected_spatial),
        })
    }
}

#[cfg(test)]
mod original_wrapper_oracle {
    use super::*;
    #[test]
    #[ignore = "requires original Go FrameNumberWrapper actual observer output"]
    fn original_frame_number_offset_preserves_unknown_dependencies_late_packets_and_gop_wraps() {
        let path =
            std::env::var("GO_AV1_WRAPPER_RESULTS").expect("actual original Go frame wrapper log");
        let input = std::fs::read_to_string(path).unwrap();
        let mut wrapper = FrameNumberWrapper::default();
        let mut count = 0;
        for line in input.lines() {
            let Some(row) = line.strip_prefix("SOURCE_DD_WRAPPER ") else {
                continue;
            };
            let row: serde_json::Value = serde_json::from_str(row).unwrap();
            let actual = wrapper.update(
                row["frame"].as_u64().unwrap(),
                row["attached"].as_bool().unwrap(),
            );
            assert_eq!(
                u64::from(actual),
                row["output"].as_u64().unwrap(),
                "actual original source wrapper row {count}"
            );
            count += 1;
        }
        assert_eq!(
            count, 10,
            "source early/late/unknown references and every GOP offset transition required"
        );
    }
}
