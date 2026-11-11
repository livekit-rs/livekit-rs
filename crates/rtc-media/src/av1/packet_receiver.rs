//! Fixed source DD parser lifetime before per-original-packet selection.
use super::{
    RawDescriptor,
    descriptor::{self, Descriptor, Structure},
    integrity::Integrity,
};
use std::time::{Duration, Instant};
use str0m::rtp::ExtensionValues;
pub(crate) struct Parsed {
    pub descriptor: Descriptor,
    pub structure: Structure,
    pub raw_template: u8,
    pub active: u32,
    pub frame: u64,
    pub keyframe: u64,
    pub generation: u64,
    pub structure_updated: bool,
    pub active_targets_updated: bool,
    pub integrity: bool,
}
#[derive(Default)]
pub(crate) struct Receiver {
    observed: bool,
    structure: Option<Structure>,
    highest: Option<u64>,
    structure_frame: u64,
    changed_at: u64,
    active: u32,
    active_sequence: Option<u64>,
    last_at: Option<Instant>,
    generation: u64,
    integrity: Integrity,
}
fn extend(raw: u16, highest: Option<u64>) -> u64 {
    let Some(last) = highest else {
        return u64::from(raw);
    };
    let mut frame = (last & !65535) | u64::from(raw);
    if frame + 32768 < last {
        frame += 65536;
    } else if frame > last + 32768 && frame >= 65536 {
        frame -= 65536;
    }
    frame
}
impl Receiver {
    pub fn is_active(&self) -> bool {
        self.observed
    }
    pub fn restart(&mut self) {
        self.structure = None;
        self.structure_frame = 0;
        self.changed_at = 0;
        self.active = 0;
        self.active_sequence = None;
        self.integrity = Default::default();
        self.generation = self.generation.wrapping_add(1);
    }
    pub fn parse(
        &mut self,
        values: &ExtensionValues,
        sequence: u64,
        now: Instant,
        restart_enabled: bool,
    ) -> Option<Parsed> {
        let raw = &values.user_values.get::<RawDescriptor>()?.0;
        self.observed = true;
        let dd = descriptor::parse(raw, self.structure.as_ref())?;
        // Fixed source parses the descriptor using the prior structure before a
        // >30s optional timeout restart. A new structure is still required to resume.
        let mut restarted = false;
        if restart_enabled
            && self
                .last_at
                .is_some_and(|last| now.saturating_duration_since(last) > Duration::from_secs(30))
        {
            self.restart();
            restarted = true;
        }
        if restart_enabled {
            self.last_at = Some(now);
        }
        let mut frame = extend(dd.frame, self.highest);
        if restarted && self.highest.is_some_and(|old| frame < old) {
            frame = frame.saturating_add(65536);
        }
        self.highest = Some(self.highest.map_or(frame, |last| last.max(frame)));
        if frame < self.changed_at {
            return None;
        }
        let intact = self.integrity.add(sequence, frame, &dd);
        let updated = dd.attached.is_some();
        if let Some(s) = &dd.attached {
            if !dd.first || frame < self.structure_frame {
                return None;
            }
            if self
                .structure
                .as_ref()
                .is_none_or(|previous| previous.id != s.id)
            {
                self.changed_at = frame;
            }
            self.structure = Some(s.clone());
            self.structure_frame = frame;
        }
        let structure = self.structure.clone()?;
        let mut active_targets_updated = updated;
        if let Some(active) = dd.active {
            if self.active_sequence.is_none_or(|old| sequence > old) {
                active_targets_updated |= self.active != active;
                self.active = active;
                self.active_sequence = Some(sequence);
            }
        }
        Some(Parsed {
            descriptor: dd,
            structure,
            raw_template: raw[0] & 63,
            active: self.active,
            frame,
            keyframe: self.structure_frame,
            generation: self.generation,
            structure_updated: updated,
            active_targets_updated,
            integrity: intact,
        })
    }
}

impl Parsed {
    pub fn spatial(&self) -> u8 {
        self.descriptor.dependency.spatial
    }
    pub fn temporal(&self) -> u8 {
        self.descriptor.dependency.temporal
    }
    pub fn attached(&self) -> bool {
        self.structure_updated
    }
    pub fn resolutions(&self) -> Option<&[(u32, u32)]> {
        self.descriptor
            .attached
            .as_ref()
            .map(|s| s.resolutions.as_slice())
            .filter(|r| !r.is_empty())
    }

    pub fn active_spatial_ceiling(&self) -> Option<usize> {
        let active = self
            .descriptor
            .active
            .filter(|_| self.active_targets_updated)?;
        Some(
            (0..self.structure.targets)
                .filter(|target| active & (1 << target) != 0)
                .map(|target| self.target_max_layer(target).0)
                .max()
                .unwrap_or(0),
        )
    }

    fn target_max_layer(&self, target: usize) -> (usize, usize) {
        self.structure
            .templates
            .iter()
            .filter(|template| {
                template
                    .indications
                    .get(target)
                    .is_some_and(|&indication| indication != 0)
            })
            .fold((0, 0), |(spatial, temporal), template| {
                (
                    spatial.max(usize::from(template.spatial)),
                    temporal.max(usize::from(template.temporal)),
                )
            })
    }

    pub fn cost_targets(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        (0..self.structure.targets)
            .filter(|&target| {
                self.descriptor
                    .dependency
                    .indications
                    .get(target)
                    .is_some_and(|&indication| indication != 0)
            })
            .map(|target| self.target_max_layer(target))
    }
}
