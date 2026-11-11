//! Fixed Go simulcast DD temporal selection and per-subscriber descriptor clock.
use super::{OutPacketDescriptor, PacketParsed, packet_descriptor};

#[derive(Default)]
struct FrameHistory {
    highest: Option<u64>,
    decided: u64,
    forwarded: u64,
}

impl FrameHistory {
    fn decision(&self, frame: u64) -> Option<bool> {
        let delta = self.highest?.checked_sub(frame)?;
        if delta >= 64 || self.decided & (1 << delta) == 0 {
            return None;
        }
        Some(self.forwarded & (1 << delta) != 0)
    }

    fn record(&mut self, frame: u64, forwarded: bool) -> bool {
        let highest = self.highest.unwrap_or(frame);
        if self.highest.is_none() || frame >= highest.saturating_add(64) {
            *self = Self {
                highest: Some(frame),
                ..Self::default()
            };
        } else if frame > highest {
            self.decided <<= frame - highest;
            self.forwarded <<= frame - highest;
            self.highest = Some(frame);
        } else if highest - frame >= 64 {
            return false;
        }
        let bit = 1 << (self.highest.unwrap_or(frame) - frame);
        self.decided |= bit;
        if forwarded {
            self.forwarded |= bit;
        }
        true
    }
}

#[derive(Default)]
pub(crate) struct OneSelector {
    pub source: Option<u64>,
    pub reference: Option<i32>,
    pub disabled: bool,
    temporal: u8,
    references: FrameHistory,
    decisions: FrameHistory,
    descriptor_source: Option<(u64, u64)>,
    frame_offset: u64,
    last_frame: Option<u64>,
}

impl OneSelector {
    pub fn select(
        &mut self,
        packet: &PacketParsed,
        cap: u8,
        key: bool,
        late: bool,
        marker: bool,
    ) -> Option<u8> {
        let first = packet.descriptor.first && !late;
        let mut current = self.temporal;
        if current < cap && key {
            current = cap;
        } else if current < cap
            && first
            && self.references.highest.is_some()
            && packet.descriptor.dependency.diffs.iter().all(|diff| {
                *diff != 0
                    && packet
                        .frame
                        .checked_sub(u64::from(*diff))
                        .is_some_and(|frame| self.references.decision(frame) == Some(true))
            })
        {
            for (target, maximum) in target_temporals(packet) {
                if maximum > current
                    && maximum <= cap
                    && packet.descriptor.dependency.indications.get(target) == Some(&2)
                {
                    current = maximum;
                }
            }
        }
        self.temporal = if current > cap && marker {
            cap
        } else {
            current
        };
        if key {
            self.references = FrameHistory::default();
        }
        if key || first {
            self.references
                .record(packet.frame, packet.temporal() <= current);
        }
        let forwarded = self.decisions.decision(packet.frame).unwrap_or_else(|| {
            let selected = packet.temporal() <= current;
            self.decisions.record(packet.frame, selected) && selected
        });
        forwarded.then_some(current)
    }

    pub fn start_source(&mut self, source: u64, layer: i32) {
        self.source = Some(source);
        self.reference.get_or_insert(layer);
        self.decisions = FrameHistory::default();
    }

    pub fn descriptor(
        &mut self,
        source: u64,
        packet: &PacketParsed,
        temporal: u8,
    ) -> Option<OutPacketDescriptor> {
        let epoch = (source, packet.generation);
        if self.descriptor_source != Some(epoch) {
            if !packet.attached() {
                return None;
            }
            if let Some(last) = self.last_frame {
                if packet.frame.wrapping_add(self.frame_offset) <= last {
                    self.frame_offset = last.wrapping_add(1).wrapping_sub(packet.frame);
                }
            }
            self.descriptor_source = Some(epoch);
        }
        let frame = packet.frame.wrapping_add(self.frame_offset);
        let mask = target_temporals(packet)
            .filter(|(_, maximum)| *maximum <= temporal)
            .fold(0, |mask, (target, _)| mask | (1 << target))
            & packet.active;
        let raw = packet_descriptor::encode(
            &packet.descriptor,
            &packet.structure,
            packet.raw_template,
            frame as u16,
            mask,
        )?;
        self.last_frame = Some(self.last_frame.map_or(frame, |last| last.max(frame)));
        Some(OutPacketDescriptor(raw.into()))
    }
}

fn target_temporals(packet: &PacketParsed) -> impl Iterator<Item = (usize, u8)> + '_ {
    (0..packet.structure.targets).filter_map(|target| {
        packet
            .structure
            .templates
            .iter()
            .filter(|template| {
                template
                    .indications
                    .get(target)
                    .is_some_and(|dti| *dti != 0)
            })
            .map(|template| template.temporal)
            .max()
            .map(|maximum| (target, maximum))
    })
}
