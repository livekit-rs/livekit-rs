//! AV1 SVC dependency-aware frame bridge. The DD parser and output remapping
//! retain reference closure; codec OBUs retain their actual spatial dependencies.
mod decisions;
mod descriptor;
mod integrity;
mod one;
pub(crate) use one::OneSelector;
mod packet_descriptor;
mod packet_receiver;
mod packet_selector;
mod sequence;
use decisions::{Decision, Decisions};
use descriptor::{Descriptor, Structure};
pub(crate) use packet_receiver::{Parsed as PacketParsed, Receiver as PacketReceiver};
pub(crate) use packet_selector::Selector as PacketSelector;
pub(crate) use sequence::{Sequences as PacketSequences, Slot as PacketSlot};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use str0m::rtp::{ExtensionSerializer, ExtensionValues, FramePacketization, FrameUserExtensions};

pub(crate) const URI: &str =
    "https://aomediacodec.github.io/av1-rtp-spec/#dependency-descriptor-rtp-header-extension";
#[derive(Clone, Debug)]
pub(crate) struct RawDescriptor(pub Arc<[u8]>);
#[derive(Clone, Debug)]
pub(crate) struct OutPacketDescriptor(pub Arc<[u8]>);
#[derive(Clone, Debug)]
pub(crate) struct OutDescriptor {
    first: Arc<[u8]>,
    following: Arc<[u8]>,
}
#[derive(Debug)]
pub(crate) struct Serializer;
impl ExtensionSerializer for Serializer {
    fn write_to(&self, out: &mut [u8], values: &ExtensionValues) -> usize {
        if let Some(raw) = values.user_values.get::<OutPacketDescriptor>() {
            if out.len() < raw.0.len() {
                return 0;
            }
            out[..raw.0.len()].copy_from_slice(&raw.0);
            return raw.0.len();
        }
        let Some(value) = values.user_values.get::<OutDescriptor>() else {
            return 0;
        };
        let Some(position) = values.user_values.get::<FramePacketization>() else {
            return 0;
        };
        let bytes = if position.first {
            &value.first
        } else {
            &value.following
        };
        if out.len() < bytes.len() {
            return 0;
        }
        out[..bytes.len()].copy_from_slice(bytes);
        out[0] = (out[0] & 63) | (u8::from(position.first) << 7) | (u8::from(position.last) << 6);
        bytes.len()
    }
    fn parse_value(&self, input: &[u8], values: &mut ExtensionValues) -> bool {
        // Retain presence even when malformed: a present invalid DD must not
        // be confused with a missing extension and disable VP9's DD consumer.
        values.user_values.set(RawDescriptor(Arc::from(input)));
        (3..=255).contains(&input.len())
    }
    fn is_video(&self) -> bool {
        true
    }
    fn is_audio(&self) -> bool {
        false
    }
    fn requires_two_byte_form(&self, _: &ExtensionValues) -> bool {
        true
    }
}

struct Obu {
    range: Range<usize>,
    spatial: u8,
    temporal: u8,
    global: bool,
    coded: bool,
}
fn obus(data: &[u8]) -> Option<Vec<Obu>> {
    let mut cursor = 0;
    let mut result = Vec::new();
    while cursor < data.len() {
        if result.len() >= 4096 {
            return None;
        }
        let start = cursor;
        let header = *data.get(cursor)?;
        cursor += 1;
        if header & 0x81 != 0 || header & 2 == 0 {
            return None;
        }
        let kind = (header >> 3) & 15;
        let (spatial, temporal) = if header & 4 != 0 {
            let ext = *data.get(cursor)?;
            cursor += 1;
            if ext & 7 != 0 {
                return None;
            }
            ((ext >> 3) & 3, ext >> 5)
        } else {
            (0, 0)
        };
        let mut size = 0u64;
        let mut terminated = false;
        for byte in 0..8 {
            let value = *data.get(cursor)?;
            cursor += 1;
            size |= u64::from(value & 127) << (byte * 7);
            if value & 128 == 0 {
                terminated = true;
                break;
            }
        }
        if !terminated {
            return None;
        }
        cursor = cursor.checked_add(usize::try_from(size).ok()?)?;
        if cursor > data.len() {
            return None;
        }
        let coded = matches!(kind, 3 | 4 | 6 | 7);
        if spatial > 2 || temporal > 3 {
            return None;
        }
        result.push(Obu {
            range: start..cursor,
            spatial,
            temporal,
            global: !coded && header & 4 == 0,
            coded,
        });
    }
    (!result.is_empty()).then_some(result)
}

pub(crate) struct Picture {
    frames: Vec<Descriptor>,
    packet_layers: Vec<Option<(u8, u8)>>,
    packet_frames: Vec<Option<u16>>,
    structure: Structure,
    active_targets: u32,
    frame_target_masks: Vec<u32>,
    units: Vec<Obu>,
    pub highest: i32,
    pub temporal: u8,
    pub keyframe: bool,
    resolutions: Vec<(u32, u32)>,
}
impl Picture {
    pub(crate) fn packet_layers(&self) -> &[Option<(u8, u8)>] {
        &self.packet_layers
    }
    #[cfg(test)]
    fn selected_target(&self, mask: u32, spatial: i32, temporal: u8) -> Option<usize> {
        self.selected_intact_target(mask, spatial, temporal, |_| true)
    }
    fn selected_intact_target(
        &self,
        mask: u32,
        spatial: i32,
        temporal: u8,
        mut intact: impl FnMut(usize) -> bool,
    ) -> Option<usize> {
        // Pinned ProcessFrameDependencyStructure computes each target's highest
        // actual non-NotPresent SID/TID, then selects active targets high to low.
        (0..self.structure.targets)
            .filter_map(|target| {
                if mask & (1u32 << target) == 0 || !intact(target) {
                    return None;
                }
                let mut layer = (0u8, 0u8);
                for template in &self.structure.templates {
                    if *template.indications.get(target)? != 0 {
                        layer.0 = layer.0.max(template.spatial);
                        layer.1 = layer.1.max(template.temporal);
                    }
                }
                (i32::from(layer.0) <= spatial && layer.1 <= temporal).then_some((target, layer))
            })
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
            .map(|(target, _)| target)
    }
    #[cfg(test)]
    pub(crate) fn has_target_frames(&self, spatial: i32, temporal: u8) -> bool {
        self.frames.iter().enumerate().any(|(index, frame)| {
            self.selected_target(
                self.frame_target_masks
                    .get(index)
                    .copied()
                    .unwrap_or(self.active_targets),
                spatial,
                temporal,
            )
            .is_some_and(|target| {
                frame
                    .dependency
                    .indications
                    .get(target)
                    .is_some_and(|dti| *dti != 0)
            })
        })
    }
    pub(crate) fn packet_frames(&self) -> &[Option<u16>] {
        &self.packet_frames
    }
    /// Only an actual attached DD structure carries a new source size observation.
    pub(crate) fn attached_resolutions(&self) -> Option<&[(u32, u32)]> {
        self.frames
            .iter()
            .find_map(|f| f.attached.as_ref().map(|s| s.resolutions.as_slice()))
            .filter(|s| !s.is_empty())
    }

    pub fn cumulative_bytes(&self, spatial: usize) -> usize {
        self.units
            .iter()
            .filter(|obu| obu.global || obu.spatial as usize <= spatial)
            .map(|obu| obu.range.len())
            .sum()
    }
    #[cfg(test)]
    fn payload(&self, data: &[u8], spatial: i32) -> Option<Arc<[u8]>> {
        if spatial < 0 || spatial > self.highest {
            return None;
        }
        let mut output = Vec::with_capacity(self.cumulative_bytes(spatial as usize));
        for unit in &self.units {
            if unit.global || unit.spatial as i32 <= spatial {
                output.extend_from_slice(data.get(unit.range.clone())?);
            }
        }
        Some(output.into())
    }
}

#[derive(Default)]
pub(crate) struct Receiver {
    structure: Option<Structure>,
    active_targets: Option<u32>,
}
impl Receiver {
    /// Parse transactionally: malformed packets cannot replace the cached
    /// template structure. A descriptor is required for every coded component.
    pub fn picture(
        &mut self,
        data: &[u8],
        values: &ExtensionValues,
        keyframe: bool,
    ) -> Option<Picture> {
        let packet_values = values.user_values.get::<FrameUserExtensions>()?;
        let mut structure = self.structure.clone();
        let mut active_targets = self.active_targets;
        let mut frames: Vec<Descriptor> = Vec::new();
        let mut packet_layers = Vec::with_capacity(packet_values.0.len());
        let mut packet_frames = Vec::with_capacity(packet_values.0.len());
        let mut frame_target_masks = Vec::new();
        for packet in &packet_values.0 {
            let Some(raw) = packet.get::<RawDescriptor>() else {
                packet_layers.push(None);
                packet_frames.push(None);
                continue;
            };
            let dd = descriptor::parse(&raw.0, structure.as_ref())?;
            packet_layers.push(Some((dd.dependency.spatial, dd.dependency.temporal)));
            packet_frames.push(Some(dd.frame));
            if let Some(attached) = &dd.attached {
                structure = Some(attached.clone());
                active_targets = Some(if attached.targets == 32 {
                    u32::MAX
                } else {
                    (1u32 << attached.targets) - 1
                });
            }
            if let Some(mask) = dd.active {
                active_targets = Some(mask);
            }
            if let Some(last) = frames
                .last_mut()
                .filter(|previous| previous.frame == dd.frame)
            {
                if dd.first || last.last || dd.dependency != last.dependency {
                    return None;
                }
                last.last = dd.last;
            } else {
                if !dd.first
                    || frames.last().is_some_and(|previous| !previous.last)
                    || frames.len() >= 3
                {
                    return None;
                }
                frame_target_masks.push(active_targets.unwrap_or(u32::MAX));
                frames.push(dd);
            }
        }
        if frames.is_empty() || !frames.last()?.last {
            return None;
        }
        let temporal = frames[0].dependency.temporal;
        if temporal > 3 {
            return None;
        }
        // DD frame IDs and actual SID order define components; independent S
        // modes do not require lower SID components in this RTP picture.
        let mut previous = None;
        for frame in &frames {
            let sid = frame.dependency.spatial;
            if sid > 2
                || previous.is_some_and(|p| sid <= p)
                || frame.dependency.temporal != temporal
            {
                return None;
            }
            previous = Some(sid);
        }
        let units = obus(data)?;
        let highest = i32::from(frames.last()?.dependency.spatial);
        for frame in &frames {
            if !units.iter().any(|unit| {
                unit.coded && unit.spatial == frame.dependency.spatial && unit.temporal == temporal
            }) {
                return None;
            }
        }
        if units.iter().any(|unit| {
            unit.coded
                && (unit.temporal != temporal
                    || !frames
                        .iter()
                        .any(|frame| frame.dependency.spatial == unit.spatial))
        }) {
            return None;
        }
        let independent = keyframe && frames[0].dependency.diffs.is_empty();
        let resolutions = structure.as_ref()?.resolutions.clone();
        let structure = structure?;
        let active_targets = active_targets.unwrap_or(u32::MAX);
        self.structure = Some(structure.clone());
        self.active_targets = Some(active_targets);
        Some(Picture {
            frames,
            packet_layers,
            packet_frames,
            structure,
            active_targets,
            frame_target_masks,
            units,
            highest,
            temporal,
            keyframe: independent,
            resolutions,
        })
    }
}

#[derive(Default)]
pub(crate) struct Forwarder {
    next: u16,
    history: HashMap<u16, u16>,
    order: VecDeque<(u16, u16)>,
    source_structure: Option<Structure>,
    source_chains: Vec<SourceChain>,
    source_decisions: Decisions,
    selected_inputs: HashSet<u16>,
    selected_layers: HashMap<u16, (u8, u8)>,
}
#[derive(Default)]
struct SourceChain {
    active: bool,
    broken: bool,
    expected: HashSet<u16>,
}

pub(crate) struct Prepared {
    pub payload: Arc<[u8]>,
    pub descriptor: OutDescriptor,
    pub(crate) input_frames: Vec<u16>,
    number: u16,
    keyframe: bool,
}
impl Forwarder {
    fn record_source_decision(&mut self, frame: u16, forwarded: bool) {
        self.source_decisions.add(
            frame,
            if forwarded {
                Decision::Forwarded
            } else {
                Decision::Dropped
            },
        );
        for chain in &mut self.source_chains {
            chain
                .expected
                .retain(|frame| match self.source_decisions.get(*frame) {
                    Decision::Unknown => true,
                    Decision::Forwarded => false,
                    Decision::Missing | Decision::Dropped => {
                        chain.broken = true;
                        false
                    }
                });
        }
    }
    /// Observe actual DD chain transitions, including NotPresent frames that
    /// never enter an outgoing payload. Unknown chain predecessors can arrive
    /// later; an explicit dropped decision invalidates all waiting chains.
    pub(crate) fn observe_picture(
        &mut self,
        picture: &Picture,
        spatial: i32,
        temporal: u8,
    ) -> bool {
        self.observe_integrity(picture, spatial, temporal, true)
    }
    fn observe_integrity(
        &mut self,
        picture: &Picture,
        spatial: i32,
        temporal: u8,
        integrity: bool,
    ) -> bool {
        self.selected_inputs.clear();
        self.selected_layers.clear();
        for (index, frame) in picture.frames.iter().enumerate() {
            if self.source_structure.is_none() || frame.attached.is_some() {
                self.source_structure = Some(
                    frame
                        .attached
                        .as_ref()
                        .unwrap_or(&picture.structure)
                        .clone(),
                );
                self.source_chains = (0..picture.structure.chains)
                    .map(|_| SourceChain {
                        broken: true,
                        ..Default::default()
                    })
                    .collect();
            }
            let structure = self.source_structure.as_ref().unwrap();
            let mask = picture
                .frame_target_masks
                .get(index)
                .copied()
                .unwrap_or(picture.active_targets);
            let mut active = vec![false; self.source_chains.len()];
            if structure.chains > 0 {
                for target in 0..structure.targets {
                    if mask & (1u32 << target) != 0 {
                        if let Some(chain) = structure
                            .protected_by_chain
                            .get(target)
                            .and_then(|c| active.get_mut(*c))
                        {
                            *chain = true;
                        }
                    }
                }
            }
            for (chain_index, chain) in self.source_chains.iter_mut().enumerate() {
                if active[chain_index] && !chain.active {
                    chain.broken = true;
                }
                chain.active = active[chain_index];
                if !chain.active {
                    continue;
                }
                let Some(diff) = frame.dependency.chains.get(chain_index) else {
                    chain.broken = true;
                    continue;
                };
                if *diff == 0 {
                    chain.broken = false;
                    chain.expected.clear();
                } else if !chain.broken {
                    let previous = frame.frame.wrapping_sub(u16::from(*diff));
                    match self.source_decisions.get(previous) {
                        Decision::Forwarded => {}
                        Decision::Unknown if self.source_decisions.expect(previous) => {
                            chain.expected.insert(previous);
                        }
                        _ => chain.broken = true,
                    }
                }
            }
            let target = picture.selected_intact_target(mask, spatial, temporal, |target| {
                structure.chains == 0
                    || structure
                        .protected_by_chain
                        .get(target)
                        .and_then(|index| self.source_chains.get(*index))
                        .is_some_and(|chain| !chain.broken)
            });
            let selected = target.is_some_and(|target| {
                frame
                    .dependency
                    .indications
                    .get(target)
                    .is_some_and(|dti| *dti != 0)
            }) && !frame.dependency.diffs.iter().any(|diff| {
                self.source_decisions.get(frame.frame.wrapping_sub(*diff)) == Decision::Dropped
            });
            if selected {
                self.selected_inputs.insert(frame.frame);
                if let Some(target) = target {
                    let mut layer = (0, 0);
                    for t in &picture.structure.templates {
                        if t.indications.get(target).is_some_and(|dti| *dti != 0) {
                            layer.0 = layer.0.max(t.spatial);
                            layer.1 = layer.1.max(t.temporal);
                        }
                    }
                    self.selected_layers.insert(frame.frame, layer);
                }
                if integrity {
                    self.record_source_decision(frame.frame, true);
                }
            } else {
                self.record_source_decision(frame.frame, false);
            }
        }
        !self.selected_inputs.is_empty()
    }
    /// No dependency on a dropped or unknown input frame may be advertised as
    /// decodable. Dependencies among the actually retained DD frame IDs are internal.
    #[cfg(test)]
    pub fn prepare(&mut self, picture: &Picture, data: &[u8], spatial: i32) -> Option<Prepared> {
        if !self.observe_picture(picture, spatial, u8::MAX) {
            return None;
        }
        self.prepare_target(picture, data, spatial, u8::MAX)
    }
    pub fn prepare_target(
        &mut self,
        picture: &Picture,
        data: &[u8],
        _spatial: i32,
        _temporal: u8,
    ) -> Option<Prepared> {
        let frames: Vec<_> = picture
            .frames
            .iter()
            .filter(|frame| self.selected_inputs.contains(&frame.frame))
            .collect();
        if frames.is_empty() {
            return None;
        }
        let input_frames: Vec<_> = frames.iter().map(|frame| frame.frame).collect();
        let retained_spatial: Vec<_> = frames
            .iter()
            .map(|frame| frame.dependency.spatial)
            .collect();
        let highest = *retained_spatial.iter().max()?;
        let mut diffs = Vec::new();
        for frame in &frames {
            for diff in &frame.dependency.diffs {
                let input = frame.frame.wrapping_sub(*diff);
                if input_frames.contains(&input) {
                    continue;
                }
                if picture.keyframe {
                    return None;
                }
                let mapped = *self.history.get(&input)?;
                let distance = self.next.wrapping_sub(mapped);
                if distance == 0 || distance > 4096 {
                    return None;
                }
                if !diffs.contains(&distance) {
                    diffs.push(distance);
                }
            }
        }
        let resolution = picture
            .resolutions
            .get(highest as usize)
            .copied()
            .unwrap_or((0, 0));
        let first = descriptor::encode(self.next, picture.keyframe, &diffs, resolution)?;
        let following = if picture.keyframe {
            vec![0, (self.next >> 8) as u8, self.next as u8]
        } else {
            first.clone()
        };
        Some(Prepared {
            payload: {
                let mut retained = Vec::new();
                for unit in &picture.units {
                    if unit.global || retained_spatial.contains(&unit.spatial) {
                        retained.extend_from_slice(data.get(unit.range.clone())?);
                    }
                }
                if retained.is_empty() {
                    return None;
                }
                retained.into()
            },
            descriptor: OutDescriptor {
                first: first.into(),
                following: following.into(),
            },
            input_frames,
            number: self.next,
            keyframe: picture.keyframe,
        })
    }
    pub fn commit(&mut self, prepared: &Prepared) {
        if prepared.keyframe {
            self.history.clear();
            self.order.clear();
        }
        for input in &prepared.input_frames {
            self.history.insert(*input, prepared.number);
            self.order.push_back((*input, prepared.number));
        }
        while self.order.len() > 512 {
            if let Some((input, number)) = self.order.pop_front()
                && self.history.get(&input) == Some(&number)
            {
                self.history.remove(&input);
            }
        }
        self.next = prepared.number.wrapping_add(1);
    }
}

/// Test-owned original-wire three independent S decode targets. This is not
/// used by production packetization; production always parses actual peer DD.
#[cfg(test)]
pub(crate) fn independent_test_descriptor(spatial: u8, frame: u16, attached: bool) -> Vec<u8> {
    let mut w = descriptor::Writer::default();
    w.bits(3, 2);
    w.bits(u32::from(spatial), 6);
    w.bits(u32::from(frame), 16);
    if attached {
        w.bits(1, 1);
        w.bits(0, 4);
        w.bits(0, 6);
        w.bits(2, 5);
        w.bits(2, 2);
        w.bits(2, 2);
        w.bits(3, 2);
        for source in 0..3 {
            for target in 0..3 {
                w.bits(if source == target { 2 } else { 0 }, 2);
            }
        }
        for _ in 0..3 {
            w.bits(0, 1);
        }
        w.bits(0, 2);
        w.bits(0, 1);
    }
    w.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use descriptor::{Template, Writer};
    fn obu(spatial: u8, temporal: u8) -> Vec<u8> {
        vec![0x36, (temporal << 5) | (spatial << 3), 1, 0]
    }
    fn picture(first: u16, keyframe: bool, reference: Option<u16>) -> (Picture, Vec<u8>) {
        let mut data = Vec::new();
        let mut frames = Vec::new();
        for spatial in 0..3 {
            data.extend(obu(spatial, 0));
            let frame = first.wrapping_add(spatial as u16);
            let diffs = if spatial > 0 {
                vec![1]
            } else {
                reference
                    .map(|r| vec![frame.wrapping_sub(r)])
                    .unwrap_or_default()
            };
            frames.push(Descriptor {
                first: true,
                last: true,
                frame,
                dependency: Template {
                    spatial,
                    temporal: 0,
                    diffs,
                    indications: (0..3)
                        .map(|target| if target >= spatial { 2 } else { 0 })
                        .collect(),
                    ..Default::default()
                },
                attached: None,
                active: None,
            });
        }
        let units = obus(&data).unwrap();
        (
            Picture {
                packet_layers: Vec::new(),
                packet_frames: Vec::new(),
                structure: Structure {
                    id: 0,
                    targets: 3,
                    chains: 0,
                    protected_by_chain: vec![],
                    templates: frames.iter().map(|f| f.dependency.clone()).collect(),
                    resolutions: vec![(240, 136), (480, 272), (960, 544)],
                },
                active_targets: 7,
                frame_target_masks: Vec::new(),
                frames,
                units,
                highest: 2,
                temporal: 0,
                keyframe,
                resolutions: vec![(240, 136), (480, 272), (960, 544)],
            },
            data,
        )
    }
    #[test]
    fn dependency_closure_drops_lost_references_and_recovers_only_with_independent_picture() {
        let mut forwarder = Forwarder::default();
        let (key, data) = picture(65535, true, None);
        let prepared = forwarder.prepare(&key, &data, 0).unwrap();
        assert_eq!(prepared.payload.as_ref(), obu(0, 0));
        forwarder.commit(&prepared);
        let (inter, data) = picture(2, false, Some(65535));
        let prepared = forwarder.prepare(&inter, &data, 0).unwrap();
        forwarder.commit(&prepared);
        let (lost_dependent, data) = picture(8, false, Some(5));
        assert!(
            forwarder.prepare(&lost_dependent, &data, 2).is_none(),
            "input 5 was not forwarded"
        );
        let (recovery, data) = picture(11, true, None);
        let prepared = forwarder.prepare(&recovery, &data, 2).unwrap();
        assert_eq!(
            prepared.payload.len(),
            12,
            "all lower spatial OBUs retained"
        );
        forwarder.commit(&prepared);
        assert!(
            !forwarder.history.contains_key(&65535),
            "keyframe resets reference history"
        );
    }
    #[test]
    fn output_descriptor_flags_follow_actual_packetization_and_structure_only_first_packet() {
        let (picture, data) = picture(10, true, None);
        let prepared = Forwarder::default().prepare(&picture, &data, 2).unwrap();
        let mut values = ExtensionValues::default();
        values.user_values.set(prepared.descriptor);
        values.user_values.set(FramePacketization {
            first: true,
            last: false,
        });
        let mut bytes = [0u8; 255];
        let size = Serializer.write_to(&mut bytes, &values);
        let first = descriptor::parse(&bytes[..size], None).unwrap();
        assert!(first.first && !first.last);
        values.user_values.set(FramePacketization {
            first: false,
            last: true,
        });
        let size = Serializer.write_to(&mut bytes, &values);
        assert_eq!(size, 3);
        let last = descriptor::parse(&bytes[..size], first.attached.as_ref()).unwrap();
        assert!(!last.first && last.last);
        assert_eq!(first.frame, last.frame);
        assert_eq!(first.dependency, last.dependency);
    }
    #[test]
    fn receiver_reads_all_spatial_descriptors_and_rejects_missing_frame_boundaries() {
        // Build a three-spatial-template structure: dependency diff=1 refers to
        // the lower component in this same picture, not to a previous picture.
        let mut w = Writer::default();
        w.bits(3, 2);
        w.bits(0, 6);
        w.bits(10, 16);
        w.bits(1, 1);
        w.bits(0, 4);
        w.bits(0, 6);
        w.bits(0, 5);
        w.bits(2, 2);
        w.bits(2, 2);
        w.bits(3, 2);
        for _ in 0..3 {
            w.bits(2, 2);
        }
        w.bits(0, 1);
        for _ in 0..2 {
            w.bits(1, 1);
            w.bits(0, 4);
            w.bits(0, 1);
        }
        w.bits(0, 1);
        w.bits(0, 1); // no chains / resolutions
        let mut packet_values = Vec::new();
        for raw in [w.finish(), vec![0xc1, 0, 11], vec![0xc2, 0, 12]] {
            let mut values = str0m::rtp::UserExtensionValues::default();
            values.set(RawDescriptor(raw.into()));
            packet_values.push(values);
        }
        let mut values = ExtensionValues::default();
        values
            .user_values
            .set(FrameUserExtensions(packet_values.clone()));
        let data: Vec<_> = (0..3).flat_map(|s| obu(s, 0)).collect();
        let mut receiver = Receiver::default();
        let picture = receiver.picture(&data, &values, true).unwrap();
        assert_eq!(
            picture.packet_layers(),
            &[Some((0, 0)), Some((1, 0)), Some((2, 0))]
        );
        // A non-DD original packet keeps its index and is not assigned the
        // preceding/last descriptor's layer for forwarding statistics.
        let mut with_missing = packet_values.clone();
        let mut no_dd = str0m::rtp::UserExtensionValues::default();
        no_dd.set(123u64);
        with_missing.insert(1, no_dd);
        let mut missing_values = ExtensionValues::default();
        missing_values
            .user_values
            .set(FrameUserExtensions(with_missing));
        let with_missing = Receiver::default()
            .picture(&data, &missing_values, true)
            .unwrap();
        assert_eq!(
            with_missing.packet_layers(),
            &[Some((0, 0)), None, Some((1, 0)), Some((2, 0))]
        );
        assert_eq!(picture.highest, 2);
        assert!(picture.keyframe);
        assert_eq!(picture.payload(&data, 1).unwrap().len(), 8);
        packet_values[1].set(RawDescriptor(Arc::from([0x41, 0, 11]))); // missing first
        values.user_values.set(FrameUserExtensions(packet_values));
        assert!(receiver.picture(&data, &values, true).is_none());
    }
    #[test]
    fn independent_high_only_decode_target_has_no_same_picture_lower_requirement() {
        // Three independent S modes: each template is present only in its own
        // target. The first packet may legitimately carry template 2 + structure.
        let mut w = Writer::default();
        w.bits(3, 2);
        w.bits(2, 6);
        w.bits(10, 16);
        w.bits(1, 1);
        w.bits(0, 4); // attached, default remaining flags
        w.bits(0, 6);
        w.bits(2, 5); // structure 0, three targets
        w.bits(2, 2);
        w.bits(2, 2);
        w.bits(3, 2); // S0,S1,S2
        for spatial in 0..3 {
            for target in 0..3 {
                w.bits(if target == spatial { 2 } else { 0 }, 2);
            }
        }
        for _ in 0..3 {
            w.bits(0, 1);
        } // no frame diffs
        w.bits(0, 2); // ns(4): no chains
        w.bits(0, 1); // no resolutions
        let mut entry = str0m::rtp::UserExtensionValues::default();
        entry.set(RawDescriptor(w.finish().into()));
        let mut values = ExtensionValues::default();
        values.user_values.set(FrameUserExtensions(vec![entry]));
        let data = obu(2, 0);
        let picture = Receiver::default()
            .picture(&data, &values, true)
            .expect("original target2 accepts a valid independent high-only frame");
        let prepared = Forwarder::default()
            .prepare(&picture, &data, 2)
            .expect("valid original high-only dependency closure is empty");
        assert_eq!(prepared.payload.as_ref(), data);
        assert_eq!(prepared.input_frames, vec![10]);
    }
    #[test]
    fn independent_dti_retains_real_frame_ids_not_a_spatial_prefix() {
        let (mut picture, data) = picture(10, true, None);
        for (index, frame) in picture.frames.iter_mut().enumerate() {
            frame.dependency.diffs.clear();
            frame.dependency.indications = (0..3)
                .map(|target| if target == index { 2 } else { 0 })
                .collect();
        }
        picture.structure.templates = picture
            .frames
            .iter()
            .map(|f| f.dependency.clone())
            .collect();
        let mut forwarder = Forwarder::default();
        let prepared = forwarder.prepare(&picture, &data, 2).unwrap();
        assert_eq!(prepared.input_frames, vec![12]);
        assert_eq!(prepared.payload.as_ref(), obu(2, 0));
        forwarder.commit(&prepared);
        assert_eq!(forwarder.history.len(), 1);
        assert_eq!(forwarder.history.get(&12), Some(&0));
        assert!(!forwarder.history.contains_key(&10));
        // Actual active target updates change selection, not source SID prefixes.
        picture.active_targets = 1;
        let low = forwarder.prepare(&picture, &data, 2).unwrap();
        assert_eq!(low.input_frames, vec![10]);
        assert_eq!(low.payload.as_ref(), obu(0, 0));
        picture.active_targets = 0;
        assert!(!picture.has_target_frames(2, 2));
        assert!(forwarder.prepare(&picture, &data, 2).is_none());
        picture.active_targets = 4;
        picture.frames[2].dependency.diffs = vec![2];
        assert!(
            forwarder.prepare(&picture, &data, 2).is_none(),
            "DTI cannot relabel omitted frame10 as a forwarded dependency"
        );
    }
    #[test]
    #[ignore = "requires pinned original DD selector GO_AV1_TARGET_RESULTS"]
    fn original_go_dti_target_rows_match_actual_parsed_frame_ids() {
        let log = std::fs::read_to_string(std::env::var("GO_AV1_TARGET_RESULTS").unwrap()).unwrap();
        let rows: Vec<serde_json::Value> = log
            .lines()
            .filter_map(|line| line.strip_prefix("SOURCE_AV1_TARGET "))
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            rows.len(),
            4,
            "original two selected tests must actually execute"
        );
        for name in ["independent_high_only", "independent_full"] {
            let selected: Vec<_> = rows.iter().filter(|r| r["case"] == name).collect();
            let mut values = Vec::new();
            let mut data = Vec::new();
            for (index, row) in selected.iter().enumerate() {
                let spatial = row["spatial"].as_u64().unwrap() as u8;
                let frame = row["frame"].as_u64().unwrap() as u16;
                let mut entry = str0m::rtp::UserExtensionValues::default();
                entry.set(RawDescriptor(
                    independent_test_descriptor(spatial, frame, index == 0).into(),
                ));
                values.push(entry);
                data.extend(obu(spatial, 0));
            }
            let mut ext = ExtensionValues::default();
            ext.user_values.set(FrameUserExtensions(values));
            let picture = Receiver::default().picture(&data, &ext, true).unwrap();
            let prepared = Forwarder::default().prepare(&picture, &data, 2).unwrap();
            for row in selected {
                assert_eq!(
                    prepared
                        .input_frames
                        .contains(&(row["frame"].as_u64().unwrap() as u16)),
                    row["selected"].as_bool().unwrap()
                );
            }
            assert_eq!(prepared.payload.as_ref(), obu(2, 0));
        }
    }
    #[test]
    fn protected_chain_must_reject_explicitly_dropped_predecessor_without_frame_diffs() {
        fn chain(frame: u16, indication: u8, chain_diff: u8, keyframe: bool) -> (Picture, Vec<u8>) {
            let (mut picture, _) = picture(frame, keyframe, None);
            picture.frames.truncate(1);
            let f = &mut picture.frames[0];
            f.dependency.diffs.clear();
            f.dependency.indications = vec![indication];
            f.dependency.chains = vec![chain_diff];
            picture.structure.targets = 1;
            picture.structure.chains = 1;
            picture.structure.protected_by_chain = vec![0];
            picture.structure.templates = vec![f.dependency.clone()];
            picture.active_targets = 1;
            picture.highest = 0;
            let data = obu(0, 0);
            picture.units = obus(&data).unwrap();
            (picture, data)
        }
        let mut forwarder = Forwarder::default();
        let (initial, data) = chain(10, 2, 0, true);
        let first = forwarder.prepare(&initial, &data, 0).unwrap();
        forwarder.commit(&first);
        let (dropped, data) = chain(11, 0, 1, false);
        assert!(forwarder.prepare(&dropped, &data, 0).is_none());
        let (dependent, data) = chain(12, 3, 1, false);
        assert!(
            forwarder.prepare(&dependent, &data, 0).is_none(),
            "original protected chain rejects frame12 referencing explicitly dropped11 even when FrameDiffs is empty"
        );
        let (recovery, data) = chain(13, 2, 0, false);
        let recovery = forwarder.prepare(&recovery, &data, 0).unwrap();
        forwarder.commit(&recovery);
        assert!(forwarder.history.contains_key(&13));
    }
    #[test]
    #[ignore = "requires unchanged original DD chain trace GO_AV1_CHAIN_RESULTS"]
    fn original_go_protected_chain_rows_match_production_observation() {
        let log = std::fs::read_to_string(std::env::var("GO_AV1_CHAIN_RESULTS").unwrap()).unwrap();
        let rows: Vec<serde_json::Value> = log
            .lines()
            .filter_map(|line| line.strip_prefix("SOURCE_AV1_CHAIN "))
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            rows.len(),
            8,
            "actual original consumer must run all eight source frames"
        );
        let mut forwarder = Forwarder::default();
        for row in rows {
            let frame = row["frame"].as_u64().unwrap() as u16;
            let (mut picture, _) = picture(frame, frame == 10, None);
            picture.frames.truncate(1);
            picture.frames[0].dependency.indications = vec![row["dti"].as_u64().unwrap() as u8];
            picture.frames[0].dependency.chains = vec![row["chain_diff"].as_u64().unwrap() as u8];
            picture.frames[0].dependency.diffs = row["frame_diffs"]
                .as_array()
                .map(|v| v.iter().map(|d| d.as_u64().unwrap() as u16).collect())
                .unwrap_or_default();
            picture.structure.targets = 1;
            picture.structure.chains = 1;
            picture.structure.protected_by_chain = vec![0];
            picture.structure.templates = vec![picture.frames[0].dependency.clone()];
            picture.active_targets = 1;
            assert_eq!(
                forwarder.observe_picture(&picture, 0, 0),
                row["selected"].as_bool().unwrap(),
                "original source chain/frame {frame}"
            );
        }
        // This checks the actual chain consumer used before Writer admission;
        // it does not certify output reference remapping for unknown frames.
    }
    #[test]
    fn malformed_obu_sizes_and_extensions_fail_closed() {
        for bytes in [
            vec![],
            vec![0x36],
            vec![0x36, 0, 128],
            vec![0x36, 0, 10, 0],
            vec![0x36, 1, 0],
            vec![0xb6, 0, 0],
        ] {
            assert!(obus(&bytes).is_none(), "{bytes:?}");
        }
        assert!(
            obus(&obu(0, 3)).is_some(),
            "TID3 is valid within fixed Go max temporal3"
        );
        assert!(
            obus(&obu(0, 4)).is_none(),
            "TID4 exceeds the fixed Go range and remains rejected"
        );
    }
    #[test]
    fn actual_attached_dependency_structure_provides_three_real_media_sizes() {
        // Build a three-spatial-template structure: dependency diff=1 refers to
        // the lower component in this same picture, not to a previous picture.
        let mut w = Writer::default();
        w.bits(3, 2);
        w.bits(0, 6);
        w.bits(10, 16);
        w.bits(1, 1);
        w.bits(0, 4);
        w.bits(0, 6);
        w.bits(0, 5);
        w.bits(2, 2);
        w.bits(2, 2);
        w.bits(3, 2);
        for _ in 0..3 {
            w.bits(2, 2);
        }
        w.bits(0, 1);
        for _ in 0..2 {
            w.bits(1, 1);
            w.bits(0, 4);
            w.bits(0, 1);
        }
        w.bits(0, 1);
        w.bits(1, 1); // actual attached resolutions, not advertised track dimensions
        for (width, height) in [(240, 136), (480, 272), (960, 544)] {
            w.bits(width - 1, 16);
            w.bits(height - 1, 16);
        }
        let mut packet_values = Vec::new();
        for raw in [w.finish(), vec![0xc1, 0, 11], vec![0xc2, 0, 12]] {
            let mut values = str0m::rtp::UserExtensionValues::default();
            values.set(RawDescriptor(raw.into()));
            packet_values.push(values);
        }
        let mut values = ExtensionValues::default();
        values
            .user_values
            .set(FrameUserExtensions(packet_values.clone()));
        let data: Vec<_> = (0..3).flat_map(|s| obu(s, 0)).collect();
        let mut receiver = Receiver::default();
        let picture = receiver.picture(&data, &values, true).unwrap();
        assert_eq!(
            picture.attached_resolutions(),
            Some([(240, 136), (480, 272), (960, 544)].as_slice())
        );
        assert_eq!(picture.highest, 2);
        assert!(picture.keyframe);
        assert_eq!(picture.payload(&data, 1).unwrap().len(), 8);
        packet_values[1].set(RawDescriptor(Arc::from([0x41, 0, 11]))); // missing first
        values.user_values.set(FrameUserExtensions(packet_values));
        assert!(receiver.picture(&data, &values, true).is_none());
    }

    #[test]
    fn declared_one_mode_dd_keeps_whole_encoding_cost_without_inventing_spatial_tracks() {
        use crate::audio_level::AudioLevelParams;
        use crate::uptrack::UpTrack;
        use rtc_core::ids::{ParticipantSid, TrackSid, TransportId};
        use rtc_core::media_api::PublishedTrackSpec;
        use rtc_core::model::{
            TrackCodec, TrackKind, TrackSource, VideoLayer, VideoLayerMode, VideoQuality,
        };
        use std::time::{Duration, Instant};
        use str0m::media::{Frequency, MediaTime, Mid};

        for mode in [
            VideoLayerMode::OneSpatialLayerPerStream,
            VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr,
            VideoLayerMode::MultipleSpatialLayersPerStream,
        ] {
            let now = Instant::now();
            let low = VideoLayer {
                quality: VideoQuality::Low,
                width: 320,
                height: 180,
                bitrate: 0,
                ssrc: 0,
                spatial_layer: 0,
                rid: String::new(),
                repair_ssrc: 0,
            };
            let spec = PublishedTrackSpec {
                backup_codec_policy: Default::default(),
                sid: TrackSid::from("TR_declared_dd"),
                kind: TrackKind::Video,
                source: TrackSource::Camera,
                cid: "camera".into(),
                layers: vec![low.clone()],
                codecs: vec![TrackCodec {
                    mime_type: "video/AV1".into(),
                    layer_mode: mode,
                    layers: vec![low],
                    ..Default::default()
                }],
                stereo: false,
                disable_dtx: false,
                disable_red: false,
                encrypted: false,
                muted: false,
            };
            let mut up = UpTrack::new(
                spec,
                ParticipantSid::from("PA_source"),
                TransportId::next(),
                Mid::from("0"),
                TrackKind::Video,
                "video/AV1".into(),
                vec![],
                &AudioLevelParams {
                    active_level: 35,
                    min_percentile: 40,
                    update_interval: Duration::from_millis(500),
                    smooth_intervals: 2,
                },
                Duration::from_secs(5),
                now,
            );
            // Existing exact AV1 module fixture: three genuine syntactic spatial
            // OBUs and a dependency-closed picture, not a pixel decoder surrogate.
            let (picture, payload) = picture(0, true, None);
            let is_svc = mode == VideoLayerMode::MultipleSpatialLayersPerStream;
            up.on_av1_frame(&picture, MediaTime::new(0, Frequency::NINETY_KHZ), now);
            assert_eq!(
                up.svc, is_svc,
                "DD presence cannot override declared stream-spatial mode {mode:?}"
            );
            assert_eq!(
                up.available(now),
                if is_svc {
                    [true, true, true]
                } else {
                    [true, false, false]
                }
            );
            assert_eq!(
                up.layers[0].width, 320,
                "metadata remains the source ONE-mode LOW size"
            );
            let generation = up.bitrate_observation().generation;
            up.on_av1_frame(
                &picture,
                MediaTime::new(9000, Frequency::NINETY_KHZ),
                now + Duration::from_millis(100),
            );
            assert_eq!(
                up.bitrate_observation().generation,
                generation,
                "steady ONE mode cannot reset its cost window on every frame"
            );
            up.roll_bitrate_window(now + Duration::from_secs(1));
            let costs = up.bitrates();
            if is_svc {
                assert_eq!(
                    costs,
                    [64, 128, 192],
                    "explicit MULTIPLE keeps genuine cumulative spatial costs"
                );
            } else {
                assert_eq!(
                    costs,
                    [192, 0, 0],
                    "one encoding cost includes ALL real spatial OBU bytes"
                );
                let prepared = Forwarder::default()
                    .prepare(&picture, &payload, picture.highest)
                    .unwrap();
                assert_eq!(
                    prepared.payload.as_ref(),
                    payload.as_slice(),
                    "retaining the stream must not crop to metadata LOW"
                );
            }
        }
    }
}

/// Actual original DD packet metadata for the fixed source rate calculator.
#[derive(Default)]
pub(crate) struct RateParser {
    structure: Option<Structure>,
    active: Option<(u64, u32)>,
}
pub(crate) struct RatePacket {
    pub frame: u16,
    pub spatial: usize,
    pub temporal: usize,
    pub diffs: Vec<u16>,
    pub maximum: (usize, usize),
}
impl RateParser {
    pub(crate) fn parse(&mut self, values: &ExtensionValues, sequence: u64) -> Option<RatePacket> {
        let raw = values.user_values.get::<RawDescriptor>()?;
        let descriptor = descriptor::parse(&raw.0, self.structure.as_ref())?;
        if descriptor.attached.is_some() && !descriptor.first {
            return None;
        }
        if let Some(structure) = descriptor.attached {
            self.structure = Some(structure)
        }
        if let Some(mask) = descriptor.active {
            if self.active.is_none_or(|(last, _)| sequence > last) {
                self.active = Some((sequence, mask))
            }
        }
        let structure = self.structure.as_ref()?;
        let mask = self.active.map_or(0, |(_, mask)| mask);
        let mut maximum = (0usize, 0usize);
        for target in 0..structure.targets {
            if mask & (1 << target) == 0 {
                continue;
            }
            for template in &structure.templates {
                if template
                    .indications
                    .get(target)
                    .is_some_and(|dti| *dti != 0)
                {
                    maximum.0 = maximum.0.max(usize::from(template.spatial));
                    maximum.1 = maximum.1.max(usize::from(template.temporal));
                }
            }
        }
        Some(RatePacket {
            frame: descriptor.frame,
            spatial: usize::from(descriptor.dependency.spatial),
            temporal: usize::from(descriptor.dependency.temporal),
            diffs: descriptor.dependency.diffs,
            maximum,
        })
    }
}
