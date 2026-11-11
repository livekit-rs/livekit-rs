//! AV1 dependency descriptor reader. Wire layout follows LiveKit's Apache-2.0
//! reader at e5655ac20267fa5005dccaf3f0e3bfc3ea688c07 (Copyright 2023 LiveKit,
//! Inc.). See rtc-media/THIRD_PARTY.md for attribution and retained license.

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Template {
    pub spatial: u8,
    pub temporal: u8,
    pub indications: Vec<u8>,
    pub diffs: Vec<u16>,
    pub chains: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Structure {
    pub id: u8,
    pub targets: usize,
    pub chains: usize,
    pub protected_by_chain: Vec<usize>,
    pub templates: Vec<Template>,
    pub resolutions: Vec<(u32, u32)>,
}
#[derive(Clone, Debug)]
pub(super) struct Descriptor {
    pub first: bool,
    pub last: bool,
    pub frame: u16,
    pub dependency: Template,
    pub attached: Option<Structure>,
    pub active: Option<u32>,
}

pub(super) struct Bits<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Bits<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    pub fn read(&mut self, count: usize) -> Option<u32> {
        if count > 32 || self.pos.checked_add(count)? > self.bytes.len().checked_mul(8)? {
            return None;
        }
        let mut result = 0;
        for _ in 0..count {
            result = (result << 1) | ((self.bytes[self.pos / 8] >> (7 - self.pos % 8)) & 1) as u32;
            self.pos += 1;
        }
        Some(result)
    }
    fn flag(&mut self) -> Option<bool> {
        Some(self.read(1)? != 0)
    }
    fn ns(&mut self, count: u32) -> Option<u32> {
        if count == 0 {
            return None;
        }
        if count == 1 {
            return Some(0);
        }
        let width = (32 - count.leading_zeros()) as usize;
        let cut = (1 << width) - count;
        let value = self.read(width - 1)?;
        if value < cut {
            Some(value)
        } else {
            Some((value << 1) + self.read(1)? - cut)
        }
    }
}

fn structure(bits: &mut Bits<'_>) -> Option<Structure> {
    let id = bits.read(6)? as u8;
    let targets = bits.read(5)? as usize + 1;
    let mut templates = Vec::<Template>::new();
    let (mut spatial, mut temporal) = (0u8, 0u8);
    loop {
        if templates.len() == 64 {
            return None;
        }
        templates.push(Template {
            spatial,
            temporal,
            ..Default::default()
        });
        match bits.read(2)? {
            0 => {}
            1 => {
                temporal += 1;
                if temporal >= 8 {
                    return None;
                }
            }
            2 => {
                spatial += 1;
                temporal = 0;
                if spatial >= 4 {
                    return None;
                }
            }
            3 => break,
            _ => unreachable!(),
        }
    }
    for template in &mut templates {
        for _ in 0..targets {
            template.indications.push(bits.read(2)? as u8);
        }
    }
    for template in &mut templates {
        while bits.flag()? {
            if template.diffs.len() >= 32 {
                return None;
            }
            template.diffs.push(bits.read(4)? as u16 + 1);
        }
    }
    let chains = bits.ns(targets as u32 + 1)? as usize;
    if chains > targets {
        return None;
    }
    let mut protected_by_chain = Vec::with_capacity(targets);
    if chains > 0 {
        for _ in 0..targets {
            let chain = bits.ns(chains as u32)? as usize;
            if chain >= chains {
                return None;
            }
            protected_by_chain.push(chain);
        }
        for template in &mut templates {
            for _ in 0..chains {
                template.chains.push(bits.read(4)? as u8);
            }
        }
    }
    let mut resolutions = Vec::new();
    if bits.flag()? {
        for _ in 0..=spatial {
            resolutions.push((bits.read(16)? + 1, bits.read(16)? + 1));
        }
    }
    Some(Structure {
        id,
        targets,
        chains,
        protected_by_chain,
        templates,
        resolutions,
    })
}

pub(super) fn parse(bytes: &[u8], previous: Option<&Structure>) -> Option<Descriptor> {
    if !(3..=255).contains(&bytes.len()) {
        return None;
    }
    let mut bits = Bits::new(bytes);
    let first = bits.flag()?;
    let last = bits.flag()?;
    let template_id = bits.read(6)? as u8;
    let frame = bits.read(16)? as u16;
    let (mut attached, mut active) = (None, None);
    let (mut active_flag, mut custom_dti, mut custom_diffs, mut custom_chains) =
        (false, false, false, false);
    if bytes.len() > 3 {
        let has_structure = bits.flag()?;
        active_flag = bits.flag()?;
        custom_dti = bits.flag()?;
        custom_diffs = bits.flag()?;
        custom_chains = bits.flag()?;
        if has_structure {
            let value = structure(&mut bits)?;
            active = Some(if value.targets == 32 {
                u32::MAX
            } else {
                (1 << value.targets) - 1
            });
            attached = Some(value);
        }
    }
    let structure = attached.as_ref().or(previous)?;
    if active_flag {
        active = Some(bits.read(structure.targets)?);
    }
    let index = template_id.wrapping_sub(structure.id) & 63;
    let mut dependency = structure.templates.get(index as usize)?.clone();
    if custom_dti {
        for indication in &mut dependency.indications {
            *indication = bits.read(2)? as u8;
        }
    }
    if custom_diffs {
        dependency.diffs.clear();
        loop {
            let size = bits.read(2)? as usize;
            if size == 0 {
                break;
            }
            if dependency.diffs.len() >= 32 {
                return None;
            }
            dependency.diffs.push(bits.read(size * 4)? as u16 + 1);
        }
    }
    if custom_chains {
        for chain in &mut dependency.chains {
            *chain = bits.read(8)? as u8;
        }
    }
    if !structure.resolutions.is_empty()
        && dependency.spatial as usize >= structure.resolutions.len()
    {
        return None;
    }
    Some(Descriptor {
        first,
        last,
        frame,
        dependency,
        attached,
        active,
    })
}

#[derive(Default)]
pub(super) struct Writer {
    bytes: Vec<u8>,
    position: usize,
}
impl Writer {
    pub fn bits(&mut self, value: u32, count: usize) {
        for shift in (0..count).rev() {
            if self.position.is_multiple_of(8) {
                self.bytes.push(0);
            }
            self.bytes[self.position / 8] |=
                (((value >> shift) & 1) as u8) << (7 - self.position % 8);
            self.position += 1;
        }
    }
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// Flatten selected AV1 components to one decode target. Dependencies refer to
/// the new outgoing picture sequence; spatial dependencies inside this picture
/// have been retained in its OBU payload and need no external reference.
pub(super) fn encode(
    frame: u16,
    keyframe: bool,
    diffs: &[u16],
    resolution: (u32, u32),
) -> Option<Vec<u8>> {
    if diffs.len() > 32 || diffs.iter().any(|diff| *diff == 0 || *diff > 4096) {
        return None;
    }
    let mut w = Writer::default();
    w.bits(0, 2);
    w.bits(u32::from(!keyframe), 6);
    w.bits(frame as u32, 16);
    if keyframe {
        w.bits(1, 1);
        w.bits(0, 4); // attached structure; no custom flags
        w.bits(0, 6);
        w.bits(0, 5); // structure ID 0; one decode target
        w.bits(0, 2);
        w.bits(3, 2); // two templates at S0T0
        w.bits(2, 2);
        w.bits(3, 2); // keyframe SWITCH, interframe REQUIRED
        w.bits(0, 1); // keyframe no frame dependencies
        w.bits(1, 1);
        w.bits(0, 4);
        w.bits(0, 1); // interframe diff=1
        w.bits(0, 1); // zero chains, ns(2)
        let (width, height) = resolution;
        if width > 0 && width <= 65536 && height > 0 && height <= 65536 {
            w.bits(1, 1);
            w.bits(width - 1, 16);
            w.bits(height - 1, 16);
        } else {
            w.bits(0, 1);
        }
    } else if diffs != [1] {
        w.bits(0, 3);
        w.bits(1, 1);
        w.bits(0, 1); // custom frame diffs
        for diff in diffs {
            let value = u32::from(*diff) - 1;
            let size = if value < 16 {
                1
            } else if value < 256 {
                2
            } else {
                3
            };
            w.bits(size, 2);
            w.bits(value, (size * 4) as usize);
        }
        w.bits(0, 2);
    }
    Some(w.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_descriptor_roundtrip_covers_structure_and_custom_references() {
        let mut bytes = encode(65535, true, &[], (960, 544)).unwrap();
        bytes[0] |= 0xc0;
        let first = parse(&bytes, None).unwrap();
        assert!(first.first && first.last);
        assert_eq!(first.frame, 65535);
        assert!(first.dependency.diffs.is_empty());
        let structure = first.attached.unwrap();
        assert_eq!(structure.resolutions, vec![(960, 544)]);
        for diffs in [vec![1], vec![2, 17, 4096], vec![]] {
            let mut bytes = encode(0, false, &diffs, (0, 0)).unwrap();
            bytes[0] |= 0xc0;
            assert_eq!(
                parse(&bytes, Some(&structure)).unwrap().dependency.diffs,
                diffs
            );
        }
        for end in 0..bytes.len() {
            assert!(
                parse(&bytes[..end], None).is_none(),
                "truncated attached structure at {end}"
            );
        }
        assert!(encode(0, false, &[0], (0, 0)).is_none());
        assert!(encode(0, false, &[4097], (0, 0)).is_none());
        assert!(parse(&[0xc0, 0, 0], None).is_none());
        assert!(parse(&[0xff, 0, 0], Some(&structure)).is_none());
    }
    #[test]
    fn untrusted_extension_inputs_are_bounded_and_never_panic() {
        // Deterministic broad malformed input coverage, without allocating from
        // unchecked wire counts or relying on a particular template structure.
        let mut random = 0x4d595df4d0f33173u64;
        for len in 0..=256 {
            for _ in 0..16 {
                let bytes: Vec<_> = (0..len)
                    .map(|_| {
                        random ^= random << 13;
                        random ^= random >> 7;
                        random ^= random << 17;
                        random as u8
                    })
                    .collect();
                let _ = parse(&bytes, None);
            }
        }
        let mut bits = Bits::new(&[0xff]);
        assert_eq!(bits.read(9), None);
        assert_eq!(bits.ns(0), None);
    }
}
