//! Original per-packet DD serialization. Unlike picture flattening, source
//! template/frame dependencies and frame boundaries remain unchanged. Layout
//! follows fixed Apache-2.0 LiveKit dependencydescriptorwriter.go.
use super::descriptor::{Descriptor, Structure, Writer};
fn ns(w: &mut Writer, value: u32, count: u32) -> Option<()> {
    if count == 0 || value >= count {
        return None;
    }
    if count == 1 {
        return Some(());
    }
    let width = (32 - count.leading_zeros()) as usize;
    let cut = (1u32 << width) - count;
    if value < cut {
        w.bits(value, width - 1);
    } else {
        w.bits(value + cut, width);
    }
    Some(())
}
fn structure(w: &mut Writer, s: &Structure) -> Option<()> {
    if s.targets == 0 || s.targets > 32 || s.templates.is_empty() || s.templates.len() > 64 {
        return None;
    }
    w.bits(u32::from(s.id), 6);
    w.bits((s.targets - 1) as u32, 5);
    for (index, t) in s.templates.iter().enumerate() {
        let transition = if let Some(n) = s.templates.get(index + 1) {
            if (n.spatial, n.temporal) == (t.spatial, t.temporal) {
                0
            } else if n.spatial == t.spatial && n.temporal == t.temporal + 1 {
                1
            } else if n.spatial == t.spatial + 1 && n.temporal == 0 {
                2
            } else {
                return None;
            }
        } else {
            3
        };
        w.bits(transition, 2);
    }
    for t in &s.templates {
        if t.indications.len() != s.targets {
            return None;
        }
        for d in &t.indications {
            if *d > 3 {
                return None;
            }
            w.bits(u32::from(*d), 2);
        }
    }
    for t in &s.templates {
        for d in &t.diffs {
            if !(1..=16).contains(d) {
                return None;
            }
            w.bits(1, 1);
            w.bits(u32::from(*d - 1), 4);
        }
        w.bits(0, 1);
    }
    ns(w, s.chains as u32, s.targets as u32 + 1)?;
    if s.chains > 0 {
        if s.protected_by_chain.len() != s.targets {
            return None;
        }
        for chain in &s.protected_by_chain {
            ns(w, *chain as u32, s.chains as u32)?;
        }
        for t in &s.templates {
            if t.chains.len() != s.chains {
                return None;
            }
            for d in &t.chains {
                if *d > 15 {
                    return None;
                }
                w.bits(u32::from(*d), 4);
            }
        }
    }
    w.bits(u32::from(!s.resolutions.is_empty()), 1);
    if !s.resolutions.is_empty() {
        let count = usize::from(s.templates.last()?.spatial) + 1;
        if s.resolutions.len() != count {
            return None;
        }
        for (width, height) in &s.resolutions {
            if !(1..=65536).contains(width) || !(1..=65536).contains(height) {
                return None;
            }
            w.bits(*width - 1, 16);
            w.bits(*height - 1, 16);
        }
    }
    Some(())
}
/// Rewrite only public output frame numbering and active-target mask. Original
/// source first/last boundaries, SID/TID, DTI, frame/chain diffs are preserved.
pub(super) fn encode(
    dd: &Descriptor,
    s: &Structure,
    template_id: u8,
    frame: u16,
    active: u32,
) -> Option<Vec<u8>> {
    let template = s
        .templates
        .get(usize::from(template_id.wrapping_sub(s.id) & 63))?;
    let custom_dti = dd.dependency.indications != template.indications;
    let custom_diffs = dd.dependency.diffs != template.diffs;
    let custom_chains = dd.dependency.chains != template.chains;
    let mut w = Writer::default();
    w.bits(u32::from(dd.first), 1);
    w.bits(u32::from(dd.last), 1);
    w.bits(u32::from(template_id), 6);
    w.bits(u32::from(frame), 16);
    w.bits(u32::from(dd.attached.is_some()), 1);
    w.bits(1, 1);
    w.bits(u32::from(custom_dti), 1);
    w.bits(u32::from(custom_diffs), 1);
    w.bits(u32::from(custom_chains), 1);
    if let Some(attached) = &dd.attached {
        structure(&mut w, attached)?;
    }
    w.bits(active, s.targets);
    if custom_dti {
        if dd.dependency.indications.len() != s.targets {
            return None;
        }
        for d in &dd.dependency.indications {
            if *d > 3 {
                return None;
            }
            w.bits(u32::from(*d), 2);
        }
    }
    if custom_diffs {
        for diff in &dd.dependency.diffs {
            let value = u32::from(diff.checked_sub(1)?);
            let size = if value < 16 {
                1
            } else if value < 256 {
                2
            } else if value < 4096 {
                3
            } else {
                return None;
            };
            w.bits(size, 2);
            w.bits(value, size as usize * 4);
        }
        w.bits(0, 2);
    }
    if custom_chains {
        if dd.dependency.chains.len() != s.chains {
            return None;
        }
        for diff in &dd.dependency.chains {
            w.bits(u32::from(*diff), 8);
        }
    }
    let out = w.finish();
    (out.len() <= 255).then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::av1::descriptor::Template;
    #[test]
    fn original_fragment_flags_and_unknown_reference_survive_actual_wire_serialization() {
        let template = Template {
            spatial: 0,
            temporal: 3,
            indications: vec![3],
            diffs: vec![1],
            chains: vec![1],
        };
        // Real structures start at S0/T0; the T3 template has its actual source ID3.
        let mut templates = vec![];
        for temporal in 0..4 {
            templates.push(Template {
                temporal,
                ..template.clone()
            })
        }
        let structure = Structure {
            id: 0,
            targets: 1,
            chains: 1,
            protected_by_chain: vec![0],
            templates,
            resolutions: vec![],
        };
        for (frame, first, last) in [(16, true, false), (16, false, true), (15, true, true)] {
            let dd = Descriptor {
                first,
                last,
                frame,
                dependency: template.clone(),
                attached: None,
                active: Some(1),
            };
            let bytes = encode(&dd, &structure, 3, frame, 1).unwrap();
            let actual = crate::av1::descriptor::parse(&bytes, Some(&structure)).unwrap();
            assert_eq!(
                (actual.frame, actual.first, actual.last),
                (frame, first, last)
            );
            assert_eq!(
                (actual.dependency.spatial, actual.dependency.temporal),
                (0, 3)
            );
            assert_eq!(
                actual.dependency.diffs,
                vec![1],
                "unknown frame15 may arrive after16: never invent a new output reference graph"
            );
            assert_eq!(actual.dependency.chains, vec![1]);
        }
    }
}
