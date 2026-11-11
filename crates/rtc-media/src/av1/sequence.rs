//! Original RTPMunger sequence range semantics, including dropped slots after
//! gaps. Contiguous selected-out packets compact; gaps retain NACK-able holes.
use std::collections::VecDeque;
struct Range {
    start: u64,
    end: Option<u64>,
    offset: u64,
}
#[derive(Default)]
pub(crate) struct Sequences {
    ranges: VecDeque<Range>,
    highest: Option<u64>,
    last_out: u64,
}
pub(crate) enum Slot {
    Media(u64),
    Padding(u64),
}
impl Sequences {
    pub fn is_late(&self, input: u64) -> bool {
        self.highest
            .is_some_and(|highest| (input.wrapping_sub(highest) as i64) < 0)
    }
    pub fn is_initialized(&self) -> bool {
        self.highest.is_some()
    }
    pub fn reset(&mut self, input: u64, output: u64) {
        self.ranges.clear();
        self.ranges.push_back(Range {
            start: input,
            end: None,
            offset: input.wrapping_sub(output),
        });
        self.highest = Some(input.wrapping_sub(1));
        self.last_out = output.wrapping_sub(1);
    }
    fn offset(&self, input: u64) -> Option<u64> {
        self.ranges
            .iter()
            .rev()
            .find(|r| input >= r.start && r.end.is_none_or(|end| input <= end))
            .map(|r| r.offset)
    }
    pub fn translate(&mut self, input: u64, selected: bool) -> Option<Slot> {
        let highest = self.highest?;
        let difference = input.wrapping_sub(highest) as i64;
        if difference == 0 {
            return None;
        }
        if difference < 0 {
            let output = input.wrapping_sub(self.offset(input)?);
            if output >= self.last_out {
                return None;
            }
            return Some(if selected {
                Slot::Media(output)
            } else {
                Slot::Padding(output)
            });
        }
        self.highest = Some(input);
        let offset = self.ranges.back()?.offset;
        let output = input.wrapping_sub(offset);
        if !selected && difference == 1 {
            // Exact contiguous PacketDropped rollback: only this incoming slot is
            // excluded. An already reserved unknown earlier gap is never compacted.
            let open = self.ranges.back_mut()?;
            if open.start == input {
                open.start = input.saturating_add(1);
                open.offset = open.offset.wrapping_add(1);
            } else {
                open.end = Some(input - 1);
                self.ranges.push_back(Range {
                    start: input.saturating_add(1),
                    end: None,
                    offset: offset.wrapping_add(1),
                });
            }
            while self.ranges.len() > 101 {
                self.ranges.pop_front();
            }
            return None;
        }
        self.last_out = output;
        Some(if selected {
            Slot::Media(output)
        } else {
            Slot::Padding(output)
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_gap_then_dropped_slot_preserves_late_packet_and_contiguous_drop_compacts() {
        let mut m = Sequences::default();
        m.reset(1000, 40);
        assert!(matches!(m.translate(1000, true), Some(Slot::Media(40))));
        assert!(matches!(m.translate(1002, false), Some(Slot::Padding(42))));
        assert!(matches!(m.translate(1001, true), Some(Slot::Media(41))));
        assert!(m.translate(1003, false).is_none());
        assert!(matches!(m.translate(1004, true), Some(Slot::Media(43))));
        assert!(
            m.translate(1003, true).is_none(),
            "deliberately excluded source slot cannot be revived"
        );
    }
}

#[cfg(test)]
mod original_source_oracle {
    use super::*;
    #[test]
    #[ignore = "requires fixed original RTPMunger actual observer output"]
    fn source_packet_holes_and_dropped_slot_offsets_match_original_rtp_munger() {
        let path =
            std::env::var("GO_AV1_PACKET_RESULTS").expect("actual original Go RTPMunger log");
        let input = std::fs::read_to_string(path).unwrap();
        let mut sequences = Sequences::default();
        sequences.reset(1000, 40);
        let mut count = 0;
        for line in input.lines() {
            let Some(row) = line.strip_prefix("SOURCE_PACKET_MUNGER ") else {
                continue;
            };
            let row: serde_json::Value = serde_json::from_str(row).unwrap();
            let output = sequences.translate(
                row["sequence"].as_u64().unwrap(),
                row["selected"].as_bool().unwrap(),
            );
            let (kind, offset) = match output {
                Some(Slot::Media(sn)) => ("media", Some(sn - 40)),
                Some(Slot::Padding(sn)) => ("padding", Some(sn - 40)),
                None => ("none", None),
            };
            assert_eq!(
                kind,
                row["kind"].as_str().unwrap(),
                "actual RTPMunger row {count}"
            );
            assert_eq!(
                offset,
                row["output"].as_u64().map(|sn| sn - 1000),
                "actual source offset row {count}"
            );
            count += 1;
        }
        assert_eq!(
            count, 6,
            "original selected/gap/dropped/late/excluded source cases all executed"
        );
    }
}
