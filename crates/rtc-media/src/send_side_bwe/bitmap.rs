// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! `livekit/protocol utils.Bitmap[T]` slot layout replay. Slot count is
//! `1 << bits.Len64((size+63)/64)`, so `len()` can exceed the requested size and
//! indices beyond it alias into masked slots exactly as upstream does.
//! Upstream `livekit-stable e5655ac2.. pkg/sfu/bwe/sendsidebwe/packet_group.go`
//! builds `utils.NewBitmap[uint64](params.Config.MinPackets)` and indexes it with
//! the raw sequence-number difference.

/// `bits.Len64`: index of the most significant set bit plus one, zero for zero.
const fn len64(value: u64) -> u32 {
    64 - value.leading_zeros()
}

/// Number of 64-bit words backing a bitmap of `size` bits, verbatim from
/// `NewBitmap`: `1 << bits.Len64(uint64((size+63)/64))`.
const fn word_count(size: usize) -> usize {
    let words = (size as u64 + 63) / 64;
    (1_u64 << len64(words)) as usize
}

/// Bitmap with upstream slot aliasing: index `val` touches word
/// `(val >> 6) & (words - 1)` at bit `val & 63`.
pub(crate) struct Bitmap {
    words: Vec<u64>,
    mask: usize,
}

impl Bitmap {
    pub(crate) fn new(size: usize) -> Self {
        let words = word_count(size);
        Self {
            words: vec![0; words],
            mask: words - 1,
        }
    }

    /// `Len()`: allocated words times 64, which is the bound upstream compares
    /// a sequence-number delta against.
    pub(crate) fn len(&self) -> usize {
        self.words.len() << 6
    }

    pub(crate) fn set(&mut self, value: usize) {
        let word = (value >> 6) & self.mask;
        self.words[word] |= 1_u64 << (value & 63);
    }

    pub(crate) fn is_set(&self, value: usize) -> bool {
        let word = (value >> 6) & self.mask;
        self.words[word] & (1_u64 << (value & 63)) != 0
    }

    pub(crate) fn clear(&mut self, value: usize) {
        let word = (value >> 6) & self.mask;
        self.words[word] &= !(1_u64 << (value & 63));
    }

    /// Sum of set bits over every allocated word, the extent a late arrival can
    /// clear upstream.
    pub(crate) fn count(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn word_count_replays_the_pinned_upstream_formula() {
        assert_eq!(word_count(0), 1);
        assert_eq!(word_count(1), 2);
        assert_eq!(word_count(20), 2);
        assert_eq!(word_count(30), 2);
        assert_eq!(word_count(64), 2);
        assert_eq!(word_count(65), 4);
        assert_eq!(word_count(128), 4);
        assert_eq!(word_count(16384), 512);
        assert_eq!(Bitmap::new(30).len(), 128);
        assert_eq!(Bitmap::new(16384).len(), 32768);
    }

    #[test]
    fn oversized_indices_alias_into_masked_words() {
        let mut b = Bitmap::new(30);
        assert_eq!(b.len(), 128);
        // Index 128 lands in word 0 bit 0, the same cell as index 0.
        b.set(128);
        assert!(b.is_set(0));
        b.clear(0);
        assert!(!b.is_set(128));
    }

    #[test]
    fn count_spans_every_allocated_word() {
        let mut b = Bitmap::new(200);
        for i in [0usize, 63, 64, 199, 256] {
            b.set(i);
        }
        assert_eq!(b.count(), 5);
        b.clear(64);
        assert_eq!(b.count(), 4);
        assert!(!b.is_set(64));
    }
}
