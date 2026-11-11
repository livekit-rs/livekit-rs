// Copyright 2022 The Go Authors. All rights reserved.
// Rust translation of Go 1.26.0 slices.SortFunc's comparator PDQsort.
// BSD-3-Clause license retained in go-sort.LICENSE alongside this file.
//! Exact fixed-toolchain equal-key permutation, not stable sorting. This matters
//! when modern and legacy Redis catalogs contain the same ID with different data.
use std::cmp::Ordering;

pub(super) fn sort_by<T, F: Fn(&T, &T) -> Ordering>(data: &mut [T], cmp: F) {
    let n = data.len();
    pdq(data, 0, n, (usize::BITS - n.leading_zeros()) as usize, &cmp);
}
fn less<T, F: Fn(&T, &T) -> Ordering>(d: &[T], a: usize, b: usize, c: &F) -> bool {
    c(&d[a], &d[b]) == Ordering::Less
}
fn insertion<T, F: Fn(&T, &T) -> Ordering>(d: &mut [T], a: usize, b: usize, c: &F) {
    for i in a + 1..b {
        let mut j = i;
        while j > a && less(d, j, j - 1, c) {
            d.swap(j, j - 1);
            j -= 1;
        }
    }
}
fn sift<T, F: Fn(&T, &T) -> Ordering>(
    d: &mut [T],
    mut root: usize,
    hi: usize,
    first: usize,
    c: &F,
) {
    loop {
        let mut child = 2 * root + 1;
        if child >= hi {
            break;
        }
        if child + 1 < hi && less(d, first + child, first + child + 1, c) {
            child += 1;
        }
        if !less(d, first + root, first + child, c) {
            return;
        }
        d.swap(first + root, first + child);
        root = child;
    }
}
fn heap<T, F: Fn(&T, &T) -> Ordering>(d: &mut [T], a: usize, b: usize, c: &F) {
    let hi = b - a;
    for i in (0..=(hi - 1) / 2).rev() {
        sift(d, i, hi, a, c);
    }
    for i in (0..hi).rev() {
        d.swap(a, a + i);
        sift(d, 0, i, a, c);
    }
}
#[derive(PartialEq)]
enum Hint {
    Unknown,
    Increasing,
    Decreasing,
}
fn pdq<T, F: Fn(&T, &T) -> Ordering>(
    d: &mut [T],
    mut a: usize,
    mut b: usize,
    mut limit: usize,
    c: &F,
) {
    let mut balanced = true;
    let mut partitioned = true;
    loop {
        let length = b - a;
        if length <= 12 {
            insertion(d, a, b, c);
            return;
        }
        if limit == 0 {
            heap(d, a, b, c);
            return;
        }
        if !balanced {
            break_patterns(d, a, b);
            limit -= 1;
        }
        let (mut pivot, mut hint) = choose_pivot(d, a, b, c);
        if hint == Hint::Decreasing {
            d[a..b].reverse();
            pivot = (b - 1) - (pivot - a);
            hint = Hint::Increasing;
        }
        if balanced && partitioned && hint == Hint::Increasing && partial_insertion(d, a, b, c) {
            return;
        }
        if a > 0 && !less(d, a - 1, pivot, c) {
            a = partition_equal(d, a, b, pivot, c);
            continue;
        }
        let (mid, already) = partition(d, a, b, pivot, c);
        partitioned = already;
        let left = mid - a;
        let right = b - mid;
        let threshold = length / 8;
        if left < right {
            balanced = left >= threshold;
            pdq(d, a, mid, limit, c);
            a = mid + 1;
        } else {
            balanced = right >= threshold;
            pdq(d, mid + 1, b, limit, c);
            b = mid;
        }
    }
}
fn partition<T, F: Fn(&T, &T) -> Ordering>(
    d: &mut [T],
    a: usize,
    b: usize,
    pivot: usize,
    c: &F,
) -> (usize, bool) {
    d.swap(a, pivot);
    let (mut i, mut j) = (a + 1, b - 1);
    while i <= j && less(d, i, a, c) {
        i += 1;
    }
    while i <= j && !less(d, j, a, c) {
        j -= 1;
    }
    if i > j {
        d.swap(j, a);
        return (j, true);
    }
    d.swap(i, j);
    i += 1;
    j -= 1;
    loop {
        while i <= j && less(d, i, a, c) {
            i += 1;
        }
        while i <= j && !less(d, j, a, c) {
            j -= 1;
        }
        if i > j {
            break;
        }
        d.swap(i, j);
        i += 1;
        j -= 1;
    }
    d.swap(j, a);
    (j, false)
}
fn partition_equal<T, F: Fn(&T, &T) -> Ordering>(
    d: &mut [T],
    a: usize,
    b: usize,
    pivot: usize,
    c: &F,
) -> usize {
    d.swap(a, pivot);
    let (mut i, mut j) = (a + 1, b - 1);
    loop {
        while i <= j && !less(d, a, i, c) {
            i += 1;
        }
        while i <= j && less(d, a, j, c) {
            j -= 1;
        }
        if i > j {
            break;
        }
        d.swap(i, j);
        i += 1;
        j -= 1;
    }
    i
}
fn partial_insertion<T, F: Fn(&T, &T) -> Ordering>(d: &mut [T], a: usize, b: usize, c: &F) -> bool {
    let mut i = a + 1;
    for _ in 0..5 {
        while i < b && !less(d, i, i - 1, c) {
            i += 1;
        }
        if i == b {
            return true;
        }
        if b - a < 50 {
            return false;
        }
        d.swap(i, i - 1);
        if i - a >= 2 {
            let mut j = i - 1;
            while j >= 1 {
                if !less(d, j, j - 1, c) {
                    break;
                }
                d.swap(j, j - 1);
                j -= 1;
            }
        }
        if b - i >= 2 {
            for j in i + 1..b {
                if !less(d, j, j - 1, c) {
                    break;
                }
                d.swap(j, j - 1);
            }
        }
    }
    false
}
fn break_patterns<T>(d: &mut [T], a: usize, b: usize) {
    let length = b - a;
    if length < 8 {
        return;
    }
    let mut random = length as u64;
    let modulus = 1usize << (usize::BITS - length.leading_zeros());
    for idx in a + (length / 4) * 2 - 1..=a + (length / 4) * 2 + 1 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let mut other = (random as usize) & (modulus - 1);
        if other >= length {
            other -= length;
        }
        d.swap(idx, a + other);
    }
}
fn order2<T, F: Fn(&T, &T) -> Ordering>(
    d: &[T],
    a: usize,
    b: usize,
    swaps: &mut usize,
    c: &F,
) -> (usize, usize) {
    if less(d, b, a, c) {
        *swaps += 1;
        (b, a)
    } else {
        (a, b)
    }
}
fn median<T, F: Fn(&T, &T) -> Ordering>(
    d: &[T],
    a: usize,
    b: usize,
    cidx: usize,
    swaps: &mut usize,
    c: &F,
) -> usize {
    let (a, b) = order2(d, a, b, swaps, c);
    let (b, _) = order2(d, b, cidx, swaps, c);
    let (_, b) = order2(d, a, b, swaps, c);
    b
}
fn choose_pivot<T, F: Fn(&T, &T) -> Ordering>(d: &[T], a: usize, b: usize, c: &F) -> (usize, Hint) {
    let l = b - a;
    let mut swaps = 0;
    let (mut i, mut j, mut k) = (a + l / 4, a + l / 4 * 2, a + l / 4 * 3);
    if l >= 8 {
        if l >= 50 {
            i = median(d, i - 1, i, i + 1, &mut swaps, c);
            j = median(d, j - 1, j, j + 1, &mut swaps, c);
            k = median(d, k - 1, k, k + 1, &mut swaps, c);
        }
        j = median(d, i, j, k, &mut swaps, c);
    }
    (
        j,
        match swaps {
            0 => Hint::Increasing,
            12 => Hint::Decreasing,
            _ => Hint::Unknown,
        },
    )
}
