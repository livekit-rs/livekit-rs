#!/usr/bin/env python3
"""Report how far the host libm sits from the pinned Go toolchain's math.

Reads the table emitted by math_probe.go and, for the same inputs, calls
Python's ``math.log``/``math.log10``/``math.exp``. CPython binds the platform
libm directly, so this is the same function a Rust ``f64::ln``/``f64::log10``/
``f64::exp`` resolves to on this host. The counts answer one question only: is
reproducing upstream's arithmetic in traffic.rs load-bearing, or is it
decoration?

Usage: python3 math_census.py math_probe.txt
"""
import math
import struct
import sys


def to_bits(x):
    return int.from_bytes(struct.pack(">d", x), "big")


def from_bits(word):
    return struct.unpack(">d", word.to_bytes(8, "big"))[0]


def ordered(word):
    """IEEE total-order index, so the distance between two words is ULPs."""
    return (1 << 64) - word if word >> 63 else word


def ulps(x, y):
    return abs(ordered(to_bits(x)) - ordered(to_bits(y)))


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "math_probe.txt"
    tables = {"log": math.log, "log10": math.log10, "exp": math.exp}
    counts = {kind: [0, 0, 0] for kind in tables}  # n, differing, worst ULPs
    examples = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            kind, in_hex, out_hex = line.split()
            x = from_bits(int(in_hex, 16))
            want = from_bits(int(out_hex, 16))
            if math.isnan(x) or math.isnan(want) or math.isinf(want):
                continue
            try:
                got = tables[kind](x)
            except (ValueError, OverflowError):
                continue
            tally = counts[kind]
            tally[0] += 1
            if to_bits(got) != to_bits(want):
                tally[1] += 1
                gap = ulps(got, want)
                tally[2] = max(tally[2], gap)
                if len(examples) < 12:
                    examples.append((kind, x, got, want, gap))
    for kind in ("log", "log10", "exp"):
        total, differing, worst = counts[kind]
        share = 100.0 * differing / total if total else 0.0
        print(f"{kind:5s} inputs={total:5d} differ_from_host_libm={differing:5d} "
              f"({share:.1f}%) worst_gap={worst} ulp")
    print("first mismatches (the host answer is the correctly rounded one, "
          "upstream's is not):")
    for kind, x, got, want, gap in examples:
        print(f"  {kind:5s} x={x!r} libm={got!r} go={want!r} gap={gap}")


if __name__ == "__main__":
    main()
