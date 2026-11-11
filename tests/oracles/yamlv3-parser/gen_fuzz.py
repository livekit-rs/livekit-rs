#!/usr/bin/env python3
"""Randomized corpus generator (same format as corpus.txt) for ad-hoc
differential runs: python3 -I tests/data/gen_fuzz.py <seed> <count> > fuzz.txt"""
import random, sys

FRAGS = ["a", "b", "key", "1", "~", "é", "日本", ": ", ":", "- ", "-", "? ", "?", "[", "]", "{", "}", ", ", ",",
         "#c", " # x", "# h\n", "\n", "\n", "\n", "  ", " ", "    ", "\t", "'q'", "'", '"d\\n"', '"', '"\\x41"',
         '"\\q"', "&x ", "&y", "*x", "*x ", "!!str ", "!t ", "! ", "!<u> ", "!e!f ", "|", "|-", ">+", "|2", ">",
         "---", "--- ", "...", "%YAML 1.1\n", "%TAG !e! tag:e:\n", "\r\n", "\r", "\xc2\x85", "\xef\xbb\xbf",
         "\xff", "\x01", "\x00", "''", "a b", "x\ny", "\n  ", "\n    ", "\n- ", "\n  - ", "\n? ", "\n: "]


def esc(b):
    out = []
    for c in b:
        if c == 10: out.append("\\n")
        elif c == 13: out.append("\\r")
        elif c == 9: out.append("\\t")
        elif c == 92: out.append("\\\\")
        elif 32 <= c < 127: out.append(chr(c))
        else: out.append("\\x%02x" % c)
    return "".join(out)


seed, count = int(sys.argv[1]), int(sys.argv[2])
rnd = random.Random(seed)
for i in range(count):
    n = rnd.randint(1, 40)
    s = "".join(rnd.choice(FRAGS) for _ in range(n))
    b = s.encode("utf-8", "surrogatepass")
    # Keep raw bytes for the latin-1 style escapes in FRAGS.
    b = s.encode("latin-1", "ignore") if all(ord(c) < 256 for c in s) else b
    print("fuzz-%d-%d\t%s" % (seed, i, esc(b)))
