#!/usr/bin/env python3
"""Targeted randomized corpus for the flow-sequence single-pair mapping value
(`[k: ]`, `[k:, x]`) where yaml.v3 reads a stale *yaml_token_t; varies the token
queue state with prefixes, simple-key lookahead and comments:
python3 -I tests/data/gen_fuzz_pairs.py <seed> <count> > out.txt"""
import random, sys
seed, count = int(sys.argv[1]), int(sys.argv[2])
rnd = random.Random(seed)
pre = ["", "a: ", "- ", "- - ", "x:\n  ", "[", "[[", "{k: ", "{", "? ", "[1, 2, ", "&a ", "!!seq ", "# c\n", "---\n",
       "[a, b, c, d, e, f, g, ", "{a: [", "[x]: ", "[[x, y]: "]
pair = ["k:", "k: ", "k:\n", "'k':", '"k":', "? k:", "? k :", "k: # c\n", "k:\n# c\n", "&x k:", "!t k:", ":", ": ",
        "[a]:", "{a: b}:", "k:\n\n  "]
sep = ["]", ",]", ", x]", ",\n]", " ]", "\n]", "# c\n]", ", y, z]", "]: v", "] ", ", ]: w"]
post = ["", "", "]", "]]", "}", "\n", "\nb: c\n", " # t\n", "]: v\n"]


def esc(b):
    return "".join({10: "\\n", 13: "\\r", 9: "\\t", 92: "\\\\"}.get(c, chr(c) if 32 <= c < 127 else "\\x%02x" % c) for c in b)


for i in range(count):
    s = rnd.choice(pre) + "[" + "".join(rnd.choice(pair) + rnd.choice(["", " ", "\n", ", "]) for _ in range(rnd.randint(0, 2)))
    s += rnd.choice(pair) + rnd.choice(sep) + rnd.choice(post)
    print("pfuzz-%d-%d\t%s" % (seed, i, esc(s.encode())))
