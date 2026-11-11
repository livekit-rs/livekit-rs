#!/usr/bin/env python3
"""Structured randomized corpus (mostly valid YAML with comments everywhere,
plus byte mutations of tests/data/corpus.txt cases) for ad-hoc differential runs:
python3 -I tests/data/gen_fuzz_struct.py <seed> <count> <corpus.txt> > out.txt"""
import random, sys

seed, count, corpus = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
rnd = random.Random(seed)


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


def unesc(s):
    out = bytearray(); i = 0
    while i < len(s):
        c = s[i]
        if c != "\\":
            out += c.encode(); i += 1; continue
        n = s[i + 1]
        if n == "x":
            out.append(int(s[i + 2:i + 4], 16)); i += 4; continue
        out += {"n": b"\n", "r": b"\r", "t": b"\t", "\\": b"\\"}[n]; i += 2
    return bytes(out)


def comment():
    return rnd.choice(["", "", "", " # c", "  #x", "\t# t"])


def head(ind):
    r = rnd.random()
    if r < 0.15:
        return " " * ind + "# head\n"
    if r < 0.2:
        return "\n" + " " * rnd.choice([0, ind, ind + 2]) + "# far\n\n"
    return ""


def scalar():
    return rnd.choice(["a", "b c", "'s q'", '"d q"', "1", "~", "&a v", "*a", "!!str x", "!t y", "é",
                       '"m\n  l"', "p\n  q"])


def node(ind, depth):
    r = rnd.random()
    if depth > 3 or r < 0.3:
        return " " + scalar() + comment() + "\n"
    if r < 0.4:
        return " [" + ", ".join(scalar() for _ in range(rnd.randint(0, 3))) + "]" + comment() + "\n"
    if r < 0.45:
        return " {" + ", ".join("k%d: %s" % (i, scalar()) for i in range(rnd.randint(0, 3))) + "}" + comment() + "\n"
    if r < 0.55:
        return " " + rnd.choice(["|", ">", "|-", ">+", "|2"]) + comment() + "\n" + \
            "".join(" " * (ind + 2) + rnd.choice(["line", "", " more", "x y"]) + "\n" for _ in range(rnd.randint(0, 3)))
    ni = ind + rnd.choice([0, 1, 2, 2, 4]) if rnd.random() < 0.9 else max(0, ind - 1)
    out = comment() + "\n"
    if rnd.random() < 0.5:
        for _ in range(rnd.randint(1, 3)):
            out += head(ni) + " " * ni + "-" + node(ni + 2, depth + 1)
    else:
        for i in range(rnd.randint(1, 3)):
            out += head(ni) + " " * ni + "k%d:" % i + node(ni + 2, depth + 1)
    return out


lines = [l for l in open(corpus).read().split("\n") if l]
for i in range(count):
    if rnd.random() < 0.7:
        doc = head(0) + "".join("k%d:" % j + node(2, 0) for j in range(rnd.randint(1, 3)))
        if rnd.random() < 0.2:
            doc = "---" + comment() + "\n" + doc + rnd.choice(["", "...\n", "---\n" + doc])
        if rnd.random() < 0.1:
            doc = doc.replace("\n", "\r\n")
        b = doc.encode()
    else:
        b = bytearray(unesc(rnd.choice(lines).split("\t", 1)[1]))
        for _ in range(rnd.randint(1, 3)):
            if not b:
                break
            p = rnd.randrange(len(b))
            op = rnd.random()
            if op < 0.4:
                del b[p]
            elif op < 0.8:
                b.insert(p, rnd.choice(b" \t\n#:-?,[]{}'\"&*!|>%"))
            else:
                b[p:p] = b[p:p + rnd.randint(1, 10)]
        b = bytes(b)
    print("sfuzz-%d-%d\t%s" % (seed, i, esc(b)))
